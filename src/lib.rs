//! Rust bindings for the Velr runtime.
//!
//! This crate exposes a high-level API over the Velr runtime ABI (loaded via the `runtime` module),
//! wrapping raw FFI pointers in RAII types with predictable lifetimes.
//!
//! # Threading model
//!
//! Velr uses a *connection-affine* model:
//!
//! 1) [`Velr`] (the connection) is **`Send` + `!Sync`**.
//!    - ✅ You may **move** a connection to another thread.
//!      Example: spawn a worker thread and move the connection into it.
//!    - ❌ Wrapping a connection in `Arc` does not make it safe to share across threads;
//!      `Velr` is `!Sync`, so concurrent shared use is not supported.
//!
//! 2) In-flight / handle-based objects are **`!Send` + `!Sync`** (thread-affine):
//!    [`ExecTables`], [`TableResult`], [`RowIter`], [`VelrTx`], [`ExecTablesTx`],
//!    [`VelrSavepoint`], [`ExplainTrace`].
//!    - ❌ You may not move these to another thread.
//!    - ❌ You may not share these across threads.
//!
//! Practical implications:
//! - ✅ Many connections across many threads is fine (open one connection per thread).
//! - ✅ You can move a connection between threads (e.g., create in main, move into worker).
//! - ❌ You cannot run concurrent operations on the same connection across threads.
//!
//! If you need parallelism, open multiple connections and/or use a pool.
//!
//! # Results and lifetimes
//!
//! Queries can produce **zero or more result tables**:
//! - [`Velr::exec`] / [`VelrTx::exec`] stream tables via [`ExecTables`] / [`ExecTablesTx`].
//! - [`Velr::exec_one`] / [`VelrTx::exec_one`] return a single [`TableResult`].
//!
//! Rows are processed via callbacks. Individual cell values are represented by [`CellRef`], which
//! may borrow bytes from buffers owned by the underlying row cursor. For `Text`/`Json` values, the
//! borrowed bytes remain valid until the next call to [`RowIter::next`] on the same iterator (or
//! until the iterator is dropped). In typical usage this means the borrows are scoped to the row
//! callback invocation.
//!
//! # Errors
//!
//! Most operations return [`Result<T>`]. On failure, you get an [`Error`] containing a numeric
//! code (originating from the runtime ABI) and an optional message.
#![allow(unsafe_code)]

mod api;
mod runtime;
mod sys;

use std::{
    cell::{Cell, RefCell},
    ffi::{CStr, CString},
    fmt,
    marker::PhantomData,
    os::raw::c_char,
    ptr::NonNull,
    rc::Rc,
};

use sys as ffi;

/// Convenience result type used throughout the public API.
pub type Result<T> = std::result::Result<T, Error>;

/// Error returned by the Velr API.
///
/// - `code` is an integer error code returned by the runtime ABI. This is subject for change later.
/// - `message` is an optional, human-readable message (may be empty).
///
/// The runtime may or may not provide an error message for a given code.
#[derive(Debug)]
pub struct Error {
    pub code: i32,
    pub message: String,
}

impl Error {
    fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "velr error (code {})", self.code)
        } else {
            write!(f, "velr error (code {}): {}", self.code, self.message)
        }
    }
}

impl std::error::Error for Error {}

/// Get a reference to the loaded runtime API.
///
/// This ensures the runtime is initialized (via [`runtime::runtime`]) and then returns the
/// resolved ABI function table.
///
/// # Errors
///
/// Returns an [`Error`] if the runtime cannot be loaded or initialized.
fn velr_api() -> Result<&'static api::Api> {
    Ok(&runtime::runtime()?.api)
}
/*
fn borrowed_bytes<'a>(ptr: *const u8, len: usize, what: &str) -> Result<&'a [u8]> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(Error::new(
            ffi::velr_code::VELR_EERR as i32,
            format!("{what} is null with non-zero length"),
        ));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}
*/
/// Convert an ABI-owned error string into a Rust [`String`], freeing it via the runtime.
///
/// # Safety
///
/// `p` must be either null or a pointer to a NUL-terminated C string allocated by the Velr runtime.
/// On success, this function attempts to free the string using `velr_string_free`.
unsafe fn take_err(p: *mut c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    let s = CStr::from_ptr(p).to_string_lossy().into_owned();

    // Free via runtime API (best-effort; if runtime isn't available, leak rather than crash).
    if let Ok(a) = velr_api() {
        (a.velr_string_free)(p);
    }

    s
}

fn free_unexpected_err(err: *mut c_char) {
    if !err.is_null() {
        if let Ok(a) = velr_api() {
            unsafe { (a.velr_string_free)(err) };
        }
    }
}

fn take_owned_bytes(a: &api::Api, ptr: *mut u8, len: usize, what: &str) -> Result<Vec<u8>> {
    if ptr.is_null() {
        return if len == 0 {
            Ok(Vec::new())
        } else {
            Err(Error::new(
                ffi::velr_code::VELR_EERR as i32,
                format!("{what} returned null pointer with non-zero length"),
            ))
        };
    }

    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
    unsafe { (a.velr_free)(ptr, len) };
    Ok(bytes)
}

/// Convert a Velr return code plus optional error string into [`Result<()>`].
///
/// On success, frees `err` if it is unexpectedly non-null. On failure, converts `err` into an
/// [`Error`] and frees it via the runtime.
fn rc_to_result(rc: ffi::velr_code, err: *mut c_char) -> Result<()> {
    let code = rc as i32;
    if code == ffi::velr_code::VELR_OK as i32 {
        free_unexpected_err(err);
        Ok(())
    } else {
        let msg = unsafe { take_err(err) };
        Err(Error::new(code, msg))
    }
}

fn rc_to_result_noerr(rc: ffi::velr_code, context: impl Into<String>) -> Result<()> {
    let code = rc as i32;
    if code == ffi::velr_code::VELR_OK as i32 {
        Ok(())
    } else {
        Err(Error::new(code, context.into()))
    }
}

fn strview_to_string(v: ffi::velr_strview, what: &str) -> Result<String> {
    if v.len == 0 {
        return Ok(String::new());
    }
    if v.ptr.is_null() {
        return Err(Error::new(
            ffi::velr_code::VELR_EERR as i32,
            format!("{what} is null with non-zero length"),
        ));
    }

    let bytes = unsafe { std::slice::from_raw_parts(v.ptr, v.len) };
    let s = std::str::from_utf8(bytes).map_err(|_| {
        Error::new(
            ffi::velr_code::VELR_EUTF as i32,
            format!("{what} is not valid UTF-8"),
        )
    })?;
    Ok(s.to_string())
}

fn opt_strview_to_string(v: ffi::velr_strview, what: &str) -> Result<Option<String>> {
    if v.ptr.is_null() && v.len == 0 {
        return Ok(None);
    }
    Ok(Some(strview_to_string(v, what)?))
}

/// Owned plan metadata returned from an [`ExplainTrace`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainPlanMeta {
    pub plan_id: String,
    pub cypher: String,
    pub step_count: usize,
}

/// Owned step metadata returned from an [`ExplainTrace`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStepMeta {
    pub step_no: usize,
    pub group_id: String,
    pub op_index: String,
    pub phase: String,
    pub title: String,
    pub source: String,
    pub note: Option<String>,
    pub statement_count: usize,
}

/// Owned statement metadata returned from an [`ExplainTrace`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStatementMeta {
    pub stmt_id: String,
    pub kind: String,
    pub sql: String,
    pub note: Option<String>,
    pub sqlite_plan_count: usize,
}

/// One explain statement plus its SQLite query-plan detail lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStatement {
    pub meta: ExplainStatementMeta,
    pub sqlite_plan: Vec<String>,
}

/// One explain step plus all statements in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStep {
    pub meta: ExplainStepMeta,
    pub statements: Vec<ExplainStatement>,
}

/// One explain plan plus all steps in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainPlan {
    pub meta: ExplainPlanMeta,
    pub steps: Vec<ExplainStep>,
}

/// EXPLAIN / EXPLAIN ANALYZE trace handle.
///
/// This is an in-flight type and is **`!Send` + `!Sync`** (thread-affine).
/// Dropping it closes the underlying runtime trace handle.
///
/// All strings exposed by this type are copied into owned Rust `String`s before being returned.
pub struct ExplainTrace {
    trace: NonNull<ffi::velr_explain_trace>,
    _nosend: PhantomData<Rc<()>>, // !Send + !Sync
}

impl ExplainTrace {
    fn from_raw(ptr: *mut ffi::velr_explain_trace) -> Result<Self> {
        let trace = NonNull::new(ptr).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_EERR as i32,
                "runtime returned null explain trace",
            )
        })?;

        Ok(Self {
            trace,
            _nosend: PhantomData,
        })
    }

    /// Return the number of top-level plans in this trace.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the runtime API cannot be loaded.
    pub fn plan_count(&self) -> Result<usize> {
        let a = velr_api()?;
        Ok(unsafe { (a.velr_explain_trace_plan_count)(self.trace.as_ptr()) })
    }

    /// Fetch metadata for one plan.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - the runtime API cannot be loaded
    /// - `plan_idx` is out of range
    /// - returned string fields are not valid UTF-8
    pub fn plan_meta(&self, plan_idx: usize) -> Result<ExplainPlanMeta> {
        let a = velr_api()?;
        let mut out = std::mem::MaybeUninit::<ffi::velr_explain_plan_meta>::uninit();

        let rc = unsafe {
            (a.velr_explain_trace_plan_meta)(self.trace.as_ptr(), plan_idx, out.as_mut_ptr())
        };
        rc_to_result_noerr(
            rc,
            format!("velr_explain_trace_plan_meta failed at plan_idx={plan_idx}"),
        )?;

        let out = unsafe { out.assume_init() };
        Ok(ExplainPlanMeta {
            plan_id: strview_to_string(out.plan_id, "plan_id")?,
            cypher: strview_to_string(out.cypher, "cypher")?,
            step_count: out.step_count,
        })
    }

    /// Return the number of steps in a plan.
    ///
    /// This is a convenience wrapper over [`ExplainTrace::plan_meta`].
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if `plan_idx` is out of range or metadata decoding fails.
    pub fn step_count(&self, plan_idx: usize) -> Result<usize> {
        Ok(self.plan_meta(plan_idx)?.step_count)
    }

    /// Fetch metadata for one step.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - the runtime API cannot be loaded
    /// - `plan_idx` or `step_idx` is out of range
    /// - returned string fields are not valid UTF-8
    pub fn step_meta(&self, plan_idx: usize, step_idx: usize) -> Result<ExplainStepMeta> {
        let a = velr_api()?;
        let mut out = std::mem::MaybeUninit::<ffi::velr_explain_step_meta>::uninit();

        let rc = unsafe {
            (a.velr_explain_trace_step_meta)(
                self.trace.as_ptr(),
                plan_idx,
                step_idx,
                out.as_mut_ptr(),
            )
        };
        rc_to_result_noerr(
            rc,
            format!(
                "velr_explain_trace_step_meta failed at plan_idx={plan_idx}, step_idx={step_idx}"
            ),
        )?;

        let out = unsafe { out.assume_init() };
        Ok(ExplainStepMeta {
            step_no: out.step_no,
            group_id: strview_to_string(out.group_id, "group_id")?,
            op_index: strview_to_string(out.op_index, "op_index")?,
            phase: strview_to_string(out.phase, "phase")?,
            title: strview_to_string(out.title, "title")?,
            source: strview_to_string(out.source, "source")?,
            note: opt_strview_to_string(out.note, "step.note")?,
            statement_count: out.statement_count,
        })
    }

    /// Return the number of statements in a step.
    ///
    /// This is a convenience wrapper over [`ExplainTrace::step_meta`].
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if `plan_idx` / `step_idx` are out of range or metadata decoding fails.
    pub fn statement_count(&self, plan_idx: usize, step_idx: usize) -> Result<usize> {
        Ok(self.step_meta(plan_idx, step_idx)?.statement_count)
    }

    /// Fetch metadata for one statement.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - the runtime API cannot be loaded
    /// - `plan_idx`, `step_idx`, or `stmt_idx` is out of range
    /// - returned string fields are not valid UTF-8
    pub fn statement_meta(
        &self,
        plan_idx: usize,
        step_idx: usize,
        stmt_idx: usize,
    ) -> Result<ExplainStatementMeta> {
        let a = velr_api()?;
        let mut out = std::mem::MaybeUninit::<ffi::velr_explain_stmt_meta>::uninit();

        let rc = unsafe {
            (a.velr_explain_trace_statement_meta)(
                self.trace.as_ptr(),
                plan_idx,
                step_idx,
                stmt_idx,
                out.as_mut_ptr(),
            )
        };
        rc_to_result_noerr(
            rc,
            format!(
                "velr_explain_trace_statement_meta failed at plan_idx={plan_idx}, step_idx={step_idx}, stmt_idx={stmt_idx}"
            ),
        )?;

        let out = unsafe { out.assume_init() };
        Ok(ExplainStatementMeta {
            stmt_id: strview_to_string(out.stmt_id, "stmt_id")?,
            kind: strview_to_string(out.kind, "kind")?,
            sql: strview_to_string(out.sql, "sql")?,
            note: opt_strview_to_string(out.note, "statement.note")?,
            sqlite_plan_count: out.sqlite_plan_count,
        })
    }

    /// Return the number of SQLite query-plan detail lines for one statement.
    ///
    /// This is a convenience wrapper over [`ExplainTrace::statement_meta`].
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if indices are out of range or metadata decoding fails.
    pub fn sqlite_plan_count(
        &self,
        plan_idx: usize,
        step_idx: usize,
        stmt_idx: usize,
    ) -> Result<usize> {
        Ok(self
            .statement_meta(plan_idx, step_idx, stmt_idx)?
            .sqlite_plan_count)
    }

    /// Fetch one SQLite query-plan detail line.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - the runtime API cannot be loaded
    /// - any index is out of range
    /// - the returned detail line is not valid UTF-8
    pub fn sqlite_plan_detail(
        &self,
        plan_idx: usize,
        step_idx: usize,
        stmt_idx: usize,
        detail_idx: usize,
    ) -> Result<String> {
        let a = velr_api()?;
        let mut out = std::mem::MaybeUninit::<ffi::velr_strview>::uninit();

        let rc = unsafe {
            (a.velr_explain_trace_sqlite_plan_detail)(
                self.trace.as_ptr(),
                plan_idx,
                step_idx,
                stmt_idx,
                detail_idx,
                out.as_mut_ptr(),
            )
        };
        rc_to_result_noerr(
            rc,
            format!(
                "velr_explain_trace_sqlite_plan_detail failed at plan_idx={plan_idx}, step_idx={step_idx}, stmt_idx={stmt_idx}, detail_idx={detail_idx}"
            ),
        )?;

        let out = unsafe { out.assume_init() };
        strview_to_string(out, "sqlite_plan_detail")
    }

    /// Fetch all SQLite query-plan detail lines for one statement.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if indices are out of range or any returned detail line
    /// cannot be decoded.
    pub fn sqlite_plan_details(
        &self,
        plan_idx: usize,
        step_idx: usize,
        stmt_idx: usize,
    ) -> Result<Vec<String>> {
        let n = self.sqlite_plan_count(plan_idx, step_idx, stmt_idx)?;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(self.sqlite_plan_detail(plan_idx, step_idx, stmt_idx, i)?);
        }
        Ok(out)
    }

    /// Materialize the entire trace into owned Rust structs.
    ///
    /// This walks all plans, steps, statements, and SQLite plan details and returns
    /// a fully owned snapshot detached from the borrowed runtime string views.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if any nested metadata/detail lookup fails.
    pub fn snapshot(&self) -> Result<Vec<ExplainPlan>> {
        let plan_count = self.plan_count()?;
        let mut plans = Vec::with_capacity(plan_count);

        for plan_idx in 0..plan_count {
            let plan_meta = self.plan_meta(plan_idx)?;
            let mut steps = Vec::with_capacity(plan_meta.step_count);

            for step_idx in 0..plan_meta.step_count {
                let step_meta = self.step_meta(plan_idx, step_idx)?;
                let mut statements = Vec::with_capacity(step_meta.statement_count);

                for stmt_idx in 0..step_meta.statement_count {
                    let stmt_meta = self.statement_meta(plan_idx, step_idx, stmt_idx)?;
                    let sqlite_plan = self.sqlite_plan_details(plan_idx, step_idx, stmt_idx)?;
                    statements.push(ExplainStatement {
                        meta: stmt_meta,
                        sqlite_plan,
                    });
                }

                steps.push(ExplainStep {
                    meta: step_meta,
                    statements,
                });
            }

            plans.push(ExplainPlan {
                meta: plan_meta,
                steps,
            });
        }

        Ok(plans)
    }

    /// Return the size in bytes of the compact rendering.
    ///
    /// The compact rendering is UTF-8 text, but this method returns the raw byte count.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the runtime API cannot be loaded or the runtime
    /// fails to render the compact form.
    pub fn compact_len(&self) -> Result<usize> {
        let a = velr_api()?;
        let mut len: usize = 0;
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc =
            unsafe { (a.velr_explain_trace_compact_len)(self.trace.as_ptr(), &mut len, &mut err) };
        rc_to_result(rc, err)?;
        Ok(len)
    }

    /// Render the trace to compact UTF-8 bytes.
    ///
    /// The returned bytes are owned by Rust and independent of the runtime buffer.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - the runtime API cannot be loaded
    /// - the runtime fails to render the compact form
    /// - the runtime returns an invalid null/non-null pointer + length combination
    pub fn to_compact_bytes(&self) -> Result<Vec<u8>> {
        let a = velr_api()?;
        let mut ptr: *mut u8 = std::ptr::null_mut();
        let mut len: usize = 0;
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (a.velr_explain_trace_compact_malloc)(self.trace.as_ptr(), &mut ptr, &mut len, &mut err)
        };
        rc_to_result(rc, err)?;
        take_owned_bytes(a, ptr, len, "velr_explain_trace_compact_malloc")
    }

    /// Render the trace to a compact UTF-8 string.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the compact rendering cannot be produced or if the
    /// returned bytes are not valid UTF-8.
    pub fn to_compact_string(&self) -> Result<String> {
        let bytes = self.to_compact_bytes()?;
        let s = String::from_utf8(bytes).map_err(|e| {
            Error::new(
                ffi::velr_code::VELR_EUTF as i32,
                format!("compact explain is not valid UTF-8: {e}"),
            )
        })?;
        Ok(s)
    }

    /// Write the compact rendering into any Rust writer.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if rendering fails or if writing to `out` fails.
    pub fn write_compact(&self, mut out: impl std::io::Write) -> Result<()> {
        let bytes = self.to_compact_bytes()?;
        out.write_all(&bytes).map_err(|e| {
            Error::new(
                ffi::velr_code::VELR_EERR as i32,
                format!("failed to write compact explain: {e}"),
            )
        })
    }
}

impl Drop for ExplainTrace {
    fn drop(&mut self) {
        if let Ok(a) = velr_api() {
            unsafe { (a.velr_explain_trace_close)(self.trace.as_ptr()) };
        }
    }
}

// -------------------------- CellRef --------------------------

/// Borrowed view of a single cell value in a result row.
///
/// This is a lightweight, non-owning representation used when iterating rows.
/// Text/JSON values are exposed as raw bytes.
///
/// - For text data, use [`CellRef::as_str_utf8`] if you want a UTF-8 `&str`.
/// - JSON is returned as raw bytes
#[derive(Debug, Copy, Clone)]
pub enum CellRef<'a> {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    Text(&'a [u8]),
    Json(&'a [u8]),
}

impl<'a> CellRef<'a> {
    /// If this cell is [`CellRef::Text`], attempt to interpret it as UTF-8.
    ///
    /// Returns:
    /// - `Some(Ok(&str))` if the cell is text and valid UTF-8
    /// - `Some(Err(_))` if the cell is text but invalid UTF-8
    /// - `None` if the cell is not text
    pub fn as_str_utf8(&self) -> Option<std::result::Result<&'a str, std::str::Utf8Error>> {
        match self {
            CellRef::Text(b) => Some(std::str::from_utf8(b)),
            _ => None,
        }
    }
}

// -------------------------- Velr (Connection) --------------------------
//
// Velr is Send + !Sync (movable, not shareable).
//

pub struct Velr {
    db: NonNull<ffi::velr_db>,
    _not_sync: PhantomData<Cell<()>>, // Send + !Sync
}

impl Velr {
    /// Open a Velr connection.
    ///
    /// ## Path semantics
    ///
    /// - If `path` is `None`, an **in-memory** database is opened.
    /// - If `path` is `Some(":memory:")`, an **in-memory** database is opened.
    /// - Otherwise, `path` is treated as a filesystem path for a file-backed database.
    ///
    /// # Errors
    ///
    /// Returns an error if `path` contains an interior NUL byte or if the runtime fails to open.
    pub fn open(path: Option<&str>) -> Result<Self> {
        let a = velr_api()?; // ensure runtime is loaded

        let mut out_db: *mut ffi::velr_db = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let cpath;
        let path_ptr = match path {
            None => std::ptr::null(),
            Some(p) => {
                cpath = CString::new(p).map_err(|_| {
                    Error::new(ffi::velr_code::VELR_EUTF as i32, "path contains NUL")
                })?;
                cpath.as_ptr()
            }
        };

        let rc = unsafe { (a.velr_open)(path_ptr, &mut out_db, &mut err) };
        rc_to_result(rc, err)?;

        let nn = NonNull::new(out_db).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_EERR as i32,
                "velr_open returned null db",
            )
        })?;

        Ok(Self {
            db: nn,
            _not_sync: PhantomData,
        })
    }

    /// Execute `openCypher` and return a stream of result tables.
    ///
    /// Use [`ExecTables::next_table`] to pull tables until it returns `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - `openCypher` contains an interior NUL (\0)
    /// - the runtime reports an execution/planning/parsing error
    pub fn exec<'db>(&'db self, cypher: &str) -> Result<ExecTables<'db>> {
        let a = velr_api()?;

        let cy = CString::new(cypher)
            .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "openCypher contains NUL"))?;

        let mut out_stream: *mut ffi::velr_stream = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (a.velr_exec_start)(self.db.as_ptr(), cy.as_ptr(), &mut out_stream, &mut err)
        };
        rc_to_result(rc, err)?;

        let nn = NonNull::new(out_stream).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_EERR as i32,
                "velr_exec_start returned null stream",
            )
        })?;

        Ok(ExecTables {
            stream: Some(nn),
            _db: PhantomData,
            _nosend: PhantomData,
        })
    }

    /// Execute `openCypher` and return exactly one result table.
    ///
    /// This method succeeds only if executing the provided openCypher text produces exactly one
    /// result table. If execution yields zero tables or more than one table, this returns an error.
    ///
    /// Use [`exec`] to stream multiple result tables.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - `openCypher` contains an interior NUL (\0)
    /// - the runtime reports an execution/planning/parsing error
    /// - the execution yields zero or multiple result tables
    pub fn exec_one(&self, cypher: &str) -> Result<TableResult> {
        let a = velr_api()?;

        let cy = CString::new(cypher)
            .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "openCypher contains NUL"))?;

        let mut out_table: *mut ffi::velr_table = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc =
            unsafe { (a.velr_exec_one)(self.db.as_ptr(), cy.as_ptr(), &mut out_table, &mut err) };
        rc_to_result(rc, err)?;
        TableResult::from_raw(out_table)
    }

    /// Execute a query and discard all results.
    ///
    /// This is a convenience wrapper around [`Velr::exec`] that drains all tables and rows.
    pub fn run(&self, cypher: &str) -> Result<()> {
        let mut st = self.exec(cypher)?;
        while let Some(mut t) = st.next_table()? {
            t.for_each_row(|_| Ok(()))?;
        }
        Ok(())
    }

    /// Build an EXPLAIN trace for `openCypher`.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - `openCypher` contains an interior NUL (`\0`)
    /// - the runtime reports a planning/explain error
    pub fn explain(&self, cypher: &str) -> Result<ExplainTrace> {
        let a = velr_api()?;

        let cy = CString::new(cypher)
            .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "openCypher contains NUL"))?;

        let mut out_trace: *mut ffi::velr_explain_trace = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc =
            unsafe { (a.velr_explain)(self.db.as_ptr(), cy.as_ptr(), &mut out_trace, &mut err) };
        rc_to_result(rc, err)?;
        ExplainTrace::from_raw(out_trace)
    }

    /// Build an EXPLAIN ANALYZE trace for `openCypher`.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - `openCypher` contains an interior NUL (`\0`)
    /// - the runtime reports a planning/explain error
    pub fn explain_analyze(&self, cypher: &str) -> Result<ExplainTrace> {
        let a = velr_api()?;

        let cy = CString::new(cypher)
            .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "openCypher contains NUL"))?;

        let mut out_trace: *mut ffi::velr_explain_trace = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (a.velr_explain_analyze)(self.db.as_ptr(), cy.as_ptr(), &mut out_trace, &mut err)
        };
        rc_to_result(rc, err)?;
        ExplainTrace::from_raw(out_trace)
    }

    /// Begin a transaction.
    ///
    /// The transaction handle is closed automatically on drop. To explicitly finalize a
    /// transaction, use [`VelrTx::commit`] or [`VelrTx::rollback`].
    /// Begin a transaction.
    ///
    /// The transaction handle is closed automatically on drop. To explicitly finalize a
    /// transaction, use [`VelrTx::commit`] or [`VelrTx::rollback`].
    pub fn begin_tx(&self) -> Result<VelrTx<'_>> {
        let a = velr_api()?;

        let mut out_tx: *mut ffi::velr_tx = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe { (a.velr_tx_begin)(self.db.as_ptr(), &mut out_tx, &mut err) };
        rc_to_result(rc, err)?;

        let nn = NonNull::new(out_tx).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_EERR as i32,
                "velr_tx_begin returned null tx",
            )
        })?;

        Ok(VelrTx {
            tx: Some(nn),
            named_savepoints: RefCell::new(Vec::new()),
            _db: PhantomData,
            _nosend: PhantomData,
        })
    }

    /// Bind Arrow arrays (Arrow C Data Interface) to a logical name.
    ///
    /// Available only when built with the `arrow-ipc` feature.
    ///
    /// This transfers ownership of the provided Arrow arrays into Velr for the lifetime of the bind.
    /// (At the ABI level, the ArrowArray structs are consumed during the call.)
    #[cfg(feature = "arrow-ipc")]
    pub fn bind_arrow(
        &self,
        logical: &str,
        col_names: Vec<String>,
        arrays: Vec<Box<dyn arrow2::array::Array>>,
    ) -> Result<()> {
        arrow_bind::bind_arrow_db(self.db.as_ptr(), logical, col_names, arrays)
    }

    /// Bind chunked Arrow arrays per column to a logical name.
    ///
    /// Available only when built with the `arrow-ipc` feature.
    ///
    /// This transfers ownership of the provided Arrow arrays into Velr for the lifetime of the bind.
    /// (At the ABI level, the ArrowArray structs are consumed during the call.)
    ///
    /// All columns must have the same total row count (sum of chunk lengths); otherwise the bind
    /// returns an error.
    #[cfg(feature = "arrow-ipc")]
    pub fn bind_arrow_chunks(
        &self,
        logical: &str,
        col_names: Vec<String>,
        chunks_per_col: Vec<Vec<Box<dyn arrow2::array::Array>>>,
    ) -> Result<()> {
        arrow_bind::bind_arrow_chunks_db(self.db.as_ptr(), logical, col_names, chunks_per_col)
    }
}

impl Drop for Velr {
    /// Close the connection handle.
    fn drop(&mut self) {
        if let Ok(a) = velr_api() {
            unsafe { (a.velr_close)(self.db.as_ptr()) };
        }
    }
}

// -------------------------- ExecTables --------------------------

/// Streaming result of an execution that may yield multiple tables.
///
/// This is an *in-flight* type and is **`!Send` + `!Sync`** (thread-affine).
///
/// Use [`ExecTables::next_table`] to pull result tables sequentially. Dropping this value will
/// close the underlying execution stream
pub struct ExecTables<'db> {
    stream: Option<NonNull<ffi::velr_stream>>,
    _db: PhantomData<&'db Velr>,
    _nosend: PhantomData<Rc<()>>, // !Send + !Sync
}

impl<'db> ExecTables<'db> {
    /// Fetch the next result table from the execution stream.
    ///
    /// Returns:
    /// - `Ok(Some(table))` when a new table is available
    /// - `Ok(None)` when the stream is exhausted (and the runtime stream is closed)
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the runtime reports an error while advancing the stream.
    pub fn next_table(&mut self) -> Result<Option<TableResult>> {
        let a = velr_api()?;

        let Some(stream) = self.stream else {
            return Ok(None);
        };

        let mut out_table: *mut ffi::velr_table = std::ptr::null_mut();
        let mut has: i32 = 0;
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (a.velr_stream_next_table)(stream.as_ptr(), &mut out_table, &mut has, &mut err)
        };
        rc_to_result(rc, err)?;

        if has == 0 {
            unsafe { (a.velr_exec_close)(stream.as_ptr()) };
            self.stream = None;
            return Ok(None);
        }

        TableResult::from_raw(out_table).map(Some)
    }
}

impl Drop for ExecTables<'_> {
    /// Close the underlying execution stream if still open.
    fn drop(&mut self) {
        if let Some(st) = self.stream.take() {
            if let Ok(a) = velr_api() {
                unsafe { (a.velr_exec_close)(st.as_ptr()) };
            }
        }
    }
}

// -------------------------- TableResult --------------------------

/// A single result table produced by query execution.
///
/// This is an *in-flight* type and is **`!Send` + `!Sync`** (thread-affine).
///
/// A table exposes:
/// - column metadata (names and count)
/// - row iteration via [`TableResult::rows`], [`TableResult::for_each_row`], or [`TableResult::collect`]
///
pub struct TableResult {
    table: NonNull<ffi::velr_table>,
    col_names: Vec<String>,
    col_count: usize,
    _nosend: PhantomData<Rc<()>>, // !Send + !Sync
}

impl TableResult {
    /// Construct a [`TableResult`] from a raw runtime table pointer.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if `ptr` is null, if the runtime fails to retrieve column names,
    /// or if column name bytes are not valid UTF-8.
    fn from_raw(ptr: *mut ffi::velr_table) -> Result<Self> {
        let a = velr_api()?;

        let table = NonNull::new(ptr)
            .ok_or_else(|| Error::new(ffi::velr_code::VELR_EERR as i32, "null table"))?;

        let build = (|| -> Result<(Vec<String>, usize)> {
            let col_count = unsafe { (a.velr_table_column_count)(table.as_ptr()) };

            let mut names = Vec::with_capacity(col_count);
            for i in 0..col_count {
                let mut p: *const u8 = std::ptr::null();
                let mut len: usize = 0;

                let rc = unsafe { (a.velr_table_column_name)(table.as_ptr(), i, &mut p, &mut len) };

                if rc as i32 != ffi::velr_code::VELR_OK as i32 {
                    return Err(Error::new(
                        rc as i32,
                        format!("velr_table_column_name failed at idx={i}"),
                    ));
                }

                let bytes: &[u8] = if len == 0 {
                    &[]
                } else if p.is_null() {
                    return Err(Error::new(
                        ffi::velr_code::VELR_EERR as i32,
                        format!("column name at idx={i} is null with non-zero length"),
                    ));
                } else {
                    unsafe { std::slice::from_raw_parts(p, len) }
                };

                let s = std::str::from_utf8(bytes).map_err(|_| {
                    Error::new(
                        ffi::velr_code::VELR_EUTF as i32,
                        format!("column name at idx={i} is not valid UTF-8"),
                    )
                })?;
                names.push(s.to_string());
            }

            Ok((names, col_count))
        })();

        match build {
            Ok((col_names, col_count)) => Ok(Self {
                table,
                col_names,
                col_count,
                _nosend: PhantomData,
            }),
            Err(e) => {
                unsafe { (a.velr_table_close)(table.as_ptr()) };
                Err(e)
            }
        }
    }

    /// Return the column names for this table.
    pub fn column_names(&self) -> &[String] {
        &self.col_names
    }

    /// Return the number of columns in this table.
    pub fn column_count(&self) -> usize {
        self.col_count
    }

    /// Open a row iterator for this table.
    ///
    /// This requires `&mut self`, so only one active row iterator may exist for a table at a time.
    /// Row iteration is callback-based via [`RowIter::next`], producing a borrowed slice of
    /// [`CellRef`] for each row.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the runtime fails to open the row cursor.
    pub fn rows<'t>(&'t mut self) -> Result<RowIter<'t>> {
        let a = velr_api()?;

        let mut out_rows: *mut ffi::velr_rows = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe { (a.velr_table_rows_open)(self.table.as_ptr(), &mut out_rows, &mut err) };
        rc_to_result(rc, err)?;

        let nn = NonNull::new(out_rows).ok_or_else(|| {
            Error::new(ffi::velr_code::VELR_EERR as i32, "rows_open returned null")
        })?;

        Ok(RowIter {
            rows: Some(nn),
            col_count: self.col_count,
            buf: vec![
                ffi::velr_cell {
                    ty: ffi::velr_cell_type::VELR_NULL,
                    i64_: 0,
                    f64_: 0.0,
                    ptr: std::ptr::null(),
                    len: 0,
                };
                self.col_count
            ],
            _table: PhantomData,
            _nosend: PhantomData,
        })
    }

    /// Visit each row in this table.
    ///
    /// The callback receives a slice of [`CellRef`] representing the row’s cells.
    /// The borrow is scoped to the callback invocation (and remains valid until the next row is fetched).
    pub fn for_each_row<F>(&mut self, mut on_row: F) -> Result<()>
    where
        F: for<'row> FnMut(&[CellRef<'row>]) -> Result<()>,
    {
        let mut it = self.rows()?;
        while it.next(|cells| on_row(cells))? {}
        Ok(())
    }

    /// Map each row to a value and collect into a vector.
    ///
    /// This is a convenience wrapper around [`TableResult::for_each_row`].
    pub fn collect<T, F>(&mut self, mut map: F) -> Result<Vec<T>>
    where
        F: for<'row> FnMut(&[CellRef<'row>]) -> Result<T>,
    {
        let mut out = Vec::new();
        self.for_each_row(|cells| {
            out.push(map(cells)?);
            Ok(())
        })?;
        Ok(out)
    }

    /// Encode this table as an Arrow IPC file in memory.
    ///
    /// Available only when built with the `arrow-ipc` feature.
    ///
    /// Returns the IPC file bytes produced by the runtime.
    #[cfg(feature = "arrow-ipc")]
    pub fn to_arrow_ipc_file(&mut self) -> Result<Vec<u8>> {
        let a = velr_api()?;

        let mut ptr: *mut u8 = std::ptr::null_mut();
        let mut len: usize = 0;
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (a.velr_table_ipc_file_malloc)(self.table.as_ptr(), &mut ptr, &mut len, &mut err)
        };
        rc_to_result(rc, err)?;
        take_owned_bytes(a, ptr, len, "velr_table_ipc_file_malloc")
    }
}

impl Drop for TableResult {
    /// Close the table handle.
    fn drop(&mut self) {
        if let Ok(a) = velr_api() {
            unsafe { (a.velr_table_close)(self.table.as_ptr()) };
        }
    }
}

// -------------------------- RowIter --------------------------

/// Iterator over rows of a table.
///
/// This is an *in-flight* type and is **`!Send` + `!Sync`** (thread-affine).
///
/// Rows are produced via [`RowIter::next`], which invokes a callback with a borrowed slice of
/// [`CellRef`].
pub struct RowIter<'t> {
    rows: Option<NonNull<ffi::velr_rows>>,
    col_count: usize,
    buf: Vec<ffi::velr_cell>,
    _table: PhantomData<&'t mut TableResult>,
    _nosend: PhantomData<Rc<()>>, // !Send + !Sync
}

impl<'t> RowIter<'t> {
    /// Advance to the next row and invoke `on_row`.
    ///
    /// Returns:
    /// - `Ok(true)` if a row was produced and `on_row` was called
    /// - `Ok(false)` if the iterator is exhausted
    ///
    /// ## Lifetimes
    ///
    /// For `CellRef::Text` and `CellRef::Json`, the returned byte slices remain valid until the next
    /// call to [`RowIter::next`] on the same iterator (or until the iterator is dropped).
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if the runtime reports an error while advancing.
    pub fn next<F>(&mut self, on_row: F) -> Result<bool>
    where
        F: for<'row> FnOnce(&[CellRef<'row>]) -> Result<()>,
    {
        let a = velr_api()?;

        let Some(rows) = self.rows else {
            return Ok(false);
        };

        let mut written: usize = 0;
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (a.velr_rows_next)(
                rows.as_ptr(),
                self.buf.as_mut_ptr(),
                self.buf.len(),
                &mut written,
                &mut err,
            )
        };

        if rc == 0 {
            free_unexpected_err(err);
            unsafe { (a.velr_rows_close)(rows.as_ptr()) };
            self.rows = None;
            return Ok(false);
        }
        if rc < 0 {
            let msg = unsafe { take_err(err) };
            return Err(Error::new(rc, msg));
        }

        free_unexpected_err(err);

        if written > self.buf.len() {
            return Err(Error::new(
                ffi::velr_code::VELR_EERR as i32,
                format!(
                    "velr_rows_next reported {} cells, buffer holds {}",
                    written,
                    self.buf.len()
                ),
            ));
        }

        let mut scratch: Vec<CellRef<'_>> = Vec::with_capacity(written);
        for c in self.buf.iter().take(written) {
            let cell = match c.ty {
                ffi::velr_cell_type::VELR_NULL => CellRef::Null,
                ffi::velr_cell_type::VELR_BOOL => CellRef::Bool(c.i64_ != 0),
                ffi::velr_cell_type::VELR_INT64 => CellRef::Integer(c.i64_),
                ffi::velr_cell_type::VELR_DOUBLE => CellRef::Float(c.f64_),

                ffi::velr_cell_type::VELR_TEXT => {
                    let b: &[u8] = if c.len == 0 {
                        &[]
                    } else if c.ptr.is_null() {
                        return Err(Error::new(
                            ffi::velr_code::VELR_EERR as i32,
                            "VELR_TEXT cell had null pointer with non-zero length",
                        ));
                    } else {
                        unsafe { std::slice::from_raw_parts(c.ptr, c.len) }
                    };
                    CellRef::Text(b)
                }

                ffi::velr_cell_type::VELR_JSON => {
                    let b: &[u8] = if c.len == 0 {
                        &[]
                    } else if c.ptr.is_null() {
                        return Err(Error::new(
                            ffi::velr_code::VELR_EERR as i32,
                            "VELR_JSON cell had null pointer with non-zero length",
                        ));
                    } else {
                        unsafe { std::slice::from_raw_parts(c.ptr, c.len) }
                    };
                    CellRef::Json(b)
                }
            };
            scratch.push(cell);
        }

        on_row(&scratch)?;
        Ok(true)
    }
}

impl Drop for RowIter<'_> {
    fn drop(&mut self) {
        if let Some(r) = self.rows.take() {
            if let Ok(a) = velr_api() {
                unsafe { (a.velr_rows_close)(r.as_ptr()) };
            }
        }
    }
}

#[derive(Debug)]
struct NamedSavepoint {
    name: String,
    sp: NonNull<ffi::velr_sp>,
}
// -------------------------- Transactions --------------------------
//

/// A transaction handle (thread-affine).
///
/// Finalization:
/// - [`VelrTx::commit`] consumes `self` and commits.
/// - [`VelrTx::rollback`] consumes `self` and rolls back.
///
/// ## Drop behavior
///
/// If a transaction is dropped without an explicit commit/rollback, the runtime rolls it back.
pub struct VelrTx<'db> {
    tx: Option<NonNull<ffi::velr_tx>>,
    named_savepoints: RefCell<Vec<NamedSavepoint>>,
    _db: PhantomData<&'db Velr>,
    _nosend: PhantomData<Rc<()>>, // !Send + !Sync
}

impl<'db> VelrTx<'db> {
    fn ptr(&self) -> Result<NonNull<ffi::velr_tx>> {
        self.tx
            .ok_or_else(|| Error::new(ffi::velr_code::VELR_ESTATE as i32, "tx already consumed"))
    }

    fn find_named_index(&self, name: &str) -> Option<usize> {
        self.named_savepoints
            .borrow()
            .iter()
            .position(|sp| sp.name == name)
    }

    /// Execute `openCypher` within this transaction and return a stream of result tables.
    ///
    /// See [`Velr::exec`] for general streaming semantics.
    pub fn exec<'tx>(&'tx self, cypher: &str) -> Result<ExecTablesTx<'tx>> {
        let a = velr_api()?;

        let tx = self.ptr()?;
        let cy = CString::new(cypher)
            .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "openCypher contains NUL"))?;

        let mut out_stream: *mut ffi::velr_stream_tx = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc =
            unsafe { (a.velr_tx_exec_start)(tx.as_ptr(), cy.as_ptr(), &mut out_stream, &mut err) };
        rc_to_result(rc, err)?;

        let nn = NonNull::new(out_stream).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_EERR as i32,
                "tx_exec_start returned null stream",
            )
        })?;

        Ok(ExecTablesTx {
            stream: Some(nn),
            _tx: PhantomData,
            _nosend: PhantomData,
        })
    }

    /// Execute a query expected to produce exactly one table within this transaction.
    ///
    /// This method is implemented by streaming (`exec`) and validating that exactly one table is
    /// produced. If you expect multiple tables, use [`VelrTx::exec`].
    pub fn exec_one(&self, cypher: &str) -> Result<TableResult> {
        let mut st = self.exec(cypher)?;
        let first = match st.next_table()? {
            Some(t) => t,
            None => {
                return Err(Error::new(
                    ffi::velr_code::VELR_EERR as i32,
                    "query produced no result tables",
                ))
            }
        };
        if st.next_table()?.is_some() {
            return Err(Error::new(
                ffi::velr_code::VELR_EERR as i32,
                "query produced multiple tables; use exec()",
            ));
        }
        Ok(first)
    }

    /// Execute a query within this transaction and discard all results.
    pub fn run(&self, cypher: &str) -> Result<()> {
        let mut st = self.exec(cypher)?;
        while let Some(mut t) = st.next_table()? {
            t.for_each_row(|_| Ok(()))?;
        }
        Ok(())
    }

    /// Build an EXPLAIN trace for `openCypher`.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - `openCypher` contains an interior NUL (`\0`)
    /// - the runtime reports a planning/explain error
    pub fn explain(&self, cypher: &str) -> Result<ExplainTrace> {
        let a = velr_api()?;
        let tx = self.ptr()?;

        let cy = CString::new(cypher)
            .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "openCypher contains NUL"))?;

        let mut out_trace: *mut ffi::velr_explain_trace = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe { (a.velr_tx_explain)(tx.as_ptr(), cy.as_ptr(), &mut out_trace, &mut err) };
        rc_to_result(rc, err)?;
        ExplainTrace::from_raw(out_trace)
    }

    /// Build an EXPLAIN ANALYZE trace for `openCypher`.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - `openCypher` contains an interior NUL (`\0`)
    /// - the runtime reports a planning/explain error
    pub fn explain_analyze(&self, cypher: &str) -> Result<ExplainTrace> {
        let a = velr_api()?;
        let tx = self.ptr()?;

        let cy = CString::new(cypher)
            .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "openCypher contains NUL"))?;

        let mut out_trace: *mut ffi::velr_explain_trace = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (a.velr_tx_explain_analyze)(tx.as_ptr(), cy.as_ptr(), &mut out_trace, &mut err)
        };
        rc_to_result(rc, err)?;
        ExplainTrace::from_raw(out_trace)
    }

    /// Commit this transaction.
    ///
    /// Consumes the transaction handle. After this call, the transaction is finalized and cannot be
    /// used again.
    ///
    /// Note: the underlying C ABI consumes the transaction handle even if an error is returned.
    pub fn commit(mut self) -> Result<()> {
        let a = velr_api()?;

        // After commit the runtime transaction owns final cleanup of any outstanding named savepoints.
        self.named_savepoints.get_mut().clear();

        let tx = self
            .tx
            .take()
            .ok_or_else(|| Error::new(ffi::velr_code::VELR_ESTATE as i32, "tx already consumed"))?;

        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { (a.velr_tx_commit)(tx.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }

    /// Roll back this transaction.
    ///
    /// Consumes the transaction handle. After this call, the transaction is finalized and cannot be
    /// used again.
    ///
    /// Note: the underlying C ABI consumes the transaction handle even if an error is returned.
    pub fn rollback(mut self) -> Result<()> {
        let a = velr_api()?;

        // After rollback the runtime transaction owns final cleanup of any outstanding named savepoints.
        self.named_savepoints.get_mut().clear();

        let tx = self
            .tx
            .take()
            .ok_or_else(|| Error::new(ffi::velr_code::VELR_ESTATE as i32, "tx already consumed"))?;

        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { (a.velr_tx_rollback)(tx.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }

    fn create_named_savepoint_raw(&self, name: &str) -> Result<NonNull<ffi::velr_sp>> {
        let a = velr_api()?;
        let tx = self.ptr()?;

        let cname = CString::new(name).map_err(|_| {
            Error::new(
                ffi::velr_code::VELR_EUTF as i32,
                "savepoint name contains NUL",
            )
        })?;

        let mut out_sp: *mut ffi::velr_sp = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (a.velr_tx_savepoint_named)(tx.as_ptr(), cname.as_ptr(), &mut out_sp, &mut err)
        };
        rc_to_result(rc, err)?;

        NonNull::new(out_sp).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_EERR as i32,
                "savepoint_named returned null",
            )
        })
    }

    /// Create an unnamed scoped savepoint inside this transaction.
    ///
    /// The returned handle is RAII-managed:
    /// - call [`VelrSavepoint::release`] to keep the work since the savepoint
    /// - call [`VelrSavepoint::rollback`] to undo back to the savepoint
    /// - dropping the handle rolls back to the savepoint and releases it
    pub fn savepoint<'tx>(&'tx self) -> Result<VelrSavepoint<'tx>> {
        let a = velr_api()?;

        let tx = self.ptr()?;
        let mut out_sp: *mut ffi::velr_sp = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe { (a.velr_tx_savepoint)(tx.as_ptr(), &mut out_sp, &mut err) };
        rc_to_result(rc, err)?;

        let nn = NonNull::new(out_sp).ok_or_else(|| {
            Error::new(ffi::velr_code::VELR_EERR as i32, "savepoint returned null")
        })?;

        Ok(VelrSavepoint {
            sp: Some(nn),
            _tx: PhantomData,
            _nosend: PhantomData,
        })
    }

    /// Create a detached named savepoint inside this transaction.
    ///
    /// Unlike [`VelrTx::savepoint`], this does not return a guard. The named savepoint remains
    /// active in the transaction until:
    /// - [`VelrTx::rollback_to`] rolls back to it
    /// - [`VelrTx::release_savepoint`] explicitly releases it
    /// - the transaction is committed, rolled back, or dropped
    ///
    /// Active names must be unique within the transaction.
    pub fn savepoint_named(&self, name: &str) -> Result<()> {
        if self.find_named_index(name).is_some() {
            return Err(Error::new(
                ffi::velr_code::VELR_ESTATE as i32,
                format!("named savepoint {name:?} already exists"),
            ));
        }

        let sp = self.create_named_savepoint_raw(name)?;

        self.named_savepoints.borrow_mut().push(NamedSavepoint {
            name: name.to_string(),
            sp,
        });

        Ok(())
    }

    /// Roll back to a previously-created named savepoint.
    ///
    /// Driver semantics:
    /// - all newer named savepoints are discarded
    /// - the target named savepoint remains active after rollback
    ///
    /// This is implemented using the stored savepoint handle, because the runtime's
    /// `velr_tx_rollback_to(...)` is not guaranteed to interoperate with savepoints
    /// created through `velr_tx_savepoint_named(...)`.
    pub fn rollback_to(&self, name: &str) -> Result<()> {
        let idx = self.find_named_index(name).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_ESTATE as i32,
                format!("no such active named savepoint {name:?}"),
            )
        })?;

        let target_name = {
            let named = self.named_savepoints.borrow();
            named[idx].name.clone()
        };

        let target_sp = {
            let named = self.named_savepoints.borrow();
            named[idx].sp
        };

        // Roll back using the savepoint handle itself. This consumes the runtime savepoint
        // and invalidates any newer savepoints as part of the rollback.
        let a = velr_api()?;
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { (a.velr_sp_rollback)(target_sp.as_ptr(), &mut err) };
        rc_to_result(rc, err)?;

        // After a successful rollback:
        // - savepoints before idx are still valid
        // - the target savepoint handle is consumed
        // - newer savepoints are invalid
        {
            let mut named = self.named_savepoints.borrow_mut();
            named.truncate(idx);
        }

        // Recreate the target savepoint so it remains active after rollback.
        // This matches the external API semantics we want.
        let recreated = self.create_named_savepoint_raw(&target_name)?;
        self.named_savepoints.borrow_mut().push(NamedSavepoint {
            name: target_name,
            sp: recreated,
        });

        Ok(())
    }

    /// Release the most recently-created active named savepoint.
    ///
    /// Releasing a non-topmost named savepoint is intentionally rejected here to keep the driver
    /// semantics simple and well-defined.
    pub fn release_savepoint(&self, name: &str) -> Result<()> {
        let a = velr_api()?;

        let mut named = self.named_savepoints.borrow_mut();
        let last_idx = named.len().checked_sub(1).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_ESTATE as i32,
                "no active named savepoints",
            )
        })?;

        if named[last_idx].name != name {
            return Err(Error::new(
                ffi::velr_code::VELR_ESTATE as i32,
                format!(
                    "release_savepoint({name:?}) requires {name:?} to be the most recent active named savepoint"
                ),
            ));
        }

        let entry = named.pop().unwrap();

        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { (a.velr_sp_release)(entry.sp.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }

    /// Bind Arrow arrays (Arrow C Data Interface) to a logical name.
    ///
    /// Available only when built with the `arrow-ipc` feature.
    ///
    /// This consumes the exported ArrowArray values at the ABI boundary.
    /// Callers must not reuse or release those exported ArrowArray values after the call.
    /// (At the ABI level, the ArrowArray structs are consumed during the call.)
    #[cfg(feature = "arrow-ipc")]
    pub fn bind_arrow(
        &self,
        logical: &str,
        col_names: Vec<String>,
        arrays: Vec<Box<dyn arrow2::array::Array>>,
    ) -> Result<()> {
        let tx = self.ptr()?;
        arrow_bind::bind_arrow_tx(tx.as_ptr(), logical, col_names, arrays)
    }

    /// Bind chunked Arrow arrays per column to a logical name.
    ///
    /// Available only when built with the `arrow-ipc` feature.
    ///
    /// This transfers ownership of the provided Arrow arrays into Velr for the lifetime of the bind.
    /// (At the ABI level, the ArrowArray structs are consumed during the call.)
    ///
    /// All columns must have the same total row count (sum of chunk lengths); otherwise the bind
    /// returns an error.
    #[cfg(feature = "arrow-ipc")]
    pub fn bind_arrow_chunks(
        &self,
        logical: &str,
        col_names: Vec<String>,
        chunks_per_col: Vec<Vec<Box<dyn arrow2::array::Array>>>,
    ) -> Result<()> {
        let tx = self.ptr()?;
        arrow_bind::bind_arrow_chunks_tx(tx.as_ptr(), logical, col_names, chunks_per_col)
    }
}

impl Drop for VelrTx<'_> {
    /// Close the transaction handle if still open.
    fn drop(&mut self) {
        // Do not attempt to individually close named savepoints here; the transaction finalization
        // owns that cleanup.
        self.named_savepoints.get_mut().clear();

        if let Some(tx) = self.tx.take() {
            if let Ok(a) = velr_api() {
                unsafe { (a.velr_tx_close)(tx.as_ptr()) };
            }
        }
    }
}

// -------------------------- ExecTablesTx --------------------------
//

/// Streaming result of an execution within a transaction.
///
/// This is an *in-flight* type and is **`!Send` + `!Sync`** (thread-affine).
pub struct ExecTablesTx<'tx> {
    stream: Option<NonNull<ffi::velr_stream_tx>>,
    _tx: PhantomData<&'tx VelrTx<'tx>>,
    _nosend: PhantomData<Rc<()>>, // !Send + !Sync
}

impl ExecTablesTx<'_> {
    /// Fetch the next result table from the transaction execution stream.
    ///
    /// Returns `Ok(None)` when exhausted (and closes the underlying stream).
    pub fn next_table(&mut self) -> Result<Option<TableResult>> {
        let a = velr_api()?;

        let Some(stream) = self.stream else {
            return Ok(None);
        };

        let mut out_table: *mut ffi::velr_table = std::ptr::null_mut();
        let mut has: i32 = 0;
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            (a.velr_stream_tx_next_table)(stream.as_ptr(), &mut out_table, &mut has, &mut err)
        };
        rc_to_result(rc, err)?;

        if has == 0 {
            unsafe { (a.velr_exec_tx_close)(stream.as_ptr()) };
            self.stream = None;
            return Ok(None);
        }

        TableResult::from_raw(out_table).map(Some)
    }
}

impl Drop for ExecTablesTx<'_> {
    /// Close the underlying transaction execution stream if still open.
    fn drop(&mut self) {
        if let Some(st) = self.stream.take() {
            if let Ok(a) = velr_api() {
                unsafe { (a.velr_exec_tx_close)(st.as_ptr()) };
            }
        }
    }
}
// -------------------------- Savepoints --------------------------
//

/// A scoped savepoint handle within a transaction (thread-affine).
///
/// This is the RAII/scoped savepoint API:
/// - [`VelrTx::savepoint`] creates one
/// - [`VelrSavepoint::release`] keeps the work since the savepoint
/// - [`VelrSavepoint::rollback`] undoes back to the savepoint
///
/// ## Drop behavior
///
/// If dropped without explicit release/rollback, the runtime rolls back to the savepoint and
/// releases it.
#[must_use = "savepoint guards are RAII; bind the returned value to a variable or explicitly call release()/rollback()"]
pub struct VelrSavepoint<'tx> {
    sp: Option<NonNull<ffi::velr_sp>>,
    _tx: PhantomData<&'tx VelrTx<'tx>>,
    _nosend: PhantomData<Rc<()>>, // !Send + !Sync
}

impl VelrSavepoint<'_> {
    /// Release this savepoint.
    ///
    /// Consumes the savepoint handle. After this call, the savepoint is finalized and cannot be used
    /// again.
    ///
    /// Note: the underlying C ABI consumes the savepoint handle even if an error is returned.
    pub fn release(mut self) -> Result<()> {
        let a = velr_api()?;

        let sp = self.sp.take().ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_ESTATE as i32,
                "savepoint already consumed",
            )
        })?;

        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { (a.velr_sp_release)(sp.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }

    /// Roll back to this savepoint and release it.
    ///
    /// Consumes the savepoint handle. After this call, the savepoint is finalized and cannot be used
    /// again.
    ///
    /// Note: the underlying C ABI consumes the savepoint handle even if an error is returned.
    pub fn rollback(mut self) -> Result<()> {
        let a = velr_api()?;

        let sp = self.sp.take().ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_ESTATE as i32,
                "savepoint already consumed",
            )
        })?;

        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { (a.velr_sp_rollback)(sp.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }
}

impl Drop for VelrSavepoint<'_> {
    /// Close the savepoint handle if still open.
    fn drop(&mut self) {
        if let Some(sp) = self.sp.take() {
            if let Ok(a) = velr_api() {
                unsafe { (a.velr_sp_close)(sp.as_ptr()) };
            }
        }
    }
}

// -------------------------- Arrow binding helpers --------------------------
/// Arrow binding support.
///
/// This module is only compiled with the `arrow-ipc` feature enabled.
///
/// It exports Arrow arrays/schemas using `arrow2`’s Arrow C Data Interface helpers and passes
/// them to the Velr runtime ABI. The code uses `ManuallyDrop` to avoid dropping the exported
/// `ArrowArray` values after the call; this matches an ownership-transfer pattern typical of
/// the Arrow C Data Interface (the runtime is expected to manage release thereafter).

#[cfg(feature = "arrow-ipc")]
mod arrow_bind {
    use super::*;
    use std::mem::ManuallyDrop;

    use arrow2::{
        array::Array,
        datatypes::Field,
        ffi::{export_array_to_c, export_field_to_c, ArrowArray, ArrowSchema},
    };

    fn cstring(s: &str, what: &str) -> Result<CString> {
        CString::new(s).map_err(|_| {
            Error::new(
                ffi::velr_code::VELR_EUTF as i32,
                format!("{what} contains NUL"),
            )
        })
    }

    pub fn bind_arrow_db(
        db: *mut ffi::velr_db,
        logical: &str,
        col_names: Vec<String>,
        arrays: Vec<Box<dyn Array>>,
    ) -> Result<()> {
        let a = super::velr_api()?;
        bind_arrow_common(
            |logical_ptr, schemas_pp, arrays_pp, names_ptr, n, err| unsafe {
                (a.velr_bind_arrow)(db, logical_ptr, schemas_pp, arrays_pp, names_ptr, n, err)
            },
            logical,
            col_names,
            arrays,
        )
    }

    pub fn bind_arrow_tx(
        tx: *mut ffi::velr_tx,
        logical: &str,
        col_names: Vec<String>,
        arrays: Vec<Box<dyn Array>>,
    ) -> Result<()> {
        let a = super::velr_api()?;
        bind_arrow_common(
            |logical_ptr, schemas_pp, arrays_pp, names_ptr, n, err| unsafe {
                (a.velr_tx_bind_arrow)(tx, logical_ptr, schemas_pp, arrays_pp, names_ptr, n, err)
            },
            logical,
            col_names,
            arrays,
        )
    }

    fn bind_arrow_common(
        f: impl FnOnce(
            *const c_char,
            *const *const ArrowSchema,
            *const *const ArrowArray,
            *const ffi::velr_strview,
            usize,
            *mut *mut c_char,
        ) -> ffi::velr_code,
        logical: &str,
        col_names: Vec<String>,
        arrays: Vec<Box<dyn Array>>,
    ) -> Result<()> {
        if col_names.is_empty() {
            return Err(Error::new(
                ffi::velr_code::VELR_EARG as i32,
                "bind_arrow: no columns",
            ));
        }
        if arrays.len() != col_names.len() {
            return Err(Error::new(
                ffi::velr_code::VELR_EARG as i32,
                format!(
                    "bind_arrow: arrays len {} != col_names len {}",
                    arrays.len(),
                    col_names.len()
                ),
            ));
        }

        let logical_c = cstring(logical, "logical")?;

        let mut schemas: Vec<ArrowSchema> = Vec::with_capacity(col_names.len());
        let mut array_cs: Vec<ManuallyDrop<ArrowArray>> = Vec::with_capacity(col_names.len());
        let mut schema_ptrs: Vec<*const ArrowSchema> = Vec::with_capacity(col_names.len());
        let mut array_ptrs: Vec<*const ArrowArray> = Vec::with_capacity(col_names.len());
        let mut name_views: Vec<ffi::velr_strview> = Vec::with_capacity(col_names.len());

        for (name, arr) in col_names.iter().zip(arrays.into_iter()) {
            let field = Field::new(name.clone(), arr.data_type().clone(), true);
            let schema = export_field_to_c(&field);
            schemas.push(schema);

            let a = ManuallyDrop::new(export_array_to_c(arr));
            array_cs.push(a);
        }

        for i in 0..col_names.len() {
            schema_ptrs.push(&schemas[i] as *const ArrowSchema);
            array_ptrs.push((&*array_cs[i]) as *const ArrowArray);

            let b = col_names[i].as_bytes();
            name_views.push(ffi::velr_strview {
                ptr: b.as_ptr(),
                len: b.len(),
            });
        }

        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = f(
            logical_c.as_ptr(),
            schema_ptrs.as_ptr(),
            array_ptrs.as_ptr(),
            name_views.as_ptr(),
            col_names.len(),
            &mut err,
        );
        super::rc_to_result(rc, err)
    }

    pub fn bind_arrow_chunks_db(
        db: *mut ffi::velr_db,
        logical: &str,
        col_names: Vec<String>,
        chunks_per_col: Vec<Vec<Box<dyn Array>>>,
    ) -> Result<()> {
        let a = super::velr_api()?;
        bind_chunks_common(
            |logical_ptr, cols_ptr, names_ptr, n, err| unsafe {
                (a.velr_bind_arrow_chunks)(db, logical_ptr, cols_ptr, names_ptr, n, err)
            },
            logical,
            col_names,
            chunks_per_col,
        )
    }

    pub fn bind_arrow_chunks_tx(
        tx: *mut ffi::velr_tx,
        logical: &str,
        col_names: Vec<String>,
        chunks_per_col: Vec<Vec<Box<dyn Array>>>,
    ) -> Result<()> {
        let a = super::velr_api()?;
        bind_chunks_common(
            |logical_ptr, cols_ptr, names_ptr, n, err| unsafe {
                (a.velr_tx_bind_arrow_chunks)(tx, logical_ptr, cols_ptr, names_ptr, n, err)
            },
            logical,
            col_names,
            chunks_per_col,
        )
    }

    fn bind_chunks_common(
        f: impl FnOnce(
            *const c_char,
            *const ffi::velr_arrow_chunks,
            *const ffi::velr_strview,
            usize,
            *mut *mut c_char,
        ) -> ffi::velr_code,
        logical: &str,
        col_names: Vec<String>,
        chunks_per_col: Vec<Vec<Box<dyn Array>>>,
    ) -> Result<()> {
        if col_names.is_empty() {
            return Err(Error::new(
                ffi::velr_code::VELR_EARG as i32,
                "bind_arrow_chunks: no columns",
            ));
        }
        if chunks_per_col.len() != col_names.len() {
            return Err(Error::new(
                ffi::velr_code::VELR_EARG as i32,
                format!(
                    "bind_arrow_chunks: chunks_per_col {} != col_names {}",
                    chunks_per_col.len(),
                    col_names.len()
                ),
            ));
        }

        let logical_c = cstring(logical, "logical")?;

        let mut all_schema_storage: Vec<Vec<ArrowSchema>> = Vec::with_capacity(col_names.len());
        let mut all_array_storage: Vec<Vec<ManuallyDrop<ArrowArray>>> =
            Vec::with_capacity(col_names.len());
        let mut all_schema_ptrs: Vec<Vec<*const ArrowSchema>> = Vec::with_capacity(col_names.len());
        let mut all_array_ptrs: Vec<Vec<*const ArrowArray>> = Vec::with_capacity(col_names.len());

        for (ci, chunks) in chunks_per_col.into_iter().enumerate() {
            if chunks.is_empty() {
                return Err(Error::new(
                    ffi::velr_code::VELR_EARG as i32,
                    format!("bind_arrow_chunks: col {ci} has 0 chunks"),
                ));
            }

            let mut schemas: Vec<ArrowSchema> = Vec::with_capacity(chunks.len());
            let mut arrays: Vec<ManuallyDrop<ArrowArray>> = Vec::with_capacity(chunks.len());

            for arr in chunks.into_iter() {
                let field = Field::new(col_names[ci].clone(), arr.data_type().clone(), true);
                schemas.push(export_field_to_c(&field));
                arrays.push(ManuallyDrop::new(export_array_to_c(arr)));
            }

            let mut sp: Vec<*const ArrowSchema> = Vec::with_capacity(schemas.len());
            let mut ap: Vec<*const ArrowArray> = Vec::with_capacity(arrays.len());
            for i in 0..schemas.len() {
                sp.push(&schemas[i] as *const ArrowSchema);
                ap.push((&*arrays[i]) as *const ArrowArray);
            }

            all_schema_storage.push(schemas);
            all_array_storage.push(arrays);
            all_schema_ptrs.push(sp);
            all_array_ptrs.push(ap);
        }

        let mut cols_desc: Vec<ffi::velr_arrow_chunks> = Vec::with_capacity(col_names.len());
        for i in 0..col_names.len() {
            cols_desc.push(ffi::velr_arrow_chunks {
                schemas: all_schema_ptrs[i].as_ptr(),
                arrays: all_array_ptrs[i].as_ptr(),
                chunk_count: all_schema_ptrs[i].len(),
            });
        }

        let mut name_views: Vec<ffi::velr_strview> = Vec::with_capacity(col_names.len());
        for name in &col_names {
            let b = name.as_bytes();
            name_views.push(ffi::velr_strview {
                ptr: b.as_ptr(),
                len: b.len(),
            });
        }

        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = f(
            logical_c.as_ptr(),
            cols_desc.as_ptr(),
            name_views.as_ptr(),
            col_names.len(),
            &mut err,
        );
        super::rc_to_result(rc, err)
    }
}
