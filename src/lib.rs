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
//!    - ❌ You may **not share** a single connection across threads concurrently.
//!      Example: `Arc<Velr>` won't compile.
//!
//! 2) In-flight / borrowing objects are **`!Send` + `!Sync`** (thread-affine):
//!    [`ExecTables`], [`TableResult`], [`RowIter`], [`VelrTx`], [`ExecTablesTx`], [`VelrSavepoint`].
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
    cell::Cell,
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

/// Convert a Velr return code plus optional error string into [`Result<()>`].
///
/// On success, frees `err` if it is unexpectedly non-null. On failure, converts `err` into an
/// [`Error`] and frees it via the runtime.
fn rc_to_result(rc: ffi::velr_code, err: *mut c_char) -> Result<()> {
    let code = rc as i32;
    if code == ffi::velr_code::VELR_OK as i32 {
        // usually err is null on OK, but be defensive
        if !err.is_null() {
            if let Ok(a) = velr_api() {
                unsafe { (a.velr_string_free)(err) };
            }
        }
        Ok(())
    } else {
        let msg = unsafe { take_err(err) };
        Err(Error::new(code, msg))
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

            let bytes = unsafe { std::slice::from_raw_parts(p, len) };
            let s = std::str::from_utf8(bytes).map_err(|_| {
                Error::new(ffi::velr_code::VELR_EUTF as i32, "column name not utf-8")
            })?;
            names.push(s.to_string());
        }

        Ok(Self {
            table,
            col_names: names,
            col_count,
            _nosend: PhantomData,
        })
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

        if ptr.is_null() {
            return Ok(Vec::new());
        }

        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
        unsafe { (a.velr_free)(ptr, len) };
        Ok(bytes)
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
            return Ok(false);
        }
        if rc < 0 {
            let msg = unsafe { take_err(err) };
            return Err(Error::new(rc, msg));
        }

        let mut scratch: Vec<CellRef<'_>> = Vec::with_capacity(written);
        for c in self.buf.iter().take(written) {
            let cell = match c.ty {
                ffi::velr_cell_type::VELR_NULL => CellRef::Null,
                ffi::velr_cell_type::VELR_BOOL => CellRef::Bool(c.i64_ != 0),
                ffi::velr_cell_type::VELR_INT64 => CellRef::Integer(c.i64_),
                ffi::velr_cell_type::VELR_DOUBLE => CellRef::Float(c.f64_),
                ffi::velr_cell_type::VELR_TEXT => {
                    let b = unsafe { std::slice::from_raw_parts(c.ptr, c.len) };
                    CellRef::Text(b)
                }
                ffi::velr_cell_type::VELR_JSON => {
                    let b = unsafe { std::slice::from_raw_parts(c.ptr, c.len) };
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
    _db: PhantomData<&'db Velr>,
    _nosend: PhantomData<Rc<()>>, // !Send + !Sync
}

impl<'db> VelrTx<'db> {
    fn ptr(&self) -> Result<NonNull<ffi::velr_tx>> {
        self.tx
            .ok_or_else(|| Error::new(ffi::velr_code::VELR_ESTATE as i32, "tx already consumed"))
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

    /// Commit this transaction.
    ///
    /// Consumes the transaction handle. After this call, the transaction is finalized and cannot be
    /// used again.
    ///
    /// Note: the underlying C ABI consumes the transaction handle even if an error is returned.
    pub fn commit(mut self) -> Result<()> {
        let a = velr_api()?;

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

        let tx = self
            .tx
            .take()
            .ok_or_else(|| Error::new(ffi::velr_code::VELR_ESTATE as i32, "tx already consumed"))?;

        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { (a.velr_tx_rollback)(tx.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }

    /// Create an unnamed savepoint inside this transaction.
    ///
    /// The savepoint handle can be released ([`VelrSavepoint::release`]) or rolled back to
    /// ([`VelrSavepoint::rollback`]).
    ///
    /// If the savepoint is dropped without explicit release/rollback, the runtime rolls back to the
    /// savepoint and releases it.
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

    /// Create a named savepoint inside this transaction.
    ///
    /// `name` must not contain interior NUL bytes (it is passed to the runtime as a C string).
    ///
    /// If the savepoint is dropped without explicit release/rollback, the runtime rolls back to the
    /// savepoint and releases it.
    pub fn savepoint_named<'tx>(&'tx self, name: &str) -> Result<VelrSavepoint<'tx>> {
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

        let nn = NonNull::new(out_sp).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_EERR as i32,
                "savepoint_named returned null",
            )
        })?;

        Ok(VelrSavepoint {
            sp: Some(nn),
            _tx: PhantomData,
            _nosend: PhantomData,
        })
    }

    /// Roll back to a named savepoint and release it.
    ///
    /// `name` must not contain interior NUL bytes (it is passed to the runtime as a C string).
    pub fn rollback_to(&self, name: &str) -> Result<()> {
        let a = velr_api()?;

        let tx = self.ptr()?;
        let cname = CString::new(name).map_err(|_| {
            Error::new(
                ffi::velr_code::VELR_EUTF as i32,
                "savepoint name contains NUL",
            )
        })?;

        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { (a.velr_tx_rollback_to)(tx.as_ptr(), cname.as_ptr(), &mut err) };
        rc_to_result(rc, err)
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

/// A savepoint handle within a transaction (thread-affine).
///
/// Use:
/// - [`VelrSavepoint::release`] to release the savepoint
/// - [`VelrSavepoint::rollback`] to roll back to the savepoint
///
/// ## Drop behavior
///
/// If dropped without explicit release/rollback, the runtime rolls back to the savepoint and
/// releases it.
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
