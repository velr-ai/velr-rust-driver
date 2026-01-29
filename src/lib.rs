#![allow(unsafe_code)]

mod sys; // <-- your generated sys.rs
use std::{
    ffi::{CStr, CString},
    fmt,
    marker::PhantomData,
    os::raw::c_char,
    ptr::NonNull,
    rc::Rc,
};
use sys as ffi;

pub type Result<T> = std::result::Result<T, Error>;

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

unsafe fn take_err(p: *mut c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    let s = CStr::from_ptr(p).to_string_lossy().into_owned();
    ffi::velr_string_free(p);
    s
}

fn rc_to_result(rc: ffi::velr_code, err: *mut c_char) -> Result<()> {
    let code = rc as i32;
    if code == ffi::velr_code::VELR_OK as i32 {
        // usually err is null on OK, but be defensive
        if !err.is_null() {
            unsafe { ffi::velr_string_free(err) };
        }
        Ok(())
    } else {
        let msg = unsafe { take_err(err) };
        Err(Error::new(code, msg))
    }
}

// -------------------------- CellRef --------------------------

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
    pub fn as_str_utf8(&self) -> Option<std::result::Result<&'a str, std::str::Utf8Error>> {
        match self {
            CellRef::Text(b) => Some(std::str::from_utf8(b)),
            _ => None,
        }
    }
}

// -------------------------- Velr --------------------------

pub struct Velr {
    db: NonNull<ffi::velr_db>,
    // make !Send + !Sync like rusqlite::Connection
    _nosend: PhantomData<Rc<()>>,
}

impl Velr {
    pub fn open(path: Option<&str>) -> Result<Self> {
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

        let rc = unsafe { ffi::velr_open(path_ptr, &mut out_db, &mut err) };
        rc_to_result(rc, err)?;
        let nn = NonNull::new(out_db).ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_EERR as i32,
                "velr_open returned null db",
            )
        })?;
        Ok(Self {
            db: nn,
            _nosend: PhantomData,
        })
    }

    pub fn exec<'db>(&'db self, cypher: &str) -> Result<ExecTables<'db>> {
        let cy = CString::new(cypher)
            .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "cypher contains NUL"))?;

        let mut out_stream: *mut ffi::velr_stream = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            ffi::velr_exec_start(self.db.as_ptr(), cy.as_ptr(), &mut out_stream, &mut err)
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

    pub fn exec_one(&self, cypher: &str) -> Result<TableResult> {
        let cy = CString::new(cypher)
            .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "cypher contains NUL"))?;
        let mut out_table: *mut ffi::velr_table = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc =
            unsafe { ffi::velr_exec_one(self.db.as_ptr(), cy.as_ptr(), &mut out_table, &mut err) };
        rc_to_result(rc, err)?;
        TableResult::from_raw(out_table)
    }

    pub fn run(&self, cypher: &str) -> Result<()> {
        let mut st = self.exec(cypher)?;
        while let Some(mut t) = st.next_table()? {
            t.for_each_row(|_| Ok(()))?;
        }
        Ok(())
    }

    pub fn begin_tx(&self) -> Result<VelrTx<'_>> {
        let mut out_tx: *mut ffi::velr_tx = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { ffi::velr_tx_begin(self.db.as_ptr(), &mut out_tx, &mut err) };
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

    #[cfg(feature = "arrow-ipc")]
    pub fn bind_arrow(
        &self,
        logical: &str,
        col_names: Vec<String>,
        arrays: Vec<Box<dyn arrow2::array::Array>>,
    ) -> Result<()> {
        arrow_bind::bind_arrow_db(self.db.as_ptr(), logical, col_names, arrays)
    }

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
    fn drop(&mut self) {
        unsafe { ffi::velr_close(self.db.as_ptr()) };
    }
}

// -------------------------- ExecTables --------------------------

pub struct ExecTables<'db> {
    stream: Option<NonNull<ffi::velr_stream>>,
    _db: PhantomData<&'db Velr>,
    _nosend: PhantomData<Rc<()>>,
}

impl<'db> ExecTables<'db> {
    pub fn next_table(&mut self) -> Result<Option<TableResult>> {
        let Some(stream) = self.stream else {
            return Ok(None);
        };

        let mut out_table: *mut ffi::velr_table = std::ptr::null_mut();
        let mut has: i32 = 0;
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            ffi::velr_stream_next_table(stream.as_ptr(), &mut out_table, &mut has, &mut err)
        };
        rc_to_result(rc, err)?;

        if has == 0 {
            // auto-close stream at EOF to match “resource finished” semantics
            unsafe { ffi::velr_exec_close(stream.as_ptr()) };
            self.stream = None;
            return Ok(None);
        }

        TableResult::from_raw(out_table).map(Some)
    }
}

impl Drop for ExecTables<'_> {
    fn drop(&mut self) {
        if let Some(st) = self.stream.take() {
            unsafe { ffi::velr_exec_close(st.as_ptr()) };
        }
    }
}

// -------------------------- TableResult --------------------------

pub struct TableResult {
    table: NonNull<ffi::velr_table>,
    col_names: Vec<String>,
    col_count: usize,
    _nosend: PhantomData<Rc<()>>,
}

impl TableResult {
    fn from_raw(ptr: *mut ffi::velr_table) -> Result<Self> {
        let table = NonNull::new(ptr)
            .ok_or_else(|| Error::new(ffi::velr_code::VELR_EERR as i32, "null table"))?;
        let col_count = unsafe { ffi::velr_table_column_count(table.as_ptr()) };

        let mut names = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let mut p: *const u8 = std::ptr::null();
            let mut len: usize = 0;
            let rc = unsafe { ffi::velr_table_column_name(table.as_ptr(), i, &mut p, &mut len) };

            // column_name doesn't provide out_err; treat non-OK as argument/state error
            if rc as i32 != ffi::velr_code::VELR_OK as i32 {
                return Err(Error::new(
                    rc as i32,
                    format!("velr_table_column_name failed at idx={i}"),
                ));
            }

            let bytes = unsafe { std::slice::from_raw_parts(p, len) };
            let s = std::str::from_utf8(bytes)
                .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "column name not utf-8"))?
                .to_string();
            names.push(s);
        }

        Ok(Self {
            table,
            col_names: names,
            col_count,
            _nosend: PhantomData,
        })
    }

    pub fn column_names(&self) -> &[String] {
        &self.col_names
    }

    pub fn column_count(&self) -> usize {
        self.col_count
    }

    pub fn rows<'t>(&'t mut self) -> Result<RowIter<'t>> {
        let mut out_rows: *mut ffi::velr_rows = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { ffi::velr_table_rows_open(self.table.as_ptr(), &mut out_rows, &mut err) };
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

    pub fn for_each_row<F>(&mut self, mut on_row: F) -> Result<()>
    where
        F: for<'row> FnMut(&[CellRef<'row>]) -> Result<()>,
    {
        let mut it = self.rows()?;
        while it.next(|cells| on_row(cells))? {}
        Ok(())
    }

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

    #[cfg(feature = "arrow-ipc")]
    pub fn to_arrow_ipc_file(&mut self) -> Result<Vec<u8>> {
        // Use malloc + velr_free path to keep FFI simple
        let mut ptr: *mut u8 = std::ptr::null_mut();
        let mut len: usize = 0;
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            ffi::velr_table_ipc_file_malloc(self.table.as_ptr(), &mut ptr, &mut len, &mut err)
        };
        rc_to_result(rc, err)?;

        if ptr.is_null() {
            return Ok(Vec::new());
        }

        let bytes = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
        unsafe { ffi::velr_free(ptr, len) };
        Ok(bytes)
    }
}

impl Drop for TableResult {
    fn drop(&mut self) {
        unsafe { ffi::velr_table_close(self.table.as_ptr()) };
    }
}

// -------------------------- RowIter --------------------------

pub struct RowIter<'t> {
    rows: Option<NonNull<ffi::velr_rows>>,
    col_count: usize,
    buf: Vec<ffi::velr_cell>,
    _table: PhantomData<&'t mut TableResult>,
    _nosend: PhantomData<Rc<()>>,
}

impl<'t> RowIter<'t> {
    pub fn next<F>(&mut self, on_row: F) -> Result<bool>
    where
        F: for<'row> FnOnce(&[CellRef<'row>]) -> Result<()>,
    {
        let Some(rows) = self.rows else {
            return Ok(false);
        };

        let mut written: usize = 0;
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            ffi::velr_rows_next(
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

        // Build borrowed CellRef slice for this callback only.
        // IMPORTANT: TEXT/JSON pointers remain valid until the next velr_rows_next call.
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
            unsafe { ffi::velr_rows_close(r.as_ptr()) };
        }
    }
}

// -------------------------- Transactions --------------------------

pub struct VelrTx<'db> {
    tx: Option<NonNull<ffi::velr_tx>>,
    _db: PhantomData<&'db Velr>,
    _nosend: PhantomData<Rc<()>>,
}

impl<'db> VelrTx<'db> {
    fn ptr(&self) -> Result<NonNull<ffi::velr_tx>> {
        self.tx
            .ok_or_else(|| Error::new(ffi::velr_code::VELR_ESTATE as i32, "tx already consumed"))
    }

    pub fn exec<'tx>(&'tx self, cypher: &str) -> Result<ExecTablesTx<'tx>> {
        let tx = self.ptr()?;
        let cy = CString::new(cypher)
            .map_err(|_| Error::new(ffi::velr_code::VELR_EUTF as i32, "cypher contains NUL"))?;

        let mut out_stream: *mut ffi::velr_stream_tx = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc =
            unsafe { ffi::velr_tx_exec_start(tx.as_ptr(), cy.as_ptr(), &mut out_stream, &mut err) };
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

    pub fn exec_one(&self, cypher: &str) -> Result<TableResult> {
        // FFI doesn’t expose velr_tx_exec_one; implement via stream like your Rust driver.
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

    pub fn run(&self, cypher: &str) -> Result<()> {
        let mut st = self.exec(cypher)?;
        while let Some(mut t) = st.next_table()? {
            t.for_each_row(|_| Ok(()))?;
        }
        Ok(())
    }

    pub fn commit(mut self) -> Result<()> {
        let tx = self
            .tx
            .take()
            .ok_or_else(|| Error::new(ffi::velr_code::VELR_ESTATE as i32, "tx already consumed"))?;
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { ffi::velr_tx_commit(tx.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }

    pub fn rollback(mut self) -> Result<()> {
        let tx = self
            .tx
            .take()
            .ok_or_else(|| Error::new(ffi::velr_code::VELR_ESTATE as i32, "tx already consumed"))?;
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { ffi::velr_tx_rollback(tx.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }

    pub fn savepoint<'tx>(&'tx self) -> Result<VelrSavepoint<'tx>> {
        let tx = self.ptr()?;
        let mut out_sp: *mut ffi::velr_sp = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { ffi::velr_tx_savepoint(tx.as_ptr(), &mut out_sp, &mut err) };
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

    pub fn savepoint_named<'tx>(&'tx self, name: &str) -> Result<VelrSavepoint<'tx>> {
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
            ffi::velr_tx_savepoint_named(tx.as_ptr(), cname.as_ptr(), &mut out_sp, &mut err)
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

    pub fn rollback_to(&self, name: &str) -> Result<()> {
        let tx = self.ptr()?;
        let cname = CString::new(name).map_err(|_| {
            Error::new(
                ffi::velr_code::VELR_EUTF as i32,
                "savepoint name contains NUL",
            )
        })?;
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { ffi::velr_tx_rollback_to(tx.as_ptr(), cname.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }

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
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            unsafe { ffi::velr_tx_close(tx.as_ptr()) };
        }
    }
}

// -------------------------- ExecTablesTx --------------------------

pub struct ExecTablesTx<'tx> {
    stream: Option<NonNull<ffi::velr_stream_tx>>,
    _tx: PhantomData<&'tx VelrTx<'tx>>,
    _nosend: PhantomData<Rc<()>>,
}

impl ExecTablesTx<'_> {
    pub fn next_table(&mut self) -> Result<Option<TableResult>> {
        let Some(stream) = self.stream else {
            return Ok(None);
        };

        let mut out_table: *mut ffi::velr_table = std::ptr::null_mut();
        let mut has: i32 = 0;
        let mut err: *mut c_char = std::ptr::null_mut();

        let rc = unsafe {
            ffi::velr_stream_tx_next_table(stream.as_ptr(), &mut out_table, &mut has, &mut err)
        };
        rc_to_result(rc, err)?;

        if has == 0 {
            unsafe { ffi::velr_exec_tx_close(stream.as_ptr()) };
            self.stream = None;
            return Ok(None);
        }

        TableResult::from_raw(out_table).map(Some)
    }
}

impl Drop for ExecTablesTx<'_> {
    fn drop(&mut self) {
        if let Some(st) = self.stream.take() {
            unsafe { ffi::velr_exec_tx_close(st.as_ptr()) };
        }
    }
}

// -------------------------- Savepoints --------------------------

pub struct VelrSavepoint<'tx> {
    sp: Option<NonNull<ffi::velr_sp>>,
    _tx: PhantomData<&'tx VelrTx<'tx>>,
    _nosend: PhantomData<Rc<()>>,
}

impl VelrSavepoint<'_> {
    pub fn release(mut self) -> Result<()> {
        let sp = self.sp.take().ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_ESTATE as i32,
                "savepoint already consumed",
            )
        })?;
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { ffi::velr_sp_release(sp.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }

    pub fn rollback(mut self) -> Result<()> {
        let sp = self.sp.take().ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_ESTATE as i32,
                "savepoint already consumed",
            )
        })?;
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { ffi::velr_sp_rollback(sp.as_ptr(), &mut err) };
        rc_to_result(rc, err)
    }
}

impl Drop for VelrSavepoint<'_> {
    fn drop(&mut self) {
        if let Some(sp) = self.sp.take() {
            unsafe { ffi::velr_sp_close(sp.as_ptr()) };
        }
    }
}

// -------------------------- Arrow binding helpers --------------------------

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
        bind_arrow_common(
            |logical_ptr, schemas_pp, arrays_pp, names_ptr, n, err| unsafe {
                ffi::velr_bind_arrow(db, logical_ptr, schemas_pp, arrays_pp, names_ptr, n, err)
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
        bind_arrow_common(
            |logical_ptr, schemas_pp, arrays_pp, names_ptr, n, err| unsafe {
                ffi::velr_tx_bind_arrow(tx, logical_ptr, schemas_pp, arrays_pp, names_ptr, n, err)
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

        // Export schemas (by ref) + arrays (ownership transferred via ptr::read in FFI)
        let mut schemas: Vec<ArrowSchema> = Vec::with_capacity(col_names.len());
        let mut array_cs: Vec<ManuallyDrop<ArrowArray>> = Vec::with_capacity(col_names.len());
        let mut schema_ptrs: Vec<*const ArrowSchema> = Vec::with_capacity(col_names.len());
        let mut array_ptrs: Vec<*const ArrowArray> = Vec::with_capacity(col_names.len());
        let mut name_views: Vec<ffi::velr_strview> = Vec::with_capacity(col_names.len());

        for (name, arr) in col_names.iter().zip(arrays.into_iter()) {
            let field = Field::new(name.clone(), arr.data_type().clone(), true);
            let schema = export_field_to_c(&field);
            schemas.push(schema);

            // export_array_to_c consumes the Box<dyn Array>
            let a = ManuallyDrop::new(export_array_to_c(arr));
            array_cs.push(a);
        }

        // pointers must be built after vectors are finalized
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
        bind_chunks_common(
            |logical_ptr, cols_ptr, names_ptr, n, err| unsafe {
                ffi::velr_bind_arrow_chunks(db, logical_ptr, cols_ptr, names_ptr, n, err)
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
        bind_chunks_common(
            |logical_ptr, cols_ptr, names_ptr, n, err| unsafe {
                ffi::velr_tx_bind_arrow_chunks(tx, logical_ptr, cols_ptr, names_ptr, n, err)
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

        // For each column:
        // - export per-chunk schema+array
        // - store stable pointer arrays
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

        // Build velr_arrow_chunks array referencing the per-column pointer vectors
        let mut cols_desc: Vec<ffi::velr_arrow_chunks> = Vec::with_capacity(col_names.len());
        for i in 0..col_names.len() {
            cols_desc.push(ffi::velr_arrow_chunks {
                schemas: all_schema_ptrs[i].as_ptr(),
                arrays: all_array_ptrs[i].as_ptr(),
                chunk_count: all_schema_ptrs[i].len(),
            });
        }

        // Column-name views
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
