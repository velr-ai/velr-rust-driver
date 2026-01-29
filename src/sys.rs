#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
use std::os::raw::{c_char, c_int};
#[cfg(feature = "arrow-ipc")]
use arrow2::ffi::{ArrowArray, ArrowSchema};
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum velr_cell_type {
    VELR_NULL = 0,
    VELR_BOOL = 1,
    VELR_INT64 = 2,
    VELR_DOUBLE = 3,
    VELR_TEXT = 4,
    VELR_JSON = 5,
}
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct velr_cell {
    pub ty: velr_cell_type,
    pub i64_: i64,
    pub f64_: f64,
    pub ptr: *const u8,
    pub len: usize,
}
#[allow(dead_code)]
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct velr_strview {
    pub ptr: *const u8,
    pub len: usize,
}
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum velr_code {
    VELR_OK = 0,
    VELR_EARG = -1,
    VELR_EUTF = -2,
    VELR_ESTATE = -3,
    VELR_EERR = -4,
}
#[cfg(feature = "arrow-ipc")]
#[repr(C)]
pub struct velr_arrow_chunks {
    pub schemas: *const *const ArrowSchema,
    pub arrays: *const *const ArrowArray,
    pub chunk_count: usize,
}
#[repr(C)]
pub struct velr_db {
    _private: [u8; 0],
}
#[repr(C)]
pub struct velr_stream {
    _private: [u8; 0],
}
#[repr(C)]
pub struct velr_table {
    _private: [u8; 0],
}
#[repr(C)]
pub struct velr_rows {
    _private: [u8; 0],
}
#[repr(C)]
pub struct velr_tx {
    _private: [u8; 0],
}
#[repr(C)]
pub struct velr_sp {
    _private: [u8; 0],
}
#[repr(C)]
pub struct velr_stream_tx {
    _private: [u8; 0],
}
unsafe extern "C" {
    pub fn velr_string_free(p: *mut c_char);
    pub fn velr_open(
        path: *const c_char,
        out_db: *mut *mut velr_db,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_close(db: *mut velr_db);
    pub fn velr_exec_start(
        db: *mut velr_db,
        cypher: *const c_char,
        out_stream: *mut *mut velr_stream,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_exec_close(stream: *mut velr_stream);
    pub fn velr_stream_next_table(
        stream: *mut velr_stream,
        out_table: *mut *mut velr_table,
        out_has: *mut c_int,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_exec_one(
        db: *mut velr_db,
        cypher: *const c_char,
        out_table: *mut *mut velr_table,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_table_close(table: *mut velr_table);
    pub fn velr_table_column_count(table: *mut velr_table) -> usize;
    pub fn velr_table_column_name(
        table: *mut velr_table,
        idx: usize,
        out_ptr: *mut *const u8,
        out_len: *mut usize,
    ) -> velr_code;
    pub fn velr_table_rows_open(
        table: *mut velr_table,
        out_rows: *mut *mut velr_rows,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_rows_close(rows: *mut velr_rows);
    pub fn velr_rows_next(
        rows: *mut velr_rows,
        buf: *mut velr_cell,
        buf_len: usize,
        out_written: *mut usize,
        out_err: *mut *mut c_char,
    ) -> c_int;
    pub fn velr_tx_begin(
        db: *mut velr_db,
        out_tx: *mut *mut velr_tx,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_tx_commit(tx: *mut velr_tx, out_err: *mut *mut c_char) -> velr_code;
    pub fn velr_tx_rollback(tx: *mut velr_tx, out_err: *mut *mut c_char) -> velr_code;
    pub fn velr_tx_close(tx: *mut velr_tx);
    pub fn velr_tx_exec_start(
        tx: *mut velr_tx,
        cypher: *const c_char,
        out_stream: *mut *mut velr_stream_tx,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_exec_tx_close(stream: *mut velr_stream_tx);
    pub fn velr_stream_tx_next_table(
        stream: *mut velr_stream_tx,
        out_table: *mut *mut velr_table,
        out_has: *mut c_int,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_tx_savepoint(
        tx: *mut velr_tx,
        out_sp: *mut *mut velr_sp,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_sp_release(sp: *mut velr_sp, out_err: *mut *mut c_char) -> velr_code;
    pub fn velr_sp_rollback(sp: *mut velr_sp, out_err: *mut *mut c_char) -> velr_code;
    pub fn velr_sp_close(sp: *mut velr_sp);
    pub fn velr_tx_savepoint_named(
        tx: *mut velr_tx,
        name: *const c_char,
        out_sp: *mut *mut velr_sp,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_tx_rollback_to(
        tx: *mut velr_tx,
        name: *const c_char,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    #[cfg(feature = "arrow-ipc")]
    pub fn velr_table_ipc_file_len(
        table: *mut velr_table,
        out_len: *mut usize,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    #[cfg(feature = "arrow-ipc")]
    pub fn velr_table_ipc_file_write(
        table: *mut velr_table,
        dst_ptr: *mut u8,
        dst_len: usize,
        out_written: *mut usize,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    pub fn velr_free(p: *mut u8, len: usize);
    #[cfg(feature = "arrow-ipc")]
    pub fn velr_table_ipc_file_malloc(
        table: *mut velr_table,
        out_ptr: *mut *mut u8,
        out_len: *mut usize,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    #[cfg(feature = "arrow-ipc")]
    pub fn velr_bind_arrow(
        db: *mut velr_db,
        logical: *const c_char,
        schemas: *const *const ArrowSchema,
        arrays: *const *const ArrowArray,
        colnames: *const velr_strview,
        col_count: usize,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    #[cfg(feature = "arrow-ipc")]
    pub fn velr_tx_bind_arrow(
        tx: *mut velr_tx,
        logical: *const c_char,
        schemas: *const *const ArrowSchema,
        arrays: *const *const ArrowArray,
        colnames: *const velr_strview,
        col_count: usize,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    #[cfg(feature = "arrow-ipc")]
    pub fn velr_bind_arrow_chunks(
        db: *mut velr_db,
        logical: *const c_char,
        cols: *const velr_arrow_chunks,
        colnames: *const velr_strview,
        col_count: usize,
        out_err: *mut *mut c_char,
    ) -> velr_code;
    #[cfg(feature = "arrow-ipc")]
    pub fn velr_tx_bind_arrow_chunks(
        tx: *mut velr_tx,
        logical: *const c_char,
        cols: *const velr_arrow_chunks,
        colnames: *const velr_strview,
        col_count: usize,
        out_err: *mut *mut c_char,
    ) -> velr_code;
}
