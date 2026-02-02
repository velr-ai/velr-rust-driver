#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
use std::os::raw::{c_char, c_int};
#[cfg(feature = "arrow-ipc")]
use arrow2::ffi::{ArrowArray, ArrowSchema};
/// Discriminant for [`velr_cell`].
///
/// Note: for `VELR_TEXT` and `VELR_JSON`, `ptr` is not NUL-terminated and `len` must be used.
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
/// A single cell in a row.
///
/// - For `VELR_BOOL`, `i64_` is `0` or `1`.
/// - For `VELR_INT64`, `i64_` holds the integer.
/// - For `VELR_DOUBLE`, `f64_` holds the double.
/// - For `VELR_TEXT` / `VELR_JSON`, `ptr` points to a byte slice of length `len`.
///
/// For non-`VELR_TEXT`/`VELR_JSON` cells, `ptr`/`len` are unspecified; ignore them.
///
/// `ptr` may be non-null even when `len == 0`; do not dereference when `len == 0`.
///
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
/// Borrowed byte slice view (not NUL-terminated).
///
/// Used for passing column names without allocating C strings.
#[allow(dead_code)]
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct velr_strview {
    pub ptr: *const u8,
    pub len: usize,
}
/// Result codes returned by most ABI functions.
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
/// Chunked Arrow column descriptor.
///
/// For each logical column:
/// - `schemas` points to an array of `chunk_count` `ArrowSchema*`
/// - `arrays` points to an array of `chunk_count` `ArrowArray*`
///
/// Each column must have `chunk_count > 0`.
///
/// (feature: `arrow-ipc`)
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
