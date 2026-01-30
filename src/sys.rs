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
