// GENERATED FILE - DO NOT EDIT BY HAND.
// Source of truth: rust/velr-ffi/src/ffi_c.rs
// Edit the FFI source, then regenerate with: cargo run -p xtask -- gen-sys

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
use std::os::raw::{c_char, c_int, c_void};
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
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct velr_query_options {
    pub has_max_result_rows: c_int,
    pub max_result_rows: usize,
    pub params: *const velr_query_params,
}
/// Borrowed byte slice view (not NUL-terminated).
///
/// Used for passing UTF-8 names/strings without allocating C strings, and for returning borrowed
/// string data from some ABI calls.
#[allow(dead_code)]
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct velr_strview {
    pub ptr: *const u8,
    pub len: usize,
}
/// Migration status returned by [`velr_migrate`].
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum velr_migration_status {
    VELR_MIGRATION_ALREADY_CURRENT = 0,
    VELR_MIGRATION_MIGRATED = 1,
}
/// Migration report returned by [`velr_migrate`].
///
/// `steps` is a NUL-terminated, comma-separated UTF-8 string allocated by Velr, or NULL when no
/// steps were applied. Free it with [`velr_migration_report_clear`] or [`velr_string_free`].
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct velr_migration_report {
    pub from_version: i32,
    pub to_version: i32,
    pub status: velr_migration_status,
    pub step_count: usize,
    pub steps: *mut c_char,
}
/// Why Velr is requesting vectors from a registered embedder.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum velr_vector_embedding_purpose {
    VELR_VECTOR_EMBEDDING_INDEX_ENTITY = 0,
    VELR_VECTOR_EMBEDDING_QUERY = 1,
}
/// Graph entity kind for vector embedding inputs.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum velr_vector_entity_kind {
    VELR_VECTOR_ENTITY_NONE = 0,
    VELR_VECTOR_ENTITY_NODE = 1,
    VELR_VECTOR_ENTITY_RELATIONSHIP = 2,
}
/// Public Velr property value kind.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum velr_property_value_type {
    VELR_PROPERTY_NULL = 0,
    VELR_PROPERTY_BOOL = 1,
    VELR_PROPERTY_INT64 = 2,
    VELR_PROPERTY_DOUBLE = 3,
    VELR_PROPERTY_STRING = 4,
    VELR_PROPERTY_DATE = 5,
    VELR_PROPERTY_LOCAL_TIME = 6,
    VELR_PROPERTY_ZONED_TIME = 7,
    VELR_PROPERTY_LOCAL_DATETIME = 8,
    VELR_PROPERTY_ZONED_DATETIME = 9,
    VELR_PROPERTY_DURATION = 10,
    VELR_PROPERTY_POINT = 11,
    VELR_PROPERTY_GEOMETRY = 12,
    VELR_PROPERTY_GEOGRAPHY = 13,
    VELR_PROPERTY_LIST = 14,
    VELR_PROPERTY_VECTOR = 15,
    VELR_PROPERTY_BYTES = 16,
}
/// SQLite-compatible storage kind for exact property reconstruction.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum velr_storage_value_type {
    VELR_STORAGE_NULL = 0,
    VELR_STORAGE_INT64 = 1,
    VELR_STORAGE_DOUBLE = 2,
    VELR_STORAGE_TEXT = 3,
    VELR_STORAGE_BLOB = 4,
}
/// One source field passed to a vector embedding callback.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct velr_vector_embedding_field {
    pub has_name: c_int,
    pub name: velr_strview,
    pub value_type: velr_property_value_type,
    pub storage_type: velr_storage_value_type,
    pub i64_: i64,
    pub f64_: f64,
    /// TEXT or BLOB storage payload. For BLOB values this is the canonical Velr encoded blob.
    pub bytes: velr_strview,
    /// Canonical JSON rendering of the Velr property value.
    pub json: velr_strview,
    /// Display rendering of the Velr property value.
    pub display: velr_strview,
}
/// One source row passed to a vector embedding callback.
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct velr_vector_embedding_input {
    pub index_name: velr_strview,
    pub dimensions: usize,
    pub purpose: velr_vector_embedding_purpose,
    pub entity_kind: velr_vector_entity_kind,
    pub has_entity_id: c_int,
    pub entity_id: i64,
    pub fields: *const velr_vector_embedding_field,
    pub field_count: usize,
}
#[allow(non_camel_case_types)]
pub type velr_vector_embedder_callback = unsafe extern "C" fn(
    user_data: *mut c_void,
    inputs: *const velr_vector_embedding_input,
    input_count: usize,
    dimensions: usize,
    out_vectors: *mut f32,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> velr_code;
#[allow(non_camel_case_types)]
pub type velr_vector_embedder_free_callback = unsafe extern "C" fn(
    user_data: *mut c_void,
);
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
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct velr_explain_trace;
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct velr_explain_plan_meta {
    pub plan_id: velr_strview,
    pub cypher: velr_strview,
    pub step_count: usize,
}
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct velr_explain_step_meta {
    pub step_no: usize,
    pub group_id: velr_strview,
    pub op_index: velr_strview,
    pub phase: velr_strview,
    pub title: velr_strview,
    pub source: velr_strview,
    pub note: velr_strview,
    pub statement_count: usize,
}
#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub struct velr_explain_stmt_meta {
    pub stmt_id: velr_strview,
    pub kind: velr_strview,
    pub sql: velr_strview,
    pub note: velr_strview,
    pub sqlite_plan_count: usize,
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
pub struct velr_query_params {
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
