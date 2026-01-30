#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
use libloading::{Library, Symbol};
use std::os::raw::{c_char, c_int};
use crate::sys::*;
#[cfg(feature = "arrow-ipc")]
use arrow2::ffi::{ArrowArray, ArrowSchema};
pub struct Api {
    pub velr_string_free: unsafe extern "C" fn(*mut c_char),
    pub velr_open: unsafe extern "C" fn(
        *const c_char,
        *mut *mut velr_db,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_close: unsafe extern "C" fn(*mut velr_db),
    pub velr_exec_start: unsafe extern "C" fn(
        *mut velr_db,
        *const c_char,
        *mut *mut velr_stream,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_exec_close: unsafe extern "C" fn(*mut velr_stream),
    pub velr_stream_next_table: unsafe extern "C" fn(
        *mut velr_stream,
        *mut *mut velr_table,
        *mut c_int,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_exec_one: unsafe extern "C" fn(
        *mut velr_db,
        *const c_char,
        *mut *mut velr_table,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_table_close: unsafe extern "C" fn(*mut velr_table),
    pub velr_table_column_count: unsafe extern "C" fn(*mut velr_table) -> usize,
    pub velr_table_column_name: unsafe extern "C" fn(
        *mut velr_table,
        usize,
        *mut *const u8,
        *mut usize,
    ) -> velr_code,
    pub velr_table_rows_open: unsafe extern "C" fn(
        *mut velr_table,
        *mut *mut velr_rows,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_rows_close: unsafe extern "C" fn(*mut velr_rows),
    pub velr_rows_next: unsafe extern "C" fn(
        *mut velr_rows,
        *mut velr_cell,
        usize,
        *mut usize,
        *mut *mut c_char,
    ) -> c_int,
    pub velr_tx_begin: unsafe extern "C" fn(
        *mut velr_db,
        *mut *mut velr_tx,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_tx_commit: unsafe extern "C" fn(
        *mut velr_tx,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_tx_rollback: unsafe extern "C" fn(
        *mut velr_tx,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_tx_close: unsafe extern "C" fn(*mut velr_tx),
    pub velr_tx_exec_start: unsafe extern "C" fn(
        *mut velr_tx,
        *const c_char,
        *mut *mut velr_stream_tx,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_exec_tx_close: unsafe extern "C" fn(*mut velr_stream_tx),
    pub velr_stream_tx_next_table: unsafe extern "C" fn(
        *mut velr_stream_tx,
        *mut *mut velr_table,
        *mut c_int,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_tx_savepoint: unsafe extern "C" fn(
        *mut velr_tx,
        *mut *mut velr_sp,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_sp_release: unsafe extern "C" fn(
        *mut velr_sp,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_sp_rollback: unsafe extern "C" fn(
        *mut velr_sp,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_sp_close: unsafe extern "C" fn(*mut velr_sp),
    pub velr_tx_savepoint_named: unsafe extern "C" fn(
        *mut velr_tx,
        *const c_char,
        *mut *mut velr_sp,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_tx_rollback_to: unsafe extern "C" fn(
        *mut velr_tx,
        *const c_char,
        *mut *mut c_char,
    ) -> velr_code,
    #[cfg(feature = "arrow-ipc")]
    pub velr_table_ipc_file_len: unsafe extern "C" fn(
        *mut velr_table,
        *mut usize,
        *mut *mut c_char,
    ) -> velr_code,
    #[cfg(feature = "arrow-ipc")]
    pub velr_table_ipc_file_write: unsafe extern "C" fn(
        *mut velr_table,
        *mut u8,
        usize,
        *mut usize,
        *mut *mut c_char,
    ) -> velr_code,
    pub velr_free: unsafe extern "C" fn(*mut u8, usize),
    #[cfg(feature = "arrow-ipc")]
    pub velr_table_ipc_file_malloc: unsafe extern "C" fn(
        *mut velr_table,
        *mut *mut u8,
        *mut usize,
        *mut *mut c_char,
    ) -> velr_code,
    #[cfg(feature = "arrow-ipc")]
    pub velr_bind_arrow: unsafe extern "C" fn(
        *mut velr_db,
        *const c_char,
        *const *const ArrowSchema,
        *const *const ArrowArray,
        *const velr_strview,
        usize,
        *mut *mut c_char,
    ) -> velr_code,
    #[cfg(feature = "arrow-ipc")]
    pub velr_tx_bind_arrow: unsafe extern "C" fn(
        *mut velr_tx,
        *const c_char,
        *const *const ArrowSchema,
        *const *const ArrowArray,
        *const velr_strview,
        usize,
        *mut *mut c_char,
    ) -> velr_code,
    #[cfg(feature = "arrow-ipc")]
    pub velr_bind_arrow_chunks: unsafe extern "C" fn(
        *mut velr_db,
        *const c_char,
        *const velr_arrow_chunks,
        *const velr_strview,
        usize,
        *mut *mut c_char,
    ) -> velr_code,
    #[cfg(feature = "arrow-ipc")]
    pub velr_tx_bind_arrow_chunks: unsafe extern "C" fn(
        *mut velr_tx,
        *const c_char,
        *const velr_arrow_chunks,
        *const velr_strview,
        usize,
        *mut *mut c_char,
    ) -> velr_code,
}
impl Api {
    pub unsafe fn load(lib: &Library) -> Result<Self, libloading::Error> {
        unsafe fn get<T: Copy>(
            lib: &Library,
            name: &'static [u8],
        ) -> Result<T, libloading::Error> {
            let sym: Symbol<T> = lib.get::<T>(name)?;
            Ok(*sym)
        }
        Ok(Self {
            velr_string_free: get(
                lib,
                concat!(stringify!(velr_string_free), "\0").as_bytes(),
            )?,
            velr_open: get(lib, concat!(stringify!(velr_open), "\0").as_bytes())?,
            velr_close: get(lib, concat!(stringify!(velr_close), "\0").as_bytes())?,
            velr_exec_start: get(
                lib,
                concat!(stringify!(velr_exec_start), "\0").as_bytes(),
            )?,
            velr_exec_close: get(
                lib,
                concat!(stringify!(velr_exec_close), "\0").as_bytes(),
            )?,
            velr_stream_next_table: get(
                lib,
                concat!(stringify!(velr_stream_next_table), "\0").as_bytes(),
            )?,
            velr_exec_one: get(
                lib,
                concat!(stringify!(velr_exec_one), "\0").as_bytes(),
            )?,
            velr_table_close: get(
                lib,
                concat!(stringify!(velr_table_close), "\0").as_bytes(),
            )?,
            velr_table_column_count: get(
                lib,
                concat!(stringify!(velr_table_column_count), "\0").as_bytes(),
            )?,
            velr_table_column_name: get(
                lib,
                concat!(stringify!(velr_table_column_name), "\0").as_bytes(),
            )?,
            velr_table_rows_open: get(
                lib,
                concat!(stringify!(velr_table_rows_open), "\0").as_bytes(),
            )?,
            velr_rows_close: get(
                lib,
                concat!(stringify!(velr_rows_close), "\0").as_bytes(),
            )?,
            velr_rows_next: get(
                lib,
                concat!(stringify!(velr_rows_next), "\0").as_bytes(),
            )?,
            velr_tx_begin: get(
                lib,
                concat!(stringify!(velr_tx_begin), "\0").as_bytes(),
            )?,
            velr_tx_commit: get(
                lib,
                concat!(stringify!(velr_tx_commit), "\0").as_bytes(),
            )?,
            velr_tx_rollback: get(
                lib,
                concat!(stringify!(velr_tx_rollback), "\0").as_bytes(),
            )?,
            velr_tx_close: get(
                lib,
                concat!(stringify!(velr_tx_close), "\0").as_bytes(),
            )?,
            velr_tx_exec_start: get(
                lib,
                concat!(stringify!(velr_tx_exec_start), "\0").as_bytes(),
            )?,
            velr_exec_tx_close: get(
                lib,
                concat!(stringify!(velr_exec_tx_close), "\0").as_bytes(),
            )?,
            velr_stream_tx_next_table: get(
                lib,
                concat!(stringify!(velr_stream_tx_next_table), "\0").as_bytes(),
            )?,
            velr_tx_savepoint: get(
                lib,
                concat!(stringify!(velr_tx_savepoint), "\0").as_bytes(),
            )?,
            velr_sp_release: get(
                lib,
                concat!(stringify!(velr_sp_release), "\0").as_bytes(),
            )?,
            velr_sp_rollback: get(
                lib,
                concat!(stringify!(velr_sp_rollback), "\0").as_bytes(),
            )?,
            velr_sp_close: get(
                lib,
                concat!(stringify!(velr_sp_close), "\0").as_bytes(),
            )?,
            velr_tx_savepoint_named: get(
                lib,
                concat!(stringify!(velr_tx_savepoint_named), "\0").as_bytes(),
            )?,
            velr_tx_rollback_to: get(
                lib,
                concat!(stringify!(velr_tx_rollback_to), "\0").as_bytes(),
            )?,
            #[cfg(feature = "arrow-ipc")]
            velr_table_ipc_file_len: get(
                lib,
                concat!(stringify!(velr_table_ipc_file_len), "\0").as_bytes(),
            )?,
            #[cfg(feature = "arrow-ipc")]
            velr_table_ipc_file_write: get(
                lib,
                concat!(stringify!(velr_table_ipc_file_write), "\0").as_bytes(),
            )?,
            velr_free: get(lib, concat!(stringify!(velr_free), "\0").as_bytes())?,
            #[cfg(feature = "arrow-ipc")]
            velr_table_ipc_file_malloc: get(
                lib,
                concat!(stringify!(velr_table_ipc_file_malloc), "\0").as_bytes(),
            )?,
            #[cfg(feature = "arrow-ipc")]
            velr_bind_arrow: get(
                lib,
                concat!(stringify!(velr_bind_arrow), "\0").as_bytes(),
            )?,
            #[cfg(feature = "arrow-ipc")]
            velr_tx_bind_arrow: get(
                lib,
                concat!(stringify!(velr_tx_bind_arrow), "\0").as_bytes(),
            )?,
            #[cfg(feature = "arrow-ipc")]
            velr_bind_arrow_chunks: get(
                lib,
                concat!(stringify!(velr_bind_arrow_chunks), "\0").as_bytes(),
            )?,
            #[cfg(feature = "arrow-ipc")]
            velr_tx_bind_arrow_chunks: get(
                lib,
                concat!(stringify!(velr_tx_bind_arrow_chunks), "\0").as_bytes(),
            )?,
        })
    }
}
