//! Velr runtime loader and process-wide API singleton.
//!
//! This module is responsible for:
//! - Selecting the correct *bundled* Velr runtime dynamic library for the current target.
//! - Materializing that library to a stable on-disk cache location (to support dynamic loading).
//! - Dynamically loading the library and resolving ABI symbols into an [`Api`] handle.
//! - Exposing a process-wide singleton [`Runtime`] via [`runtime()`].
//!
//! ## Environment variables
//!
//! - `VELR_RUNTIME_PATH` *(optional)*: development escape hatch. If set, Velr will load the runtime
//!   dynamic library directly from this path instead of using the bundled runtime.
//! - `VELR_CACHE_DIR` *(optional)*: overrides the base directory used to cache the materialized
//!   runtime library.
//!
//! ## Caching behavior
//!
//! The bundled runtime bytes are written to a cache path whose filename includes a content hash.
//! This is important on platforms like Windows where loaded DLLs cannot be overwritten.
//!
//! ## Initialization semantics
//!
//! The runtime is initialized lazily on first use and stored in a global [`OnceLock`]. If
//! initialization fails, subsequent calls to [`runtime()`] will return the same error (cloned).

use std::{
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::OnceLock,
};

use blake3::Hasher;
use libloading::Library;

use crate::{api::Api, Error, Result};

/// Process-wide singleton storage for the runtime.
///
/// Note that this stores `Result<Runtime>` rather than `Runtime`, which means a failed initialization
/// is also cached and will be returned on subsequent calls to [`runtime()`].
static RUNTIME: OnceLock<Result<Runtime>> = OnceLock::new();

/// Loaded Velr runtime and resolved ABI API.
///
/// A [`Runtime`] instance owns the loaded dynamic library to ensure it remains alive for the
/// lifetime of the process. The [`Api`] is resolved from that library and is expected to remain
/// valid as long as the library remains loaded.
///
/// - `_lib` is intentionally kept to extend the lifetime of the loaded dynamic library.
/// - `api` provides access to the runtime's resolved ABI symbols.
/// - `path` indicates the filesystem location of the loaded runtime library (either cached bundled
///   bytes or a user-provided path via `VELR_RUNTIME_PATH`).
pub struct Runtime {
    /// Keep the library alive for the lifetime of the process.
    _lib: Library,
    /// ABI entrypoints resolved from the loaded runtime library.
    pub api: Api,
    /// Filesystem path from which the runtime library was loaded.
    pub path: PathBuf,
}

/// Get the process-wide Velr runtime singleton.
///
/// The runtime is lazily initialized on first call. If initialization fails, the error is cached
/// and later returned by subsequent calls.
///
/// # Errors
///
/// Returns an [`Error`] if:
/// - The runtime cannot be selected for the current target.
/// - The runtime cannot be written to the cache directory.
/// - The dynamic library cannot be loaded.
/// - The required ABI symbols cannot be resolved into an [`Api`].
pub fn runtime() -> Result<&'static Runtime> {
    match RUNTIME.get_or_init(Runtime::init) {
        Ok(rt) => Ok(rt),
        Err(e) => Err(Error {
            code: e.code,
            message: e.message.clone(),
        }),
    }
}

impl Runtime {
    /// Initialize a [`Runtime`] by selecting a runtime library and loading it.
    ///
    /// Selection order:
    /// 1. If `VELR_RUNTIME_PATH` is set, load directly from that path (development escape hatch).
    /// 2. Otherwise, select the bundled runtime bytes for the current target, materialize them
    ///    into the cache, and load from that path.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if materialization fails or the runtime cannot be loaded.
    fn init() -> Result<Self> {
        // Optional escape hatch for development:
        // point directly at a runtime file on disk.
        if let Ok(p) = env::var("VELR_RUNTIME_PATH") {
            let path = PathBuf::from(p);
            return unsafe { Self::load_from_path(path) };
        }

        let (bytes, filename) = bundled_runtime_bytes_and_name();
        let path = materialize(bytes, filename)?;
        unsafe { Self::load_from_path(path) }
    }

    /// Load the runtime dynamic library from `path` and resolve its ABI into an [`Api`].
    ///
    /// # Safety
    ///
    /// This function is `unsafe` because it loads and binds to a dynamic library at runtime.
    /// Callers must ensure:
    /// - `path` points to a valid dynamic library file.
    /// - The library is a compatible Velr runtime for the current process (platform/arch/ABI).
    /// - The library exports the expected Velr ABI symbols required by [`Api::load`].
    ///
    /// Violating these expectations may lead to undefined behavior when calling resolved symbols.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if:
    /// - The library cannot be loaded.
    /// - The Velr ABI symbols cannot be resolved into an [`Api`].
    unsafe fn load_from_path(path: PathBuf) -> Result<Self> {
        let lib = Library::new(&path).map_err(|e| Error {
            code: -1,
            message: format!("Failed to load Velr runtime '{}': {e}", path.display()),
        })?;

        let api = Api::load(&lib).map_err(|e| Error {
            code: -1,
            message: format!(
                "Failed to resolve Velr ABI symbols from '{}': {e}",
                path.display()
            ),
        })?;

        Ok(Self {
            _lib: lib,
            api,
            path,
        })
    }
}

/// Select the correct embedded runtime for this target.
///
/// This function is compile-time gated via `#[cfg(...)]` to ensure that only supported targets
/// are built. For unsupported targets, compilation fails with a clear error message.
///
/// The returned tuple is:
/// - The embedded runtime bytes.
/// - The filename to use when materializing the runtime to disk.
fn bundled_runtime_bytes_and_name() -> (&'static [u8], &'static str) {
    // macOS universal dylib
    #[cfg(target_os = "macos")]
    {
        (
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/prebuilt/macos-universal/libvelrc.dylib"
            )),
            "libvelrc.dylib",
        )
    }

    // Linux x86_64
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        (
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/prebuilt/linux-x86_64/libvelrc.so"
            )),
            "libvelrc.so",
        )
    }

    // Linux aarch64
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        (
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/prebuilt/linux-aarch64/libvelrc.so"
            )),
            "libvelrc.so",
        )
    }

    // Windows x86_64: load the DLL (not the import lib)
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        (
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/prebuilt/windows-x86_64/velrc.dll"
            )),
            "velrc.dll",
        )
    }

    #[cfg(not(any(
        target_os = "macos",
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "aarch64"),
        all(target_os = "windows", target_arch = "x86_64"),
    )))]
    {
        compile_error!("No bundled Velr runtime for this target.");
    }
}

/// Write runtime bytes to a stable cache path and return the path.
/// Uses a content hash in the filename (important on Windows: loaded DLLs can’t be overwritten).
fn materialize(bytes: &[u8], filename: &str) -> Result<PathBuf> {
    let dir = runtime_cache_dir()?.join("velr").join("runtime");
    fs::create_dir_all(&dir).map_err(|e| Error {
        code: -1,
        message: format!(
            "Failed to create runtime cache dir '{}': {e}",
            dir.display()
        ),
    })?;

    let mut h = Hasher::new();
    h.update(bytes);
    let hash = h.finalize().to_hex(); // 64 chars

    let out = dir.join(format!("{hash}-{filename}"));
    if out.exists() {
        return Ok(out);
    }

    // Write temp, then rename.
    let tmp = dir.join(format!(
        "{}.tmp",
        out.file_name().unwrap().to_string_lossy()
    ));

    {
        let mut f = fs::File::create(&tmp).map_err(|e| Error {
            code: -1,
            message: format!("Failed to create '{}': {e}", tmp.display()),
        })?;
        f.write_all(bytes).map_err(|e| Error {
            code: -1,
            message: format!("Failed to write '{}': {e}", tmp.display()),
        })?;
        let _ = f.flush();
    }

    match fs::rename(&tmp, &out) {
        Ok(()) => Ok(out),
        Err(e) => {
            // Possible race: another process wrote it first. If it exists now, accept it.
            if out.exists() {
                let _ = fs::remove_file(&tmp);
                Ok(out)
            } else {
                Err(Error {
                    code: -1,
                    message: format!(
                        "Failed to rename '{}' -> '{}': {e}",
                        tmp.display(),
                        out.display()
                    ),
                })
            }
        }
    }
}

/// Determine the base directory used for caching the materialized runtime.
///
/// Selection order:
/// 1. `VELR_CACHE_DIR` if set.
/// 2. Platform-specific defaults:
///    - Windows: `LOCALAPPDATA`, else `TEMP`
///    - macOS: `$HOME/Library/Caches`
///    - Linux / fallback: `XDG_CACHE_HOME`, else `$HOME/.cache`
/// 3. Final fallback: [`env::temp_dir`]
///
/// # Errors
///
/// This function returns `Ok(...)` in all branches. It uses a fall-through strategy that
/// ends with [`env::temp_dir`]. The [`Result`] return type allows future expansion where failures
/// may be surfaced explicitly.
fn runtime_cache_dir() -> Result<PathBuf> {
    // Optional override
    if let Ok(d) = env::var("VELR_CACHE_DIR") {
        return Ok(PathBuf::from(d));
    }

    // Windows
    #[cfg(target_os = "windows")]
    {
        if let Ok(d) = env::var("LOCALAPPDATA") {
            return Ok(PathBuf::from(d));
        }
        if let Ok(d) = env::var("TEMP") {
            return Ok(PathBuf::from(d));
        }
    }

    // macOS
    #[cfg(target_os = "macos")]
    {
        if let Ok(home) = env::var("HOME") {
            return Ok(PathBuf::from(home).join("Library").join("Caches"));
        }
    }

    // Linux + fallback (XDG)
    if let Ok(xdg) = env::var("XDG_CACHE_HOME") {
        return Ok(PathBuf::from(xdg));
    }
    if let Ok(home) = env::var("HOME") {
        return Ok(PathBuf::from(home).join(".cache"));
    }

    Ok(env::temp_dir())
}
