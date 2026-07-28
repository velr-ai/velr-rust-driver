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
    io::{Read, Write},
    path::{Path, PathBuf},
    process,
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use blake3::Hasher;
use libloading::Library;

use crate::{api::Api, Error, Result};

#[cfg(all(target_os = "linux", target_arch = "x86_64", target_env = "gnu"))]
use velr_runtime_linux_x86_64 as selected_runtime;

#[cfg(all(target_os = "linux", target_arch = "aarch64", target_env = "gnu"))]
use velr_runtime_linux_aarch64 as selected_runtime;

#[cfg(all(
    target_os = "macos",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
use velr_runtime_macos_universal as selected_runtime;

#[cfg(all(target_os = "windows", target_arch = "x86_64", target_env = "msvc"))]
use velr_runtime_windows_x86_64 as selected_runtime;

#[cfg(not(any(
    all(target_os = "linux", target_arch = "x86_64", target_env = "gnu"),
    all(target_os = "linux", target_arch = "aarch64", target_env = "gnu"),
    all(
        target_os = "macos",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ),
    all(target_os = "windows", target_arch = "x86_64", target_env = "msvc"),
)))]
compile_error!("No bundled Velr runtime for this target.");

/// Process-wide singleton storage for the runtime.
///
/// Note that this stores `Result<Runtime>` rather than `Runtime`, which means a failed initialization
/// is also cached and will be returned on subsequent calls to [`runtime()`].
static RUNTIME: OnceLock<Result<Runtime>> = OnceLock::new();
static MATERIALIZE_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Loaded Velr runtime and resolved ABI API.
///
/// A [`Runtime`] instance owns the loaded dynamic library to ensure it remains alive for the
/// lifetime of the process. The [`Api`] is resolved from that library and is expected to remain
/// valid as long as the library remains loaded.
///
/// - `_lib` is intentionally kept to extend the lifetime of the loaded dynamic library.
/// - `api` provides access to the runtime's resolved ABI symbols.
/// - `_path` records the filesystem location of the loaded runtime library (either cached bundled
///   bytes or a user-provided path via `VELR_RUNTIME_PATH`).
pub struct Runtime {
    /// Keep the library alive for the lifetime of the process.
    _lib: Library,
    /// ABI entrypoints resolved from the loaded runtime library.
    pub api: Api,
    /// Filesystem path from which the runtime library was loaded.
    _path: PathBuf,
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
            _path: path,
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
    selected_runtime::bytes_and_name()
}

/// Write runtime bytes to a stable cache path and return the path.
/// Uses a content hash in the filename (important on Windows: loaded DLLs can’t be overwritten).
fn materialize(bytes: &[u8], filename: &str) -> Result<PathBuf> {
    materialize_in_cache_dir(bytes, filename, &runtime_cache_dir()?)
}

fn materialize_in_cache_dir(bytes: &[u8], filename: &str, cache_dir: &Path) -> Result<PathBuf> {
    let dir = cache_dir.join("velr").join("runtime");
    fs::create_dir_all(&dir).map_err(|e| Error {
        code: -1,
        message: format!(
            "Failed to create runtime cache dir '{}': {e}",
            dir.display()
        ),
    })?;

    let hash = blake3_hex(bytes);

    let out = dir.join(format!("{hash}-{filename}"));
    if cached_runtime_is_valid(&out, bytes.len() as u64, &hash)? {
        return Ok(out);
    }

    remove_invalid_cached_runtime(&out, bytes.len() as u64, &hash)?;

    // Write to a unique temp file before publishing the final hash path. A deterministic temp name
    // lets parallel cold starts clobber one another and can make dlopen see a partial library.
    let tmp = unique_materialize_tmp_path(&dir, &out);

    let write_result = (|| -> Result<()> {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(|e| Error {
                code: -1,
                message: format!("Failed to create '{}': {e}", tmp.display()),
            })?;
        f.write_all(bytes).map_err(|e| Error {
            code: -1,
            message: format!("Failed to write '{}': {e}", tmp.display()),
        })?;
        f.sync_all().map_err(|e| Error {
            code: -1,
            message: format!("Failed to sync '{}': {e}", tmp.display()),
        })?;
        Ok(())
    })();

    if let Err(e) = write_result {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }

    if cached_runtime_is_valid(&out, bytes.len() as u64, &hash)? {
        let _ = fs::remove_file(&tmp);
        return Ok(out);
    }

    match fs::rename(&tmp, &out) {
        Ok(()) => {
            if cached_runtime_is_valid(&out, bytes.len() as u64, &hash)? {
                Ok(out)
            } else {
                Err(Error {
                    code: -1,
                    message: format!(
                        "Materialized Velr runtime '{}' failed content verification",
                        out.display()
                    ),
                })
            }
        }
        Err(rename_error) => {
            if cached_runtime_is_valid(&out, bytes.len() as u64, &hash)? {
                let _ = fs::remove_file(&tmp);
                Ok(out)
            } else {
                let _ = fs::remove_file(&tmp);
                Err(Error {
                    code: -1,
                    message: format!(
                        "Failed to rename '{}' -> '{}': {rename_error}",
                        tmp.display(),
                        out.display()
                    ),
                })
            }
        }
    }
}

fn blake3_hex(bytes: &[u8]) -> String {
    let mut h = Hasher::new();
    h.update(bytes);
    h.finalize().to_hex().to_string()
}

fn unique_materialize_tmp_path(dir: &Path, out: &Path) -> PathBuf {
    let counter = MATERIALIZE_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let file_name = out
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "velr-runtime".into());

    dir.join(format!(
        "{file_name}.{}.{}.{}.tmp",
        process::id(),
        nanos,
        counter
    ))
}

fn remove_invalid_cached_runtime(
    path: &Path,
    expected_len: u64,
    expected_hash: &str,
) -> Result<()> {
    if !path.exists() || cached_runtime_is_valid(path, expected_len, expected_hash)? {
        return Ok(());
    }

    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error {
            code: -1,
            message: format!(
                "Failed to remove invalid cached Velr runtime '{}': {e}",
                path.display()
            ),
        }),
    }
}

fn cached_runtime_is_valid(path: &Path, expected_len: u64, expected_hash: &str) -> Result<bool> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => {
            return Err(Error {
                code: -1,
                message: format!(
                    "Failed to inspect cached Velr runtime '{}': {e}",
                    path.display()
                ),
            })
        }
    };

    if !metadata.is_file() || metadata.len() != expected_len {
        return Ok(false);
    }

    let mut file = fs::File::open(path).map_err(|e| Error {
        code: -1,
        message: format!(
            "Failed to open cached Velr runtime '{}': {e}",
            path.display()
        ),
    })?;
    let mut hasher = Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|e| Error {
            code: -1,
            message: format!(
                "Failed to read cached Velr runtime '{}': {e}",
                path.display()
            ),
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hasher.finalize().to_hex().as_str() == expected_hash)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_cache_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        env::temp_dir().join(format!("velr-runtime-{name}-{}-{nanos}", process::id()))
    }

    #[test]
    fn materialize_replaces_invalid_cached_runtime() {
        let cache_dir = temp_cache_dir("invalid-cache");
        let bytes = b"runtime bytes";
        let filename = "libvelrc-test";

        let path = materialize_in_cache_dir(bytes, filename, &cache_dir).expect("materialize");
        fs::write(&path, b"corrupt").expect("corrupt cached runtime");

        let repaired = materialize_in_cache_dir(bytes, filename, &cache_dir).expect("repair");
        assert_eq!(repaired, path);
        assert_eq!(fs::read(&repaired).expect("read repaired runtime"), bytes);

        let _ = fs::remove_dir_all(cache_dir);
    }

    #[test]
    fn materialize_parallel_cold_cache_publishes_valid_runtime() {
        let cache_dir = temp_cache_dir("parallel-cold-cache");
        let bytes = vec![42_u8; 1024 * 1024];
        let filename = "libvelrc-test";

        let handles = (0..32)
            .map(|_| {
                let cache_dir = cache_dir.clone();
                let bytes = bytes.clone();
                std::thread::spawn(move || materialize_in_cache_dir(&bytes, filename, &cache_dir))
            })
            .collect::<Vec<_>>();

        let mut paths = Vec::new();
        for handle in handles {
            paths.push(
                handle
                    .join()
                    .expect("thread panicked")
                    .expect("materialize"),
            );
        }

        for path in &paths {
            assert_eq!(path, &paths[0]);
        }
        assert_eq!(fs::read(&paths[0]).expect("read runtime"), bytes);

        let runtime_dir = cache_dir.join("velr").join("runtime");
        let tmp_files = fs::read_dir(runtime_dir)
            .expect("read runtime dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().map_or(false, |ext| ext == "tmp"))
            .collect::<Vec<_>>();
        assert!(tmp_files.is_empty(), "left temp files: {tmp_files:?}");

        let _ = fs::remove_dir_all(cache_dir);
    }
}
