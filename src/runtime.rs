use std::{
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::OnceLock,
};

use blake3::Hasher;
use libloading::Library;

use crate::{api::Api, Error, Result};

static RUNTIME: OnceLock<Result<Runtime>> = OnceLock::new();

pub struct Runtime {
    // Keep the library alive for the lifetime of the process.
    _lib: Library,
    pub api: Api,
    pub path: PathBuf,
}

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
