use std::{ffi::CString, path};

use crate::{ffi, runtime, Error, Result, Velr};

/// A trusted native extension and its configuration for a new connection.
///
/// Extension packages construct this descriptor from their bundled library.
/// Use [`Velr::builder`] to enable one or more extensions when opening a database.
pub struct Extension {
    filename: &'static str,
    bytes: &'static [u8],
    memory_limit_bytes: usize,
}

impl Extension {
    /// Describe a bundled native library implementing the Velr extension ABI.
    ///
    /// The driver materializes the library using its content-addressed cache when
    /// the connection opens. The filename must be a single ASCII component.
    ///
    /// # Safety
    ///
    /// The library must be trusted in-process code implementing the Velr native
    /// extension ABI. Merely loading a library can execute its initialization
    /// code even if the runtime later rejects its version or interface.
    pub unsafe fn from_bundled_native(
        filename: &'static str,
        bytes: &'static [u8],
        memory_limit_bytes: usize,
    ) -> Self {
        Self {
            filename,
            bytes,
            memory_limit_bytes,
        }
    }

    fn materialize(&self) -> Result<CString> {
        if self.memory_limit_bytes == 0 {
            return Err(Error::new(
                ffi::velr_code::VELR_EARG as i32,
                "extension memory limit must be positive",
            ));
        }
        if self.filename.is_empty()
            || !self
                .filename
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        {
            return Err(Error::new(
                ffi::velr_code::VELR_EARG as i32,
                "extension filename must be a single ASCII path component",
            ));
        }
        let cached = runtime::materialize(self.bytes, self.filename)?;
        let path = path::absolute(&cached).map_err(|source| {
            Error::with_source(
                ffi::velr_code::VELR_EERR as i32,
                format!("cannot resolve extension cache path '{}'", cached.display()),
                source,
            )
        })?;
        let path = path.to_str().ok_or_else(|| {
            Error::new(
                ffi::velr_code::VELR_EUTF as i32,
                "extension cache path is not UTF-8",
            )
        })?;
        CString::new(path).map_err(|_| {
            Error::new(
                ffi::velr_code::VELR_EUTF as i32,
                "extension cache path contains NUL",
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_builder_uses_ordinary_open() -> Result<()> {
        let db = Velr::builder().open(None)?;
        let mut table = db.exec_one("RETURN 42 AS answer")?;
        let rows = table.collect(|row| match row[0] {
            crate::CellRef::Integer(n) => Ok(n),
            ref value => panic!("expected integer, got {value:?}"),
        })?;
        assert_eq!(rows, [42]);
        Ok(())
    }

    #[test]
    fn rejects_zero_memory_and_invalid_filenames_before_caching() {
        for (filename, memory_limit_bytes) in [("extension.so", 0), (" ../extension.so", 1)] {
            let extension = Extension {
                filename,
                bytes: &[],
                memory_limit_bytes,
            };
            let error = extension.materialize().unwrap_err();
            assert_eq!(error.code, ffi::velr_code::VELR_EARG as i32);
        }
    }
}

/// Configures a connection with explicitly selected native extensions.
#[derive(Default)]
pub struct VelrBuilder {
    extensions: Vec<Extension>,
}

impl VelrBuilder {
    /// Enable an installed extension on the database connection this builder opens.
    #[must_use]
    pub fn with_extension(mut self, extension: impl Into<Extension>) -> Self {
        self.extensions.push(extension.into());
        self
    }

    /// Open a connection and register the selected extensions.
    ///
    /// The path has the same meaning as in [`Velr::open`]. The connection owns its
    /// registered extensions and their state.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot open, an extension cannot load
    /// or register, or the runtime and extension versions are incompatible.
    pub fn open(self, path: Option<&str>) -> Result<Velr> {
        let paths = self
            .extensions
            .iter()
            .map(Extension::materialize)
            .collect::<Result<Vec<_>>>()?;
        let config: Vec<_> = self
            .extensions
            .iter()
            .zip(&paths)
            .map(|(extension, path)| ffi::velr_extension_config {
                path: path.as_ptr(),
                memory_limit_bytes: extension.memory_limit_bytes,
            })
            .collect();
        Velr::open_configured(path, &config)
    }
}
