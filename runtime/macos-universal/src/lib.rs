//! Bundled Velr native runtime for macOS universal.

pub const FILENAME: &str = "libvelrc.dylib";
pub const BYTES: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/prebuilt/libvelrc.dylib"
));

pub fn bytes_and_name() -> (&'static [u8], &'static str) {
    (BYTES, FILENAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_payload_is_present() {
        assert_eq!(FILENAME, "libvelrc.dylib");
        assert!(!BYTES.is_empty());
    }
}
