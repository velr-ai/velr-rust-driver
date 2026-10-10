//! Bundled Velr native runtime for macOS universal.

pub const FILENAME: &str = "libvelrc.dylib";
// The published payload is expanded at build time. Consumers still receive
// the original dylib bytes, with no decoding in the runtime loader.
pub const BYTES: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/libvelrc.dylib"));

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
