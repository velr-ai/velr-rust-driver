//! Bundled Velr native runtime for macos-aarch64.

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
    fn runtime_payload_matches_architecture() {
        assert_eq!(FILENAME, "libvelrc.dylib");
        // Little-endian Mach-O 64 header and CPU type: no universal or wrong-CPU payload.
        assert!(BYTES.starts_with(&[0xcf, 0xfa, 0xed, 0xfe, 12, 0, 0, 1]));
    }
}
