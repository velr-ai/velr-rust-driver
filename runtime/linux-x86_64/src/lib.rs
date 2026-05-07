//! Bundled Velr native runtime for Linux x86_64.

pub const FILENAME: &str = "libvelrc.so";
pub const BYTES: &[u8] =
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/prebuilt/libvelrc.so"));

pub fn bytes_and_name() -> (&'static [u8], &'static str) {
    (BYTES, FILENAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_payload_is_present() {
        assert_eq!(FILENAME, "libvelrc.so");
        assert!(!BYTES.is_empty());
    }
}
