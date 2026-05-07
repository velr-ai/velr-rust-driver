//! Bundled Velr native runtime for Windows x86_64 MSVC.

pub const FILENAME: &str = "velrc.dll";
pub const BYTES: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/prebuilt/velrc.dll"));

pub fn bytes_and_name() -> (&'static [u8], &'static str) {
    (BYTES, FILENAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_payload_is_present() {
        assert_eq!(FILENAME, "velrc.dll");
        assert!(!BYTES.is_empty());
    }
}
