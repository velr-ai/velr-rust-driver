//! Bundled Velr native runtime for Linux x86_64.

pub const FILENAME: &str = "libvelrc.so";
pub const BYTES: &[u8] = include_bytes!(env!("VELR_LINUX_X86_64_RUNTIME_PATH"));

pub fn bytes_and_name() -> (&'static [u8], &'static str) {
    (BYTES, FILENAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_payload_matches_architecture() {
        assert_eq!(FILENAME, "libvelrc.so");
        assert!(BYTES.starts_with(b"\x7fELF\x02\x01"));
        assert_eq!(&BYTES[18..20], &62_u16.to_le_bytes());
    }
}
