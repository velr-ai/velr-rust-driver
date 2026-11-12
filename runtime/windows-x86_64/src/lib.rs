//! Bundled Velr native runtime for Windows x86_64 MSVC.

pub const FILENAME: &str = "velrc.dll";
pub const BYTES: &[u8] = include_bytes!(env!("VELR_WINDOWS_X86_64_RUNTIME_PATH"));

pub fn bytes_and_name() -> (&'static [u8], &'static str) {
    (BYTES, FILENAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_payload_matches_architecture() {
        assert_eq!(FILENAME, "velrc.dll");
        assert!(BYTES.starts_with(b"MZ"));
        let pe_header = u32::from_le_bytes(BYTES[0x3c..0x40].try_into().unwrap()) as usize;
        assert_eq!(&BYTES[pe_header..pe_header + 6], b"PE\0\0\x64\x86");
    }
}
