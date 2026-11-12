use std::{env, fs::File, io, path::PathBuf};

fn main() -> io::Result<()> {
    let prebuilt = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("prebuilt");
    println!("cargo:rerun-if-changed={}", prebuilt.display());

    let raw = prebuilt.join("velrc.dll");
    let runtime = if raw.is_file() {
        raw
    } else {
        let compressed = prebuilt.join("velrc.dll.zst");
        let output = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("velrc.dll");
        zstd::stream::copy_decode(File::open(compressed)?, File::create(&output)?)?;
        output
    };
    println!(
        "cargo:rustc-env=VELR_WINDOWS_X86_64_RUNTIME_PATH={}",
        runtime.display()
    );
    Ok(())
}
