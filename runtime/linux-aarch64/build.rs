use std::{env, fs::File, io, path::PathBuf};

fn main() -> io::Result<()> {
    let prebuilt = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("prebuilt");
    println!("cargo:rerun-if-changed={}", prebuilt.display());

    let raw = prebuilt.join("libvelrc.so");
    let runtime = if raw.is_file() {
        raw
    } else {
        let compressed = prebuilt.join("libvelrc.so.zst");
        let output = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("libvelrc.so");
        zstd::stream::copy_decode(File::open(compressed)?, File::create(&output)?)?;
        output
    };
    println!(
        "cargo:rustc-env=VELR_LINUX_AARCH64_RUNTIME_PATH={}",
        runtime.display()
    );
    Ok(())
}
