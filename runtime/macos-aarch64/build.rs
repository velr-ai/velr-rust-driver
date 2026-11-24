use std::{env, fs::File, io, path::PathBuf};

fn main() -> io::Result<()> {
    let prebuilt = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("prebuilt");
    println!("cargo:rerun-if-changed={}", prebuilt.display());

    let compressed = prebuilt.join("libvelrc.dylib.zst");
    let runtime = if compressed.is_file() {
        let output = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("libvelrc.dylib");
        zstd::stream::copy_decode(File::open(compressed)?, File::create(&output)?)?;
        output
    } else {
        prebuilt.join("libvelrc.dylib")
    };
    println!(
        "cargo:rustc-env=VELR_MACOS_AARCH64_RUNTIME_PATH={}",
        runtime.display()
    );
    Ok(())
}
