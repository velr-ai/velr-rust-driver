use std::{
    env, fs,
    io::{BufReader, BufWriter, Write},
    path::PathBuf,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let raw = root.join("prebuilt/libvelrc.dylib");
    let compressed = root.join("prebuilt/libvelrc.dylib.xz");
    let output = PathBuf::from(env::var("OUT_DIR")?).join("libvelrc.dylib");
    // Watch the directory so an intentionally absent raw/compressed sibling
    // does not make Cargo rerun this script on every incremental build.
    println!("cargo:rerun-if-changed={}", root.join("prebuilt").display());

    // Workspace builds use the current raw artifact. Published crates include
    // only its compressed copy, decoded by Rust without external tools.
    if raw.is_file() {
        fs::copy(raw, output)?;
    } else {
        let mut reader = BufReader::new(fs::File::open(&compressed)?);
        let mut writer = BufWriter::new(fs::File::create(output)?);
        lzma_rs::xz_decompress(&mut reader, &mut writer)?;
        writer.flush()?;
    }
    Ok(())
}
