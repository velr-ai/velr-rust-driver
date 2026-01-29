use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=VELR_LIB_DIR");

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
    let target = env::var("TARGET").unwrap_or_default();

    // Optional override for dev/CI:
    // - macOS: dir containing libvelrc.dylib
    // - Linux: dir containing libvelrc.so
    // - Windows: dir containing velrc.lib + velrc.dll
    if let Ok(dir) = env::var("VELR_LIB_DIR") {
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=dylib=velrc");
        return;
    }

    let (subdir, filename) = match (target_os.as_str(), target_arch.as_str()) {
        ("macos", _) => ("macos-universal", "libvelrc.dylib"),
        ("linux", "x86_64") => ("linux-x86_64", "libvelrc.so"),
        ("linux", "aarch64") => ("linux-aarch64", "libvelrc.so"),
        ("windows", "x86_64") => ("windows-x86_64", "velrc.lib"),
        _ => panic!(
            "velr: no bundled runtime for target='{target}' (os='{target_os}', arch='{target_arch}'). \
             Set VELR_LIB_DIR."
        ),
    };

    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let libdir = manifest.join("prebuilt").join(subdir);
    let lib = libdir.join(filename);

    if !lib.exists() {
        panic!("Missing bundled runtime at {}", lib.display());
    }

    // On Windows, ensure DLL is present too (needed at runtime)
    if target_os == "windows" {
        let dll = libdir.join("velrc.dll");
        if !dll.exists() {
            panic!("Missing bundled runtime DLL at {}", dll.display());
        }
        println!("cargo:rerun-if-changed={}", dll.display());
    }

    println!("cargo:rerun-if-changed={}", lib.display());
    println!("cargo:rustc-link-search=native={}", libdir.display());
    println!("cargo:rustc-link-lib=dylib=velrc");
}
