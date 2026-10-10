#!/usr/bin/env python3
"""Prepare the build-time XZ payload without changing native library bytes."""

import hashlib
import lzma
import os
from pathlib import Path
import tempfile


def main():
    prebuilt = Path(__file__).resolve().parent / "prebuilt"
    source = prebuilt / "libvelrc.dylib"
    destination = prebuilt / "libvelrc.dylib.xz"
    original = source.read_bytes()
    # CRC32 is supported by the pure-Rust build-time decoder. Preset 6 keeps
    # release preparation inexpensive while leaving ample package headroom.
    compressed = lzma.compress(original, check=lzma.CHECK_CRC32, preset=6)
    if lzma.decompress(compressed) != original:
        raise RuntimeError("macOS payload did not round-trip exactly")
    with tempfile.NamedTemporaryFile(dir=prebuilt, delete=False) as temporary:
        try:
            temporary.write(compressed)
            temporary.close()
            os.replace(temporary.name, destination)
        finally:
            Path(temporary.name).unlink(missing_ok=True)
    print(
        f"macOS runtime payload: {len(original)} -> {len(compressed)} bytes; "
        f"native SHA-256={hashlib.sha256(original).hexdigest()}"
    )


if __name__ == "__main__":
    main()
