#!/usr/bin/env bash
# Finalize the distributable dylib before testing or uploading it. Cargo's
# stripping does not reliably discard all private Mach-O symbol metadata.
set -euo pipefail

runtime="${1:?usage: strip_macos_runtime.sh <dylib>}"
before="$(wc -c < "$runtime" | tr -d ' ')"
symbols="$(mktemp -d)"
trap 'rm -rf "$symbols"' EXIT

nm -gUj "$runtime" | LC_ALL=C sort > "$symbols/before"
# -x removes local symbols while preserving the exported C/C++ ABI and code.
strip -x "$runtime"
codesign --force --sign - "$runtime"
codesign --verify --strict "$runtime"
nm -gUj "$runtime" | LC_ALL=C sort > "$symbols/after"
diff -u "$symbols/before" "$symbols/after"

after="$(wc -c < "$runtime" | tr -d ' ')"
echo "macOS runtime stripped: $before -> $after bytes; exported symbols unchanged"
