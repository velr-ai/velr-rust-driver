#!/usr/bin/env bash
# Finalize the distributable shared library before testing or uploading it.
# Private ELF symbol tables can remain even with Cargo's release stripping.
set -euo pipefail

runtime="${1:?usage: strip_linux_runtime.sh <so>}"
nm_tool="${NM:-nm}"
strip_tool="${STRIP:-strip}"
before="$(wc -c < "$runtime" | tr -d ' ')"
symbols="$(mktemp -d)"
trap 'rm -rf "$symbols"' EXIT

"$nm_tool" -D --format=posix "$runtime" | LC_ALL=C sort > "$symbols/before"
# Remove private symbol/debug metadata while retaining dynamic linking symbols.
"$strip_tool" --strip-unneeded "$runtime"
"$nm_tool" -D --format=posix "$runtime" | LC_ALL=C sort > "$symbols/after"
diff -u "$symbols/before" "$symbols/after"

after="$(wc -c < "$runtime" | tr -d ' ')"
echo "Linux runtime stripped: $before -> $after bytes; dynamic symbols unchanged"
