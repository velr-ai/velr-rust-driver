#!/usr/bin/env bash
set -euo pipefail

prebuilt="${1:?usage: pack_runtime.sh <prebuilt-directory> <runtime-filename>}"
filename="${2:?usage: pack_runtime.sh <prebuilt-directory> <runtime-filename>}"
raw="$prebuilt/$filename"
compressed="$raw.zst"

if ! command -v zstd >/dev/null; then
  echo "ERROR: zstd is required to package $raw" >&2
  exit 1
fi

if [[ -e "$compressed" && ! -f "$compressed" ]]; then
  echo "ERROR: expected a file at $compressed" >&2
  exit 1
fi

if [[ ! -f "$raw" ]]; then
  zstd -qt -- "$compressed"
  exit 0
fi

temporary="$(mktemp "$prebuilt/.$filename.zst.XXXXXX")"
trap 'rm -f -- "$temporary"' EXIT
zstd -q -19 --check -c -- "$raw" > "$temporary"
if ! zstd -qdc -- "$temporary" | cmp -s "$raw" -; then
  echo "ERROR: compressed runtime $raw does not round-trip byte-for-byte" >&2
  exit 1
fi

chmod 0644 "$temporary"
mv -f -- "$temporary" "$compressed"
rm -- "$raw"
