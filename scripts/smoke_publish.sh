#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

MAX_CRATE_BYTES="${MAX_CRATE_BYTES:-10485760}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target-smoke}"
export VELR_CACHE_DIR="${VELR_CACHE_DIR:-$CARGO_TARGET_DIR/cache}"

manifests=(
  "runtime/linux-x86_64/Cargo.toml"
  "runtime/linux-aarch64/Cargo.toml"
  "runtime/macos-universal/Cargo.toml"
  "runtime/windows-x86_64/Cargo.toml"
)

crate_name() {
  awk -F '"' '/^name[[:space:]]*=/ { print $2; exit }' "$1"
}

crate_version() {
  awk -F '"' '/^version[[:space:]]*=/ { print $2; exit }' "$1"
}

echo "Packaging Velr runtime crates..."
for manifest in "${manifests[@]}"; do
  name="$(crate_name "$manifest")"
  version="$(crate_version "$manifest")"
  crate_file="$CARGO_TARGET_DIR/package/$name-$version.crate"

  cargo package --manifest-path "$manifest" --allow-dirty --no-verify

  if [[ ! -f "$crate_file" ]]; then
    echo "ERROR: expected package file missing: $crate_file" >&2
    exit 1
  fi

  size="$(wc -c < "$crate_file" | tr -d ' ')"
  echo "$name-$version.crate size=$size bytes"

  if (( size > MAX_CRATE_BYTES )); then
    echo "ERROR: $crate_file exceeds MAX_CRATE_BYTES=$MAX_CRATE_BYTES" >&2
    exit 1
  fi
done

driver_version="$(crate_version Cargo.toml)"
driver_estimate="$CARGO_TARGET_DIR/package/velr-$driver_version-local-estimate.crate"
driver_file_list="$CARGO_TARGET_DIR/package/velr-$driver_version.files"

mkdir -p "$CARGO_TARGET_DIR/package"
cargo package --manifest-path Cargo.toml --allow-dirty --list > "$driver_file_list"

if grep -E '^(prebuilt|runtime)/' "$driver_file_list"; then
  echo "ERROR: public velr package file list includes native runtime payloads" >&2
  exit 1
fi

tar -czf "$driver_estimate" Cargo.toml LICENSE LICENSE.runtime README.md src
driver_size="$(wc -c < "$driver_estimate" | tr -d ' ')"
echo "velr-$driver_version local package estimate size=$driver_size bytes"

if (( driver_size > MAX_CRATE_BYTES )); then
  echo "ERROR: $driver_estimate exceeds MAX_CRATE_BYTES=$MAX_CRATE_BYTES" >&2
  exit 1
fi

if [[ "${VELR_SMOKE_SKIP_TESTS:-0}" == "1" ]]; then
  echo "Skipping cargo tests because VELR_SMOKE_SKIP_TESTS=1"
  exit 0
fi

echo "Testing runtime crates..."
for manifest in "${manifests[@]}"; do
  cargo test --manifest-path "$manifest"
done

echo "Testing public velr crate..."
cargo test --manifest-path Cargo.toml
cargo test --manifest-path Cargo.toml --features arrow-ipc
