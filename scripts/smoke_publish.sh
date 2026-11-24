#!/usr/bin/env bash
set -euo pipefail

runtime_only=false
if (( $# > 0 )); then
  if [[ $# -ne 1 || "$1" != "--runtime-only" ]]; then
    echo "Usage: ${BASH_SOURCE[0]} [--runtime-only]" >&2
    exit 2
  fi
  runtime_only=true
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

MAX_CRATE_BYTES="${MAX_CRATE_BYTES:-10485760}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target-smoke}"
export VELR_CACHE_DIR="${VELR_CACHE_DIR:-$CARGO_TARGET_DIR/cache}"

stage="$(mktemp -d)"
trap 'rm -rf -- "$stage"' EXIT
cp -R runtime/{linux-x86_64,linux-aarch64,macos-aarch64,macos-x86_64,windows-x86_64} "$stage/"
linux_x86_64_runtime="$stage/linux-x86_64"
linux_aarch64_runtime="$stage/linux-aarch64"
macos_aarch64_runtime="$stage/macos-aarch64"
macos_x86_64_runtime="$stage/macos-x86_64"
windows_x86_64_runtime="$stage/windows-x86_64"
bash scripts/pack_runtime.sh "$linux_x86_64_runtime/prebuilt" libvelrc.so
bash scripts/pack_runtime.sh "$linux_aarch64_runtime/prebuilt" libvelrc.so
bash scripts/pack_runtime.sh "$macos_aarch64_runtime/prebuilt" libvelrc.dylib
bash scripts/pack_runtime.sh "$macos_x86_64_runtime/prebuilt" libvelrc.dylib
bash scripts/pack_runtime.sh "$windows_x86_64_runtime/prebuilt" velrc.dll

manifests=(
  "$linux_x86_64_runtime/Cargo.toml"
  "$linux_aarch64_runtime/Cargo.toml"
  "$macos_aarch64_runtime/Cargo.toml"
  "$macos_x86_64_runtime/Cargo.toml"
  "$windows_x86_64_runtime/Cargo.toml"
)

crate_name() {
  awk -F '"' '/^name[[:space:]]*=/ { print $2; exit }' "$1"
}

crate_version() {
  awk -F '"' '/^version[[:space:]]*=/ { print $2; exit }' "$1"
}

if [[ "$runtime_only" == false && -d velr-types ]]; then
  echo "Packaging velr-types crate..."
  types_name="$(crate_name velr-types/Cargo.toml)"
  types_version="$(crate_version velr-types/Cargo.toml)"
  types_crate="$CARGO_TARGET_DIR/package/$types_name-$types_version.crate"

  cargo package --manifest-path velr-types/Cargo.toml --allow-dirty --no-verify

  if [[ ! -f "$types_crate" ]]; then
    echo "ERROR: expected package file missing: $types_crate" >&2
    exit 1
  fi

  types_size="$(wc -c < "$types_crate" | tr -d ' ')"
  echo "$types_name-$types_version.crate size=$types_size bytes"

  if (( types_size > MAX_CRATE_BYTES )); then
    echo "ERROR: $types_crate exceeds MAX_CRATE_BYTES=$MAX_CRATE_BYTES" >&2
    exit 1
  fi
fi

echo "Packaging Velr runtime crates..."
for manifest in "${manifests[@]}"; do
  name="$(crate_name "$manifest")"
  version="$(crate_version "$manifest")"
  crate_file="$CARGO_TARGET_DIR/package/$name-$version.crate"

  cargo package --manifest-path "$manifest" --allow-dirty

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

if [[ "$runtime_only" == false ]]; then
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
fi

if [[ "${VELR_SMOKE_SKIP_TESTS:-0}" == "1" ]]; then
  echo "Skipping cargo tests because VELR_SMOKE_SKIP_TESTS=1"
  exit 0
fi

if [[ "$runtime_only" == false && -d velr-types ]]; then
  echo "Testing velr-types crate..."
  cargo test --manifest-path velr-types/Cargo.toml
fi

echo "Testing runtime crates..."
for manifest in "${manifests[@]}"; do
  cargo test --manifest-path "$manifest"
done

if [[ "$runtime_only" == true ]]; then
  exit 0
fi

echo "Testing public velr crate..."
cargo test --manifest-path Cargo.toml
cargo test --manifest-path Cargo.toml --no-default-features

echo "Checking table owner lifetimes in the public Rust API..."
python3 -m unittest discover -s scripts -p 'test_check_table_lifetimes.py'
python3 scripts/check_table_lifetimes.py --manifest-path Cargo.toml
python3 scripts/check_table_lifetimes.py --manifest-path Cargo.toml --no-default-features
