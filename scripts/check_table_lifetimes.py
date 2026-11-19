#!/usr/bin/env python3
"""Check Rust table ownership contracts without linking or running the runtime."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


class CheckFailure(Exception):
    pass


def driver_metadata(
    manifest: Path, feature_flags: list[str], receipt: dict,
) -> tuple[Path, list[Path]]:
    cargo = os.environ.get("CARGO", "cargo")
    identity = subprocess.run(
        [cargo, "metadata", "--locked", "--no-deps", "--format-version=1",
         "--manifest-path", str(manifest)],
        capture_output=True, text=True, cwd=manifest.parent,
    )
    if identity.returncode:
        raise CheckFailure(f"cannot identify the selected driver:\n{identity.stderr}")
    workspace = json.loads(identity.stdout)
    packages = [package for package in workspace["packages"]
                if package["name"] == "velr"]
    if len(packages) != 1:
        raise CheckFailure("expected one velr package in the selected workspace")
    package = packages[0]
    expected_source = (Path(package["manifest_path"]).parent / "src/lib.rs").resolve()
    # Cargo artifact names can collide across checkouts sharing a target directory.
    # Keep these compiler checks apart from ordinary builds and other manifests.
    partition = hashlib.sha256(str(manifest).encode()).hexdigest()[:16]
    target = Path(workspace["target_directory"]) / "table-lifetime-contracts" / partition
    receipt.update(
        package_id=package["id"], source_path=str(expected_source),
        source_sha256_before=hashlib.sha256(expected_source.read_bytes()).hexdigest(),
        isolated_target_dir=str(target),
    )
    command = [
        cargo,
        "check",
        "--locked",
        "--target-dir",
        str(target),
        "--manifest-path",
        str(manifest),
        "-p",
        "velr",
        "--message-format=json",
        *feature_flags,
    ]
    result = subprocess.run(command, capture_output=True, text=True, cwd=manifest.parent)
    if result.returncode:
        raise CheckFailure(f"driver check failed:\n{result.stderr}\n{result.stdout}")
    artifacts = set()
    dependency_dirs = set()
    for line in result.stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("reason") != "compiler-artifact":
            continue
        # Cargo can place each crate in a separate build directory. Use its
        # reported artifact paths for rustc's transitive dependency lookup.
        dependency_dirs.update(
            Path(filename).resolve().parent for filename in event.get("filenames", [])
        )
        if (
            event.get("target", {}).get("name") == "velr"
            and event.get("package_id") == package["id"]
        ):
            if Path(event["target"]["src_path"]).resolve() != expected_source:
                raise CheckFailure("driver artifact belongs to a different source file")
            artifacts.update(
                Path(filename)
                for filename in event.get("filenames", [])
                if filename.endswith(".rmeta")
            )
    if len(artifacts) != 1:
        raise CheckFailure(f"expected one exact velr metadata artifact, found {sorted(artifacts)}")
    artifact = artifacts.pop().resolve()
    dependencies = sorted(dependency_dirs)
    receipt.update(
        artifact=str(artifact), dependency_dirs=[str(path) for path in dependencies],
        cargo_command=command,
    )
    return artifact, dependencies


def source(setup: list[str], action: str, cleanup: list[str], positive: bool):
    lines = ["use velr::{QueryOptions, QueryParams, Velr};", "pub fn probe() {"]
    lines.extend(f"    {line}" for line in setup)
    if positive:
        lines.extend(f"    {line}" for line in cleanup)
    action_line = len(lines) + 1
    lines.append(f"    {action}")
    if not positive:
        lines.extend(f"    {line}" for line in cleanup)
    lines.append("}")
    return "\n".join(lines) + "\n", action_line


def cases():
    connection = {
        "one": ['let table = db.exec_one("RETURN 1 AS x").unwrap();'],
        "options": [
            'let table = db.exec_one_with_options("RETURN 1 AS x", QueryOptions::new()).unwrap();'
        ],
        "params": [
            'let table = db.exec_one_with_params("RETURN 1 AS x", QueryParams::new()).unwrap();'
        ],
        "stream": [
            'let mut stream = db.exec("RETURN 1 AS x").unwrap();',
            "let table = stream.next_table().unwrap().unwrap();",
            "drop(stream);",
        ],
    }
    for producer, setup in connection.items():
        prefix = ["let db = Velr::open(None).unwrap();", *setup]
        for action_name, action in [
            ("drop", "drop(db);"),
            ("thread", 'std::thread::spawn(move || db.run("CREATE (:N)"));'),
        ]:
            yield f"connection_{producer}_{action_name}", prefix, action, ["drop(table);"]

    transaction = {
        "one": ['let table = tx.exec_one("RETURN 1 AS x").unwrap();'],
        "options": [
            'let table = tx.exec_one_with_options("RETURN 1 AS x", QueryOptions::new()).unwrap();'
        ],
        "params": [
            'let table = tx.exec_one_with_params("RETURN 1 AS x", QueryParams::new()).unwrap();'
        ],
        "stream": [
            'let mut stream = tx.exec("RETURN 1 AS x").unwrap();',
            "let table = stream.next_table().unwrap().unwrap();",
            "drop(stream);",
        ],
    }
    for producer, setup in transaction.items():
        prefix = ["let db = Velr::open(None).unwrap();", "let tx = db.begin_tx().unwrap();", *setup]
        for action_name, action in [
            ("commit", "tx.commit().unwrap();"),
            ("rollback", "tx.rollback().unwrap();"),
            ("drop", "drop(tx);"),
        ]:
            yield f"transaction_{producer}_{action_name}", prefix, action, ["drop(table);"]

    cursor = [
        "let db = Velr::open(None).unwrap();",
        'let mut table = db.exec_one("RETURN 1 AS x").unwrap();',
        "let rows = table.rows().unwrap();",
    ]
    yield "cursor_blocks_table_drop", cursor, "drop(table);", ["drop(rows);"]
    yield "cursor_blocks_connection_drop", cursor, "drop(db);", ["drop(rows);", "drop(table);"]


def compile_case(directory: Path, metadata: Path, dependencies: list[Path], name: str, code: str):
    path = directory / f"{name}.rs"
    path.write_text(code)
    command = [
        os.environ.get("RUSTC", "rustc"),
        "--edition=2021",
        "--crate-type=lib",
        "--emit=metadata",
        "--error-format=json",
        "--extern",
        f"velr={metadata}",
        *[argument for path in dependencies for argument in ("-L", f"dependency={path}")],
        "-o",
        str(directory / f"{name}.rmeta"),
        str(path),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    diagnostics = []
    for line in result.stderr.splitlines():
        try:
            diagnostics.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return path, result, diagnostics


def check_case(directory: Path, metadata: Path, dependencies: Path, name: str, setup, action, cleanup):
    positive, _ = source(setup, action, cleanup, positive=True)
    _, result, _ = compile_case(directory, metadata, dependencies, f"{name}_positive", positive)
    if result.returncode:
        raise CheckFailure(f"{name}: valid companion failed:\n{positive}\n{result.stderr}")

    forbidden, action_line = source(setup, action, cleanup, positive=False)
    path, result, diagnostics = compile_case(directory, metadata, dependencies, name, forbidden)
    matched = any(
        diagnostic.get("level") == "error"
        and (diagnostic.get("code") or {}).get("code") == "E0505"
        and any(
            span.get("is_primary")
            and span.get("line_start") == action_line
            and Path(span.get("file_name", "")).resolve() == path.resolve()
            for span in diagnostic.get("spans", [])
        )
        for diagnostic in diagnostics
    )
    if result.returncode == 0 or not matched:
        raise CheckFailure(
            f"{name}: expected E0505 on owner action at line {action_line}; "
            f"compiler exited {result.returncode}:\n{forbidden}\n{result.stderr}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "Cargo.toml",
    )
    features = parser.add_mutually_exclusive_group()
    features.add_argument("--all-features", action="store_true")
    features.add_argument("--no-default-features", action="store_true")
    parser.add_argument("--receipt", type=Path, help="write source and metadata provenance as JSON")
    args = parser.parse_args()
    flags = ["--all-features"] if args.all_features else []
    if args.no_default_features:
        flags = ["--no-default-features"]
    receipt = {"feature_flags": flags, "validated_cases": 0}
    artifact = None
    try:
        contracts = list(cases())
        if len(contracts) != 22 or len({case[0] for case in contracts}) != 22:
            raise CheckFailure("expected 22 distinct table lifetime contracts")
        artifact, dependencies = driver_metadata(args.manifest_path.resolve(), flags, receipt)
        with tempfile.TemporaryDirectory(prefix="velr-table-lifetime-contracts-") as temporary:
            directory = Path(temporary)
            data = artifact.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            receipt["artifact_sha256_before"] = digest
            metadata = directory / artifact.name
            metadata.write_bytes(data)
            if hashlib.sha256(artifact.read_bytes()).hexdigest() != digest:
                raise CheckFailure("driver metadata changed while taking its snapshot")
            receipt["snapshot_sha256"] = hashlib.sha256(metadata.read_bytes()).hexdigest()
            count = 0
            for name, setup, action, cleanup in contracts:
                check_case(directory, metadata, dependencies, name, setup, action, cleanup)
                count += 1
                receipt["validated_cases"] = count
        source_file = Path(receipt["source_path"])
        receipt["source_sha256_after"] = hashlib.sha256(source_file.read_bytes()).hexdigest()
        if receipt["source_sha256_after"] != receipt["source_sha256_before"]:
            raise CheckFailure("driver source changed during the contract checks")
        if hashlib.sha256(artifact.read_bytes()).hexdigest() != digest:
            raise CheckFailure("driver metadata changed during the contract checks")
        print(f"Validated {count} forbidden lifetime cases and {count} positive companions.")
        receipt["passed"] = True
        return 0
    except (CheckFailure, OSError) as error:
        receipt["error"] = str(error)
        print(error, file=sys.stderr)
        return 1
    finally:
        if artifact is not None:
            receipt["artifact_sha256_after"] = hashlib.sha256(artifact.read_bytes()).hexdigest()
        if "source_path" in receipt:
            receipt["source_sha256_after"] = hashlib.sha256(Path(receipt["source_path"]).read_bytes()).hexdigest()
        if args.receipt:
            args.receipt.write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    sys.exit(main())
