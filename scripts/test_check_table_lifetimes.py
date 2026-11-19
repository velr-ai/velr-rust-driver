#!/usr/bin/env python3
"""Regression checks for Cargo artifact discovery used by lifetime contracts."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "check_table_lifetimes", Path(__file__).with_name("check_table_lifetimes.py")
)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class CargoArtifactTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="velr-cargo-artifact-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.manifest = self.root / "driver/Cargo.toml"
        self.source = self.root / "driver/src/lib.rs"
        self.source.parent.mkdir(parents=True)
        self.source.write_text("pub struct Velr;\n")
        self.manifest.write_text('[package]\nname = "velr"\nversion = "0.2.62"\n')
        self.package_id = f"path+{self.manifest.parent.as_uri()}#velr@0.2.62"
        self.target = self.root / "target"
        self.workspace = {
            "packages": [{
                "name": "velr", "id": self.package_id,
                "manifest_path": str(self.manifest),
            }],
            "target_directory": str(self.target),
        }
        environment = patch.dict(os.environ, {
            "CARGO": "fixture-cargo", "RUSTC": "fixture-rustc",
        })
        environment.start()
        self.addCleanup(environment.stop)

    def artifact(self, relative):
        path = self.target / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fixture metadata")
        return path

    def event(self, name, filenames, *, package_id=None, source=None):
        return {
            "reason": "compiler-artifact",
            "package_id": package_id or self.package_id,
            "target": {"name": name, "src_path": str(source or self.source)},
            "filenames": [str(path) for path in filenames],
            "fresh": True,
        }

    def discover(self, events, flags=()):
        processes = [
            subprocess.CompletedProcess([], 0, json.dumps(self.workspace), ""),
            subprocess.CompletedProcess(
                [], 0, "not a JSON message\n" + "\n".join(map(json.dumps, events)), ""
            ),
        ]
        receipt = {}
        with patch.object(checker.subprocess, "run", side_effect=processes) as run:
            result = checker.driver_metadata(self.manifest, list(flags), receipt)
        self.assertEqual(run.call_count, 2)
        command = run.call_args_list[1].args[0]
        self.assertIn("--locked", command)
        self.assertIn("--message-format=json", command)
        if flags:
            self.assertEqual(command[-len(flags):], list(flags))
        return result, receipt

    def test_legacy_shared_dependency_directory(self):
        metadata = self.artifact("debug/deps/libvelr-1234.rmeta")
        dependency = self.artifact("debug/deps/libvelr_types-5678.rmeta")
        (actual, directories), receipt = self.discover([
            self.event("velr_types", [dependency]),
            self.event("velr", [metadata]),
            self.event("velr", [metadata]),
            {"reason": "build-finished", "success": True,
             "filenames": [str(self.root / "not-an-artifact/ignored")]},
        ], ["--no-default-features"])
        self.assertEqual(actual, metadata)
        self.assertEqual(directories, [metadata.parent])
        self.assertEqual(receipt["package_id"], self.package_id)
        self.assertEqual(receipt["source_path"], str(self.source))

    def test_split_build_units_include_transitive_and_proc_macro_directories(self):
        metadata = self.artifact("debug/build/velr/1234/out/libvelr-1234.rmeta")
        direct = self.artifact("debug/build/velr-types/5678/out/libvelr_types-5678.rmeta")
        transitive = self.artifact("debug/build/serde/9abc/out/libserde-9abc.rlib")
        proc_macro = self.artifact("debug/build/serde_derive/def0/out/libserde_derive-def0.so")
        (actual, directories), _ = self.discover([
            self.event("velr", [metadata]),
            self.event("velr_types", [direct]),
            self.event("serde", [transitive]),
            self.event("serde_derive", [proc_macro]),
        ])
        self.assertEqual(actual, metadata)
        self.assertEqual(set(directories), {
            path.parent for path in [metadata, direct, transitive, proc_macro]
        })
        self.assertEqual(len(directories), 4)

    def test_symlinked_artifacts_resolve_and_dependency_paths_are_deduplicated(self):
        metadata = self.artifact("debug/build/velr/1234/out/libvelr-1234.rmeta")
        dependency = self.artifact("debug/build/dep/5678/out/libdep-5678.rmeta")
        alias = self.root / "target-alias"
        alias.symlink_to(self.target, target_is_directory=True)
        aliased_metadata = alias / metadata.relative_to(self.target)
        aliased_dependency = alias / dependency.relative_to(self.target)
        (actual, directories), _ = self.discover([
            self.event("velr", [aliased_metadata]),
            self.event("dep", [dependency, aliased_dependency]),
        ])
        self.assertEqual(actual, metadata)
        self.assertEqual(set(directories), {metadata.parent, dependency.parent})
        self.assertEqual(len(directories), 2)

    def test_matching_package_with_wrong_source_is_rejected(self):
        metadata = self.artifact("debug/deps/libvelr-1234.rmeta")
        with self.assertRaisesRegex(checker.CheckFailure, "different source"):
            self.discover([
                self.event("velr", [metadata], source=self.root / "other/src/lib.rs")
            ])

    def test_other_package_cannot_supply_driver_metadata(self):
        metadata = self.artifact("debug/deps/libvelr-1234.rmeta")
        with self.assertRaisesRegex(checker.CheckFailure, "one exact velr metadata"):
            self.discover([
                self.event("velr", [metadata], package_id="registry+other#velr@0.2.62")
            ])

    def test_wrong_target_cannot_supply_driver_metadata(self):
        metadata = self.artifact("debug/deps/libvelr-1234.rmeta")
        with self.assertRaisesRegex(checker.CheckFailure, "one exact velr metadata"):
            self.discover([self.event("other", [metadata])])

    def test_multiple_matching_metadata_artifacts_are_rejected(self):
        first = self.artifact("debug/build/velr/1234/out/libvelr-1234.rmeta")
        second = self.artifact("debug/build/velr/5678/out/libvelr-5678.rmeta")
        with self.assertRaisesRegex(checker.CheckFailure, "one exact velr metadata"):
            self.discover([self.event("velr", [first, second])])

    def test_compile_case_passes_exact_metadata_and_each_dependency_directory(self):
        metadata = self.artifact("debug/build/velr/1234/out/libvelr-1234.rmeta")
        dependencies = [metadata.parent, self.target / "debug/build/dep/5678/out"]
        code = "pub fn probe() {}\n"
        result = subprocess.CompletedProcess(
            [], 0, "", json.dumps({"level": "warning", "message": "fixture"}) + "\n"
        )
        with patch.object(checker.subprocess, "run", return_value=result) as run:
            path, actual_result, diagnostics = checker.compile_case(
                self.root, metadata, dependencies, "probe", code
            )
        command = run.call_args.args[0]
        self.assertEqual(command[0], "fixture-rustc")
        self.assertEqual(command[command.index("--extern") + 1], f"velr={metadata}")
        self.assertEqual([
            command[index + 1] for index, value in enumerate(command) if value == "-L"
        ], [f"dependency={directory}" for directory in dependencies])
        self.assertEqual(path.read_text(), code)
        self.assertIs(actual_result, result)
        self.assertEqual(diagnostics, [{"level": "warning", "message": "fixture"}])


if __name__ == "__main__":
    unittest.main()
