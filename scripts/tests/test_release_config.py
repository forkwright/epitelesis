from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "release-please-config.json"

SCRIPT = ROOT / "scripts" / "verify_release_truth.py"
SPEC = importlib.util.spec_from_file_location("verify_release_truth", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
verifier = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verifier
SPEC.loader.exec_module(verifier)

# WHY: release-please updates .release-please-manifest.json itself as the
# version store; it is the one release fact that is never an extra-file.
MANIFEST_PATH = ".release-please-manifest.json"

# WHY: release-please does not match a compound filter predicate, so a
# Cargo.lock updater written as `?(@.name=='epitelesis' && !@.source)` is a
# silent no-op and the lockfile version never moves. Verified in history:
# harmonia and hamma, which use the form below, carry Cargo.lock in their
# release commits; epitelesis, which used the compound form, did not carry it
# in `chore(main): release 0.2.0 (#8)`. The lockfile has exactly one
# source-free package, so the name predicate selected nothing extra anyway.
LOCK_JSONPATH = "$.package[?(!@.source)].version"


class ReleaseConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.package = json.loads(CONFIG.read_text(encoding="utf-8"))["packages"]["."]
        self.extra_files = self.package["extra-files"]

    def test_every_release_fact_has_an_updater(self) -> None:
        updated = {entry["path"] for entry in self.extra_files} | {MANIFEST_PATH}
        self.assertEqual(
            updated,
            set(verifier.RELEASE_PATHS),
            "release-please must update exactly the release facts "
            "verify_release_truth.py requires to move together",
        )

    def test_lockfile_updater_uses_a_matchable_predicate(self) -> None:
        lock = [entry for entry in self.extra_files if entry["path"] == "Cargo.lock"]
        self.assertEqual(len(lock), 1, "Cargo.lock needs exactly one updater")
        self.assertEqual(lock[0]["type"], "toml")
        self.assertEqual(lock[0]["jsonpath"], LOCK_JSONPATH)


if __name__ == "__main__":
    unittest.main()
