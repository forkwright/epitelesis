from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "verify_release_truth.py"
SPEC = importlib.util.spec_from_file_location("verify_release_truth", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
verifier = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verifier
SPEC.loader.exec_module(verifier)


class ReleaseTruthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self._git("init", "--initial-branch=main")
        self._git("config", "user.name", "Release Truth Test")
        self._git("config", "user.email", "release-truth@example.invalid")
        self._write_all("0.2.0")
        self.base = self._commit("base release truth")
        self._git("tag", "v0.2.0")

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def _git(self, *args: str) -> str:
        environment = verifier._git_environment()
        environment.update(
            {
                "GIT_AUTHOR_DATE": "2000-01-01T00:00:00+00:00",
                "GIT_COMMITTER_DATE": "2000-01-01T00:00:00+00:00",
            }
        )
        return subprocess.run(
            ["git", *args],
            cwd=self.root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def _write(self, path: str, content: str) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    def _write_all(self, version: str) -> None:
        self._write(
            "Cargo.toml",
            f'[workspace]\n[workspace.package]\nversion = "{version}"\n',
        )
        self._write(".release-please-manifest.json", f'{{".":"{version}"}}\n')
        self._write(
            "README.md",
            f'tag = "v{version}" # x-release-please-version\n',
        )
        self._write(
            "_llm/current_state.toml",
            f'[state]\nrelease_version = "{version}" # x-release-please-version\n',
        )

    def _commit(self, message: str) -> str:
        self._git("add", ".")
        self._git("commit", "-m", message)
        return self._git("rev-parse", "HEAD")

    def test_current_tag_passes(self) -> None:
        self.assertEqual(verifier.verify(self.root), "0.2.0")

    def test_ordinary_mode_rejects_prospective_version(self) -> None:
        self._write_all("1.0.0")
        self._commit("prospective release")
        with self.assertRaisesRegex(verifier.VerificationError, "local tag v1.0.0"):
            verifier.verify(self.root)

    def test_complete_prospective_update_passes(self) -> None:
        self._write_all("1.0.0")
        self._commit("complete prospective release")
        self.assertEqual(
            verifier.verify(self.root, prospective=True, base=self.base),
            "1.0.0",
        )

    def test_incomplete_prospective_update_fails(self) -> None:
        self._write_all("1.0.0")
        self._write("Cargo.toml", '[workspace]\n[workspace.package]\nversion = "0.2.0"\n')
        self._commit("incomplete prospective release")
        with self.assertRaisesRegex(verifier.VerificationError, "all four release facts"):
            verifier.verify(self.root, prospective=True, base=self.base)

    def test_misalignment_fails(self) -> None:
        self._write(
            "README.md",
            'tag = "v9.9.9" # x-release-please-version\n',
        )
        with self.assertRaisesRegex(verifier.VerificationError, "misaligned"):
            verifier.verify(self.root)


if __name__ == "__main__":
    unittest.main()
