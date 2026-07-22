#!/usr/bin/env python3
"""Verify that Epitelesis release facts agree and are properly tagged."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path


RELEASE_PATHS = (
    "Cargo.toml",
    "Cargo.lock",
    ".release-please-manifest.json",
    "README.md",
    "_llm/current_state.toml",
)
MARKER = "x-release-please-version"
VERSION_TOKEN = re.compile(
    r"(?<![0-9A-Za-z])v?(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?)(?![0-9A-Za-z])"
)


class VerificationError(RuntimeError):
    """A release invariant was not satisfied."""


@dataclass(frozen=True)
class ReleaseFacts:
    cargo: str
    lock: str
    manifest: str
    readme: str
    machine_state: str

    def aligned_version(self, label: str) -> str:
        values = {
            "Cargo.toml": self.cargo,
            "Cargo.lock": self.lock,
            ".release-please-manifest.json": self.manifest,
            "README.md": self.readme,
            "_llm/current_state.toml": self.machine_state,
        }
        versions = set(values.values())
        if len(versions) != 1:
            details = ", ".join(f"{path}={version}" for path, version in values.items())
            raise VerificationError(f"{label} release facts are misaligned: {details}")
        return versions.pop()


def _git_environment() -> dict[str, str]:
    environment = os.environ.copy()
    for name in (
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_PREFIX",
    ):
        environment.pop(name, None)
    return environment


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        env=_git_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise VerificationError(f"git {' '.join(args)} failed: {detail}")
    return result.stdout


def _annotated_version(path: str, text: str) -> str:
    marker_lines = [line for line in text.splitlines() if MARKER in line]
    if len(marker_lines) != 1:
        raise VerificationError(
            f"{path} must contain exactly one same-line {MARKER} marker; found {len(marker_lines)}"
        )
    tokens = VERSION_TOKEN.findall(marker_lines[0])
    if len(tokens) != 1:
        raise VerificationError(
            f"{path} marker line must contain exactly one version token; found {len(tokens)}"
        )
    return tokens[0]


def _facts_from_texts(texts: dict[str, str]) -> ReleaseFacts:
    try:
        cargo = tomllib.loads(texts["Cargo.toml"])["workspace"]["package"]["version"]
        lock_document = tomllib.loads(texts["Cargo.lock"])
        local_packages = [
            package
            for package in lock_document["package"]
            if package.get("name") == "epitelesis" and "source" not in package
        ]
        if len(local_packages) != 1:
            raise VerificationError(
                "Cargo.lock must contain exactly one source-free epitelesis package; "
                f"found {len(local_packages)}"
            )
        lock = local_packages[0]["version"]
        manifest = json.loads(texts[".release-please-manifest.json"])["."]
        state = tomllib.loads(texts["_llm/current_state.toml"])["state"]["release_version"]
    except (
        AttributeError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
        tomllib.TOMLDecodeError,
    ) as error:
        raise VerificationError(f"could not read a release fact: {error}") from error

    readme = _annotated_version("README.md", texts["README.md"])
    state_marker = _annotated_version(
        "_llm/current_state.toml", texts["_llm/current_state.toml"]
    )
    if state != state_marker:
        raise VerificationError(
            "_llm/current_state.toml release_version does not match its updater marker"
        )
    return ReleaseFacts(str(cargo), str(lock), str(manifest), readme, str(state))


def _working_facts(root: Path) -> ReleaseFacts:
    try:
        texts = {path: (root / path).read_text(encoding="utf-8") for path in RELEASE_PATHS}
    except OSError as error:
        raise VerificationError(f"could not read release fact: {error}") from error
    return _facts_from_texts(texts)


def _commit_facts(root: Path, revision: str) -> ReleaseFacts:
    texts = {path: _git(root, "show", f"{revision}:{path}") for path in RELEASE_PATHS}
    return _facts_from_texts(texts)


def _require_tag(root: Path, version: str, label: str, revision: str) -> None:
    tag = f"refs/tags/v{version}"
    resolved = subprocess.run(
        ["git", "rev-parse", "--verify", f"{tag}^{{commit}}"],
        cwd=root,
        env=_git_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    if resolved.returncode != 0:
        raise VerificationError(f"{label} requires the local tag v{version}")
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", resolved.stdout.strip(), revision],
        cwd=root,
        env=_git_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    if ancestor.returncode == 1:
        raise VerificationError(
            f"{label} requires tag v{version} to be an ancestor of {revision}"
        )
    if ancestor.returncode != 0:
        detail = ancestor.stderr.strip() or ancestor.stdout.strip()
        raise VerificationError(f"could not verify tag ancestry: {detail}")


def _require_ancestor(root: Path, ancestor: str, revision: str, label: str) -> None:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, revision],
        cwd=root,
        env=_git_environment(),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 1:
        raise VerificationError(f"{label} must be an ancestor of {revision}")
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise VerificationError(f"could not verify revision ancestry: {detail}")


def verify(root: Path, *, prospective: bool = False, base: str | None = None) -> str:
    root = root.resolve()
    current_facts = _working_facts(root)

    if not prospective:
        if base is not None:
            raise VerificationError("--base is valid only with --prospective")
        version = current_facts.aligned_version("current")
        _require_tag(root, version, "ordinary mode", "HEAD")
        return version

    if not base:
        raise VerificationError("prospective mode requires an explicit --base revision")
    _git(root, "rev-parse", "--verify", f"{base}^{{commit}}")
    _require_ancestor(root, base, "HEAD", "prospective base")

    changed = {
        line
        for line in _git(root, "diff", "--name-only", base, "HEAD", "--", *RELEASE_PATHS).splitlines()
        if line
    }
    required = set(RELEASE_PATHS)
    if changed != required:
        missing = ", ".join(sorted(required - changed)) or "none"
        raise VerificationError(
            "prospective mode requires all five release facts to change together; "
            f"missing: {missing}"
        )

    base_version = _commit_facts(root, base).aligned_version("base")
    _require_tag(root, base_version, "prospective base", base)
    version = current_facts.aligned_version("prospective")
    if version == base_version:
        raise VerificationError("prospective release version must differ from the base version")
    return version


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="repository root (defaults to the parent of scripts/)",
    )
    parser.add_argument(
        "--prospective",
        action="store_true",
        help="allow an untagged release-please version after validating its base",
    )
    parser.add_argument(
        "--base",
        help="explicit base commit for prospective mode",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        version = verify(args.root, prospective=args.prospective, base=args.base)
    except (OSError, VerificationError) as error:
        print(f"release truth verification failed: {error}", file=sys.stderr)
        return 1
    mode = "prospective" if args.prospective else "ordinary"
    print(f"release truth verified: {version} ({mode} mode)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
