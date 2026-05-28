<!--
scope: epitelesis repo  -  agent onboarding and dispatch conventions
defers_to: CLAUDE.md for full coding conventions; README.md for the public surface
tightens: per-crate AGENTS.md files (when added) can narrow conventions within the crate
-->

# AGENTS.md

## Purpose

Epitelesis is the project-wide command-execution wrapper substrate for the
forkwright fleet. Agents working here add or fix the wrapper surface
(builder, runners, error variants, tracing), keep the typed-error contract
honest, and maintain the docs/test set. The primary consumers are
kanon's `pragma`, `archeion`, `basanos`, `angelos`, `kanon`, `mnemosyne`,
and `stoa` crates; future fleet consumers add to the list.

## Crate

| Crate | Role |
|-------|------|
| `epitelesis` | Command builder + sync runners (`run`, `output`, `status`, `spawn_child`) + optional async runner (`spawn`, feature = `async`) + typed `Error` / `Output`. |

## Build notes

```
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
```

The async test path (`tests/spawn_async.rs`) requires the `async` feature.
The sync tests (`tests/run.rs`) run with default features.

## Gate

Before opening a PR, the gate must pass locally. Branch protection requires a
`Gate-Passed:` trailer on at least one commit in the PR:

```
Gate-Passed: kanon <version>
```

Run `kanon gate .` locally; the command prints the trailer to use. Docs-only
or workflow-only diffs may use a descriptive inline attestation (e.g.
`Gate-Passed: docs-only; no Rust changes`). Never fabricate the trailer.

## Commit convention

```
<type>(<scope>): <description in present tense imperative>
```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `ci`, `perf`.
Scope is `epitelesis` or `repo` for root-level changes. First line ≤ 72 chars.
Branch from `main`; one PR per focused change; squash-merge only.

## Key invariants

- **No direct `std::process::Command` outside this substrate.** That is the
  rule consumers fail when they bypass us. Don't add wrappers-of-wrappers.
- **Public surface is typed.** Builder methods, `Output`, and `Error` are the
  contract. Add variants behind `#[non_exhaustive]`; never remove.
- **`async` is additive.** Default builds must not pull tokio. Anything that
  needs tokio lives under the `async` feature gate.
- **Tests target portable POSIX commands.** `true`, `false`, `printenv`,
  `head`, `sleep` — anything available on the CI image and the menos
  workstation. Don't add tests that need a non-standard binary.
