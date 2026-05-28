<!--
scope: epitelesis repo conventions (command-execution substrate)
defers_to: AGENTS.md for dispatch conventions; ARCHITECTURE.md for the public surface
tightens: none today; future per-crate CLAUDE.md files can narrow conventions within their blast radius
-->

# CLAUDE.md

## At a glance

Repo-level conventions for AI coding agents working on epitelesis. Single Rust
crate (`crates/epitelesis/`) ratified Tier B EXTRACT-NOW (kanon D-067) as the
fleet-wide command-execution wrapper substrate.

## Standards

Universal: fleet standards via `~/dev/kanon/crates/basanos/standards/`.
Particularly relevant here:

- `RUST.md#command-execution` — the rule this crate exists to satisfy.
- `RUST.md#errors` — snafu typed errors, `#[non_exhaustive]`.
- `PHILOSOPHY.md` — presence/attention before reach.
- `GNOMON.md` — naming L1-L4.

## Structure

Workspace with one crate at `crates/epitelesis/`. Future siblings (e.g. a CLI
or test-helper crate) can land alongside without restructuring.

```
crates/epitelesis/
├── src/
│   ├── lib.rs          — public surface + module wiring
│   ├── command.rs      — Command builder
│   ├── error.rs        — Error / Result (snafu)
│   ├── output.rs       — Output (status + stdout + stderr + duration)
│   ├── sync.rs         — sync run / output / status / spawn_child
│   └── async_impl.rs   — async spawn (feature = "async")
└── tests/
    ├── run.rs          — sync integration coverage
    └── spawn_async.rs  — async integration coverage (feature-gated)
```

## Commands

```bash
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
cargo doc --workspace --no-deps --all-features
```

## Key invariants

- **No direct `std::process::Command` outside the substrate.** This crate is
  the canonical wrapper; consumers depend on it instead of reinventing.
- **`Command` is not `Clone`.** Each invocation owns its configuration;
  cloning would invite shared-mutable-builder patterns. Construct a fresh
  `Command` per call when launching the same logical command repeatedly.
- **Every subprocess must declare a deadline.** `Command::timeout` is the
  contract; unbounded invocations omit the method deliberately.
- **Non-zero exits are typed, not stringified.** `Error::NonZeroExit` carries
  the captured `Output`; callers that treat non-zero as expected (e.g. `grep`
  returning 1 on no match) match on the variant and inspect the payload.
- **The `async` feature is additive.** Default builds stay sync-only and do
  not pull tokio.

## Before submitting

1. `cargo check --workspace --all-features` passes
2. `cargo clippy --workspace --all-targets --all-features -- -D warnings`: zero warnings
3. `cargo nextest run --workspace --all-features` green
4. `kanon gate .` passes locally — the `Gate-Passed:` trailer is required by
   branch protection on `main`.

## Git

Conventional commits: `<type>(<scope>): <description>`. Types: `feat`, `fix`,
`refactor`, `docs`, `test`, `chore`, `ci`, `perf`. Present-tense imperative,
first line ≤72 chars. Scope is `epitelesis` (or `repo` for root-level changes).

Branch from `main`. Rebase before pushing. Always squash merge.
