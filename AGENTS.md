<!--
scope: epitelesis repository agent conventions
defers_to: CLAUDE.md for implementation conventions; ARCHITECTURE.md for the public contract
tightens: nested AGENTS.md files may narrow rules within their directory
-->

# AGENTS.md

## Purpose

Epitelesis is the typed subprocess lifecycle substrate. Work here must keep
execution policy explicit, supervisor ownership singular, errors/evidence
typed, and sync/async semantics aligned.

## Fixed v1 invariants

- `Command` typestate requires a bounded deadline or an unbounded choice with a
  reason before execution.
- Environment defaults to real `Clean`/`env_clear`; `Allowlist` is selective;
  `InheritAll` requires a reason.
- Capture defaults to 10 MiB independently for stdout and stderr and fails
  closed at the limit. Truncation and exceptional unbounded capture are
  explicit policies; managed streaming requires the fallible structural
  `streaming()` transition, which rejects non-default capture policies.
- One supervisor owns process-group termination before reap, bounded capture
  cleanup, and aggregate evidence.
- `ManagedChild` retains deadline, cancellation, and reap ownership while the
  caller owns streaming bytes and backpressure.
- Unsupported backends, including Unix targets without rustix `waitid`, return
  a typed result before spawn.
- Unix process groups are cleanup containment, not a security sandbox; a
  hostile child can escape with `setsid`.

## Release truth

The workspace version, local `epitelesis` version in `Cargo.lock`, release
manifest, README dependency marker, and `_llm/current_state.toml` marker are
one release fact. Run:

```bash
python3 scripts/verify_release_truth.py
python3 -m unittest discover -s scripts/tests -v
```

Ordinary changes require the matching local release tag. Only the dedicated PR
workflow may select prospective mode, and only for a trusted Release Please PR
whose five release files changed together from the explicit base commit. Do not
duplicate release versions outside the marked lines.

## Gate

The full local gate is:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
cargo test --workspace --all-features --doc
```

Forge is the canonical review, merge, and gate authority. Its native
`gate / gate-attestation` context preserves the full Rust gate above, with only
the enumerated documentation-only exemption. GitHub is a provider mirror for
Release Please and security scanning; its builds never establish merge truth.

## Change discipline

Preserve unrelated worktree changes. Keep typed public errors non-exhaustive.
The `async` feature is additive and must not add Tokio to default builds. Tests
must use commands available on the supported CI and development platforms.

Commits use `<type>(<scope>): <imperative description>`, with `epitelesis` or
`repo` as the scope and a subject no longer than 72 characters.
