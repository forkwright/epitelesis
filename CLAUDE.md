<!--
scope: epitelesis repository implementation conventions
defers_to: AGENTS.md for repository workflow; ARCHITECTURE.md for lifecycle design
tightens: none
-->

# CLAUDE.md

## Working model

Epitelesis turns a policy-complete `Command` into one supervised subprocess
lifecycle. Changes should make illegal or ambiguous invocation states
unrepresentable and preserve evidence when cleanup has multiple outcomes.

## Implementation invariants

- A `Command` cannot run until typestate records either a bounded deadline or
  explicit unbounded execution with a reason.
- `Clean` performs `env_clear` and is the default. `Allowlist` copies only named
  variables. `InheritAll` is reason-bearing.
- Bounded capture defaults to 10 MiB for each stream and fails closed. Explicit
  policies cover truncation, streaming, and exceptional reason-bearing
  unbounded capture.
- A single supervisor owns spawn, observation, Unix process-group kill before
  reap, bounded capture cleanup, and aggregate evidence.
- `ManagedChild` owns deadline, cancellation, and reap rather than exposing a
  detachable raw child lifecycle.
- Non-Unix implementations that cannot satisfy the lifecycle return typed
  unsupported errors.
- Process groups are not a sandbox because hostile code can call `setsid`.

Sync and async runners implement the same contract. Async support remains
behind the additive `async` feature, and default builds do not pull Tokio.
Public policy, evidence, output, and error types are SemVer commitments; typed
error enums remain non-exhaustive.

## Evidence discipline

Do not discard termination, reap, or capture-cleanup facts when another error
happened first. Return aggregate structured evidence after bounded cleanup.
Captured bytes and child-provided messages are untrusted and are not logged by
default.

## Checks

Use the exact commands documented in AGENTS.md for the full Rust gate. Release
metadata changes also run the dependency-free Python verifier and its unittest
suite. Keep release facts on the two same-line generic updater markers plus the
workspace version and release manifest; do not add unmarked copies.

## Git

Use conventional commits with an imperative subject of at most 72 characters.
Branch from `main`, keep each change focused, and preserve unrelated worktree
state.
