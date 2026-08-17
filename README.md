# epitelesis

*ἐπιτέλεσις — the process of executing-to-completion.*

Epitelesis is a typed subprocess lifecycle boundary. It makes execution policy
explicit before a command can run and returns structured evidence after the
supervisor has finished cleanup.

## Supported release

The dependency pin below is the supported release. Release Please updates this
line as a generic extra file; keep its marker and its single version token on
the same line.

```toml
[dependencies]
epitelesis = { git = "https://github.com/forkwright/epitelesis", tag = "v0.4.0" } # x-release-please-version
```

The remaining sections define the fixed breaking contract for v1. They do not
claim that the currently tagged release already exposes that surface.

## Invocation contract (v1)

`Command` uses typestate: an invocation is not runnable until the caller picks
one deadline policy:

- a bounded deadline; or
- explicitly unbounded execution with a reason.

Environment policy is also explicit and fail-closed:

- `Clean` is the default and applies real `env_clear` semantics;
- `Allowlist` exposes only named variables; and
- `InheritAll` requires a reason.

Captured stdout and stderr each default to a 10 MiB limit. Crossing either
limit fails closed. A caller may instead choose explicit truncation or
exceptional unbounded capture; unbounded capture requires justification.
Managed streaming is a different structural state created with the fallible
`command.streaming()?` transition. It rejects non-default capture policies so
non-default capture behavior cannot be silently discarded.

## Lifecycle ownership

One supervisor owns the invocation from spawn through final evidence. On Unix,
it creates and supervises a process group, pumps both capture pipes in one
fair nonblocking `poll` loop, kills the group before reaping, and performs
bounded cleanup. There are no capture reader threads. The caller-visible
cleanup allowance is 2.1 seconds. If the leader remains unsettled, the
supervisor attempts to transfer ownership to a deliberately named background
reaper; typed reap disposition, failures, and cleanup evidence report the
outcome. That fallback endpoint is established fallibly before process
creation, so its startup cannot strand a running child.

`ManagedChild` retains ownership of deadline handling, cancellation, and reap
for caller-managed execution. Construct it only from `Command<Ready>` via
`.streaming()?.spawn()` (or `spawn_managed(command.streaming()?)`). The caller
owns bytes and backpressure after taking a pipe handle. `wait` closes retained
stdin first, `poll` distinguishes running, successful, and failed terminal
states without blocking, and `cancel` returns aggregate evidence only after
complete cancellation cleanup. Drop requests cancellation without blocking;
the detached supervisor retains lifecycle ownership through bounded cleanup
and any required background-reaper handoff.

Platforms without the required process-group lifecycle return a typed
unsupported error before spawn. This includes non-Unix platforms and Unix
targets where rustix does not provide `waitid` (`cygwin`, `horizon`, `openbsd`,
`redox`, and `wasi`). Process groups are containment for ordinary descendant
cleanup, not a security sandbox: a hostile child can call `setsid` and escape.

## Evidence model

Every post-spawn terminal path owns one boxed `LifecycleEvidence`: optional
leader status, recoverable elapsed time, typed process-group signal and leader
reap outcomes, deterministic stdout/stderr reports, and a typed cleanup
outcome. Only EOF produces `Complete` or `Truncated`; read failures and cleanup
snapshots are `Incomplete`, while an unrecoverable adapter result is `Unknown`.
Timeout errors retain both their configured deadline and known actual elapsed
time. Callers decide how to render or redact bytes; Epitelesis does not trust
them.

Release truth has five synchronized files: `Cargo.toml`, the local
`epitelesis` package in `Cargo.lock`, `.release-please-manifest.json`, the README
dependency marker, and `_llm/current_state.toml`. The protected
`gate / gate-attestation` context verifies them before allowing either the
exact prose-only exemption or a successful hosted Rust build.

## Cargo features

The default build is synchronous. The additive `async` feature enables the
asynchronous runner without making Tokio a default dependency. Sync and async
entry points share the same invocation policies and supervisor invariants.

## Non-goals

Epitelesis does not provide command discovery, retry policy, shell parsing,
argument validation, credential redaction, or a security sandbox.

## License

Apache-2.0 OR MIT.
