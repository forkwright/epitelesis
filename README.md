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
epitelesis = { git = "https://github.com/forkwright/epitelesis", tag = "v0.2.0" } # x-release-please-version
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

Captured stdout and stderr each default to a 10 MiB limit. Reaching either
limit fails closed. A caller may instead choose explicit truncation, streaming,
or exceptional unbounded capture; unbounded capture requires justification.

## Lifecycle ownership

One supervisor owns the invocation from spawn through final evidence. On Unix,
it creates and supervises a process group, kills the group before reaping,
performs bounded capture cleanup, and aggregates exit, termination, and capture
evidence rather than losing later failures behind the first one.

`ManagedChild` retains ownership of deadline handling, cancellation, and reap
for caller-managed execution. Returning or dropping a handle must not silently
detach those obligations.

Platforms without the required process-group lifecycle return a typed
unsupported error. Process groups are containment for ordinary descendant
cleanup, not a security sandbox: a hostile child can call `setsid` and escape
the group.

## Evidence model

Success and failure are typed outcomes. Evidence includes the observed exit or
termination state, capture results governed by the selected policy, elapsed
time, and any cleanup failures. Callers decide how to render or redact bytes;
Epitelesis transports evidence and does not treat captured output as trusted.

## Cargo features

The default build is synchronous. The additive `async` feature enables the
asynchronous runner without making Tokio a default dependency. Sync and async
entry points share the same invocation policies and supervisor invariants.

## Non-goals

Epitelesis does not provide command discovery, retry policy, shell parsing,
argument validation, credential redaction, or a security sandbox.

## License

Apache-2.0 OR MIT.
