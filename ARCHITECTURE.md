# Architecture

Epitelesis is a typed boundary around one subprocess invocation. Configuration
becomes an executable command only after mandatory policy choices are present;
one supervisor then owns the process and all evidence until cleanup completes.

## Control flow

```text
Command typestate
  ├─ deadline: bounded | unbounded(reason)
  ├─ environment: Clean | Allowlist | InheritAll(reason)
  └─ bytes: capture bounded(10 MiB/stream, fail-closed)
            | truncate | unbounded(exceptional reason)
            | streaming()? structural transition
                         │
                         ▼
                    supervisor
              spawn → observe → terminate → reap
                         │
                         ▼
                  aggregated evidence
```

The typestate boundary prevents execution before a bounded deadline or an
explicit reason-bearing unbounded deadline has been selected. `Clean` is the
environment default and invokes real environment clearing. Environment
inheritance and unbounded resource use are visible exceptions, not implicit
fallbacks.

## Supervisor invariant

Exactly one supervisor owns each running invocation. It owns:

- process creation and observation;
- deadline and cancellation decisions;
- stdout and stderr capture in one fair, nonblocking `poll` loop;
- termination of the Unix process group before reap;
- bounded drain and capture cleanup; and
- construction of a single evidence-bearing result.

Each ready pipe receives a bounded byte/chunk turn before control checks resume.
There are no capture reader threads. Cleanup is part of the result, not
best-effort work after it. One two-second internal cleanup deadline owns group
signal, non-reaping leader observation, reap, and pipe settlement. The public
allowance is 2.1 seconds. If the leader remains unsettled, the supervisor
attempts to transfer its `Child` to the named background reaper; typed reap
disposition, failure, and cleanup evidence report whether that transfer
succeeded. Capture itself never moves to a background worker. The reaper
endpoint is created fallibly before process spawn, so thread-creation failure
cannot strand an already-running child.

`ManagedChild` is the supervised handoff produced only by the explicit,
fallible `Command<Ready>::streaming()` transition. The caller owns pipe bytes and
backpressure; the supervisor keeps deadline, cancellation, kill-before-reap,
and bounded cleanup together. `wait` closes retained stdin, typed `poll`
distinguishes running/success/error, `cancel` returns aggregate evidence only
after complete cancellation cleanup, and drop requests cancellation without
blocking while the detached supervisor finishes bounded cleanup.

## Portability boundary

The full lifecycle contract relies on Unix process groups and rustix `waitid`.
Non-Unix plus `cygwin`, `horizon`, `openbsd`, `redox`, and `wasi` return a typed
unsupported result before spawn instead of weakening kill or reap behavior.

Killing a process group is operational containment, not adversarial isolation.
A hostile descendant can call `setsid`, leave the group, and outlive group
termination. Callers needing a security boundary must use an operating-system
sandbox or container designed for that purpose.

## Capture bounds

The default maximum is 10 MiB independently for stdout and stderr. Crossing a
limit persists an overflow fact and fails closed even if the leader exits
immediately. Truncation and exceptional unbounded capture are explicit;
streaming is a separate command state whose transition rejects non-default
capture policies. Only pipe EOF produces `Complete` or `Truncated`. Read failure
and cleanup-deadline snapshots produce `Incomplete`.

## Public contract

Every post-spawn terminal result owns a boxed `LifecycleEvidence` with optional
leader status, recoverable elapsed time, typed signal and reap outcomes, stdout
then stderr reports, and a typed complete/incomplete/unknown cleanup outcome.
Timeout also retains its configured deadline. Deterministic primary precedence
is stdout limit, stderr limit, cancellation, deadline, capture failure,
supervision failure, then exit.

The SemVer surface includes typestate transitions, execution entry points,
environment and capture policy types, `ManagedChild`, output/evidence types,
typed errors, and Cargo features. Error enums remain non-exhaustive.

## Hosted release mechanism

Release truth is the aligned workspace version, local `epitelesis` lockfile
package, release manifest, README marker, and machine-state marker. Ordinary
verification requires the selected tag to be an ancestor of `HEAD`.
Prospective verification requires an authenticated Release Please PR, an
explicit base ancestor of `HEAD`, the base tag on that ancestry, and all five
facts changing together. The protected `gate / gate-attestation` context first
runs that policy, then accepts only an exact prose-only skip or a successful
hosted full Rust gate.

The synchronous default and additive `async` feature must implement the same
policy and lifecycle semantics. Async support may add runtime dependencies only
when that feature is enabled.

## Consumer boundary

Callers own program selection, argument validation, retry policy, redaction,
and any actual sandbox. Epitelesis owns subprocess policy enforcement,
lifecycle completion, and the evidence returned from that lifecycle. Each
consumer repository owns its manifest, lockfile, and CI dependency truth;
Epitelesis does not maintain a provider-side cutover ledger.
