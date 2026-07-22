# Architecture

Epitelesis is a typed boundary around one subprocess invocation. Configuration
becomes an executable command only after mandatory policy choices are present;
one supervisor then owns the process and all evidence until cleanup completes.

## Control flow

```text
Command typestate
  ├─ deadline: bounded | unbounded(reason)
  ├─ environment: Clean | Allowlist | InheritAll(reason)
  └─ capture: bounded(10 MiB/stream, fail-closed)
              | truncate | stream | unbounded(exceptional reason)
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
- stdout and stderr capture under the selected limits;
- termination of the Unix process group before reap;
- bounded drain and capture cleanup; and
- construction of a single evidence-bearing result.

Cleanup is part of the result, not best-effort work after it. If termination,
reap, or capture cleanup also fails, that evidence is aggregated so the first
failure does not erase later lifecycle facts.

`ManagedChild` is the supervised handoff for caller-managed operation. It keeps
deadline, cancellation, and reap ownership together; it is not a detached raw
child handle.

## Portability boundary

The full lifecycle contract relies on Unix process groups. Unsupported
platforms return a typed unsupported result instead of silently weakening kill
or reap behavior.

Killing a process group is operational containment, not adversarial isolation.
A hostile descendant can call `setsid`, leave the group, and outlive group
termination. Callers needing a security boundary must use an operating-system
sandbox or container designed for that purpose.

## Capture bounds

The default maximum is 10 MiB independently for stdout and stderr. Crossing a
limit fails closed. Truncation and streaming must be chosen explicitly.
Exceptional unbounded capture requires a reason and remains the caller's memory
risk. Capture cleanup itself is bounded so a pipe that never closes cannot make
the supervisor wait forever after termination.

## Public contract

The SemVer surface includes typestate transitions, execution entry points,
environment and capture policy types, `ManagedChild`, output/evidence types,
typed errors, and Cargo features. Error enums remain non-exhaustive so new
evidence can be added without encouraging exhaustive downstream matches.

The synchronous default and additive `async` feature must implement the same
policy and lifecycle semantics. Async support may add runtime dependencies only
when that feature is enabled.

## Consumer boundary

Callers own program selection, argument validation, retry policy, redaction,
and any actual sandbox. Epitelesis owns subprocess policy enforcement,
lifecycle completion, and the evidence returned from that lifecycle.
