# epitelesis

*ἐπιτέλεσις — the process of executing-to-completion.*

Epitelesis is the forkwright fleet's structurally safe subprocess substrate.
It makes lifetime, environment, capture bounds, process-group ownership, and
cleanup evidence part of the API rather than caller convention.

## Quickstart

```rust
use epitelesis::{Command, run};
use std::time::Duration;

let output = run(
    Command::new("/usr/bin/git")
        .arg("status")
        .arg("--porcelain")
        .deadline(Duration::from_secs(5))?,
)?;
# Ok::<(), epitelesis::Error>(())
```

`Command::new` returns `Command<Draft>`, which no runner accepts. A validated
`.deadline(...)` or `.unbounded(non_empty_reason)` produces `Command<Ready>`.
Deadline overflow is an `InvalidPolicy` error before spawn.

## Safety defaults

- The child environment begins with `env_clear`; choose Clean (default), an
  allowlist, or full inheritance with a non-empty reason. Explicit set/remove
  operations apply afterward. Bare names require an explicitly available PATH.
- Stdout and stderr independently fail closed at 10 MiB by default. Callers may
  choose a smaller/larger bound, bounded truncate-and-drain, or exceptional
  unbounded capture with a reason.
- Output distinguishes complete empty capture from redirection and records
  exact discarded-byte counts for truncation.
- On Unix, every child leads an owned process group. Timeout, capture limit,
  cancellation, async-future drop, and managed-handle drop signal the group,
  observe leader exit without reaping, then reap and settle capture.
- Non-Unix platforms return `UnsupportedCapability(OwnedProcessGroup)` before
  spawn until a Job Object backend exists.

## Public surface

| Item | Role |
|---|---|
| `Command<Draft/Ready>` | Typestate invocation builder. |
| `run`, `output`, `status` | Synchronous adapters over the shared supervisor. |
| `spawn` | Tokio adapter behind the `async` feature. |
| `spawn_managed`, `ManagedChild` | Streaming handle with background enforcement and no raw escape. |
| `CapturePolicy`, `EnvironmentPolicy` | Explicit capture and environment choices. |
| `Output`, `CapturedStream` | Sole owned status/buffers plus completeness evidence. |
| `Error` | Primary outcome with typed secondary signal/reap/capture/cleanup evidence. |

Process groups contain ordinary descendants; they are not a hostile-child
sandbox. A child that calls `setsid` can escape, and an escaped pipe owner is
reported truthfully as `CleanupIncomplete` after bounded cleanup.

## License

Apache-2.0 OR MIT.
