# epitelesis

*ἐπιτέλεσις  -  the process of executing-to-completion.*

The project-wide command-execution wrapper substrate for the forkwright fleet.
Every production subprocess invocation goes through one place: argument
assembly, environment and working-directory passthrough, timeout enforcement,
stdout/stderr capture, structured errors, and tracing spans live here so
consumers stop reinventing them per call site.

## Why

Direct `std::process::Command` use is forbidden in fleet code by the
[`RUST/no-direct-process-command`](https://github.com/forkwright/kanon/blob/main/crates/basanos/standards/RUST.md#command-execution)
rule. Raw `Command` invites forgotten timeout configuration, missed exit-code
handling, dropped argument quoting, and ad-hoc error types callers cannot match
on. Epitelesis centralises those concerns behind a single typed surface.

## Quickstart

```toml
[dependencies]
epitelesis = { git = "https://github.com/forkwright/epitelesis", tag = "v0.1.0" }
```

```rust
use epitelesis::{Command, run};
use std::time::Duration;

let output = run(
    Command::new("git")
        .arg("status")
        .arg("--porcelain")
        .timeout(Duration::from_secs(5)),
)?;
assert!(output.success());
# Ok::<(), epitelesis::Error>(())
```

## Surface

| Item | Role |
|---|---|
| `Command` | Builder capturing program, args, env, cwd, timeout, stdio. |
| `run` | Synchronous executor returning `Output` (success) or typed `Error`. |
| `output` | Synchronous captured-output helper preserving non-zero output. |
| `status` | Synchronous status helper preserving non-zero status. |
| `spawn_child` | Synchronous child-handle helper for streaming callers. |
| `spawn` | Asynchronous executor (gated by the `async` Cargo feature). |
| `Output` | Captured status, stdout, stderr, and elapsed duration. |
| `Error` | Typed error variants (snafu, `#[non_exhaustive]`). |

## Cargo features

| Feature | What it enables |
|---|---|
| (default) | Sync `run` / `output` / `status` / `spawn_child` over `std::process::Command`. |
| `async` | Adds `epitelesis::spawn` over `tokio::process::Command` and pulls tokio. |

## Errors

The error surface is typed and `#[non_exhaustive]`, so consumers match on
variant without losing the underlying `io::Error` chain:

- `Error::SpawnFailed`  -  kernel refused to spawn the child (program not on
  `PATH`, permission denied, fork failure).
- `Error::NonZeroExit`  -  child spawned and exited with a non-zero status.
  Carries the captured `Output` payload so callers retain access to
  `stdout`/`stderr`/`status` even on failure.
- `Error::Timeout`  -  configured `Command::timeout` elapsed; the runner has
  already killed and reaped the child by the time this error returns.
  Carries the partial `stdout`/`stderr` captured before the deadline.
- `Error::Io`  -  IO failure while waiting on the child or capturing output.

## Consumers

Today, every crate of [forkwright/kanon](https://github.com/forkwright/kanon)
(the fleet's standards and dispatch toolkit) that spawns a subprocess:
`pragma`, `archeion`, `basanos`, `angelos`, `kanon`, `mnemosyne`, `stoa`.
Future fleet consumers take a hard dependency on this crate instead of
reinventing the wrapper.

## License

Apache-2.0 OR MIT.
