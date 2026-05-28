# Architecture

Epitelesis is the fleet's command-execution substrate. It ships one Rust
crate from one workspace and exposes a single typed surface every consumer
goes through to spawn subprocesses.

## Layers

```
consumer crates (kanon::pragma, kanon::archeion, kanon::basanos, …,
                 future fleet crates)
        │
        ▼
epitelesis::{Command, run, output, status, spawn_child, spawn}
        │
        ▼
std::process::Command  /  tokio::process::Command (feature = "async")
```

The crate is intentionally thin. Its job is centralisation, not
abstraction-for-its-own-sake — every fleet subprocess passes through one
place so argument assembly, env/cwd passthrough, timeout enforcement,
stdout/stderr capture, structured errors, and tracing spans live in one
file each instead of being reinvented per call site.

## Modules

| Module | Role |
|---|---|
| `command` | Builder type capturing program, args, env, cwd, timeout, stdio. Not `Clone` by design — each invocation owns its configuration. |
| `output` | Captured `Output` (status + stdout + stderr + elapsed duration). Returned by `run` on success and carried inside `Error::NonZeroExit` on failure. |
| `error` | Typed `Error` enum (snafu, `#[non_exhaustive]`) with `SpawnFailed`, `NonZeroExit`, `Timeout`, `Io` variants. |
| `sync` | Sync runners (`run`, `output`, `status`, `spawn_child`) over `std::process::Command`. Concurrent pipe drain via dedicated reader threads so children writing more than the OS pipe buffer (~64 KiB) do not deadlock against `wait()`. |
| `async_impl` | Async `spawn` over `tokio::process::Command` (feature = `async`). Mirrors the sync semantics: success returns `Output`, non-zero exit returns `Error::NonZeroExit` with the payload, exceeded timeout returns `Error::Timeout`. |

## Public surface

Every `pub` item reachable from the crate root and every Cargo feature exposed
by the crate is the SemVer contract:

- `Command` builder methods, `Output` fields/methods, `Error` variants and
  variant fields, `run`/`output`/`status`/`spawn_child` signatures, and
  `spawn` signature (under `feature = "async"`).
- Adding variants is non-breaking (the enum is `#[non_exhaustive]`).
- Removing or renaming any of the above is a major version bump.

## Consumer boundary

Epitelesis does not own retry policy, command-discovery (PATH lookups),
sandboxing, or any domain-specific subprocess vocabulary (git, ssh, …). Those
stay in the consuming crate. Epitelesis owns the substrate that every
subprocess invocation passes through; consumers compose on top.

## Why one crate, one workspace

The workspace shape (rather than a single-crate-at-root) leaves room for
adjacent helpers (an `epitelesis-cli` smoke binary, a test-helper crate for
fixtures) without restructuring. Today there is one crate.
