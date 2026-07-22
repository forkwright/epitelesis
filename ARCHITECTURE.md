# Architecture

Epitelesis owns an invocation from policy declaration through process-group
settlement and leader reap. Public sync, Tokio, and managed-streaming surfaces
are thin adapters over one private supervisor state machine.

## Layers

```
Command<Draft>
    │ deadline(duration) / unbounded(reason)
    ▼
Command<Ready> ── validate env/PATH/backend before spawn
    ▼
Unix owned process group + armed child guard
    ▼
serialized exit / deadline / limit / cancellation outcome
    ▼
signal group → observe leader exit without reap → reap → settle capture
```

Non-Unix backends return `UnsupportedCapability(OwnedProcessGroup)` before
spawn. They do not silently degrade to direct-child ownership. A future Windows
implementation must use a Job Object.

## Modules

| Module | Role |
|---|---|
| `policy` | Typestate markers and explicit lifetime, environment, and capture policies. |
| `command` | Non-clone builder, pre-spawn validation, `env_clear` translation, and Unix process-group configuration. |
| `supervisor` | Private event-driven state machine, bounded capture workers, safe rustix signaling/wait observation, and armed lifecycle guard. |
| `managed` | Restricted streaming handle backed by a background supervisor. |
| `sync` | `run`, `output`, and `status` adapters. |
| `async_impl` | Tokio `spawn` adapter with future-drop cancellation. |
| `output` | Captured prefixes plus complete/truncated/redirected evidence. |
| `error` | Primary typed outcomes and retained secondary cleanup evidence. |

## Containment boundary

`process_group(0)` contains the leader and ordinary descendants. It is not a
hostile-child sandbox: a descendant can call `setsid` and escape. If an escaped
process retains a capture pipe past the shared cleanup deadline, the supervisor
returns `CleanupIncomplete` with exact unfinished streams and may leave only
those reader threads alive. Ordinary in-group descendants are signaled and all
capture workers settle before return.
