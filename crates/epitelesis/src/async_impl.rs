//! Asynchronous executor for [`Command`] (gated by the `async` feature).
//!
//! Uses `tokio::process::Command` and `tokio::time::timeout`. Mirrors the
//! semantics of [`crate::run`]: success returns [`Output`], non-zero exit
//! returns [`crate::Error::NonZeroExit`] with the captured payload, and
//! exceeded timeouts return [`crate::Error::Timeout`] after killing the child.

use std::process::Stdio;
use std::time::{Duration, Instant};

use snafu::ResultExt as _;
use tokio::process::Command as TokioCommand;

use crate::Command;
use crate::error::{IoSnafu, Result, SpawnFailedSnafu, TimeoutSnafu};
use crate::output::Output;

/// Asynchronously run `cmd` to completion and return its captured [`Output`].
///
/// See [`crate::run`] for the success/failure surface — the async path
/// behaves identically. This function is `cancel-safe` only at the runtime
/// level: dropping the returned future drops the in-flight `tokio::process`
/// handle, which sends `SIGKILL` to the child via tokio's reaper.
pub async fn spawn(cmd: Command) -> Result<Output> {
    let program_display = cmd.program.display().to_string();
    let span = tracing::info_span!(
        "epitelesis.spawn",
        program = %program_display,
        arg_count = cmd.args.len(),
        timeout_ms = cmd.timeout.map(duration_ms_saturating),
    );
    let _enter = span.enter();

    let mut tokio_cmd = TokioCommand::new(&cmd.program);
    tokio_cmd
        .args(&cmd.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    if let Some(cwd) = &cmd.cwd {
        tokio_cmd.current_dir(cwd);
    }
    for (key, value) in &cmd.env {
        tokio_cmd.env(key, value);
    }

    let started = Instant::now();
    let child = tokio_cmd.spawn().context(SpawnFailedSnafu {
        program: program_display.clone(),
    })?;

    let raw = if let Some(timeout) = cmd.timeout {
        match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(result) => result.context(IoSnafu {
                program: program_display.clone(),
            })?,
            Err(_) => {
                // tokio's `kill_on_drop(true)` reaps the child as the future
                // unwinds, so we don't need a manual kill. Surface as Timeout.
                return TimeoutSnafu {
                    program: program_display,
                    duration: timeout,
                }
                .fail();
            }
        }
    } else {
        child.wait_with_output().await.context(IoSnafu {
            program: program_display.clone(),
        })?
    };

    let duration = started.elapsed();

    tracing::debug!(
        status = %raw.status,
        duration_ms = duration_ms_saturating(duration),
        stdout_bytes = raw.stdout.len(),
        stderr_bytes = raw.stderr.len(),
        "epitelesis.spawn completed"
    );

    let output = Output {
        status: raw.status,
        stdout: raw.stdout,
        stderr: raw.stderr,
        duration,
    };

    if output.status.success() {
        Ok(output)
    } else {
        crate::error::NonZeroExitSnafu {
            program: program_display,
            status: output.status,
            output: output.clone(),
        }
        .fail()
    }
}

/// Convert a [`Duration`] to milliseconds, saturating at `u64::MAX`.
///
/// WHY: tracing fields are typed; `u64` is small enough for every realistic
/// invocation and friendlier to downstream sinks than `u128`. Saturation
/// avoids the impossible-overflow panic without distorting realistic values.
fn duration_ms_saturating(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}
