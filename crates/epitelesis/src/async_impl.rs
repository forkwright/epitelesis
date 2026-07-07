//! Asynchronous executor for [`Command`] (gated by the `async` feature).
//!
//! Uses `tokio::process::Command` and `tokio::time::timeout`. Mirrors the
//! semantics of [`crate::run`] by construction: the child process is
//! assembled from the same [`crate::sync::std_command`] translation the sync
//! runner uses, so stdio defaults and overrides, `env`/`env_remove`
//! call-order resolution, and cwd handling cannot drift between the two
//! paths. Success returns [`Output`], non-zero exit returns
//! [`crate::Error::NonZeroExit`] with the captured payload, and exceeded
//! timeouts return [`crate::Error::Timeout`] carrying the partial
//! stdout/stderr captured before the child was killed.

use std::time::Instant;

use snafu::ResultExt as _;
use tokio::io::AsyncReadExt as _;
use tokio::process::Command as TokioCommand;
use tracing::Instrument as _;

use crate::Command;
use crate::error::{IoSnafu, Result, SpawnFailedSnafu, TimeoutSnafu};
use crate::output::Output;
use crate::sync::{duration_ms_saturating, std_command};

/// Asynchronously run `cmd` to completion and return its captured [`Output`].
///
/// See [`crate::run`] for the success/failure surface — the async path
/// behaves identically, including builder stdio overrides, environment
/// call-order semantics, and partial-output capture on timeout. This
/// function is `cancel-safe` only at the runtime level: dropping the
/// returned future drops the in-flight `tokio::process` handle, which sends
/// `SIGKILL` to the child via tokio's reaper (`kill_on_drop`), and the
/// detached pipe-drain tasks run to EOF and finish on their own.
///
/// # Tracing
///
/// Each invocation opens a `tracing::info_span!("epitelesis.spawn")`. The
/// span is attached to the future with [`tracing::Instrument`], so it is
/// entered and exited around each poll — never held across an `.await`.
pub async fn spawn(cmd: Command) -> Result<Output> {
    let program_display = cmd.program.display().to_string();
    let span = tracing::info_span!(
        "epitelesis.spawn",
        program = %program_display,
        arg_count = cmd.args.len(),
        timeout_ms = cmd.timeout.map(duration_ms_saturating),
    );
    // WHY: `.instrument(span)` scopes the span to each poll of the future.
    // A held `Span::enter()` guard would pin the span to the worker thread's
    // current-span slot across suspension points, silently attributing other
    // tasks' events to this spawn while the task is parked at an await.
    spawn_traced(cmd, program_display).instrument(span).await
}

/// Body of [`spawn`], separated so the whole execution — including every
/// `.await` — runs inside the instrumented future.
async fn spawn_traced(cmd: Command, program_display: String) -> Result<Output> {
    let timeout = cmd.timeout;

    // WHY: derive the tokio command from the sync path's single
    // builder-to-process translation; parity between the runners is then
    // structural rather than maintained by hand.
    let mut tokio_cmd = TokioCommand::from(std_command(cmd));
    // WHY: if this future is dropped mid-flight (caller cancellation), the
    // runtime's reaper kills and reaps the child so no orphan outlives the
    // invocation.
    tokio_cmd.kill_on_drop(true);

    let started = Instant::now();
    let mut child = tokio_cmd.spawn().context(SpawnFailedSnafu {
        program: program_display.clone(),
    })?;

    // WHY: `spawn` never exposes the stdin handle, so a caller-piped stdin
    // can never be written to. Dropping it closes the write end immediately
    // so a stdin-reading child sees EOF instead of stalling until the
    // timeout. Mirrors the sync runner.
    drop(child.stdin.take());

    // WHY: drain stdout/stderr concurrently with the wait so a child writing
    // more than the OS pipe buffer (~64KB) never deadlocks against a full
    // pipe, and the buffers survive a timeout for partial-output capture —
    // `wait_with_output` would forfeit them when its future is dropped.
    let stdout_task = spawn_reader(child.stdout.take(), &program_display);
    let stderr_task = spawn_reader(child.stderr.take(), &program_display);

    let status = if let Some(timeout) = timeout {
        match tokio::time::timeout(timeout, child.wait()).await {
            Ok(result) => result.context(IoSnafu {
                program: program_display.clone(),
            })?,
            Err(_elapsed) => {
                // WHY: kill is best-effort — the child may exit between the
                // deadline firing and the signal landing. `kill` also reaps;
                // the fallback `wait` covers a kill refused because the
                // child already exited but was not yet reaped.
                if let Err(error) = child.kill().await {
                    tracing::debug!(
                        ?error,
                        "kill after timeout failed (child likely already exited)"
                    );
                    if let Err(error) = child.wait().await {
                        tracing::debug!(?error, "reap after timeout failed");
                    }
                }
                // WHY: the kill closed the child's pipe write ends, so the
                // drain tasks hit EOF promptly; their buffers carry the
                // child's final output into the Timeout payload.
                let stdout = join_captured(stdout_task, "stdout").await;
                let stderr = join_captured(stderr_task, "stderr").await;
                return TimeoutSnafu {
                    program: program_display,
                    duration: timeout,
                    stdout,
                    stderr,
                }
                .fail();
            }
        }
    } else {
        // NOTE: on a wait IO error the `?` drops `child`; `kill_on_drop`
        // reaps it via the runtime, and the drain tasks self-complete at
        // EOF — tokio tasks need no join to be reclaimed.
        child.wait().await.context(IoSnafu {
            program: program_display.clone(),
        })?
    };

    let stdout = join_reader(stdout_task, &program_display).await?;
    let stderr = join_reader(stderr_task, &program_display).await?;
    let duration = started.elapsed();

    tracing::debug!(
        status = %status,
        duration_ms = duration_ms_saturating(duration),
        stdout_bytes = stdout.len(),
        stderr_bytes = stderr.len(),
        "epitelesis.spawn completed"
    );

    let output = Output {
        status,
        stdout,
        stderr,
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

/// Spawn a task that reads `pipe` to EOF, returning its captured bytes.
///
/// WHY: mirrors the sync runner's dedicated reader threads — the pipe keeps
/// draining for the child's whole lifetime, and the buffer survives a
/// timeout. A `None` pipe (caller chose a non-piped stdio) yields an empty
/// buffer.
fn spawn_reader<R>(pipe: Option<R>, program: &str) -> tokio::task::JoinHandle<Result<Vec<u8>>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let program = program.to_owned();
    tokio::spawn(
        // WHY: `.instrument(Span::current())` propagates the spawn span into
        // the drain task per the fleet tracing standard — a bare
        // `tokio::spawn` would emit its events outside the trace.
        async move {
            let mut buf = Vec::new();
            if let Some(mut pipe) = pipe {
                pipe.read_to_end(&mut buf)
                    .await
                    .context(IoSnafu { program })?;
            }
            Ok(buf)
        }
        .instrument(tracing::Span::current()),
    )
}

/// Join a drain task spawned by [`spawn_reader`], surfacing its captured
/// bytes or its IO error. A panicked or cancelled task is reported as an
/// [`crate::Error::Io`] so the failure mode stays typed.
async fn join_reader(
    handle: tokio::task::JoinHandle<Result<Vec<u8>>>,
    program: &str,
) -> Result<Vec<u8>> {
    match handle.await {
        Ok(result) => result,
        Err(join_error) => Err::<Vec<u8>, std::io::Error>(std::io::Error::other(join_error))
            .context(IoSnafu { program }),
    }
}

/// Join a drain task during timeout cleanup, logging (never propagating) its
/// outcome and yielding whatever bytes it captured.
///
/// WHY: the Timeout error is already being returned; a secondary drain
/// failure must not mask it, but silently discarding the outcome would hide
/// diagnostics from the trace.
async fn join_captured(
    handle: tokio::task::JoinHandle<Result<Vec<u8>>>,
    stream: &'static str,
) -> Vec<u8> {
    match handle.await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => {
            tracing::debug!(?error, stream, "drain task failed during timeout cleanup");
            Vec::new()
        }
        Err(join_error) => {
            tracing::debug!(
                ?join_error,
                stream,
                "drain task panicked or was cancelled during timeout cleanup"
            );
            Vec::new()
        }
    }
}
