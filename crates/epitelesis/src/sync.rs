//! Synchronous executor for [`Command`].
//!
//! Always-available; the `async` feature gate is only required for
//! [`crate::spawn`]. The sync runner uses `std::process::Command` directly —
//! this module IS the wrapper substrate that the
//! `RUST/no-direct-process-command` rule directs all other fleet code to.

use std::io::Read;
use std::process::{Child, Command as StdCommand, ExitStatus, Stdio}; // kanon:ignore RUST/no-direct-process-command -- epitelesis is the fleet command-execution substrate this rule routes callers to; raw Command use here is the rule's own exemption (basanos dropped the path carve-out when the crate left the kanon tree)
use std::thread;
use std::time::{Duration, Instant};

use snafu::ResultExt as _;

use crate::Command;
use crate::command::EnvOp;
use crate::error::{Error, IoSnafu, Result, SpawnFailedSnafu, TimeoutSnafu};
use crate::output::Output;

/// Run `cmd` to completion and return its captured [`Output`].
///
/// On success (`exit status == 0`) returns `Ok(Output)`. On non-zero exit
/// returns [`crate::Error::NonZeroExit`] with the same `Output` payload, so
/// callers retain access to stdout/stderr regardless of which arm they hit.
///
/// A configured [`Command::timeout`] is enforced; if it elapses, the child is
/// killed and [`crate::Error::Timeout`] is returned carrying the partial
/// stdout/stderr captured before the deadline. Spawn-time and wait-time io
/// failures surface as [`crate::Error::SpawnFailed`] / [`crate::Error::Io`]
/// respectively; on every failure path the child is reaped and the pipe
/// reader threads are joined before the error is returned.
///
/// A piped stdin is closed immediately after spawn — `run` never exposes the
/// handle, so holding it open could only stall a stdin-reading child.
///
/// # Tracing
///
/// Each invocation opens a `tracing::info_span!("epitelesis.run")` recording
/// `program`, `arg_count`, captured `status`, and elapsed `duration_ms`.
pub fn run(cmd: Command) -> Result<Output> {
    let program_display = cmd.program.display().to_string();
    let timeout = cmd.timeout;
    let span = tracing::info_span!(
        "epitelesis.run",
        program = %program_display,
        arg_count = cmd.args.len(),
        timeout_ms = timeout.map(duration_ms_saturating),
    );
    let _enter = span.enter();

    let mut std_cmd = std_command(cmd);

    let started = Instant::now();
    let mut child = std_cmd.spawn().context(SpawnFailedSnafu {
        program: program_display.clone(),
    })?;

    // WHY: `run` never exposes the stdin handle, so a caller-piped stdin can
    // never be written to. Dropping it closes the write end immediately —
    // otherwise a stdin-reading child (e.g. `cat`) never sees EOF and hangs
    // until the timeout (or forever without one). `Child::wait` does this
    // internally, but the poll-based timeout path uses `try_wait`, which
    // does not.
    drop(child.stdin.take());

    // WHY: drain stdout/stderr on dedicated threads so the child never blocks
    // writing into a full OS pipe buffer (~64KB) while we wait for it to exit.
    // Reading the pipes only after `wait()` deadlocks on any output larger than
    // that buffer (e.g. `git diff` on a large branch): the child blocks on
    // write with no reader while we block in wait. Concurrent readers keep the
    // pipe draining for the child's whole lifetime.
    let stdout_reader = spawn_reader(child.stdout.take(), &program_display);
    let stderr_reader = spawn_reader(child.stderr.take(), &program_display);

    // INVARIANT: no path may leave `run` while a reader thread is unjoined or
    // the child is unreaped — every non-Exited arm below routes through
    // `reap_and_join` before returning.
    let status = if let Some(timeout) = timeout {
        match wait_with_timeout(&mut child, timeout, &program_display) {
            Ok(WaitOutcome::Exited(status)) => status,
            Ok(WaitOutcome::TimedOut) => {
                let (stdout, stderr) = reap_and_join(&mut child, stdout_reader, stderr_reader);
                return TimeoutSnafu {
                    program: program_display,
                    duration: timeout,
                    stdout,
                    stderr,
                }
                .fail();
            }
            Err(error) => {
                reap_and_join(&mut child, stdout_reader, stderr_reader);
                return Err(error);
            }
        }
    } else {
        match child.wait().context(IoSnafu {
            program: program_display.clone(),
        }) {
            Ok(status) => status,
            Err(error) => {
                reap_and_join(&mut child, stdout_reader, stderr_reader);
                return Err(error);
            }
        }
    };

    let stdout = join_reader(stdout_reader, &program_display)?;
    let stderr = join_reader(stderr_reader, &program_display)?;
    let duration = started.elapsed();

    tracing::debug!(
        status = %status,
        duration_ms = duration_ms_saturating(duration),
        stdout_bytes = stdout.len(),
        stderr_bytes = stderr.len(),
        "epitelesis.run completed"
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

/// Run `cmd` to completion and return captured output for both zero and
/// non-zero exits.
///
/// This is the semantic equivalent of `std::process::Command::output`: spawn,
/// timeout, and wait IO failures still return [`Error`], while non-zero child
/// exits return `Ok(Output)` so callers can inspect `status`, `stdout`, and
/// `stderr` themselves.
pub fn output(cmd: Command) -> Result<Output> {
    match run(cmd) {
        Ok(output) | Err(Error::NonZeroExit { output, .. }) => Ok(output),
        Err(error) => Err(error),
    }
}

/// Run `cmd` to completion and return its exit status for both zero and
/// non-zero exits.
///
/// This is the semantic equivalent of `std::process::Command::status`: spawn,
/// timeout, and wait IO failures still return [`Error`], while non-zero child
/// exits return `Ok(ExitStatus)`.
pub fn status(cmd: Command) -> Result<ExitStatus> {
    output(cmd).map(|output| output.status)
}

/// Spawn `cmd` and return the child handle.
///
/// Raw child handles are reserved for callers that must stream stdout/stderr,
/// feed stdin, or poll process state. The process creation still passes
/// through epitelesis so argument/env/cwd assembly has one substrate.
///
/// A configured [`Command::timeout`] cannot be enforced here — the caller
/// owns the child's lifecycle once the handle is returned — so it is logged
/// as a warning and otherwise ignored. Callers needing an enforced deadline
/// use [`crate::run`] / [`crate::spawn`].
pub fn spawn_child(cmd: Command) -> std::io::Result<Child> {
    // WHY: a raw handle has no runner to enforce the deadline. Warn instead
    // of silently dropping the configuration so the mismatch is visible in
    // traces rather than discovered during an incident.
    if let Some(timeout) = cmd.timeout {
        tracing::warn!(
            program = %cmd.program.display(),
            timeout_ms = duration_ms_saturating(timeout),
            "spawn_child cannot enforce Command::timeout; deadline ignored"
        );
    }
    std_command(cmd).spawn()
}

/// Assemble the `std::process::Command` for `cmd`.
///
/// This is the single builder-to-process translation both runners share:
/// stdio defaults (`null` stdin, `piped` stdout/stderr) with caller
/// overrides, cwd, and environment mutations replayed in builder-call order
/// so the later of `env` / `env_remove` wins per key — exactly the
/// `std::process::Command` contract. The async runner derives its
/// `tokio::process::Command` from this same function, so the two paths
/// cannot drift.
pub(crate) fn std_command(cmd: Command) -> StdCommand {
    let mut std_cmd = StdCommand::new(&cmd.program);
    std_cmd.args(&cmd.args);

    std_cmd.stdin(cmd.stdin.unwrap_or_else(Stdio::null));
    std_cmd.stdout(cmd.stdout.unwrap_or_else(Stdio::piped));
    std_cmd.stderr(cmd.stderr.unwrap_or_else(Stdio::piped));

    if let Some(cwd) = cmd.cwd {
        std_cmd.current_dir(cwd);
    }
    for op in cmd.env_ops {
        match op {
            EnvOp::Set(key, value) => {
                std_cmd.env(key, value);
            }
            EnvOp::Remove(key) => {
                std_cmd.env_remove(key);
            }
        }
    }

    std_cmd
}

enum WaitOutcome {
    Exited(std::process::ExitStatus),
    TimedOut,
}

/// Convert a [`Duration`] to milliseconds, saturating at `u64::MAX`.
///
/// WHY: `Duration::as_millis()` returns `u128`. Tracing fields are typed; we
/// pick `u64` because every realistic invocation comfortably fits and `u128`
/// is awkward in downstream sinks. Saturation keeps the structured field
/// honest without panicking on the impossible (>584 million-year) case.
pub(crate) fn duration_ms_saturating(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Poll `try_wait` until the child exits or `timeout` elapses.
///
/// WHY: `std::process::Child` has no native timed wait. Pulling in `wait_timeout`
/// or `subprocess` for one call site costs more than the small poll loop here.
/// 25 ms granularity bounds the wakeup overhead under sustained CI load while
/// keeping responsiveness well below human-perceptible latency for short
/// commands and well below the typical timeout magnitude (seconds-to-minutes)
/// for long ones.
fn wait_with_timeout(
    child: &mut std::process::Child,
    timeout: Duration,
    program: &str,
) -> Result<WaitOutcome> {
    // WHY: `Instant + Duration` panics on overflow. A timeout too large for
    // the Instant domain (`checked_add` returns None) cannot elapse within
    // any process lifetime, so it degrades to an unbounded wait instead of
    // panicking the caller's process over an absurd-but-legal Duration.
    let deadline = Instant::now().checked_add(timeout);
    let poll_interval = Duration::from_millis(25);

    loop {
        if let Some(status) = child.try_wait().context(IoSnafu { program })? {
            return Ok(WaitOutcome::Exited(status));
        }
        match deadline {
            Some(deadline) => {
                if Instant::now() >= deadline {
                    return Ok(WaitOutcome::TimedOut);
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                thread::sleep(poll_interval.min(remaining));
            }
            None => thread::sleep(poll_interval),
        }
    }
}

/// Spawn a thread that reads `pipe` to EOF, returning its captured bytes.
///
/// WHY: a child writing more than the OS pipe buffer (~64KB) blocks on `write`
/// until a reader consumes it. Reading concurrently on a dedicated thread keeps
/// the pipe draining for the child's whole lifetime, so `wait()` can complete
/// instead of deadlocking against a blocked writer. A `None` pipe (caller chose
/// a non-piped stdio) yields an empty buffer.
fn spawn_reader<R: Read + Send + 'static>(
    pipe: Option<R>,
    program: &str,
) -> thread::JoinHandle<Result<Vec<u8>>> {
    let program = program.to_owned();
    thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(mut pipe) = pipe {
            pipe.read_to_end(&mut buf).context(IoSnafu { program })?;
        }
        Ok(buf)
    })
}

/// Join a reader thread spawned by [`spawn_reader`], surfacing its captured
/// bytes or its IO error. A panicked reader thread is reported as an
/// [`Error::Io`] so the failure mode stays typed.
fn join_reader(handle: thread::JoinHandle<Result<Vec<u8>>>, program: &str) -> Result<Vec<u8>> {
    match handle.join() {
        Ok(result) => result,
        Err(_) => {
            Err::<Vec<u8>, std::io::Error>(std::io::Error::other("output reader thread panicked"))
                .context(IoSnafu { program })
        }
    }
}

/// Best-effort terminate + reap `child`, then join both reader threads,
/// returning whatever bytes each captured before the failure.
///
/// This is the single cleanup discipline for every non-success exit from
/// [`run`]: timeout, `wait` IO error, and `try_wait` IO error all route here
/// so no path can leak a detached drain thread or an unreaped (zombie)
/// child.
fn reap_and_join(
    child: &mut Child,
    stdout_reader: thread::JoinHandle<Result<Vec<u8>>>,
    stderr_reader: thread::JoinHandle<Result<Vec<u8>>>,
) -> (Vec<u8>, Vec<u8>) {
    // WHY: kill is best-effort — the child may already have exited between
    // the failed wait and this call (NotFound / InvalidInput). The
    // user-visible failure is whatever error the caller is about to return;
    // cleanup outcomes are logged for trace forensics, not propagated.
    if let Err(error) = child.kill() {
        tracing::debug!(
            ?error,
            "kill during cleanup failed (child likely already exited)"
        );
    }
    if let Err(error) = child.wait() {
        tracing::debug!(?error, "reap during cleanup failed");
    }
    // WHY: killing the child closed its pipe write ends, so the readers hit
    // EOF and exit; joining reclaims them instead of leaking detached
    // threads, and their buffers preserve the child's final output.
    let stdout = join_captured(stdout_reader, "stdout");
    let stderr = join_captured(stderr_reader, "stderr");
    (stdout, stderr)
}

/// Join a reader thread during cleanup, logging (never propagating) its
/// outcome and yielding whatever bytes it captured.
///
/// WHY: on the cleanup path an error is already being returned to the
/// caller; a secondary reader failure must not mask it, but silently
/// discarding the outcome would hide drain diagnostics from the trace.
fn join_captured(handle: thread::JoinHandle<Result<Vec<u8>>>, stream: &'static str) -> Vec<u8> {
    match handle.join() {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => {
            tracing::debug!(?error, stream, "reader thread failed during cleanup");
            Vec::new()
        }
        Err(_) => {
            tracing::debug!(stream, "reader thread panicked during cleanup");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::expect_used,
        reason = "unit tests assert on the named fixture step; expect surfaces the failing setup"
    )]

    use super::*;

    // WHY: proves the shared cleanup discipline directly: a live child
    // (sleep 30) with open pipes is killed, reaped, and both drain threads
    // joined promptly — the sequence every error path in `run` relies on.
    #[test]
    fn reap_and_join_kills_reaps_and_joins_promptly() {
        let started = Instant::now();
        let mut child = std_command(Command::new("sleep").arg("30"))
            .spawn()
            .expect("sleep must spawn");
        let stdout_reader = spawn_reader(child.stdout.take(), "sleep");
        let stderr_reader = spawn_reader(child.stderr.take(), "sleep");

        let (stdout, stderr) = reap_and_join(&mut child, stdout_reader, stderr_reader);

        assert!(stdout.is_empty(), "sleep writes nothing to stdout");
        assert!(stderr.is_empty(), "sleep writes nothing to stderr");
        let status = child
            .try_wait()
            .expect("try_wait after reap must not error")
            .expect("child must already be reaped, not still running");
        assert!(!status.success(), "killed child reports non-zero status");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "cleanup must not block on the child's natural 30s lifetime"
        );
    }

    // WHY: regression for the `Instant + Duration` overflow panic — an
    // oversized timeout must degrade to an unbounded wait, never abort.
    #[test]
    fn oversized_timeout_does_not_panic() {
        let mut child = std_command(Command::new("true"))
            .spawn()
            .expect("true must spawn");
        let outcome =
            wait_with_timeout(&mut child, Duration::MAX, "true").expect("wait must not error");
        assert!(
            matches!(outcome, WaitOutcome::Exited(status) if status.success()),
            "true exits zero well before any deadline"
        );
    }
}
