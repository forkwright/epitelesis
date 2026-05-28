//! Synchronous executor for [`Command`].
//!
//! Always-available; the `async` feature gate is only required for
//! [`crate::spawn`]. The sync runner uses `std::process::Command` directly —
//! this module is the wrapper substrate that the
//! `RUST/no-direct-process-command` rule exempts.

use std::io::Read;
use std::process::{Child, Command as StdCommand, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use snafu::ResultExt as _;

use crate::Command;
use crate::error::{Error, IoSnafu, Result, SpawnFailedSnafu, TimeoutSnafu};
use crate::output::Output;

/// Run `cmd` to completion and return its captured [`Output`].
///
/// On success (`exit status == 0`) returns `Ok(Output)`. On non-zero exit
/// returns [`crate::Error::NonZeroExit`] with the same `Output` payload, so
/// callers retain access to stdout/stderr regardless of which arm they hit.
///
/// A configured [`Command::timeout`] is enforced; if it elapses, the child is
/// killed and [`crate::Error::Timeout`] is returned. Spawn-time and
/// wait-time io failures surface as [`crate::Error::SpawnFailed`] /
/// [`crate::Error::Io`] respectively.
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

    // WHY: drain stdout/stderr on dedicated threads so the child never blocks
    // writing into a full OS pipe buffer (~64KB) while we wait for it to exit.
    // Reading the pipes only after `wait()` deadlocks on any output larger than
    // that buffer (e.g. `git diff` on a large branch): the child blocks on
    // write with no reader while we block in wait. Concurrent readers keep the
    // pipe draining for the child's whole lifetime.
    let stdout_reader = spawn_reader(child.stdout.take(), &program_display);
    let stderr_reader = spawn_reader(child.stderr.take(), &program_display);

    let status = if let Some(timeout) = timeout {
        match wait_with_timeout(&mut child, timeout, &program_display)? {
            WaitOutcome::Exited(status) => status,
            WaitOutcome::TimedOut => {
                // WHY: Kill is best-effort — the child may already have exited
                // between our last poll and this kill call (NotFound /
                // InvalidInput). We log at debug for visibility but do not
                // propagate; the user-visible failure is the Timeout we return
                // below, which carries the operator's intended semantics.
                if let Err(error) = child.kill() {
                    tracing::debug!(
                        ?error,
                        "kill after timeout failed (child likely already exited)"
                    );
                }
                if let Err(error) = child.wait() {
                    tracing::debug!(?error, "wait after timeout failed");
                }
                // WHY: join the reader threads before returning. Killing the
                // child closes its write ends, so the readers hit EOF and exit;
                // joining reclaims them instead of leaking detached threads.
                drop(stdout_reader.join());
                drop(stderr_reader.join());
                return TimeoutSnafu {
                    program: program_display,
                    duration: timeout,
                }
                .fail();
            }
        }
    } else {
        child.wait().context(IoSnafu {
            program: program_display.clone(),
        })?
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
pub fn spawn_child(cmd: Command) -> std::io::Result<Child> {
    std_command(cmd).spawn()
}

fn std_command(cmd: Command) -> StdCommand {
    let mut std_cmd = StdCommand::new(&cmd.program);
    std_cmd.args(&cmd.args);

    std_cmd.stdin(cmd.stdin.unwrap_or_else(Stdio::null));
    std_cmd.stdout(cmd.stdout.unwrap_or_else(Stdio::piped));
    std_cmd.stderr(cmd.stderr.unwrap_or_else(Stdio::piped));

    if let Some(cwd) = cmd.cwd {
        std_cmd.current_dir(cwd);
    }
    for key in cmd.env_remove {
        std_cmd.env_remove(key);
    }
    for (key, value) in cmd.env {
        std_cmd.env(key, value);
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
fn duration_ms_saturating(d: Duration) -> u64 {
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
    let deadline = Instant::now() + timeout;
    let poll_interval = Duration::from_millis(25);

    loop {
        if let Some(status) = child.try_wait().context(IoSnafu { program })? {
            return Ok(WaitOutcome::Exited(status));
        }
        if Instant::now() >= deadline {
            return Ok(WaitOutcome::TimedOut);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(poll_interval.min(remaining));
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
