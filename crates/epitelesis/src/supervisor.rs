//! Private owned-process-group supervisor shared by every adapter.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::Command;
use crate::command::StreamingCommand;
use crate::error::Result;
use crate::policy::Ready;

pub(crate) const CLEANUP_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

pub(crate) struct ManagedLaunch {
    pub(crate) id: u32,
    pub(crate) stdin: Option<std::process::ChildStdin>,
    pub(crate) stdout: Option<std::process::ChildStdout>,
    pub(crate) stderr: Option<std::process::ChildStderr>,
    pub(crate) cancel: std::sync::mpsc::Sender<()>,
    pub(crate) result: std::sync::mpsc::Receiver<Result<crate::output::ManagedOutput>>,
    pub(crate) supervisor: std::thread::JoinHandle<()>,
}

/// Cooperative cancellation observed by the serialized supervisor loop.
#[derive(Clone, Default)]
pub(crate) struct Cancellation(Arc<AtomicBool>);

impl Cancellation {
    #[cfg(feature = "async")]
    pub(crate) fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

pub(crate) fn execute(
    command: Command<Ready>,
    cancellation: Cancellation,
) -> Result<crate::Output> {
    let prepared = crate::command::prepare(command)?;
    #[cfg(all(
        unix,
        not(any(
            target_os = "cygwin",
            target_os = "horizon",
            target_os = "openbsd",
            target_os = "redox",
            target_os = "wasi"
        ))
    ))]
    {
        unix::execute(prepared, cancellation)
    }
    #[cfg(not(all(
        unix,
        not(any(
            target_os = "cygwin",
            target_os = "horizon",
            target_os = "openbsd",
            target_os = "redox",
            target_os = "wasi"
        ))
    )))]
    {
        drop((prepared, cancellation));
        crate::error::UnsupportedCapabilitySnafu {
            capability: crate::error::Capability::OwnedProcessGroup,
        }
        .fail()
    }
}

pub(crate) fn spawn_managed(command: StreamingCommand) -> Result<ManagedLaunch> {
    let prepared = crate::command::prepare(command.command)?;
    #[cfg(all(
        unix,
        not(any(
            target_os = "cygwin",
            target_os = "horizon",
            target_os = "openbsd",
            target_os = "redox",
            target_os = "wasi"
        ))
    ))]
    {
        unix::spawn_managed(prepared)
    }
    #[cfg(not(all(
        unix,
        not(any(
            target_os = "cygwin",
            target_os = "horizon",
            target_os = "openbsd",
            target_os = "redox",
            target_os = "wasi"
        ))
    )))]
    {
        drop(prepared);
        crate::error::UnsupportedCapabilitySnafu {
            capability: crate::error::Capability::OwnedProcessGroup,
        }
        .fail()
    }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "cygwin",
        target_os = "horizon",
        target_os = "openbsd",
        target_os = "redox",
        target_os = "wasi"
    ))
))]
mod unix {
    use std::io::Read;
    use std::process::{Child, ChildStderr, ChildStdout, ExitStatus};
    use std::sync::mpsc::{self, Receiver, RecvTimeoutError, TryRecvError};
    use std::thread;
    use std::time::{Duration, Instant};

    use rustix::event::{Nsecs, PollFd, PollFlags, Timespec, poll};
    use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
    use rustix::process::{Pid, Signal, WaitId, WaitIdOptions, kill_process_group, waitid};
    use snafu::IntoError as _;

    use super::{CLEANUP_BUDGET, Cancellation, ManagedLaunch};
    use crate::command::PreparedCommand;
    use crate::error::{
        CancelledSnafu, CaptureFailedSnafu, CaptureFailure, CaptureLimitExceededSnafu,
        CleanupIncompleteEvidence, CleanupIncompleteSnafu, FailureEvidence, LifecycleFailedSnafu,
        NonZeroExitSnafu, ReaperStartFailedSnafu, Result, SpawnFailedSnafu, SupervisionFailedSnafu,
        SupervisorStartFailedSnafu, TimeoutSnafu,
    };
    use crate::output::{
        CaptureReport, CapturedStream, CleanupOutcome, GroupSignalOutcome, LeaderReapDisposition,
        LeaderReapOutcome, LifecycleEvidence, ManagedOutput, Output, StreamName,
    };
    use crate::policy::{CapturePolicy, ExecutionPolicy, OverflowBehavior};

    const EVENT_QUANTUM: Duration = Duration::from_millis(10);
    const READ_CHUNK: usize = 8 * 1024;
    const CHUNKS_PER_STREAM_TURN: usize = 8;

    struct ManagedStartup {
        id: u32,
        stdin: Option<std::process::ChildStdin>,
        stdout: Option<ChildStdout>,
        stderr: Option<ChildStderr>,
    }

    pub(super) fn spawn_managed(prepared: PreparedCommand) -> Result<ManagedLaunch> {
        let program = prepared.program.clone();
        let (startup_tx, startup_rx) = mpsc::sync_channel(1);
        let (cancel_tx, cancel_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let supervisor = thread::Builder::new()
            .name("epitelesis-managed-supervisor".to_owned())
            .spawn(move || {
                managed_supervisor(prepared, &cancel_rx, &startup_tx, &result_tx);
            })
            .map_err(|source| {
                SupervisorStartFailedSnafu {
                    program: program.clone(),
                }
                .into_error(source)
            })?;

        match startup_rx.recv() {
            Ok(Ok(startup)) => Ok(ManagedLaunch {
                id: startup.id,
                stdin: startup.stdin,
                stdout: startup.stdout,
                stderr: startup.stderr,
                cancel: cancel_tx,
                result: result_rx,
                supervisor,
            }),
            Ok(Err(error)) => {
                let _ = supervisor.join();
                Err(error)
            }
            Err(_) => {
                let source = if supervisor.join().is_err() {
                    std::io::Error::other("managed supervisor panicked before reporting startup")
                } else {
                    std::io::Error::other("managed supervisor exited before reporting startup")
                };
                Err(SupervisionFailedSnafu {
                    program,
                    evidence: unknown_evidence(),
                }
                .into_error(source))
            }
        }
    }

    fn managed_supervisor(
        prepared: PreparedCommand,
        cancel: &Receiver<()>,
        startup: &mpsc::SyncSender<Result<ManagedStartup>>,
        result: &mpsc::SyncSender<Result<ManagedOutput>>,
    ) {
        let PreparedCommand {
            mut command,
            program,
            execution,
            stdout_capture,
            stderr_capture,
        } = prepared;
        debug_assert!(stdout_capture.is_default());
        debug_assert!(stderr_capture.is_default());
        let reaper = match start_background_reaper(&program) {
            Ok(reaper) => reaper,
            Err(error) => {
                let _ = startup.send(Err(error));
                return;
            }
        };
        let started = Instant::now();
        let deadline = match execution.deadline(started) {
            Ok(deadline) => deadline,
            Err(error) => {
                let _ = startup.send(Err(error));
                return;
            }
        };
        let child = match command.spawn() {
            Ok(child) => child,
            Err(source) => {
                let _ = startup.send(Err(SpawnFailedSnafu { program }.into_error(source)));
                return;
            }
        };
        let mut owned = OwnedChild::new(child, reaper);
        let startup_value = ManagedStartup {
            id: owned.id(),
            stdin: owned.child_mut().and_then(|child| child.stdin.take()),
            stdout: owned.child_mut().and_then(|child| child.stdout.take()),
            stderr: owned.child_mut().and_then(|child| child.stderr.take()),
        };
        if startup.send(Ok(startup_value)).is_err() {
            let _ = owned.signal_group();
            let _ = owned.handoff_to_background_reaper();
            return;
        }
        let terminal = supervise_managed(owned, program, &execution, deadline, started, cancel);
        let _ = result.send(terminal);
    }

    fn supervise_managed(
        mut owned: OwnedChild,
        program: String,
        execution: &ExecutionPolicy,
        deadline: Option<Instant>,
        started: Instant,
        cancel: &Receiver<()>,
    ) -> Result<ManagedOutput> {
        let mut observations = Observations::default();
        loop {
            observe_leader(&owned, &mut observations);
            observations.cancelled |= cancellation_received(cancel);
            observations.deadline_elapsed = deadline.is_some_and(|value| Instant::now() >= value);
            if settlement_triggered(&observations, false, false) {
                break;
            }
            let wait = wait_quantum(deadline, None);
            match cancel.recv_timeout(wait) {
                Ok(()) | Err(RecvTimeoutError::Disconnected) => observations.cancelled = true,
                Err(RecvTimeoutError::Timeout) => {}
            }
        }

        let cleanup_deadline = cleanup_deadline();
        let signal = signal_group(&owned);
        let mut status = None;
        let mut reap_disposition = LeaderReapDisposition::Unreaped;
        while owned.has_child() && Instant::now() < cleanup_deadline {
            observe_leader(&owned, &mut observations);
            if observations.leader_waitable {
                match owned.reap_waitable() {
                    Ok(reaped) => {
                        status = Some(reaped);
                        reap_disposition = LeaderReapDisposition::Reaped;
                    }
                    Err(error) => {
                        observations
                            .reap_failures
                            .push(FailureEvidence::from_io("reap process leader", &error));
                        break;
                    }
                }
            }
            if owned.has_child() {
                let wait = cleanup_deadline
                    .saturating_duration_since(Instant::now())
                    .min(EVENT_QUANTUM);
                match cancel.recv_timeout(wait) {
                    Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                        observations.cancelled = true;
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                }
            }
        }

        let leader_unsettled = owned.has_child();
        if leader_unsettled {
            match owned.handoff_to_background_reaper() {
                Ok(()) => reap_disposition = LeaderReapDisposition::BackgroundReaper,
                Err(failure) => observations.reap_failures.push(failure),
            }
        }
        let cleanup = if leader_unsettled {
            CleanupOutcome::Incomplete(CleanupIncompleteEvidence {
                unfinished_streams: Vec::new(),
                leader_unsettled: true,
                cleanup_budget: CLEANUP_BUDGET,
            })
        } else {
            CleanupOutcome::Complete
        };
        let primary = choose_primary(&mut observations, None, None);
        let evidence = Box::new(LifecycleEvidence {
            leader_status: status,
            elapsed: Some(started.elapsed()),
            signal,
            reap: LeaderReapOutcome {
                disposition: reap_disposition,
                failures: observations.reap_failures,
            },
            stdout: redirected_report(StreamName::Stdout),
            stderr: redirected_report(StreamName::Stderr),
            cleanup,
        });
        classify_managed(primary, program, execution.duration(), evidence)
    }

    fn cancellation_received(cancel: &Receiver<()>) -> bool {
        match cancel.try_recv() {
            Ok(()) | Err(TryRecvError::Disconnected) => true,
            Err(TryRecvError::Empty) => false,
        }
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "the blocking supervisor owns its cancellation token through cleanup and reap"
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "the single-owner lifecycle keeps supervision and cleanup ordering explicit"
    )]
    pub(super) fn execute(prepared: PreparedCommand, cancellation: Cancellation) -> Result<Output> {
        let PreparedCommand {
            mut command,
            program,
            execution,
            stdout_capture,
            stderr_capture,
        } = prepared;
        let span = tracing::info_span!(
            "epitelesis.supervise",
            program = %program,
            arg_count = command.get_args().count(),
            policy = ?execution,
        );
        let _entered = span.enter();
        let reaper_sender = start_background_reaper(&program)?;
        let started = Instant::now();
        let deadline = execution.deadline(started)?;
        let configured_deadline = execution.duration();
        let child = match command.spawn() {
            Ok(child) => child,
            Err(source) => return Err(SpawnFailedSnafu { program }.into_error(source)),
        };
        let mut owned = OwnedChild::new(child, reaper_sender);
        drop(owned.child_mut().and_then(|child| child.stdin.take()));

        let mut stdout = CaptureStream::new(
            StreamName::Stdout,
            owned.child_mut().and_then(|child| child.stdout.take()),
            &stdout_capture,
        );
        let mut stderr = CaptureStream::new(
            StreamName::Stderr,
            owned.child_mut().and_then(|child| child.stderr.take()),
            &stderr_capture,
        );
        stdout.make_nonblocking();
        stderr.make_nonblocking();

        let mut observations = Observations::default();
        let mut stdout_first = true;
        loop {
            observe_leader(&owned, &mut observations);
            observations.cancelled |= cancellation.is_cancelled();
            observations.deadline_elapsed = deadline.is_some_and(|value| Instant::now() >= value);
            if settlement_triggered(
                &observations,
                stdout.overflow().is_some() || stderr.overflow().is_some(),
                stdout.failed() || stderr.failed(),
            ) {
                break;
            }

            let wait = wait_quantum(deadline, None);
            match poll_streams(&stdout, &stderr, wait) {
                Ok((stdout_ready, stderr_ready)) => pump_fair(
                    &mut stdout,
                    &mut stderr,
                    stdout_ready,
                    stderr_ready,
                    &mut stdout_first,
                ),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    observations.observe_error.get_or_insert(error);
                }
            }
        }

        let cleanup_deadline = cleanup_deadline();
        let signal = signal_group(&owned);
        let mut status = None;
        let mut reap_disposition = LeaderReapDisposition::Unreaped;
        while Instant::now() < cleanup_deadline {
            observations.cancelled |= cancellation.is_cancelled();
            observations.deadline_elapsed = deadline.is_some_and(|value| Instant::now() >= value);
            observe_leader(&owned, &mut observations);
            if observations.leader_waitable && owned.has_child() {
                match owned.reap_waitable() {
                    Ok(reaped) => {
                        status = Some(reaped);
                        reap_disposition = LeaderReapDisposition::Reaped;
                    }
                    Err(error) => {
                        observations
                            .reap_failures
                            .push(FailureEvidence::from_io("reap process leader", &error));
                        break;
                    }
                }
            }
            if !owned.has_child() && stdout.terminal() && stderr.terminal() {
                break;
            }
            let wait = wait_quantum(None, Some(cleanup_deadline));
            match poll_streams(&stdout, &stderr, wait) {
                Ok((stdout_ready, stderr_ready)) => pump_fair(
                    &mut stdout,
                    &mut stderr,
                    stdout_ready,
                    stderr_ready,
                    &mut stdout_first,
                ),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    observations.observe_error.get_or_insert(error);
                }
            }
        }

        let leader_unsettled = owned.has_child();
        if leader_unsettled {
            match owned.handoff_to_background_reaper() {
                Ok(()) => reap_disposition = LeaderReapDisposition::BackgroundReaper,
                Err(failure) => observations.reap_failures.push(failure),
            }
        }
        let unfinished_streams = [
            (!stdout.terminal()).then_some(StreamName::Stdout),
            (!stderr.terminal()).then_some(StreamName::Stderr),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        let cleanup = if leader_unsettled || !unfinished_streams.is_empty() {
            CleanupOutcome::Incomplete(CleanupIncompleteEvidence {
                unfinished_streams,
                leader_unsettled,
                cleanup_budget: CLEANUP_BUDGET,
            })
        } else {
            CleanupOutcome::Complete
        };
        let stdout_overflow = stdout.overflow();
        let stderr_overflow = stderr.overflow();
        let primary = choose_primary(&mut observations, stdout_overflow, stderr_overflow);
        let elapsed = started.elapsed();
        let evidence = Box::new(LifecycleEvidence {
            leader_status: status,
            elapsed: Some(elapsed),
            signal,
            reap: LeaderReapOutcome {
                disposition: reap_disposition,
                failures: observations.reap_failures,
            },
            stdout: stdout.into_report(),
            stderr: stderr.into_report(),
            cleanup,
        });

        tracing::debug!(
            duration_ms = duration_millis(elapsed),
            stdout_bytes = evidence.stdout.captured.len(),
            stderr_bytes = evidence.stderr.captured.len(),
            "invocation supervision settled"
        );

        classify(primary, program, configured_deadline, evidence)
    }

    fn duration_millis(duration: Duration) -> u64 {
        u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
    }

    fn cleanup_deadline() -> Instant {
        let now = Instant::now();
        now.checked_add(CLEANUP_BUDGET).unwrap_or(now)
    }

    fn signal_group(owned: &OwnedChild) -> GroupSignalOutcome {
        match owned.signal_group() {
            Ok(()) => GroupSignalOutcome::Sent,
            Err(error) if error.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error()) => {
                GroupSignalOutcome::AlreadyGone
            }
            Err(error) => {
                GroupSignalOutcome::Failed(FailureEvidence::from_io("signal process group", &error))
            }
        }
    }

    fn observe_leader(owned: &OwnedChild, observations: &mut Observations) {
        if observations.leader_waitable
            || observations.observe_error.is_some()
            || !owned.has_child()
        {
            return;
        }
        match owned.leader_waitable() {
            Ok(waitable) => observations.leader_waitable = waitable,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => observations.observe_error = Some(error),
        }
    }

    fn wait_quantum(deadline: Option<Instant>, cleanup: Option<Instant>) -> Duration {
        let now = Instant::now();
        let until_deadline = deadline.map_or(EVENT_QUANTUM, |value| {
            value.saturating_duration_since(now).min(EVENT_QUANTUM)
        });
        cleanup.map_or(until_deadline, |value| {
            until_deadline.min(value.saturating_duration_since(now))
        })
    }

    fn poll_streams(
        stdout: &CaptureStream<ChildStdout>,
        stderr: &CaptureStream<ChildStderr>,
        timeout: Duration,
    ) -> std::io::Result<(bool, bool)> {
        let events = PollFlags::IN | PollFlags::HUP | PollFlags::ERR;
        // EVENT_QUANTUM is below one second, so this also fits 32-bit c_long.
        let nanoseconds = timeout.subsec_nanos() as Nsecs;
        let timespec = Timespec {
            tv_sec: 0,
            tv_nsec: nanoseconds,
        };
        match (stdout.pipe.as_ref(), stderr.pipe.as_ref()) {
            (Some(stdout_pipe), Some(stderr_pipe)) => {
                let mut descriptors = [
                    PollFd::new(stdout_pipe, events),
                    PollFd::new(stderr_pipe, events),
                ];
                poll(&mut descriptors, Some(&timespec)).map_err(errno_to_io)?;
                Ok((
                    !descriptors[0].revents().is_empty(),
                    !descriptors[1].revents().is_empty(),
                ))
            }
            (Some(stdout_pipe), None) => {
                let mut descriptors = [PollFd::new(stdout_pipe, events)];
                poll(&mut descriptors, Some(&timespec)).map_err(errno_to_io)?;
                Ok((!descriptors[0].revents().is_empty(), false))
            }
            (None, Some(stderr_pipe)) => {
                let mut descriptors = [PollFd::new(stderr_pipe, events)];
                poll(&mut descriptors, Some(&timespec)).map_err(errno_to_io)?;
                Ok((false, !descriptors[0].revents().is_empty()))
            }
            (None, None) => {
                thread::sleep(timeout);
                Ok((false, false))
            }
        }
    }

    fn pump_fair(
        stdout: &mut CaptureStream<ChildStdout>,
        stderr: &mut CaptureStream<ChildStderr>,
        stdout_ready: bool,
        stderr_ready: bool,
        stdout_first: &mut bool,
    ) {
        if *stdout_first {
            if stdout_ready {
                stdout.pump(CHUNKS_PER_STREAM_TURN);
            }
            if stderr_ready {
                stderr.pump(CHUNKS_PER_STREAM_TURN);
            }
        } else {
            if stderr_ready {
                stderr.pump(CHUNKS_PER_STREAM_TURN);
            }
            if stdout_ready {
                stdout.pump(CHUNKS_PER_STREAM_TURN);
            }
        }
        *stdout_first = !*stdout_first;
    }

    #[derive(Default)]
    struct Observations {
        cancelled: bool,
        deadline_elapsed: bool,
        leader_waitable: bool,
        observe_error: Option<std::io::Error>,
        reap_failures: Vec<FailureEvidence>,
    }

    #[derive(Debug)]
    enum Primary {
        Limit { stream: StreamName, limit: usize },
        Cancelled,
        Deadline,
        CaptureFailure,
        Supervision(std::io::Error),
        Exit,
    }

    fn settlement_triggered(
        observations: &Observations,
        overflow: bool,
        capture_failed: bool,
    ) -> bool {
        overflow
            || observations.cancelled
            || observations.deadline_elapsed
            || capture_failed
            || observations.observe_error.is_some()
            || observations.leader_waitable
    }

    fn choose_primary(
        observations: &mut Observations,
        stdout_overflow: Option<usize>,
        stderr_overflow: Option<usize>,
    ) -> Primary {
        if let Some(limit) = stdout_overflow {
            return Primary::Limit {
                stream: StreamName::Stdout,
                limit,
            };
        }
        if let Some(limit) = stderr_overflow {
            return Primary::Limit {
                stream: StreamName::Stderr,
                limit,
            };
        }
        if observations.cancelled {
            return Primary::Cancelled;
        }
        if observations.deadline_elapsed {
            return Primary::Deadline;
        }
        if observations.observe_error.is_none() {
            return Primary::Exit;
        }
        match observations.observe_error.take() {
            Some(error) => Primary::Supervision(error),
            None => Primary::Exit,
        }
    }

    fn classify(
        mut primary: Primary,
        program: String,
        deadline: Option<Duration>,
        evidence: Box<LifecycleEvidence>,
    ) -> Result<Output> {
        if evidence.stdout.failure.is_some() || evidence.stderr.failure.is_some() {
            primary = match primary {
                Primary::Limit { .. } | Primary::Cancelled | Primary::Deadline => primary,
                Primary::CaptureFailure | Primary::Supervision(_) | Primary::Exit => {
                    Primary::CaptureFailure
                }
            };
        }
        match primary {
            Primary::Limit { stream, limit } => CaptureLimitExceededSnafu {
                program,
                stream,
                limit,
                evidence,
            }
            .fail(),
            Primary::Cancelled => CancelledSnafu { program, evidence }.fail(),
            Primary::Deadline => match deadline {
                Some(configured) => TimeoutSnafu {
                    program,
                    deadline: configured,
                    evidence,
                }
                .fail(),
                None => Err(SupervisionFailedSnafu { program, evidence }.into_error(
                    std::io::Error::other("deadline elapsed without a configured deadline"),
                )),
            },
            Primary::CaptureFailure => CaptureFailedSnafu { program, evidence }.fail(),
            Primary::Supervision(source) => {
                Err(SupervisionFailedSnafu { program, evidence }.into_error(source))
            }
            Primary::Exit => classify_exit(program, evidence)
                .map(|(status, evidence)| Output::new(status, evidence)),
        }
    }

    fn classify_managed(
        primary: Primary,
        program: String,
        deadline: Option<Duration>,
        evidence: Box<LifecycleEvidence>,
    ) -> Result<ManagedOutput> {
        match primary {
            Primary::Limit { .. } | Primary::CaptureFailure => {
                CaptureFailedSnafu { program, evidence }.fail()
            }
            Primary::Cancelled => CancelledSnafu { program, evidence }.fail(),
            Primary::Deadline => match deadline {
                Some(configured) => TimeoutSnafu {
                    program,
                    deadline: configured,
                    evidence,
                }
                .fail(),
                None => Err(SupervisionFailedSnafu { program, evidence }.into_error(
                    std::io::Error::other("deadline elapsed without a configured deadline"),
                )),
            },
            Primary::Supervision(source) => {
                Err(SupervisionFailedSnafu { program, evidence }.into_error(source))
            }
            Primary::Exit => classify_exit(program, evidence)
                .map(|(status, evidence)| ManagedOutput::new(status, evidence)),
        }
    }

    fn classify_exit(
        program: String,
        evidence: Box<LifecycleEvidence>,
    ) -> Result<(ExitStatus, Box<LifecycleEvidence>)> {
        if matches!(&evidence.cleanup, CleanupOutcome::Incomplete(_)) {
            return CleanupIncompleteSnafu { program, evidence }.fail();
        }
        if matches!(&evidence.cleanup, CleanupOutcome::Unknown) {
            return Err(SupervisionFailedSnafu { program, evidence }
                .into_error(std::io::Error::other("cleanup outcome was not recovered")));
        }
        if matches!(&evidence.signal, GroupSignalOutcome::Unknown)
            || matches!(evidence.reap.disposition, LeaderReapDisposition::Unknown)
        {
            return Err(SupervisionFailedSnafu { program, evidence }.into_error(
                std::io::Error::other("signal or reap outcome was not recovered"),
            ));
        }
        if matches!(&evidence.signal, GroupSignalOutcome::Failed(_))
            || !evidence.reap.failures.is_empty()
            || matches!(evidence.reap.disposition, LeaderReapDisposition::Unreaped)
        {
            return LifecycleFailedSnafu { program, evidence }.fail();
        }
        match evidence.leader_status {
            Some(status) if status.success() => Ok((status, evidence)),
            Some(_) => NonZeroExitSnafu { program, evidence }.fail(),
            None => Err(SupervisionFailedSnafu { program, evidence }
                .into_error(std::io::Error::other("process leader had no reaped status"))),
        }
    }

    fn redirected_report(stream: StreamName) -> CaptureReport {
        CaptureReport {
            stream,
            captured: CapturedStream::redirected(),
            failure: None,
        }
    }

    fn unknown_report(stream: StreamName) -> CaptureReport {
        CaptureReport {
            stream,
            captured: CapturedStream::unknown(),
            failure: None,
        }
    }

    fn unknown_evidence() -> Box<LifecycleEvidence> {
        Box::new(LifecycleEvidence {
            leader_status: None,
            elapsed: None,
            signal: GroupSignalOutcome::Unknown,
            reap: LeaderReapOutcome {
                disposition: LeaderReapDisposition::Unknown,
                failures: Vec::new(),
            },
            stdout: unknown_report(StreamName::Stdout),
            stderr: unknown_report(StreamName::Stderr),
            cleanup: CleanupOutcome::Unknown,
        })
    }

    struct CaptureStream<Pipe> {
        name: StreamName,
        pipe: Option<Pipe>,
        storage: CaptureStorage,
        discarded: u64,
        overflow: Option<usize>,
        failure: Option<CaptureFailure>,
        eof: bool,
        redirected: bool,
    }

    enum CaptureStorage {
        Bounded {
            bytes: Vec<u8>,
            limit: usize,
            overflow: OverflowBehavior,
        },
        Unbounded(Vec<u8>),
    }

    impl<Pipe: Read + std::os::fd::AsFd> CaptureStream<Pipe> {
        fn new(name: StreamName, pipe: Option<Pipe>, policy: &CapturePolicy) -> Self {
            let redirected = pipe.is_none();
            let storage = match policy {
                CapturePolicy::Bounded { limit, overflow } => CaptureStorage::Bounded {
                    bytes: Vec::new(),
                    limit: *limit,
                    overflow: *overflow,
                },
                CapturePolicy::Unbounded(_) => CaptureStorage::Unbounded(Vec::new()),
            };
            Self {
                name,
                pipe,
                storage,
                discarded: 0,
                overflow: None,
                failure: None,
                eof: false,
                redirected,
            }
        }

        fn make_nonblocking(&mut self) {
            let result = self.pipe.as_ref().map(|pipe| {
                fcntl_getfl(pipe)
                    .and_then(|flags| fcntl_setfl(pipe, flags | OFlags::NONBLOCK))
                    .map_err(errno_to_io)
            });
            if let Some(Err(error)) = result {
                self.record_failure(&error);
            }
        }

        fn pump(&mut self, chunk_budget: usize) {
            for _ in 0..chunk_budget {
                let mut chunk = [0_u8; READ_CHUNK];
                let read = match self.pipe.as_mut() {
                    Some(pipe) => pipe.read(&mut chunk),
                    None => return,
                };
                match read {
                    Ok(0) => {
                        self.eof = true;
                        self.pipe = None;
                        return;
                    }
                    Ok(count) => self.retain(&chunk[..count]),
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return,
                    Err(error) => {
                        self.record_failure(&error);
                        return;
                    }
                }
            }
        }

        fn retain(&mut self, chunk: &[u8]) {
            let mut allocation_failed = false;
            match &mut self.storage {
                CaptureStorage::Unbounded(bytes) => {
                    if bytes.try_reserve(chunk.len()).is_ok() {
                        bytes.extend_from_slice(chunk);
                    } else {
                        self.discarded = self.discarded.saturating_add(usize_to_u64(chunk.len()));
                        allocation_failed = true;
                    }
                }
                CaptureStorage::Bounded {
                    bytes,
                    limit,
                    overflow,
                } => {
                    let retained = chunk.len().min(limit.saturating_sub(bytes.len()));
                    let discarded = chunk.len() - retained;
                    self.discarded = self.discarded.saturating_add(usize_to_u64(discarded));
                    if discarded > 0 && *overflow == OverflowBehavior::FailClosed {
                        self.overflow.get_or_insert(*limit);
                    }
                    if retained > 0 {
                        if bytes.try_reserve(retained).is_ok() {
                            bytes.extend_from_slice(&chunk[..retained]);
                        } else {
                            self.discarded = self.discarded.saturating_add(usize_to_u64(retained));
                            allocation_failed = true;
                        }
                    }
                }
            }
            if allocation_failed {
                self.failure.get_or_insert(CaptureFailure::Allocation);
                self.pipe = None;
            }
        }

        fn record_failure(&mut self, error: &std::io::Error) {
            self.failure.get_or_insert(CaptureFailure::Read {
                kind: error.kind(),
                message: error.to_string(),
            });
            self.pipe = None;
        }

        fn failed(&self) -> bool {
            self.failure.is_some()
        }

        fn overflow(&self) -> Option<usize> {
            self.overflow
        }

        fn terminal(&self) -> bool {
            self.redirected || self.eof || self.failure.is_some()
        }

        fn into_report(self) -> CaptureReport {
            let (CaptureStorage::Bounded { bytes, .. } | CaptureStorage::Unbounded(bytes)) =
                self.storage;
            let captured = if self.redirected {
                CapturedStream::redirected()
            } else if self.eof {
                CapturedStream::settled(bytes, self.discarded)
            } else {
                CapturedStream::incomplete(bytes, self.discarded)
            };
            CaptureReport {
                stream: self.name,
                captured,
                failure: self.failure,
            }
        }
    }

    fn usize_to_u64(value: usize) -> u64 {
        u64::try_from(value).unwrap_or(u64::MAX)
    }

    fn start_background_reaper(program: &str) -> Result<mpsc::SyncSender<Child>> {
        let (sender, receiver) = mpsc::sync_channel::<Child>(1);
        let _reaper = thread::Builder::new()
            .name("epitelesis-background-reaper".to_owned())
            .spawn(move || {
                if let Ok(mut child) = receiver.recv() {
                    let _ = child.wait();
                }
            })
            .map_err(|source| {
                ReaperStartFailedSnafu {
                    program: program.to_owned(),
                }
                .into_error(source)
            })?;
        Ok(sender)
    }

    struct OwnedChild {
        child: Option<Child>,
        pgid: Pid,
        reaper: Option<mpsc::SyncSender<Child>>,
    }

    impl OwnedChild {
        fn new(child: Child, reaper: mpsc::SyncSender<Child>) -> Self {
            let pgid = Pid::from_child(&child);
            Self {
                child: Some(child),
                pgid,
                reaper: Some(reaper),
            }
        }

        fn id(&self) -> u32 {
            self.child.as_ref().map_or(0, Child::id)
        }

        fn child_mut(&mut self) -> Option<&mut Child> {
            self.child.as_mut()
        }

        fn has_child(&self) -> bool {
            self.child.is_some()
        }

        fn signal_group(&self) -> std::io::Result<()> {
            kill_process_group(self.pgid, Signal::KILL).map_err(errno_to_io)
        }

        fn leader_waitable(&self) -> std::io::Result<bool> {
            if self.child.is_none() {
                return Ok(true);
            }
            let options = WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT;
            waitid(WaitId::Pid(self.pgid), options)
                .map(|status| status.is_some())
                .map_err(errno_to_io)
        }

        fn reap_waitable(&mut self) -> std::io::Result<ExitStatus> {
            match self.child.as_mut() {
                Some(child) => {
                    let status = child.wait()?;
                    self.child = None;
                    Ok(status)
                }
                None => Err(std::io::Error::other("process leader was already reaped")),
            }
        }

        fn handoff_to_background_reaper(&mut self) -> std::result::Result<(), FailureEvidence> {
            let Some(child) = self.child.take() else {
                return Ok(());
            };
            let Some(sender) = self.reaper.take() else {
                self.child = Some(child);
                return Err(FailureEvidence::from_io(
                    "transfer child to background reaper",
                    &std::io::Error::other("fallback reaper was unavailable"),
                ));
            };
            if let Err(error) = sender.send(child) {
                self.child = Some(error.0);
                return Err(FailureEvidence::from_io(
                    "transfer child to background reaper",
                    &std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "background reaper stopped before ownership transfer",
                    ),
                ));
            }
            Ok(())
        }
    }

    impl Drop for OwnedChild {
        fn drop(&mut self) {
            if self.child.is_none() {
                return;
            }
            let _ = kill_process_group(self.pgid, Signal::KILL);
            let _ = self.handoff_to_background_reaper();
        }
    }

    fn errno_to_io(error: rustix::io::Errno) -> std::io::Error {
        std::io::Error::from_raw_os_error(error.raw_os_error())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn primary_precedence_is_stable() {
            let mut observations = Observations {
                cancelled: true,
                deadline_elapsed: true,
                leader_waitable: true,
                observe_error: Some(std::io::Error::other("observe")),
                reap_failures: Vec::new(),
            };
            assert!(matches!(
                choose_primary(&mut observations, Some(7), Some(9)),
                Primary::Limit {
                    stream: StreamName::Stdout,
                    limit: 7
                }
            ));
            assert!(matches!(
                choose_primary(&mut observations, None, Some(9)),
                Primary::Limit {
                    stream: StreamName::Stderr,
                    limit: 9
                }
            ));
            assert!(matches!(
                choose_primary(&mut observations, None, None),
                Primary::Cancelled
            ));
        }

        #[test]
        fn settlement_matrix_uses_classification_and_keeps_peer_bytes() {
            let outcomes = [
                None,
                Some(CaptureFailure::Read {
                    kind: std::io::ErrorKind::Other,
                    message: "fixture".to_owned(),
                }),
                Some(CaptureFailure::Allocation),
            ];
            for stdout_failure in &outcomes {
                for stderr_failure in &outcomes {
                    let evidence = fixture_evidence(stdout_failure.clone(), stderr_failure.clone());
                    let result = classify(
                        Primary::Exit,
                        "fixture".to_owned(),
                        Some(Duration::from_secs(1)),
                        evidence,
                    );
                    if stdout_failure.is_none() && stderr_failure.is_none() {
                        let output = match result {
                            Ok(output) => output,
                            Err(error) => panic!("success matrix cell failed: {error:?}"),
                        };
                        assert_eq!(output.evidence.stdout.captured.bytes, b"out");
                        assert_eq!(output.evidence.stderr.captured.bytes, b"err");
                    } else {
                        let evidence = match result {
                            Err(crate::Error::CaptureFailed { evidence, .. }) => evidence,
                            other => panic!("capture matrix cell misclassified: {other:?}"),
                        };
                        assert_eq!(evidence.stdout.stream, StreamName::Stdout);
                        assert_eq!(evidence.stderr.stream, StreamName::Stderr);
                        assert_eq!(evidence.stdout.captured.bytes, b"out");
                        assert_eq!(evidence.stderr.captured.bytes, b"err");
                        let expected_first = if stdout_failure.is_some() {
                            StreamName::Stdout
                        } else {
                            StreamName::Stderr
                        };
                        assert_eq!(evidence.first_capture_failure(), Some(expected_first));
                    }
                }
            }
        }

        #[test]
        fn unbounded_read_failure_triggers_cleanup_with_incomplete_peer() {
            use std::os::fd::{AsFd, BorrowedFd};

            struct FaultingPipe(std::fs::File);
            impl Read for FaultingPipe {
                fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                    Err(std::io::Error::other("injected read failure"))
                }
            }
            impl AsFd for FaultingPipe {
                fn as_fd(&self) -> BorrowedFd<'_> {
                    self.0.as_fd()
                }
            }

            struct HoldingPipe(std::fs::File);
            impl Read for HoldingPipe {
                fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                    Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
                }
            }
            impl AsFd for HoldingPipe {
                fn as_fd(&self) -> BorrowedFd<'_> {
                    self.0.as_fd()
                }
            }

            let stdout_file = match std::fs::File::open("/dev/null") {
                Ok(file) => file,
                Err(error) => panic!("stdout fixture failed: {error}"),
            };
            let stderr_file = match std::fs::File::open("/dev/null") {
                Ok(file) => file,
                Err(error) => panic!("stderr fixture failed: {error}"),
            };
            let policy = match CapturePolicy::unbounded("fault-injection fixture") {
                Ok(policy) => policy,
                Err(error) => panic!("unbounded fixture policy failed: {error:?}"),
            };
            let mut stdout =
                CaptureStream::new(StreamName::Stdout, Some(FaultingPipe(stdout_file)), &policy);
            let mut stderr = CaptureStream::new(
                StreamName::Stderr,
                Some(HoldingPipe(stderr_file)),
                &CapturePolicy::bounded(8),
            );
            stdout.pump(1);
            stderr.pump(1);

            assert!(stdout.failed());
            assert!(!stderr.terminal());
            assert!(settlement_triggered(
                &Observations::default(),
                false,
                stdout.failed()
            ));
            let stdout = stdout.into_report();
            let stderr = stderr.into_report();
            assert!(stdout.failure.is_some());
            assert!(matches!(
                stdout.captured.completeness,
                crate::CaptureCompleteness::Incomplete { .. }
            ));
            assert!(matches!(
                stderr.captured.completeness,
                crate::CaptureCompleteness::Incomplete { .. }
            ));
        }

        fn fixture_evidence(
            stdout_failure: Option<CaptureFailure>,
            stderr_failure: Option<CaptureFailure>,
        ) -> Box<LifecycleEvidence> {
            let status = match std::process::Command::new("/bin/true").status() {
                Ok(status) => status,
                Err(error) => panic!("true fixture failed: {error}"),
            };
            Box::new(LifecycleEvidence {
                leader_status: Some(status),
                elapsed: Some(Duration::from_millis(1)),
                signal: GroupSignalOutcome::Sent,
                reap: LeaderReapOutcome {
                    disposition: LeaderReapDisposition::Reaped,
                    failures: Vec::new(),
                },
                stdout: CaptureReport {
                    stream: StreamName::Stdout,
                    captured: if stdout_failure.is_some() {
                        CapturedStream::incomplete(b"out".to_vec(), 0)
                    } else {
                        CapturedStream::settled(b"out".to_vec(), 0)
                    },
                    failure: stdout_failure,
                },
                stderr: CaptureReport {
                    stream: StreamName::Stderr,
                    captured: if stderr_failure.is_some() {
                        CapturedStream::incomplete(b"err".to_vec(), 0)
                    } else {
                        CapturedStream::settled(b"err".to_vec(), 0)
                    },
                    failure: stderr_failure,
                },
                cleanup: CleanupOutcome::Complete,
            })
        }
    }
}
