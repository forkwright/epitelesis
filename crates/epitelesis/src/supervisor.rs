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
    pub(crate) request: std::sync::mpsc::Sender<SupervisorRequest>,
    pub(crate) result: std::sync::mpsc::Receiver<Result<crate::output::ManagedOutput>>,
    pub(crate) supervisor: std::thread::JoinHandle<()>,
}

/// A caller request consumed only by the serialized managed supervisor.
#[derive(Debug)]
pub(crate) enum SupervisorRequest {
    Cancel,
    Forward(crate::ForwardedSignal),
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

    use super::{CLEANUP_BUDGET, Cancellation, ManagedLaunch, SupervisorRequest};
    use crate::command::PreparedCommand;
    use crate::error::{
        CancelledSnafu, CaptureFailedSnafu, CaptureFailure, CaptureLimitExceededSnafu,
        CleanupIncompleteEvidence, CleanupIncompleteSnafu, FailureEvidence, LifecycleFailedSnafu,
        NonZeroExitSnafu, ReaperStartFailedSnafu, Result, SpawnFailedSnafu, SupervisionFailedSnafu,
        SupervisorStartFailedSnafu, TimeoutSnafu,
    };
    use crate::output::{
        CaptureReport, CapturedStream, CleanupOutcome, ForwardedSignalEvidence, GroupSignalOutcome,
        LeaderReapDisposition, LeaderReapOutcome, LifecycleEvidence, ManagedOutput, Output,
        StreamName,
    };
    use crate::policy::{CapturePolicy, ExecutionPolicy, OverflowBehavior};

    const EVENT_QUANTUM: Duration = Duration::from_millis(10);
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
        let (request_tx, request_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let supervisor = thread::Builder::new()
            .name("epitelesis-managed-supervisor".to_owned())
            .spawn(move || {
                managed_supervisor(prepared, &request_rx, &startup_tx, &result_tx);
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
                request: request_tx,
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
        request: &Receiver<SupervisorRequest>,
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
            let _ = owned.signal_group(Signal::KILL);
            let _ = owned.handoff_to_background_reaper();
            return;
        }
        let terminal = supervise_managed(owned, program, &execution, deadline, started, request);
        let _ = result.send(terminal);
    }

    fn supervise_managed(
        mut owned: OwnedChild,
        program: String,
        execution: &ExecutionPolicy,
        deadline: Option<Instant>,
        started: Instant,
        request: &Receiver<SupervisorRequest>,
    ) -> Result<ManagedOutput> {
        let (mut observations, mut forwarded_signal) =
            await_managed_settlement(&owned, deadline, request);

        let cleanup_deadline = cleanup_deadline();
        let forwarded_grace_deadline = forwarded_signal.as_ref().map(|_| {
            Instant::now()
                .checked_add(CLEANUP_BUDGET / 2)
                .unwrap_or(cleanup_deadline)
                .min(cleanup_deadline)
        });
        let mut signal = forwarded_signal.as_ref().map_or_else(
            || signal_group(&owned),
            |evidence: &ForwardedSignalEvidence| evidence.outcome.clone(),
        );
        let mut status = None;
        let mut reap_disposition = LeaderReapDisposition::Unreaped;
        let mut process_group_unsettled = true;
        while (owned.has_child() || process_group_unsettled) && Instant::now() < cleanup_deadline {
            observe_leader(&owned, &mut observations);
            process_group_unsettled = process_group_is_unsettled(&owned, &mut observations);
            if forwarded_grace_deadline.is_some_and(|grace| Instant::now() >= grace)
                && process_group_unsettled
                && forwarded_signal
                    .as_ref()
                    .is_some_and(|evidence| evidence.escalation.is_none())
            {
                let escalation = signal_group(&owned);
                signal = escalation.clone();
                if let Some(evidence) = &mut forwarded_signal {
                    evidence.escalation = Some(escalation);
                }
            }
            // Keep the waitable leader unreaped while forwarding can still
            // escalate. Its reserved PID pins our group identity; reaping it
            // early could make a later kill target a reused, unrelated PGID.
            let further_signal_possible = forwarded_signal
                .as_ref()
                .is_some_and(|evidence| evidence.escalation.is_none() && process_group_unsettled);
            if observations.leader_waitable && !further_signal_possible && owned.has_child() {
                match owned.reap_waitable() {
                    Ok(reaped) => {
                        status = Some(reaped);
                        reap_disposition = LeaderReapDisposition::Reaped;
                    }
                    Err(error) => {
                        observations
                            .reap_failures
                            .push(FailureEvidence::from_io("reap child", &error));
                        observations.leader_waitable = false;
                    }
                }
                process_group_unsettled = process_group_is_unsettled(&owned, &mut observations);
            }
            if !owned.has_child() && !process_group_unsettled {
                break;
            }
            let wait = cleanup_deadline
                .saturating_duration_since(Instant::now())
                .min(EVENT_QUANTUM);
            wait_for_managed_request(
                request,
                &owned,
                &mut observations,
                &mut forwarded_signal,
                wait,
            );
        }

        let leader_unsettled = owned.has_child();
        if leader_unsettled {
            match owned.handoff_to_background_reaper() {
                Ok(()) => reap_disposition = LeaderReapDisposition::BackgroundReaper,
                Err(failure) => observations.reap_failures.push(failure),
            }
        }
        let cleanup = if leader_unsettled || process_group_unsettled {
            CleanupOutcome::Incomplete(CleanupIncompleteEvidence {
                unfinished_streams: Vec::new(),
                leader_unsettled,
                process_group_unsettled,
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
            forwarded_signal,
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

    fn process_group_is_unsettled(owned: &OwnedChild, observations: &mut Observations) -> bool {
        owned
            .has_living_process_group_member()
            .unwrap_or_else(|failure| {
                observations.reap_failures.push(failure);
                true
            })
    }

    fn await_managed_settlement(
        owned: &OwnedChild,
        deadline: Option<Instant>,
        request: &Receiver<SupervisorRequest>,
    ) -> (Observations, Option<ForwardedSignalEvidence>) {
        let mut observations = Observations::default();
        let mut forwarded_signal = None;
        loop {
            observe_leader(owned, &mut observations);
            receive_managed_request(request, owned, &mut observations, &mut forwarded_signal);
            observations.deadline_elapsed = deadline.is_some_and(|value| Instant::now() >= value);
            if settlement_triggered(&observations, false, false) || forwarded_signal.is_some() {
                return (observations, forwarded_signal);
            }
            wait_for_managed_request(
                request,
                owned,
                &mut observations,
                &mut forwarded_signal,
                wait_quantum(deadline, None),
            );
        }
    }

    fn wait_for_managed_request(
        request: &Receiver<SupervisorRequest>,
        owned: &OwnedChild,
        observations: &mut Observations,
        forwarded_signal: &mut Option<ForwardedSignalEvidence>,
        wait: Duration,
    ) {
        match request.recv_timeout(wait) {
            Ok(SupervisorRequest::Cancel) => observations.cancelled = true,
            Err(RecvTimeoutError::Disconnected) => {
                observations.cancelled = true;
                // A dropped handle closes the channel immediately. Avoid
                // spinning while the same bounded cleanup waits for exit.
                thread::park_timeout(wait);
            }
            Ok(SupervisorRequest::Forward(signal)) => {
                forward_managed_signal(owned, signal, forwarded_signal);
            }
            Err(RecvTimeoutError::Timeout) => {}
        }
    }

    fn receive_managed_request(
        request: &Receiver<SupervisorRequest>,
        owned: &OwnedChild,
        observations: &mut Observations,
        forwarded_signal: &mut Option<ForwardedSignalEvidence>,
    ) {
        match request.try_recv() {
            Ok(SupervisorRequest::Cancel) | Err(TryRecvError::Disconnected) => {
                observations.cancelled = true;
            }
            Ok(SupervisorRequest::Forward(signal)) => {
                forward_managed_signal(owned, signal, forwarded_signal);
            }
            Err(TryRecvError::Empty) => {}
        }
    }

    fn forward_managed_signal(
        owned: &OwnedChild,
        signal: crate::ForwardedSignal,
        forwarded_signal: &mut Option<ForwardedSignalEvidence>,
    ) {
        if forwarded_signal.is_none() && owned.has_child() {
            *forwarded_signal = Some(ForwardedSignalEvidence {
                signal,
                outcome: signal_group_with(owned, forwarded_signal_to_os(signal)),
                escalation: None,
            });
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
        let mut process_group_unsettled = true;
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
            if !owned.has_child() {
                process_group_unsettled = process_group_is_unsettled(&owned, &mut observations);
            }
            if !owned.has_child()
                && !process_group_unsettled
                && stdout.terminal()
                && stderr.terminal()
            {
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
        let cleanup =
            if leader_unsettled || process_group_unsettled || !unfinished_streams.is_empty() {
                CleanupOutcome::Incomplete(CleanupIncompleteEvidence {
                    unfinished_streams,
                    leader_unsettled,
                    process_group_unsettled,
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
            forwarded_signal: None,
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
        signal_group_with(owned, Signal::KILL)
    }

    fn signal_group_with(owned: &OwnedChild, signal: Signal) -> GroupSignalOutcome {
        match owned.signal_group(signal) {
            Ok(()) => GroupSignalOutcome::Sent,
            Err(error) if error.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error()) => {
                GroupSignalOutcome::AlreadyGone
            }
            Err(error) => {
                GroupSignalOutcome::Failed(FailureEvidence::from_io("signal process group", &error))
            }
        }
    }

    fn forwarded_signal_to_os(signal: crate::ForwardedSignal) -> Signal {
        match signal {
            crate::ForwardedSignal::Hangup => Signal::HUP,
            crate::ForwardedSignal::Interrupt => Signal::INT,
            crate::ForwardedSignal::Terminate => Signal::TERM,
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

    #[path = "supervisor_evidence.rs"]
    mod evidence;
    use evidence::{
        Observations, choose_primary, classify, classify_managed, redirected_report,
        settlement_triggered, unknown_evidence,
    };

    #[path = "supervisor_capture.rs"]
    mod capture;
    use capture::CaptureStream;

    #[path = "supervisor_owned_child.rs"]
    mod owned_child;
    use owned_child::{OwnedChild, start_background_reaper};

    fn errno_to_io(error: rustix::io::Errno) -> std::io::Error {
        std::io::Error::from_raw_os_error(error.raw_os_error())
    }

    #[cfg(test)]
    #[path = "supervisor_tests.rs"]
    mod tests;
}
