//! Private owned-process-group supervisor shared by every adapter.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::Command;
use crate::error::Result;
use crate::policy::Ready;

pub(crate) struct ManagedLaunch {
    pub(crate) id: u32,
    pub(crate) stdin: Option<std::process::ChildStdin>,
    pub(crate) stdout: Option<std::process::ChildStdout>,
    pub(crate) stderr: Option<std::process::ChildStderr>,
    pub(crate) cancel: std::sync::mpsc::Sender<()>,
    pub(crate) result: std::sync::mpsc::Receiver<Result<std::process::ExitStatus>>,
}

/// Cooperative cancellation observed by the serialized supervisor loop.
#[derive(Clone, Default)]
pub(crate) struct Cancellation(Arc<AtomicBool>);

impl Cancellation {
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
    #[cfg(unix)]
    {
        unix::execute(prepared, cancellation)
    }
    #[cfg(not(unix))]
    {
        drop((prepared, cancellation));
        crate::error::UnsupportedCapabilitySnafu {
            capability: crate::error::Capability::OwnedProcessGroup,
        }
        .fail()
    }
}

pub(crate) fn spawn_managed(command: Command<Ready>) -> Result<ManagedLaunch> {
    if command.stdout_capture.is_unbounded() {
        return crate::error::InvalidPolicySnafu {
            violation: crate::policy::PolicyViolation::UnboundedCaptureForManaged(
                crate::output::StreamName::Stdout,
            ),
        }
        .fail();
    }
    if command.stderr_capture.is_unbounded() {
        return crate::error::InvalidPolicySnafu {
            violation: crate::policy::PolicyViolation::UnboundedCaptureForManaged(
                crate::output::StreamName::Stderr,
            ),
        }
        .fail();
    }
    let prepared = crate::command::prepare(command)?;
    #[cfg(unix)]
    {
        unix::spawn_managed(prepared)
    }
    #[cfg(not(unix))]
    {
        drop(prepared);
        crate::error::UnsupportedCapabilitySnafu {
            capability: crate::error::Capability::OwnedProcessGroup,
        }
        .fail()
    }
}

#[cfg(unix)]
mod unix {
    use std::io::Read as _;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::process::{Child, ExitStatus};
    use std::sync::mpsc::{self, Receiver, RecvTimeoutError, TryRecvError};
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use rustix::process::{Pid, Signal, WaitId, WaitIdOptions, kill_process_group, waitid};
    use snafu::ResultExt as _;

    use super::Cancellation;
    use crate::command::PreparedCommand;
    use crate::error::{
        CancelledSnafu, CaptureFailedSnafu, CaptureLimitExceededSnafu, CaptureReport,
        CaptureWorkerFailure, CleanupIncompleteEvidence, CleanupIncompleteSnafu, FailureEvidence,
        NonZeroExitSnafu, Result, SecondaryErrors, SpawnFailedSnafu, SupervisionFailedSnafu,
        TimeoutSnafu,
    };
    use crate::output::{CapturedStream, Output, StreamName};
    use crate::policy::{CapturePolicy, ExecutionPolicy, OverflowBehavior};
    use crate::supervisor::ManagedLaunch;

    const EVENT_QUANTUM: Duration = Duration::from_millis(10);
    const CLEANUP_BUDGET: Duration = Duration::from_secs(2);
    const READ_CHUNK: usize = 8 * 1024;

    pub(super) fn spawn_managed(prepared: PreparedCommand) -> Result<ManagedLaunch> {
        let PreparedCommand {
            mut command,
            program,
            execution,
            stdout_capture: _,
            stderr_capture: _,
        } = prepared;
        let child = command.spawn().context(SpawnFailedSnafu {
            program: program.clone(),
        })?;
        let mut owned = OwnedChild::new(child);
        let id = owned.child_mut().id();
        let stdin = owned.child_mut().stdin.take();
        let stdout = owned.child_mut().stdout.take();
        let stderr = owned.child_mut().stderr.take();
        let (cancel_tx, cancel_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        thread::spawn(move || {
            let result = supervise_managed(owned, program, execution, &cancel_rx);
            let _ = result_tx.send(result);
        });
        Ok(ManagedLaunch {
            id,
            stdin,
            stdout,
            stderr,
            cancel: cancel_tx,
            result: result_rx,
        })
    }

    fn supervise_managed(
        mut owned: OwnedChild,
        program: String,
        execution: ExecutionPolicy,
        cancel: &Receiver<()>,
    ) -> Result<ExitStatus> {
        let started = Instant::now();
        let deadline = execution.deadline(started);
        let duration = execution.duration();
        let primary = loop {
            if owned.leader_exited().map_err(|source| {
                SupervisionFailedSnafu {
                    program: program.clone(),
                    source,
                    stdout: CapturedStream::redirected(),
                    stderr: CapturedStream::redirected(),
                    secondary: SecondaryErrors::default(),
                }
                .build()
            })? {
                break Primary::Exit;
            }
            if deadline.is_some_and(|value| Instant::now() >= value) {
                break Primary::Deadline;
            }
            let wait = deadline
                .map(|value| value.saturating_duration_since(Instant::now()))
                .unwrap_or(EVENT_QUANTUM)
                .min(EVENT_QUANTUM);
            match cancel.recv_timeout(wait) {
                Ok(()) | Err(RecvTimeoutError::Disconnected) => break Primary::Cancelled,
                Err(RecvTimeoutError::Timeout) => {}
            }
        };

        let mut secondary = SecondaryErrors::default();
        if let Err(error) = owned.signal_group() {
            if error.kind() != std::io::ErrorKind::NotFound {
                secondary.signal = Some(FailureEvidence::from_io("signal process group", &error));
            }
        }
        let status = owned.reap().map_err(|source| {
            SupervisionFailedSnafu {
                program: program.clone(),
                source,
                stdout: CapturedStream::redirected(),
                stderr: CapturedStream::redirected(),
                secondary: SecondaryErrors::default(),
            }
            .build()
        })?;
        match primary {
            Primary::Exit => Ok(status),
            Primary::Deadline => TimeoutSnafu {
                program,
                duration: duration.unwrap_or_default(),
                stdout: CapturedStream::redirected(),
                stderr: CapturedStream::redirected(),
                secondary,
            }
            .fail(),
            Primary::Cancelled => CancelledSnafu {
                program,
                stdout: CapturedStream::redirected(),
                stderr: CapturedStream::redirected(),
                secondary,
            }
            .fail(),
            Primary::Limit { .. }
            | Primary::CaptureFailure
            | Primary::Supervision(_) => SupervisionFailedSnafu {
                program,
                source: std::io::Error::other("invalid managed supervisor state"),
                stdout: CapturedStream::redirected(),
                stderr: CapturedStream::redirected(),
                secondary,
            }
            .fail(),
        }
    }

    pub(super) fn execute(prepared: PreparedCommand, cancellation: Cancellation) -> Result<Output> {
        let PreparedCommand {
            mut command,
            program,
            execution,
            stdout_capture,
            stderr_capture,
        } = prepared;
        let started = Instant::now();
        let deadline = execution.deadline(started);
        let timeout = execution.duration();
        let span = tracing::info_span!(
            "epitelesis.supervise",
            program = %program,
            arg_count = command.get_args().count(),
            policy = ?execution,
        );
        let _entered = span.enter();

        let child = command.spawn().context(SpawnFailedSnafu {
            program: program.clone(),
        })?;
        let mut owned = OwnedChild::new(child);
        drop(owned.child_mut().stdin.take());

        let (event_tx, event_rx) = mpsc::channel();
        let stdout_worker = Worker::spawn(
            StreamName::Stdout,
            owned.child_mut().stdout.take(),
            stdout_capture,
            event_tx.clone(),
        );
        let stderr_worker = Worker::spawn(
            StreamName::Stderr,
            owned.child_mut().stderr.take(),
            stderr_capture,
            event_tx.clone(),
        );
        drop(event_tx);

        let mut outcomes = Outcomes::new(stdout_worker, stderr_worker);
        let mut observations = Observations::default();
        let primary = loop {
            outcomes.drain_events(&event_rx, &mut observations);
            observations.cancelled |= cancellation.is_cancelled();
            if !observations.leader_exited && observations.observe_error.is_none() {
                match owned.leader_exited() {
                    Ok(exited) => observations.leader_exited = exited,
                    Err(error) => observations.observe_error = Some(error),
                }
            }
            observations.deadline_elapsed = deadline.is_some_and(|value| Instant::now() >= value);

            if let Some(primary) = choose_primary(&observations, &outcomes) {
                break primary;
            }

            let wait = deadline
                .map(|value| value.saturating_duration_since(Instant::now()))
                .unwrap_or(EVENT_QUANTUM)
                .min(EVENT_QUANTUM);
            match event_rx.recv_timeout(wait) {
                Ok(event) => outcomes.apply_event(event, &mut observations),
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {}
            }
        };

        let cleanup_deadline = Instant::now()
            .checked_add(CLEANUP_BUDGET)
            .unwrap_or_else(Instant::now);
        let mut secondary = SecondaryErrors::default();
        if let Err(error) = owned.signal_group() {
            if error.kind() != std::io::ErrorKind::NotFound {
                secondary.signal = Some(FailureEvidence::from_io("signal process group", &error));
            }
        }

        while !observations.leader_exited && Instant::now() < cleanup_deadline {
            outcomes.drain_events(&event_rx, &mut observations);
            match owned.leader_exited() {
                Ok(exited) => observations.leader_exited = exited,
                Err(error) => {
                    if observations.observe_error.is_none() {
                        observations.observe_error = Some(error);
                    }
                    break;
                }
            }
            if !observations.leader_exited {
                let remaining = cleanup_deadline.saturating_duration_since(Instant::now());
                match event_rx.recv_timeout(remaining.min(EVENT_QUANTUM)) {
                    Ok(event) => outcomes.apply_event(event, &mut observations),
                    Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {}
                }
            }
        }

        let status = match owned.reap() {
            Ok(status) => Some(status),
            Err(error) => {
                secondary.reap = Some(FailureEvidence::from_io("reap process leader", &error));
                None
            }
        };

        while !outcomes.complete() && Instant::now() < cleanup_deadline {
            let remaining = cleanup_deadline.saturating_duration_since(Instant::now());
            match event_rx.recv_timeout(remaining.min(EVENT_QUANTUM)) {
                Ok(event) => outcomes.apply_event(event, &mut observations),
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {}
            }
        }
        outcomes.drain_events(&event_rx, &mut observations);

        let cleanup = outcomes.cleanup_evidence();
        let (stdout, stderr) = outcomes.into_reports();
        secondary.stdout_capture = stdout.failure.clone();
        secondary.stderr_capture = stderr.failure.clone();
        secondary.cleanup = cleanup.clone();

        tracing::debug!(
            primary = ?primary,
            duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            stdout_bytes = stdout.captured.len(),
            stderr_bytes = stderr.captured.len(),
            "invocation supervision settled"
        );

        classify(
            primary,
            program,
            timeout,
            status,
            stdout,
            stderr,
            secondary,
            cleanup,
            started.elapsed(),
        )
    }

    fn classify(
        primary: Primary,
        program: String,
        timeout: Option<Duration>,
        status: Option<ExitStatus>,
        stdout: CaptureReport,
        stderr: CaptureReport,
        secondary: SecondaryErrors,
        cleanup: Option<CleanupIncompleteEvidence>,
        duration: Duration,
    ) -> Result<Output> {
        match primary {
            Primary::Deadline => TimeoutSnafu {
                program,
                duration: timeout.unwrap_or_default(),
                stdout: stdout.captured,
                stderr: stderr.captured,
                secondary,
            }
            .fail(),
            Primary::Limit { stream, limit } => CaptureLimitExceededSnafu {
                program,
                stream,
                limit,
                stdout: stdout.captured,
                stderr: stderr.captured,
                secondary,
            }
            .fail(),
            Primary::Cancelled => CancelledSnafu {
                program,
                stdout: stdout.captured,
                stderr: stderr.captured,
                secondary,
            }
            .fail(),
            Primary::Supervision(error) => SupervisionFailedSnafu {
                program,
                source: error,
                stdout: stdout.captured,
                stderr: stderr.captured,
                secondary,
            }
            .fail(),
            Primary::CaptureFailure => CaptureFailedSnafu {
                program,
                stdout,
                stderr,
                secondary,
            }
            .fail(),
            Primary::Exit => {
                if stdout.failure.is_some() || stderr.failure.is_some() {
                    return CaptureFailedSnafu {
                        program,
                        stdout,
                        stderr,
                        secondary,
                    }
                    .fail();
                }
                if let Some(evidence) = cleanup {
                    return CleanupIncompleteSnafu {
                        program,
                        stdout: stdout.captured,
                        stderr: stderr.captured,
                        evidence,
                        secondary,
                    }
                    .fail();
                }
                let Some(status) = status else {
                    return SupervisionFailedSnafu {
                        program,
                        source: std::io::Error::other("process leader had no reap status"),
                        stdout: stdout.captured,
                        stderr: stderr.captured,
                        secondary,
                    }
                    .fail();
                };
                let output = Output {
                    status,
                    stdout: stdout.captured,
                    stderr: stderr.captured,
                    duration,
                };
                if output.success() {
                    Ok(output)
                } else {
                    NonZeroExitSnafu { program, output }.fail()
                }
            }
        }
    }

    #[derive(Default)]
    struct Observations {
        stdout_limit: Option<usize>,
        stderr_limit: Option<usize>,
        cancelled: bool,
        deadline_elapsed: bool,
        leader_exited: bool,
        observe_error: Option<std::io::Error>,
    }

    #[derive(Debug)]
    enum Primary {
        Limit { stream: StreamName, limit: usize },
        Cancelled,
        Deadline,
        CaptureFailure,
        Exit,
        Supervision(std::io::Error),
    }

    fn choose_primary(observations: &mut Observations, outcomes: &Outcomes) -> Option<Primary> {
        if let Some(limit) = observations.stdout_limit {
            return Some(Primary::Limit {
                stream: StreamName::Stdout,
                limit,
            });
        }
        if let Some(limit) = observations.stderr_limit {
            return Some(Primary::Limit {
                stream: StreamName::Stderr,
                limit,
            });
        }
        if observations.cancelled {
            return Some(Primary::Cancelled);
        }
        if observations.deadline_elapsed {
            return Some(Primary::Deadline);
        }
        if outcomes.capture_failed() {
            return Some(Primary::CaptureFailure);
        }
        if let Some(error) = observations.observe_error.take() {
            return Some(Primary::Supervision(error));
        }
        observations.leader_exited.then_some(Primary::Exit)
    }

    enum Event {
        Overflow { stream: StreamName, limit: usize },
        Report(CaptureReport),
    }

    struct Outcomes {
        stdout: Option<CaptureReport>,
        stderr: Option<CaptureReport>,
        stdout_worker: Worker,
        stderr_worker: Worker,
    }

    impl Outcomes {
        fn new(stdout_worker: Worker, stderr_worker: Worker) -> Self {
            Self {
                stdout: stdout_worker.immediate_report(),
                stderr: stderr_worker.immediate_report(),
                stdout_worker,
                stderr_worker,
            }
        }

        fn apply_event(&mut self, event: Event, observations: &mut Observations) {
            match event {
                Event::Overflow {
                    stream: StreamName::Stdout,
                    limit,
                } => {
                    observations.stdout_limit.get_or_insert(limit);
                }
                Event::Overflow {
                    stream: StreamName::Stderr,
                    limit,
                } => {
                    observations.stderr_limit.get_or_insert(limit);
                }
                Event::Report(report) => match report.stream {
                    StreamName::Stdout => {
                        self.stdout.get_or_insert(report);
                    }
                    StreamName::Stderr => {
                        self.stderr.get_or_insert(report);
                    }
                },
            };
        }

        fn drain_events(&mut self, receiver: &Receiver<Event>, observations: &mut Observations) {
            loop {
                match receiver.try_recv() {
                    Ok(event) => self.apply_event(event, observations),
                    Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                }
            }
        }

        fn complete(&self) -> bool {
            self.stdout.is_some()
                && self.stderr.is_some()
                && self.stdout_worker.finished()
                && self.stderr_worker.finished()
        }

        fn capture_failed(&self) -> bool {
            self.stdout.is_some()
                && self.stderr.is_some()
                && (self
                    .stdout
                    .as_ref()
                    .is_some_and(|report| report.failure.is_some())
                    || self
                        .stderr
                        .as_ref()
                        .is_some_and(|report| report.failure.is_some()))
        }

        fn cleanup_evidence(&self) -> Option<CleanupIncompleteEvidence> {
            let mut unfinished_streams = Vec::new();
            if self.stdout.is_none() || !self.stdout_worker.finished() {
                unfinished_streams.push(StreamName::Stdout);
            }
            if self.stderr.is_none() || !self.stderr_worker.finished() {
                unfinished_streams.push(StreamName::Stderr);
            }
            (!unfinished_streams.is_empty()).then_some(CleanupIncompleteEvidence {
                unfinished_streams,
                cleanup_budget: CLEANUP_BUDGET,
            })
        }

        fn into_reports(self) -> (CaptureReport, CaptureReport) {
            let stdout = self
                .stdout
                .unwrap_or_else(|| self.stdout_worker.snapshot_report());
            let stderr = self
                .stderr
                .unwrap_or_else(|| self.stderr_worker.snapshot_report());
            (stdout, stderr)
        }
    }

    struct Worker {
        stream: StreamName,
        state: Option<Arc<Mutex<WorkerState>>>,
        redirected: bool,
        _handle: Option<JoinHandle<()>>,
    }

    impl Worker {
        fn spawn<R: Read + Send + 'static>(
            stream: StreamName,
            pipe: Option<R>,
            policy: CapturePolicy,
            sender: mpsc::Sender<Event>,
        ) -> Self {
            let Some(mut pipe) = pipe else {
                return Self {
                    stream,
                    state: None,
                    redirected: true,
                    _handle: None,
                };
            };
            let state = Arc::new(Mutex::new(WorkerState::new(&policy)));
            let worker_state = Arc::clone(&state);
            let handle = thread::spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    read_stream(&mut pipe, &policy, &worker_state, stream, &sender)
                }));
                let failure = match result {
                    Ok(failure) => failure,
                    Err(_) => Some(CaptureWorkerFailure::Panicked),
                };
                let captured = take_captured(&worker_state);
                let _ = sender.send(Event::Report(CaptureReport {
                    stream,
                    captured,
                    failure,
                }));
            });
            Self {
                stream,
                state: Some(state),
                redirected: false,
                _handle: Some(handle),
            }
        }

        fn immediate_report(&self) -> Option<CaptureReport> {
            self.redirected.then(|| CaptureReport {
                stream: self.stream,
                captured: CapturedStream::redirected(),
                failure: None,
            })
        }

        fn snapshot_report(&self) -> CaptureReport {
            let captured = self
                .state
                .as_ref()
                .map(snapshot_captured)
                .unwrap_or_else(CapturedStream::redirected);
            CaptureReport {
                stream: self.stream,
                captured,
                failure: None,
            }
        }

        fn finished(&self) -> bool {
            self._handle
                .as_ref()
                .is_none_or(std::thread::JoinHandle::is_finished)
        }
    }

    struct WorkerState {
        buffer: CaptureBuffer,
        discarded: u64,
    }

    enum CaptureBuffer {
        Bounded {
            storage: Box<[u8]>,
            len: usize,
        },
        Unbounded(Vec<u8>),
    }

    impl WorkerState {
        fn new(policy: &CapturePolicy) -> Self {
            let buffer = match policy {
                CapturePolicy::Bounded { limit, .. } => CaptureBuffer::Bounded {
                    storage: vec![0; *limit].into_boxed_slice(),
                    len: 0,
                },
                CapturePolicy::Unbounded(_) => {
                    CaptureBuffer::Unbounded(Vec::with_capacity(READ_CHUNK))
                }
            };
            Self {
                buffer,
                discarded: 0,
            }
        }
    }

    fn read_stream<R: Read>(
        pipe: &mut R,
        policy: &CapturePolicy,
        state: &Arc<Mutex<WorkerState>>,
        stream: StreamName,
        sender: &mpsc::Sender<Event>,
    ) -> Option<CaptureWorkerFailure> {
        let mut chunk = [0_u8; READ_CHUNK];
        let mut overflow_sent = false;
        loop {
            let count = match pipe.read(&mut chunk) {
                Ok(0) => return None,
                Ok(count) => count,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    return Some(CaptureWorkerFailure::Read {
                        kind: error.kind(),
                        message: error.to_string(),
                    });
                }
            };
            let mut locked = lock_state(state);
            match policy {
                CapturePolicy::Unbounded(_) => match &mut locked.buffer {
                    CaptureBuffer::Unbounded(bytes) => bytes.extend_from_slice(&chunk[..count]),
                    CaptureBuffer::Bounded { .. } => {
                        panic!("capture policy and worker buffer diverged")
                    }
                },
                CapturePolicy::Bounded { limit, overflow } => {
                    let retained = match &mut locked.buffer {
                        CaptureBuffer::Bounded { storage, len } => {
                            let retained = count.min(limit.saturating_sub(*len));
                            let end = *len + retained;
                            storage[*len..end].copy_from_slice(&chunk[..retained]);
                            *len = end;
                            retained
                        }
                        CaptureBuffer::Unbounded(_) => {
                            panic!("capture policy and worker buffer diverged")
                        }
                    };
                    let discarded = count - retained;
                    locked.discarded = locked
                        .discarded
                        .saturating_add(u64::try_from(discarded).unwrap_or(u64::MAX));
                    if discarded > 0 && *overflow == OverflowBehavior::FailClosed && !overflow_sent
                    {
                        overflow_sent = true;
                        let _ = sender.send(Event::Overflow {
                            stream,
                            limit: *limit,
                        });
                    }
                }
            }
        }
    }

    fn lock_state(state: &Arc<Mutex<WorkerState>>) -> MutexGuard<'_, WorkerState> {
        match state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn take_captured(state: &Arc<Mutex<WorkerState>>) -> CapturedStream {
        let mut state = lock_state(state);
        let buffer = std::mem::replace(
            &mut state.buffer,
            CaptureBuffer::Unbounded(Vec::new()),
        );
        let bytes = match buffer {
            CaptureBuffer::Bounded { storage, len } => {
                let mut bytes = storage.into_vec();
                bytes.truncate(len);
                bytes.into_boxed_slice().into_vec()
            }
            CaptureBuffer::Unbounded(bytes) => bytes,
        };
        CapturedStream::complete(bytes, state.discarded)
    }

    fn snapshot_captured(state: &Arc<Mutex<WorkerState>>) -> CapturedStream {
        let state = lock_state(state);
        let bytes = match &state.buffer {
            CaptureBuffer::Bounded { storage, len } => storage[..*len]
                .to_vec()
                .into_boxed_slice()
                .into_vec(),
            CaptureBuffer::Unbounded(bytes) => bytes.clone(),
        };
        CapturedStream::complete(bytes, state.discarded)
    }

    struct OwnedChild {
        child: Option<Child>,
        pgid: Pid,
        armed: bool,
    }

    impl OwnedChild {
        fn new(child: Child) -> Self {
            let pgid = Pid::from_child(&child);
            Self {
                child: Some(child),
                pgid,
                armed: true,
            }
        }

        fn child_mut(&mut self) -> &mut Child {
            match self.child.as_mut() {
                Some(child) => child,
                None => panic!("owned child accessed after reap"),
            }
        }

        fn signal_group(&self) -> std::io::Result<()> {
            kill_process_group(self.pgid, Signal::KILL).map_err(errno_to_io)
        }

        fn leader_exited(&self) -> std::io::Result<bool> {
            let options = WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT;
            waitid(WaitId::Pid(self.pgid), options)
                .map(|status| status.is_some())
                .map_err(errno_to_io)
        }

        fn reap(&mut self) -> std::io::Result<ExitStatus> {
            let status = self.child_mut().wait()?;
            self.armed = false;
            Ok(status)
        }
    }

    impl Drop for OwnedChild {
        fn drop(&mut self) {
            if !self.armed {
                return;
            }
            let _ = kill_process_group(self.pgid, Signal::KILL);
            if let Some(child) = self.child.as_mut() {
                let _ = child.wait();
            }
            self.armed = false;
        }
    }

    fn errno_to_io(error: rustix::io::Errno) -> std::io::Error {
        std::io::Error::from_raw_os_error(error.raw_os_error())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn primary_race_precedence_is_stable() {
            let stdout = Worker {
                stream: StreamName::Stdout,
                state: None,
                redirected: true,
                _handle: None,
            };
            let stderr = Worker {
                stream: StreamName::Stderr,
                state: None,
                redirected: true,
                _handle: None,
            };
            let outcomes = Outcomes::new(stdout, stderr);
            let mut all = Observations {
                stdout_limit: Some(7),
                stderr_limit: Some(9),
                cancelled: true,
                deadline_elapsed: true,
                leader_exited: true,
                observe_error: None,
            };
            assert!(matches!(
                choose_primary(&mut all, &outcomes),
                Some(Primary::Limit {
                    stream: StreamName::Stdout,
                    limit: 7
                })
            ));

            all.stdout_limit = None;
            all.stderr_limit = None;
            assert!(matches!(
                choose_primary(&mut all, &outcomes),
                Some(Primary::Cancelled)
            ));
            all.cancelled = false;
            assert!(matches!(
                choose_primary(&mut all, &outcomes),
                Some(Primary::Deadline)
            ));
            all.deadline_elapsed = false;
            assert!(matches!(
                choose_primary(&mut all, &outcomes),
                Some(Primary::Exit)
            ));
        }

        #[test]
        fn worker_matrix_resolves_stdout_before_stderr_with_peer_evidence() {
            let outcomes = [
                None,
                Some(CaptureWorkerFailure::Panicked),
                Some(CaptureWorkerFailure::Read {
                    kind: std::io::ErrorKind::Other,
                    message: "fixture".to_owned(),
                }),
            ];
            for stdout_failure in &outcomes {
                for stderr_failure in &outcomes {
                    let stdout = CaptureReport {
                        stream: StreamName::Stdout,
                        captured: CapturedStream::complete(b"out".to_vec(), 0),
                        failure: stdout_failure.clone(),
                    };
                    let stderr = CaptureReport {
                        stream: StreamName::Stderr,
                        captured: CapturedStream::complete(b"err".to_vec(), 0),
                        failure: stderr_failure.clone(),
                    };
                    assert_eq!(stdout.stream, StreamName::Stdout);
                    assert_eq!(stderr.stream, StreamName::Stderr);
                    assert_eq!(stdout.captured.bytes, b"out");
                    assert_eq!(stderr.captured.bytes, b"err");
                }
            }
        }

        #[test]
        fn armed_guard_cleans_up_during_unwind() {
            use std::os::unix::process::CommandExt as _;

            let pid = Arc::new(Mutex::new(None));
            let observed_pid = Arc::clone(&pid);
            let unwind = catch_unwind(AssertUnwindSafe(move || {
                let mut command = std::process::Command::new("/bin/sleep");
                command.arg("30").process_group(0);
                let child = match command.spawn() {
                    Ok(child) => child,
                    Err(error) => panic!("sleep fixture failed to spawn: {error}"),
                };
                *lock_optional_pid(&observed_pid) = Some(child.id());
                let _owned = OwnedChild::new(child);
                panic!("fixture unwind");
            }));
            assert!(unwind.is_err());
            let pid = match *lock_optional_pid(&pid) {
                Some(pid) => pid,
                None => panic!("fixture never recorded its pid"),
            };
            #[cfg(target_os = "linux")]
            assert!(
                !std::path::Path::new(&format!("/proc/{pid}")).exists(),
                "armed guard must kill and reap during unwind"
            );
        }

        fn lock_optional_pid(value: &Arc<Mutex<Option<u32>>>) -> MutexGuard<'_, Option<u32>> {
            match value.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }
    }
}
