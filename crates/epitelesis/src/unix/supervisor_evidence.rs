//! Terminal evidence classification for the Unix supervisor.

use super::*;

#[derive(Default)]
pub(super) struct Observations {
    pub(super) cancelled: bool,
    pub(super) deadline_elapsed: bool,
    pub(super) leader_waitable: bool,
    pub(super) observe_error: Option<std::io::Error>,
    pub(super) reap_failures: Vec<FailureEvidence>,
}

#[derive(Debug)]
pub(super) enum Primary {
    Limit { stream: StreamName, limit: usize },
    Cancelled,
    Deadline,
    CaptureFailure,
    Supervision(std::io::Error),
    Exit,
}

pub(super) fn settlement_triggered(
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

pub(super) fn choose_primary(
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

pub(super) fn classify(
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
        Primary::Exit => {
            classify_exit(program, evidence).map(|(status, evidence)| Output::new(status, evidence))
        }
    }
}

pub(super) fn classify_managed(
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

pub(super) fn redirected_report(stream: StreamName) -> CaptureReport {
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

pub(super) fn unknown_evidence() -> Box<LifecycleEvidence> {
    Box::new(LifecycleEvidence {
        leader_status: None,
        elapsed: None,
        signal: GroupSignalOutcome::Unknown,
        forwarded_signal: None,
        reap: LeaderReapOutcome {
            disposition: LeaderReapDisposition::Unknown,
            failures: Vec::new(),
        },
        stdout: unknown_report(StreamName::Stdout),
        stderr: unknown_report(StreamName::Stderr),
        cleanup: CleanupOutcome::Unknown,
    })
}
