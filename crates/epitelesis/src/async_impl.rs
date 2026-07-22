//! Thin Tokio adapter over the shared blocking supervisor.

use crate::Command;
use snafu::IntoError as _;

use crate::error::{Result, SupervisionFailedSnafu};
use crate::output::{
    CaptureReport, CapturedStream, CleanupOutcome, GroupSignalOutcome, LeaderReapDisposition,
    LeaderReapOutcome, LifecycleEvidence, Output, StreamName,
};
use crate::policy::Ready;

struct CancelOnDrop {
    cancellation: crate::supervisor::Cancellation,
    armed: bool,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

/// Asynchronously execute a ready command through the same owned-group state machine.
///
/// Dropping this future signals cancellation; the blocking supervisor remains
/// alive long enough to terminate the process group, settle capture, and reap.
pub async fn spawn(command: Command<Ready>) -> Result<Output> {
    let cancellation = crate::supervisor::Cancellation::default();
    let worker_cancellation = cancellation.clone();
    let mut guard = CancelOnDrop {
        cancellation,
        armed: true,
    };
    let worker = tokio::task::spawn_blocking(move || {
        crate::supervisor::execute(command, worker_cancellation)
    });
    let result = worker.await.map_err(|source| {
        SupervisionFailedSnafu {
            program: "asynchronous supervisor".to_owned(),
            evidence: Box::new(LifecycleEvidence {
                leader_status: None,
                elapsed: None,
                signal: GroupSignalOutcome::Unknown,
                reap: LeaderReapOutcome {
                    disposition: LeaderReapDisposition::Unknown,
                    failures: Vec::new(),
                },
                stdout: CaptureReport {
                    stream: StreamName::Stdout,
                    captured: CapturedStream::unknown(),
                    failure: None,
                },
                stderr: CaptureReport {
                    stream: StreamName::Stderr,
                    captured: CapturedStream::unknown(),
                    failure: None,
                },
                cleanup: CleanupOutcome::Unknown,
            }),
        }
        .into_error(std::io::Error::other(source))
    })?;
    guard.armed = false;
    result
}
