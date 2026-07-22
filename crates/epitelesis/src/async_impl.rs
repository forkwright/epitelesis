//! Thin Tokio adapter over the shared blocking supervisor.

use crate::Command;
use crate::error::{Result, SecondaryErrors, SupervisionFailedSnafu};
use crate::output::{CapturedStream, Output};
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
    let result = worker.await.map_err(|error| {
        SupervisionFailedSnafu {
            program: "asynchronous supervisor".to_owned(),
            source: std::io::Error::other(error.to_string()),
            stdout: CapturedStream::redirected(),
            stderr: CapturedStream::redirected(),
            secondary: SecondaryErrors::default(),
        }
        .build()
    })?;
    guard.armed = false;
    result
}
