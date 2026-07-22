//! Managed streaming child handle.

use std::process::{ChildStderr, ChildStdin, ChildStdout, ExitStatus};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::time::Duration;

use crate::Command;
use crate::error::{Error, Result, SupervisionFailedSnafu};
use crate::output::CapturedStream;
use crate::policy::Ready;

/// A streaming child whose deadline, cancellation, group cleanup, and reap are
/// owned by a background supervisor rather than caller polling.
#[must_use = "dropping ManagedChild cancels and reaps the owned process group"]
pub struct ManagedChild {
    id: u32,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    cancel: Sender<()>,
    receiver: Receiver<Result<ExitStatus>>,
    result: Option<Result<ExitStatus>>,
    settled: bool,
}

/// Spawn a managed streaming child.
pub fn spawn_managed(command: Command<Ready>) -> Result<ManagedChild> {
    let launch = crate::supervisor::spawn_managed(command)?;
    Ok(ManagedChild {
        id: launch.id,
        stdin: launch.stdin,
        stdout: launch.stdout,
        stderr: launch.stderr,
        cancel: launch.cancel,
        receiver: launch.result,
        result: None,
        settled: false,
    })
}

impl ManagedChild {
    /// Return the process leader identifier.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Take the streaming stdin handle at most once.
    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.stdin.take()
    }

    /// Take the streaming stdout handle at most once.
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.stdout.take()
    }

    /// Take the streaming stderr handle at most once.
    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.stderr.take()
    }

    /// Wait for the background supervisor's terminal result.
    pub fn wait(&mut self) -> Result<ExitStatus> {
        self.receive_blocking();
        match self.result.take() {
            Some(result) => result,
            None => SupervisionFailedSnafu {
                program: format!("managed child {}", self.id),
                source: std::io::Error::other("managed supervisor result channel closed"),
                stdout: CapturedStream::redirected(),
                stderr: CapturedStream::redirected(),
                secondary: crate::SecondaryErrors::default(),
            }
            .fail(),
        }
    }

    /// Poll for a successful terminal status without weakening supervision.
    ///
    /// A terminal error remains owned by the handle and is returned by
    /// [`ManagedChild::wait`] or [`ManagedChild::cancel`].
    pub fn try_wait(&mut self) -> Option<ExitStatus> {
        self.receive_nonblocking();
        match self.result.as_ref() {
            Some(Ok(status)) => Some(*status),
            Some(Err(_)) | None => None,
        }
    }

    /// Cancel the process group and wait until the background supervisor reaps it.
    pub fn cancel(&mut self) -> Result<()> {
        let _ = self.cancel.send(());
        match self.wait() {
            Ok(_) | Err(Error::Cancelled { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn receive_nonblocking(&mut self) {
        if self.result.is_some() {
            return;
        }
        match self.receiver.try_recv() {
            Ok(result) => {
                self.result = Some(result);
                self.settled = true;
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }
    }

    fn receive_blocking(&mut self) {
        if self.result.is_none() {
            self.result = self.receiver.recv().ok();
            self.settled = self.result.is_some();
        }
    }
}

impl std::fmt::Debug for ManagedChild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedChild")
            .field("id", &self.id)
            .field("stdin", &self.stdin.as_ref().map(|_| "available"))
            .field("stdout", &self.stdout.as_ref().map(|_| "available"))
            .field("stderr", &self.stderr.as_ref().map(|_| "available"))
            .field("settled", &self.result.is_some())
            .finish()
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let _ = self.cancel.send(());
        match self.receiver.recv_timeout(Duration::from_secs(3)) {
            Ok(result) => {
                self.result = Some(result);
                self.settled = true;
            }
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {}
        }
    }
}
