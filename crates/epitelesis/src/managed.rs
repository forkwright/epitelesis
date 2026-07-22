//! Managed streaming child handle.

use std::process::{ChildStderr, ChildStdin, ChildStdout};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

use snafu::IntoError as _;

use crate::command::StreamingCommand;
use crate::error::{Error, Result, SupervisionFailedSnafu};
use crate::output::{
    CaptureReport, CapturedStream, CleanupOutcome, GroupSignalOutcome, LeaderReapDisposition,
    LeaderReapOutcome, LifecycleEvidence, ManagedOutput, StreamName,
};

/// Typed nonblocking state of a managed streaming child.
#[derive(Debug)]
#[non_exhaustive]
pub enum ManagedPoll<'a> {
    /// The supervisor is running or cleaning up.
    Running,
    /// The leader exited successfully and cleanup settled.
    Exited(&'a ManagedOutput),
    /// A terminal lifecycle error is retained for [`ManagedChild::wait`].
    Failed(&'a Error),
}

/// A streaming child whose deadline, cancellation, group cleanup, and reap are
/// owned by a background supervisor rather than caller polling.
///
/// Drop requests cancellation without blocking. The detached supervisor
/// retains sole lifecycle ownership through bounded cleanup and any required
/// background-reaper handoff.
#[must_use = "dropping ManagedChild only requests cancellation"]
pub struct ManagedChild {
    id: u32,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    cancel: Sender<()>,
    receiver: Receiver<Result<ManagedOutput>>,
    supervisor: Option<std::thread::JoinHandle<()>>,
    result: Option<Result<ManagedOutput>>,
}

/// Spawn an explicitly streaming managed child.
///
/// A captured command must first make the fallible structural streaming transition:
///
/// ```compile_fail
/// use epitelesis::{Command, spawn_managed};
/// use std::time::Duration;
///
/// let captured = Command::new("/bin/true").deadline(Duration::from_secs(1))?;
/// let _ = spawn_managed(captured);
/// # Ok::<(), epitelesis::Error>(())
/// ```
pub fn spawn_managed(command: StreamingCommand) -> Result<ManagedChild> {
    let launch = crate::supervisor::spawn_managed(command)?;
    Ok(ManagedChild {
        id: launch.id,
        stdin: launch.stdin,
        stdout: launch.stdout,
        stderr: launch.stderr,
        cancel: launch.cancel,
        receiver: launch.result,
        supervisor: Some(launch.supervisor),
        result: None,
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

    /// Close retained stdin, consume the handle, and wait for its terminal result.
    pub fn wait(mut self) -> Result<ManagedOutput> {
        self.wait_inner()
    }

    fn wait_inner(&mut self) -> Result<ManagedOutput> {
        drop(self.stdin.take());
        self.receive_blocking();
        match self.result.take() {
            Some(result) => result,
            None => Err(self.channel_failure("managed supervisor result channel closed")),
        }
    }

    /// Poll without conflating a running child with a retained terminal error.
    pub fn poll(&mut self) -> ManagedPoll<'_> {
        self.receive_nonblocking();
        match self.result.as_ref() {
            Some(Ok(output)) => ManagedPoll::Exited(output),
            Some(Err(error)) => ManagedPoll::Failed(error),
            None => ManagedPoll::Running,
        }
    }

    /// Request cancellation, consume the handle, and wait for settlement.
    ///
    /// A settled cancellation returns its aggregate evidence. Incomplete or
    /// failed cleanup remains an evidence-bearing terminal error.
    pub fn cancel(mut self) -> Result<Box<LifecycleEvidence>> {
        let _ = self.cancel.send(());
        match self.wait_inner() {
            Ok(output) => Ok(output.evidence),
            Err(Error::Cancelled { evidence, .. }) if cancellation_settled(&evidence) => {
                Ok(evidence)
            }
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
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                self.result = Some(Err(
                    self.channel_failure("managed supervisor result channel closed")
                ));
            }
        }
    }

    fn receive_blocking(&mut self) {
        if self.result.is_none() {
            self.result = Some(match self.receiver.recv() {
                Ok(result) => result,
                Err(_) => Err(self.channel_failure("managed supervisor result channel closed")),
            });
        }
        self.join_after_result();
    }

    fn join_after_result(&mut self) {
        let Some(supervisor) = self.supervisor.take() else {
            return;
        };
        if supervisor.join().is_err() {
            self.result = match self.result.take() {
                Some(Ok(output)) => Some(Err(SupervisionFailedSnafu {
                    program: format!("managed child {}", self.id),
                    evidence: output.evidence,
                }
                .into_error(std::io::Error::other("managed supervisor panicked")))),
                other => other,
            };
        }
    }

    fn channel_failure(&self, message: &'static str) -> Error {
        SupervisionFailedSnafu {
            program: format!("managed child {}", self.id),
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
        .into_error(std::io::Error::other(message))
    }
}

fn cancellation_settled(evidence: &LifecycleEvidence) -> bool {
    matches!(evidence.cleanup, CleanupOutcome::Complete)
        && matches!(
            evidence.signal,
            GroupSignalOutcome::Sent | GroupSignalOutcome::AlreadyGone
        )
        && matches!(evidence.reap.disposition, LeaderReapDisposition::Reaped)
        && evidence.reap.failures.is_empty()
}

impl std::fmt::Debug for ManagedChild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedChild")
            .field("id", &self.id)
            .field("stdin", &self.stdin.as_ref().map(|_| "available"))
            .field("stdout", &self.stdout.as_ref().map(|_| "available"))
            .field("stderr", &self.stderr.as_ref().map(|_| "available"))
            .field("settled", &self.result.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if self.supervisor.is_none() {
            return;
        }
        if self.result.is_none() {
            drop(self.stdin.take());
            let _ = self.cancel.send(());
        }
        drop(self.supervisor.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_cancels_without_waiting_for_supervisor_settlement() {
        let (cancel, cancel_receiver) = std::sync::mpsc::channel();
        let (result_sender, receiver) = std::sync::mpsc::sync_channel(1);
        let (release, release_receiver) = std::sync::mpsc::channel();
        let (ready, ready_receiver) = std::sync::mpsc::sync_channel(0);
        let (finished, finished_receiver) = std::sync::mpsc::sync_channel(1);
        let supervisor = std::thread::spawn(move || {
            assert!(ready.send(()).is_ok());
            assert!(cancel_receiver.recv().is_ok());
            assert!(release_receiver.recv().is_ok());
            drop(result_sender);
            assert!(finished.send(()).is_ok());
        });
        let child = ManagedChild {
            id: 0,
            stdin: None,
            stdout: None,
            stderr: None,
            cancel,
            receiver,
            supervisor: Some(supervisor),
            result: None,
        };
        assert!(
            ready_receiver
                .recv_timeout(std::time::Duration::from_secs(1))
                .is_ok(),
            "fixture supervisor did not start"
        );

        let started = std::time::Instant::now();
        drop(child);
        let elapsed = started.elapsed();
        assert!(release.send(()).is_ok());
        assert!(
            finished_receiver
                .recv_timeout(std::time::Duration::from_secs(1))
                .is_ok(),
            "detached supervisor did not finish"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "drop blocked for {elapsed:?}"
        );
    }
}
