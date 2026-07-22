//! Captured output and aggregate lifecycle evidence.

use std::process::ExitStatus;
use std::time::Duration;

use crate::error::{CaptureFailure, CleanupIncompleteEvidence, FailureEvidence};

/// Result of the required process-group signal attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GroupSignalOutcome {
    /// The kernel accepted the signal for the process group.
    Sent,
    /// The group was already absent when signaling was attempted.
    AlreadyGone,
    /// Signaling failed for another reason.
    Failed(FailureEvidence),
    /// A supervisor adapter failed before the outcome could be recovered.
    Unknown,
}

/// Final ownership disposition of the leader reap obligation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LeaderReapDisposition {
    /// The serialized supervisor reaped the waitable leader.
    Reaped,
    /// The cleanup deadline transferred the child to the named reaper.
    BackgroundReaper,
    /// No reaper had been established when the evidence was recorded.
    Unreaped,
    /// A supervisor adapter failed before the disposition could be recovered.
    Unknown,
}

/// Typed reap disposition plus every failure encountered while settling it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaderReapOutcome {
    /// Final owner/disposition of the reap obligation.
    pub disposition: LeaderReapDisposition,
    /// Wait or background-reaper creation failures in observation order.
    pub failures: Vec<FailureEvidence>,
}

/// Identifies one standard output stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamName {
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
}

/// Whether a captured byte prefix represents the complete stream.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CaptureCompleteness {
    /// The pipe reached EOF and every byte was retained.
    Complete,
    /// The pipe reached EOF but bytes after the retained prefix were drained.
    Truncated {
        /// Number of drained but unretained bytes.
        discarded: u64,
    },
    /// EOF was not observed before a read failure or the cleanup deadline.
    Incomplete {
        /// Number of bytes known to have been drained but not retained.
        discarded: u64,
    },
    /// The stream was not connected to the supervisor capture pipe.
    Redirected,
    /// An adapter failed before the stream evidence could be recovered.
    Unknown,
}

/// Bytes retained from one stream together with their completeness state.
#[derive(Debug, Eq, PartialEq)]
pub struct CapturedStream {
    /// Bounded prefix retained by the supervisor.
    pub bytes: Vec<u8>,
    /// Whether the bytes are complete, truncated, incomplete, redirected, or
    /// unknown.
    pub completeness: CaptureCompleteness,
}

impl CapturedStream {
    pub(crate) fn settled(bytes: Vec<u8>, discarded: u64) -> Self {
        let completeness = if discarded == 0 {
            CaptureCompleteness::Complete
        } else {
            CaptureCompleteness::Truncated { discarded }
        };
        Self {
            bytes,
            completeness,
        }
    }

    pub(crate) fn incomplete(bytes: Vec<u8>, discarded: u64) -> Self {
        Self {
            bytes,
            completeness: CaptureCompleteness::Incomplete { discarded },
        }
    }

    pub(crate) fn redirected() -> Self {
        Self {
            bytes: Vec::new(),
            completeness: CaptureCompleteness::Redirected,
        }
    }

    pub(crate) fn unknown() -> Self {
        Self {
            bytes: Vec::new(),
            completeness: CaptureCompleteness::Unknown,
        }
    }

    /// View retained bytes as UTF-8.
    pub fn as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.bytes)
    }

    /// Return the number of retained bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Return whether the retained prefix is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Deterministic report for one capture stream.
#[derive(Debug, Eq, PartialEq)]
pub struct CaptureReport {
    /// Stream this report describes.
    pub stream: StreamName,
    /// Prefix and completeness evidence retained by the supervisor.
    pub captured: CapturedStream,
    /// Specific recovered capture failure, if one is available.
    ///
    /// `None` does not assert success when `captured.completeness` is
    /// [`CaptureCompleteness::Unknown`].
    pub failure: Option<CaptureFailure>,
}

/// Whether cleanup was proved complete, incomplete, or could not be recovered.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CleanupOutcome {
    /// The serialized supervisor settled every owned cleanup obligation.
    Complete,
    /// The cleanup deadline expired with the attached unsettled facts.
    Incomplete(CleanupIncompleteEvidence),
    /// An adapter failed before cleanup evidence could be recovered.
    Unknown,
}

/// Facts retained by every post-spawn terminal result.
#[derive(Debug)]
#[non_exhaustive]
pub struct LifecycleEvidence {
    /// Leader status when a non-reaping observation made reap safe and reap succeeded.
    pub leader_status: Option<ExitStatus>,
    /// Actual elapsed time from the shared pre-spawn instant, when recoverable.
    pub elapsed: Option<Duration>,
    /// Actual result of signaling the owned process group.
    pub signal: GroupSignalOutcome,
    /// Actual leader reap disposition and failures.
    pub reap: LeaderReapOutcome,
    /// Stdout report, always ordered before stderr.
    pub stdout: CaptureReport,
    /// Stderr report, always ordered after stdout.
    pub stderr: CaptureReport,
    /// Recovered cleanup outcome.
    pub cleanup: CleanupOutcome,
}

impl LifecycleEvidence {
    /// Return the first capture failure using stable stdout-before-stderr order.
    #[must_use]
    pub fn first_capture_failure(&self) -> Option<StreamName> {
        if self.stdout.failure.is_some() {
            Some(StreamName::Stdout)
        } else if self.stderr.failure.is_some() {
            Some(StreamName::Stderr)
        } else {
            None
        }
    }
}

/// Result of a completed subprocess invocation with a required leader status.
///
/// The aggregate is boxed so moving a result does not move its potentially
/// large capture buffers inline.
#[derive(Debug)]
#[non_exhaustive]
pub struct Output {
    /// Aggregate post-spawn lifecycle evidence.
    pub evidence: Box<LifecycleEvidence>,
    status: ExitStatus,
}

/// Successful terminal result for explicit managed streaming.
#[derive(Debug)]
#[non_exhaustive]
pub struct ManagedOutput {
    /// Aggregate post-spawn lifecycle evidence; stream reports are redirected
    /// because byte ownership belongs to the caller.
    pub evidence: Box<LifecycleEvidence>,
    status: ExitStatus,
}

impl ManagedOutput {
    pub(crate) fn new(status: ExitStatus, evidence: Box<LifecycleEvidence>) -> Self {
        Self { evidence, status }
    }

    /// Return the required leader status.
    #[must_use]
    pub fn status(&self) -> ExitStatus {
        self.status
    }

    /// Whether the leader exited successfully.
    #[must_use]
    pub fn success(&self) -> bool {
        self.status.success()
    }
}

impl Output {
    pub(crate) fn new(status: ExitStatus, evidence: Box<LifecycleEvidence>) -> Self {
        Self { evidence, status }
    }

    /// View retained stdout as UTF-8.
    pub fn stdout_str(&self) -> Result<&str, std::str::Utf8Error> {
        self.evidence.stdout.captured.as_str()
    }

    /// View retained stderr as UTF-8.
    pub fn stderr_str(&self) -> Result<&str, std::str::Utf8Error> {
        self.evidence.stderr.captured.as_str()
    }

    /// Return the required leader status.
    #[must_use]
    pub fn status(&self) -> ExitStatus {
        self.status
    }

    /// Whether the leader exited successfully.
    #[must_use]
    pub fn success(&self) -> bool {
        self.status.success()
    }
}
