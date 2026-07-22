//! Typed errors and secondary cleanup evidence.

use std::io::ErrorKind;
use std::time::Duration;

use snafu::Snafu;

use crate::output::{CapturedStream, Output, StreamName};
use crate::policy::PolicyViolation;

/// Capability required for the crate's lifecycle guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Capability {
    /// A child-led process group owned through signal, settlement, and reap.
    OwnedProcessGroup,
}

/// Stable, owned representation of a secondary operating-system failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureEvidence {
    /// Cleanup or lifecycle operation that failed.
    pub operation: &'static str,
    /// Portable error category.
    pub kind: ErrorKind,
    /// Operating-system error text.
    pub message: String,
}

impl FailureEvidence {
    pub(crate) fn from_io(operation: &'static str, error: &std::io::Error) -> Self {
        Self {
            operation,
            kind: error.kind(),
            message: error.to_string(),
        }
    }
}

/// Failure reported by a capture worker.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CaptureWorkerFailure {
    /// Reading the pipe returned an I/O error.
    Read {
        /// Portable error category.
        kind: ErrorKind,
        /// I/O error text.
        message: String,
    },
    /// The worker panicked; the panic was contained and typed.
    Panicked,
}

/// Deterministic report for one capture worker.
#[derive(Debug, Eq, PartialEq)]
pub struct CaptureReport {
    /// Stream this report describes.
    pub stream: StreamName,
    /// Prefix and completeness evidence retained before completion or failure.
    pub captured: CapturedStream,
    /// Worker failure, or `None` when capture completed normally.
    pub failure: Option<CaptureWorkerFailure>,
}

/// Evidence that bounded cleanup could not prove all capture workers finished.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupIncompleteEvidence {
    /// Streams whose pipe never reached a terminal worker result.
    pub unfinished_streams: Vec<StreamName>,
    /// Shared cleanup budget that elapsed.
    pub cleanup_budget: Duration,
}

/// Secondary failures retained without replacing the primary outcome.
#[derive(Debug, Default)]
pub struct SecondaryErrors {
    /// Failure while signaling the owned process group.
    pub signal: Option<FailureEvidence>,
    /// Failure while reaping the leader.
    pub reap: Option<FailureEvidence>,
    /// Stdout worker report when it failed.
    pub stdout_capture: Option<CaptureWorkerFailure>,
    /// Stderr worker report when it failed.
    pub stderr_capture: Option<CaptureWorkerFailure>,
    /// Evidence that an escaped pipe owner defeated bounded cleanup.
    pub cleanup: Option<CleanupIncompleteEvidence>,
}

/// Errors produced by Epitelesis.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[non_exhaustive]
pub enum Error {
    /// A policy was invalid before process creation.
    #[snafu(display("invalid invocation policy: {violation}"))]
    InvalidPolicy {
        /// Specific invalid policy fact.
        violation: PolicyViolation,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The current backend cannot provide owned process-group containment.
    #[snafu(display("unsupported invocation capability: {capability:?}"))]
    UnsupportedCapability {
        /// Capability that was required before spawn.
        capability: Capability,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The kernel refused to create the child.
    #[snafu(display("failed to spawn {program}: {source}"))]
    SpawnFailed {
        /// Display form of the program.
        program: String,
        /// Underlying spawn error.
        source: std::io::Error,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The leader exited non-zero; this variant owns the sole output payload.
    #[snafu(display("{program} exited with non-zero status {}", output.status))]
    NonZeroExit {
        /// Display form of the program.
        program: String,
        /// Sole captured output and status payload.
        output: Output,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The declared deadline won the serialized outcome race.
    #[snafu(display("{program} timed out after {duration:?}"))]
    Timeout {
        /// Display form of the program.
        program: String,
        /// Declared lifetime.
        duration: Duration,
        /// Stdout evidence retained before cleanup settled.
        stdout: CapturedStream,
        /// Stderr evidence retained before cleanup settled.
        stderr: CapturedStream,
        /// Secondary signal, reap, capture, or cleanup failures.
        secondary: SecondaryErrors,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// A fail-closed capture limit won the serialized outcome race.
    #[snafu(display("{program} exceeded the {stream:?} capture limit of {limit} bytes"))]
    CaptureLimitExceeded {
        /// Display form of the program.
        program: String,
        /// Stream whose limit was exceeded first by deterministic precedence.
        stream: StreamName,
        /// Declared byte bound.
        limit: usize,
        /// Stdout evidence retained before cleanup settled.
        stdout: CapturedStream,
        /// Stderr evidence retained before cleanup settled.
        stderr: CapturedStream,
        /// Secondary cleanup evidence.
        secondary: SecondaryErrors,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Explicit cancellation won the serialized outcome race.
    #[snafu(display("{program} was cancelled"))]
    Cancelled {
        /// Display form of the program.
        program: String,
        /// Stdout evidence retained before cleanup settled.
        stdout: CapturedStream,
        /// Stderr evidence retained before cleanup settled.
        stderr: CapturedStream,
        /// Secondary cleanup evidence.
        secondary: SecondaryErrors,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// One or both capture workers failed after both outcomes were resolved.
    #[snafu(display("capture failed while executing {program}"))]
    CaptureFailed {
        /// Display form of the program.
        program: String,
        /// Stdout report, always first.
        stdout: CaptureReport,
        /// Stderr report, always second.
        stderr: CaptureReport,
        /// Secondary cleanup evidence.
        secondary: SecondaryErrors,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The supervisor could not observe or wait for the child leader.
    #[snafu(display("failed while supervising {program}: {source}"))]
    SupervisionFailed {
        /// Display form of the program.
        program: String,
        /// Underlying observation error.
        source: std::io::Error,
        /// Stdout retained before supervision failed.
        stdout: CapturedStream,
        /// Stderr retained before supervision failed.
        stderr: CapturedStream,
        /// Secondary cleanup evidence.
        secondary: SecondaryErrors,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// An escaped descendant retained a pipe beyond the shared cleanup bound.
    #[snafu(display("cleanup incomplete while executing {program}"))]
    CleanupIncomplete {
        /// Display form of the program.
        program: String,
        /// Stdout evidence retained before returning.
        stdout: CapturedStream,
        /// Stderr evidence retained before returning.
        stderr: CapturedStream,
        /// Exact streams and budget that remained incomplete.
        evidence: CleanupIncompleteEvidence,
        /// Other cleanup failures.
        secondary: SecondaryErrors,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Convenience result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;
