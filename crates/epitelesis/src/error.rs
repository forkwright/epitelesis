//! Typed errors and aggregate cleanup evidence.

use std::io::ErrorKind;
use std::time::Duration;

use snafu::Snafu;

use crate::output::{LifecycleEvidence, StreamName};
use crate::policy::PolicyViolation;

/// Capability required for the crate's lifecycle guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Capability {
    /// A child-led process group owned through signal, settlement, and reap.
    OwnedProcessGroup,
}

/// Stable, owned representation of an operating-system failure.
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

/// Failure while pumping a capture pipe in the supervisor event loop.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CaptureFailure {
    /// Reading the pipe returned an I/O error.
    Read {
        /// Portable error category.
        kind: ErrorKind,
        /// I/O error text.
        message: String,
    },
    /// Retaining captured bytes failed because storage could not grow.
    ///
    /// This variant deliberately carries no allocated message so reporting an
    /// allocation failure cannot itself require another allocation.
    Allocation,
}

/// Evidence that the shared cleanup deadline expired before full settlement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupIncompleteEvidence {
    /// Streams whose pipes did not reach EOF.
    pub unfinished_streams: Vec<StreamName>,
    /// Whether the leader was unsettled when the cleanup budget expired and
    /// background-reaper transfer began.
    pub leader_unsettled: bool,
    /// Shared cleanup budget that elapsed.
    pub cleanup_budget: Duration,
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

    /// A supervisor thread could not be created before process creation.
    #[snafu(display("failed to start supervisor for {program}: {source}"))]
    SupervisorStartFailed {
        /// Display form of the program.
        program: String,
        /// Underlying thread creation error.
        source: std::io::Error,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The fallback reaper thread could not be created before process creation.
    #[snafu(display("failed to start fallback reaper for {program}: {source}"))]
    ReaperStartFailed {
        /// Display form of the program.
        program: String,
        /// Underlying thread creation error.
        source: std::io::Error,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The leader exited non-zero.
    #[snafu(display("{program} exited with a non-zero status"))]
    NonZeroExit {
        /// Display form of the program.
        program: String,
        /// Sole aggregate evidence payload.
        evidence: Box<LifecycleEvidence>,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The declared deadline won the serialized outcome decision.
    #[snafu(display("{program} exceeded its configured deadline of {deadline:?}"))]
    Timeout {
        /// Display form of the program.
        program: String,
        /// Configured lifetime; recovered elapsed time is retained in `evidence`.
        deadline: Duration,
        /// Sole aggregate evidence payload.
        evidence: Box<LifecycleEvidence>,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// A fail-closed capture limit won the serialized outcome decision.
    #[snafu(display("{program} exceeded the {stream:?} capture limit of {limit} bytes"))]
    CaptureLimitExceeded {
        /// Display form of the program.
        program: String,
        /// Stream selected by deterministic stdout-first precedence.
        stream: StreamName,
        /// Declared byte bound.
        limit: usize,
        /// Sole aggregate evidence payload.
        evidence: Box<LifecycleEvidence>,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Explicit cancellation won the serialized outcome decision.
    #[snafu(display("{program} was cancelled"))]
    Cancelled {
        /// Display form of the program.
        program: String,
        /// Sole aggregate evidence payload.
        evidence: Box<LifecycleEvidence>,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// One or both capture streams failed.
    #[snafu(display("capture failed while executing {program}"))]
    CaptureFailed {
        /// Display form of the program.
        program: String,
        /// Sole aggregate evidence payload with stdout before stderr.
        evidence: Box<LifecycleEvidence>,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The supervisor could not observe the child leader or poll its pipes.
    #[snafu(display("failed while supervising {program}: {source}"))]
    SupervisionFailed {
        /// Display form of the program.
        program: String,
        /// Underlying observation error.
        source: std::io::Error,
        /// Sole aggregate evidence payload.
        evidence: Box<LifecycleEvidence>,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Signaling or reaping failed after another lifecycle event settled.
    #[snafu(display("lifecycle cleanup failed while executing {program}"))]
    LifecycleFailed {
        /// Display form of the program.
        program: String,
        /// Sole aggregate evidence payload.
        evidence: Box<LifecycleEvidence>,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// Cleanup did not settle every lifecycle fact within the shared budget.
    #[snafu(display("cleanup incomplete while executing {program}"))]
    CleanupIncomplete {
        /// Display form of the program.
        program: String,
        /// Sole aggregate evidence payload.
        evidence: Box<LifecycleEvidence>,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Convenience result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;
