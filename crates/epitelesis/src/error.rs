//! Typed error surface for [`crate::run`] and [`crate::spawn`].
//!
//! Per ADR-002 (snafu typed errors): every variant carries enough context for
//! callers to match on the failure mode without losing the underlying
//! `io::Error` chain. `#[non_exhaustive]` reserves the right to add variants
//! (e.g. `Cancelled`) in a non-breaking way.

use std::process::ExitStatus;
use std::time::Duration;

use snafu::Snafu;

use crate::output::Output;

/// Errors produced by epitelesis runners.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
#[non_exhaustive]
pub enum Error {
    /// The kernel refused to spawn the child process (typically: program not
    /// found on `PATH`, permission denied, or fork failure).
    #[snafu(display("failed to spawn {program}: {source}"))]
    SpawnFailed {
        /// Display form of the program that failed to spawn.
        program: String,
        /// Underlying io error from `Command::spawn`.
        source: std::io::Error,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The child spawned and exited, but with a non-zero status. Callers that
    /// treat non-zero exits as expected (e.g. `grep` returning 1 on no match)
    /// can match this variant and inspect the captured `output` payload.
    #[snafu(display("{program} exited with non-zero status {status}"))]
    NonZeroExit {
        /// Display form of the program.
        program: String,
        /// Exit status reported by the kernel.
        status: ExitStatus,
        /// Captured stdout/stderr/duration even on failure.
        output: Output,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// The configured [`crate::Command::timeout`] elapsed before the child
    /// exited. The runner has already attempted to kill the child by the time
    /// this error is returned.
    #[snafu(display("{program} timed out after {duration:?}"))]
    Timeout {
        /// Display form of the program.
        program: String,
        /// The timeout that elapsed.
        duration: Duration,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },

    /// IO failure observed while waiting on the child or capturing its
    /// output (e.g. broken pipe, EIO on the captured fd).
    #[snafu(display("io error during {program} execution: {source}"))]
    Io {
        /// Display form of the program.
        program: String,
        /// Underlying io error.
        source: std::io::Error,
        /// Error creation location.
        #[snafu(implicit)]
        location: snafu::Location,
    },
}

/// Convenience alias matching the fleet convention.
pub type Result<T, E = Error> = std::result::Result<T, E>;
