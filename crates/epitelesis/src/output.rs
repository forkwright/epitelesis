//! Captured output and completeness evidence.

use std::process::ExitStatus;
use std::time::Duration;

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
    /// The stream was not connected to the supervisor capture pipe.
    Redirected,
}

/// Bytes retained from one stream together with their completeness state.
#[derive(Debug, Eq, PartialEq)]
pub struct CapturedStream {
    /// Bounded prefix retained by the capture worker.
    pub bytes: Vec<u8>,
    /// Whether the bytes are complete, truncated, or redirected.
    pub completeness: CaptureCompleteness,
}

impl CapturedStream {
    pub(crate) fn complete(bytes: Vec<u8>, discarded: u64) -> Self {
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

    pub(crate) fn redirected() -> Self {
        Self {
            bytes: Vec::new(),
            completeness: CaptureCompleteness::Redirected,
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

/// Result of a completed subprocess invocation.
///
/// This type is intentionally not `Clone`: a non-zero error owns the sole
/// status and buffer payload instead of duplicating potentially large output.
#[derive(Debug)]
#[non_exhaustive]
pub struct Output {
    /// Exit status reported by the kernel.
    pub status: ExitStatus,
    /// Captured stdout and completeness evidence.
    pub stdout: CapturedStream,
    /// Captured stderr and completeness evidence.
    pub stderr: CapturedStream,
    /// Wall-clock time from spawn through reap and capture settlement.
    pub duration: Duration,
}

impl Output {
    /// View retained stdout as UTF-8.
    pub fn stdout_str(&self) -> Result<&str, std::str::Utf8Error> {
        self.stdout.as_str()
    }

    /// View retained stderr as UTF-8.
    pub fn stderr_str(&self) -> Result<&str, std::str::Utf8Error> {
        self.stderr.as_str()
    }

    /// Whether the leader exited successfully.
    #[must_use]
    pub fn success(&self) -> bool {
        self.status.success()
    }
}
