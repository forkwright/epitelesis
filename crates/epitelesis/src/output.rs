//! Captured output of a completed (or non-zero-exiting) invocation.

use std::process::ExitStatus;
use std::time::Duration;

/// Result of a successfully *spawned* subprocess invocation.
///
/// "Successfully spawned" means the kernel created the child process and the
/// runner waited for it to exit; the child may still have failed (`status`
/// reports a non-zero code). [`crate::run`] returns `Output` only for the
/// success case (`status.success() == true`). Non-zero exits are surfaced as
/// [`crate::Error::NonZeroExit`] which carries the same `Output` payload, so
/// callers retain full access to stdout/stderr regardless of the path.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Output {
    /// Exit status reported by the kernel.
    pub status: ExitStatus,
    /// Bytes captured from the child's standard output.
    pub stdout: Vec<u8>,
    /// Bytes captured from the child's standard error.
    pub stderr: Vec<u8>,
    /// Wall-clock time between spawn and reap.
    pub duration: Duration,
}

impl Output {
    /// View `stdout` as a UTF-8 string slice.
    ///
    /// Returns the underlying [`std::str::Utf8Error`] if the output is not
    /// valid UTF-8 (callers consuming binary stdout work with `.stdout`
    /// directly).
    pub fn stdout_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.stdout)
    }

    /// View `stderr` as a UTF-8 string slice.
    pub fn stderr_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.stderr)
    }

    /// Whether the child exited with status `0`.
    #[must_use]
    pub fn success(&self) -> bool {
        self.status.success()
    }
}
