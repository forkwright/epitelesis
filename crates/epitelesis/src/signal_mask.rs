//! Unix child signal-mask policy owned by the command substrate.

use nix::sys::signal::{SigSet, SigmaskHow, sigprocmask};

/// An exact signal mask captured from the calling thread for use by one child.
///
/// Callers that temporarily block signals while they establish their own
/// observation policy capture this value first, then attach it with
/// [`crate::Command::child_signal_mask`]. Epitelesis restores this exact mask
/// in the child before `exec`, so the caller's temporary block never leaks
/// into the executed program.
#[derive(Debug)]
pub struct ChildSignalMask(SigSet);

impl ChildSignalMask {
    /// Capture the current thread's complete signal mask.
    pub fn capture_current() -> std::io::Result<Self> {
        SigSet::thread_get_mask().map(Self).map_err(io_error)
    }

    pub(crate) fn apply_in_child(&self) -> std::io::Result<()> {
        sigprocmask(SigmaskHow::SIG_SETMASK, Some(&self.0), None).map_err(io_error)
    }
}

fn io_error(source: nix::errno::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(source as i32)
}
