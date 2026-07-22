//! Thin synchronous adapters over the shared supervisor.

use std::process::ExitStatus;

use crate::Command;
use crate::error::{Error, Result};
use crate::output::Output;
use crate::policy::Ready;

/// Run a ready command and require a zero leader exit status.
pub fn run(command: Command<Ready>) -> Result<Output> {
    crate::supervisor::execute(command, crate::supervisor::Cancellation::default())
}

/// Run a ready command and preserve captured output for non-zero exits.
pub fn output(command: Command<Ready>) -> Result<Output> {
    match run(command) {
        Ok(output) | Err(Error::NonZeroExit { output, .. }) => Ok(output),
        Err(error) => Err(error),
    }
}

/// Run a ready command and preserve non-zero statuses as successful results.
pub fn status(command: Command<Ready>) -> Result<ExitStatus> {
    output(command).map(|output| output.status)
}
