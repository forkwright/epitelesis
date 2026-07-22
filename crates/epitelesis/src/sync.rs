//! Thin synchronous adapters over the shared supervisor.

use std::process::ExitStatus;

use crate::Command;
use crate::error::{Error, Result, SupervisionFailedSnafu};
use crate::output::Output;
use crate::policy::Ready;
use snafu::IntoError as _;

/// Run a ready command and require a zero leader exit status.
pub fn run(command: Command<Ready>) -> Result<Output> {
    crate::supervisor::execute(command, crate::supervisor::Cancellation::default())
}

/// Run a ready command and preserve captured output for non-zero exits.
pub fn output(command: Command<Ready>) -> Result<Output> {
    match run(command) {
        Ok(output) => Ok(output),
        Err(Error::NonZeroExit { evidence, .. }) => match evidence.leader_status {
            Some(status) => Ok(Output::new(status, evidence)),
            None => Err(SupervisionFailedSnafu {
                program: "output invocation".to_owned(),
                evidence,
            }
            .into_error(std::io::Error::other(
                "non-zero output did not contain a leader status",
            ))),
        },
        Err(error) => Err(error),
    }
}

/// Run a ready command and preserve non-zero statuses as successful results.
pub fn status(command: Command<Ready>) -> Result<ExitStatus> {
    let output = output(command)?;
    Ok(output.status())
}
