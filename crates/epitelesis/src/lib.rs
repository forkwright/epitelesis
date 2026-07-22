//! Structural subprocess ownership for the forkwright fleet.
//!
//! A command is non-runnable until its lifetime is explicit:
//!
//! ```compile_fail
//! use epitelesis::{Command, run};
//! let _ = run(Command::new("/bin/true"));
//! ```
//!
//! Bounded and intentionally unbounded policies both produce the `Ready`
//! typestate consumed by every runner:
//!
//! ```no_run
//! use epitelesis::{Command, run};
//! use std::time::Duration;
//!
//! let command = Command::new("/bin/true").deadline(Duration::from_secs(2))?;
//! let output = run(command)?;
//! assert!(output.success());
//! # Ok::<(), epitelesis::Error>(())
//! ```

#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod command;
mod error;
mod managed;
mod output;
mod policy;
mod supervisor;
mod sync;

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
mod async_impl;

pub use command::Command;
pub use error::{
    Capability, CaptureReport, CaptureWorkerFailure, CleanupIncompleteEvidence, Error,
    FailureEvidence, Result, SecondaryErrors,
};
pub use managed::{ManagedChild, spawn_managed};
pub use output::{CaptureCompleteness, CapturedStream, Output, StreamName};
pub use policy::{
    CapturePolicy, DEFAULT_CAPTURE_LIMIT, Draft, EnvironmentPolicy, ExecutionPolicy,
    NonEmptyReason, OverflowBehavior, PolicyViolation, Ready,
};
pub use sync::{output, run, status};

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use async_impl::spawn;
