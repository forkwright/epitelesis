//! # Epitelesis
//!
//! Greek ἐπιτέλεσις — *the process of executing-to-completion*. Epitelesis is
//! the project-wide command-execution wrapper substrate for the forkwright
//! fleet. Every production subprocess invocation goes through this crate so
//! the codebase has one place that handles argument assembly, environment and
//! working-directory passthrough, timeout enforcement, stdout/stderr capture,
//! structured errors, and tracing spans.
//!
//! ## Why
//!
//! Direct `std::process::Command` use is forbidden in fleet code by the
//! `RUST/no-direct-process-command` rule (see `STANDARDS/RUST.md`). Raw
//! `Command` makes it easy to forget timeout configuration, exit-code
//! handling, argument quoting, or working-directory setup, and produces
//! ad-hoc error types that callers cannot match on. Epitelesis centralises
//! those concerns.
//!
//! ## Surface
//!
//! - [`Command`] — builder that captures program, args, env, cwd, timeout.
//! - [`run`] — synchronous executor returning [`Output`] or typed [`Error`].
//! - [`output`] — synchronous captured-output helper preserving non-zero output.
//! - [`status`] — synchronous status helper preserving non-zero status.
//! - [`spawn_child`] — synchronous child-handle helper for streaming callers.
//! - [`spawn`] — asynchronous executor (gated by the `async` feature).
//! - [`Output`] — captured status, stdout, stderr, and elapsed duration.
//! - [`Error`] — typed error variants per ADR-002 (snafu, `#[non_exhaustive]`).
//!
//! ## Example
//!
//! ```no_run
//! use epitelesis::{Command, run};
//! use std::time::Duration;
//!
//! let output = run(
//!     Command::new("git")
//!         .arg("status")
//!         .arg("--porcelain")
//!         .timeout(Duration::from_secs(5)),
//! )?;
//! assert!(output.success());
//! # Ok::<(), epitelesis::Error>(())
//! ```

#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod command;
mod error;
mod output;
mod sync;

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
mod async_impl;

pub use command::Command;
pub use error::{Error, Result};
pub use output::Output;
pub use sync::{output, run, spawn_child, status};

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use async_impl::spawn;
