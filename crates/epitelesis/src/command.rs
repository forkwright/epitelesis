//! Builder type for command invocations.
//!
//! WHY: A separate builder lets callers assemble the full invocation up front
//! so [`crate::run`] / [`crate::spawn`] receive a single owned value with
//! every field already validated by the type system. The runners stay free of
//! ergonomic concerns and focus on execution semantics.

use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

/// One environment mutation, replayed onto the child in builder-call order.
///
/// WHY: storing mutations as an ordered log instead of a map+list pair makes
/// the child environment a pure function of builder-call order, matching
/// `std::process::Command` semantics where the later of `env` / `env_remove`
/// wins for the same key.
#[derive(Debug)]
pub(crate) enum EnvOp {
    /// Set `key` to `value` in the child environment.
    Set(OsString, OsString),
    /// Remove `key` from the environment the child inherits.
    Remove(OsString),
}

/// Builder describing a subprocess invocation.
///
/// `Command` is intentionally not `Clone`. Each invocation owns its
/// configuration; cloning would invite shared-mutable-builder patterns that
/// undermine the parse-don't-validate boundary. Callers that need to launch
/// the same logical command repeatedly construct a fresh [`Command`] per call.
pub struct Command {
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<OsString>,
    pub(crate) env_ops: Vec<EnvOp>,
    pub(crate) cwd: Option<PathBuf>,
    pub(crate) stdin: Option<Stdio>,
    pub(crate) stdout: Option<Stdio>,
    pub(crate) stderr: Option<Stdio>,
    pub(crate) timeout: Option<Duration>,
}

impl Command {
    /// Start building an invocation of `program`.
    ///
    /// `program` may be a bare executable name (resolved via `PATH`) or an
    /// absolute path. No validation is performed at construction time —
    /// missing programs surface at execution as [`crate::Error::SpawnFailed`].
    #[must_use]
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env_ops: Vec::new(),
            cwd: None,
            stdin: None,
            stdout: None,
            stderr: None,
            timeout: None,
        }
    }

    /// Append a single positional argument.
    #[must_use]
    pub fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.args.push(arg.as_ref().to_os_string());
        self
    }

    /// Append a sequence of positional arguments.
    #[must_use]
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.args
            .extend(args.into_iter().map(|arg| arg.as_ref().to_os_string()));
        self
    }

    /// Set a single environment variable for the child process.
    ///
    /// WHY: explicit env passthrough is required so callers cannot accidentally
    /// inherit the parent's full environment when running untrusted helpers.
    /// `epitelesis` mirrors the standard library's behaviour: variables set
    /// here are *added* to the parent environment; the child inherits the
    /// rest, and the later of [`Command::env`] / [`Command::env_remove`] wins
    /// for the same key. Callers wanting strict isolation should construct
    /// fresh `Command`s and use the runner's process namespace conventions.
    #[must_use]
    pub fn env(mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Self {
        self.env_ops.push(EnvOp::Set(
            key.as_ref().to_os_string(),
            value.as_ref().to_os_string(),
        ));
        self
    }

    /// Remove one inherited environment variable from the child process.
    ///
    /// Mirrors `std::process::Command::env_remove` call-order semantics: a
    /// removal after [`Command::env`] for the same key removes the variable;
    /// an [`Command::env`] call after the removal re-sets it.
    #[must_use]
    pub fn env_remove(mut self, key: impl AsRef<OsStr>) -> Self {
        self.env_ops
            .push(EnvOp::Remove(key.as_ref().to_os_string()));
        self
    }

    /// Set multiple environment variables at once.
    #[must_use]
    pub fn envs<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        for (key, value) in vars {
            self.env_ops.push(EnvOp::Set(
                key.as_ref().to_os_string(),
                value.as_ref().to_os_string(),
            ));
        }
        self
    }

    /// Set the working directory the child process is launched in.
    #[must_use]
    pub fn cwd(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cwd = Some(dir.into());
        self
    }

    /// Set the working directory the child process is launched in.
    ///
    /// Alias for [`Command::cwd`] for call sites migrating from
    /// `std::process::Command::current_dir`.
    #[must_use]
    pub fn current_dir(self, dir: impl Into<PathBuf>) -> Self {
        self.cwd(dir)
    }

    /// Configure the child's standard input.
    #[must_use]
    pub fn stdin(mut self, stdin: Stdio) -> Self {
        self.stdin = Some(stdin);
        self
    }

    /// Configure the child's standard output.
    #[must_use]
    pub fn stdout(mut self, stdout: Stdio) -> Self {
        self.stdout = Some(stdout);
        self
    }

    /// Configure the child's standard error.
    #[must_use]
    pub fn stderr(mut self, stderr: Stdio) -> Self {
        self.stderr = Some(stderr);
        self
    }

    /// Set a wall-clock timeout for the invocation.
    ///
    /// WHY: every fleet subprocess must declare a deadline; sweeping subprocess
    /// hangs into observable [`crate::Error::Timeout`] errors keeps the queue
    /// liveness model honest. Callers that genuinely need an unbounded
    /// invocation simply omit this method.
    ///
    /// The timeout is enforced by [`crate::run`] / [`crate::output`] /
    /// [`crate::status`] and by [`crate::spawn`]. [`crate::spawn_child`]
    /// cannot enforce it (the caller owns the raw child handle) — see its
    /// documentation for how that mismatch surfaces.
    #[must_use]
    pub fn timeout(mut self, dur: Duration) -> Self {
        self.timeout = Some(dur);
        self
    }

    /// Run this invocation and return captured output for both zero and
    /// non-zero exits.
    ///
    /// Convenience forwarding method for [`crate::output`]. The `io::Error`
    /// preserves the underlying [`std::io::ErrorKind`] where one exists
    /// (spawn/wait failures) and maps timeouts to
    /// [`std::io::ErrorKind::TimedOut`].
    pub fn output(self) -> std::io::Result<std::process::Output> {
        crate::output(self)
            .map(into_std_output)
            .map_err(into_io_error)
    }

    /// Run this invocation and return its exit status for both zero and
    /// non-zero exits.
    ///
    /// Convenience forwarding method for [`crate::status`]. The `io::Error`
    /// preserves the underlying [`std::io::ErrorKind`] where one exists
    /// (spawn/wait failures) and maps timeouts to
    /// [`std::io::ErrorKind::TimedOut`].
    pub fn status(self) -> std::io::Result<std::process::ExitStatus> {
        crate::status(self).map_err(into_io_error)
    }

    /// Spawn this invocation and return the child handle.
    ///
    /// This preserves the child-handle shape needed by streaming callers while
    /// keeping the raw process creation inside the epitelesis substrate. A
    /// configured [`Command::timeout`] cannot be enforced on a raw handle —
    /// see [`crate::spawn_child`] for how that mismatch surfaces.
    pub fn spawn(self) -> std::io::Result<std::process::Child> {
        crate::spawn_child(self)
    }

    /// Borrow the program path (used by runner implementations and tests).
    #[must_use]
    pub fn program(&self) -> &std::path::Path {
        &self.program
    }

    /// Borrow the argument vector (used by runner implementations and tests).
    #[must_use]
    pub fn arg_list(&self) -> &[OsString] {
        &self.args
    }

    /// Iterate over configured arguments.
    ///
    /// Compatibility helper for migrated call sites that previously inspected
    /// `std::process::Command` in tests.
    pub fn get_args(&self) -> impl Iterator<Item = &OsStr> {
        self.args.iter().map(OsString::as_os_str)
    }

    /// Iterate over the *effective* environment configuration.
    ///
    /// Mirrors `std::process::Command::get_envs`: entries are sorted by key,
    /// each key appears exactly once, and the value is `Some` for a variable
    /// that will be set or `None` for one that will be removed. When a key was
    /// passed to both [`Command::env`] and [`Command::env_remove`], the later
    /// builder call wins — the same resolution the runners apply.
    pub fn get_envs(&self) -> impl Iterator<Item = (&OsStr, Option<&OsStr>)> {
        let mut effective = std::collections::BTreeMap::new();
        for op in &self.env_ops {
            match op {
                EnvOp::Set(key, value) => {
                    effective.insert(key.as_os_str(), Some(value.as_os_str()));
                }
                EnvOp::Remove(key) => {
                    effective.insert(key.as_os_str(), None);
                }
            }
        }
        effective.into_iter()
    }

    /// Borrow the configured timeout, if any.
    #[must_use]
    pub fn timeout_value(&self) -> Option<Duration> {
        self.timeout
    }
}

impl std::fmt::Debug for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Command")
            .field("program", &self.program)
            .field("args", &self.args)
            .field("env_ops", &self.env_ops)
            .field("cwd", &self.cwd)
            .field("stdin", &self.stdin.as_ref().map(|_| "configured"))
            .field("stdout", &self.stdout.as_ref().map(|_| "configured"))
            .field("stderr", &self.stderr.as_ref().map(|_| "configured"))
            .field("timeout", &self.timeout)
            .finish()
    }
}

fn into_std_output(output: crate::Output) -> std::process::Output {
    std::process::Output {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    }
}

/// Convert a typed [`crate::Error`] into an `io::Error` that keeps the typed
/// error as its source while preserving a meaningful [`std::io::ErrorKind`].
///
/// WHY: `Command::output` / `Command::status` mirror the `std::process`
/// signatures for migrated call sites; those call sites match on
/// `io::Error::kind()` (e.g. `NotFound` for a missing binary). Collapsing
/// every failure to `ErrorKind::Other` would silently break that matching.
fn into_io_error(error: crate::Error) -> std::io::Error {
    let kind = match &error {
        crate::Error::SpawnFailed { source, .. } | crate::Error::Io { source, .. } => source.kind(),
        crate::Error::Timeout { .. } => std::io::ErrorKind::TimedOut,
        // NOTE: NonZeroExit never reaches here (output/status return it as
        // Ok); the arm also absorbs future #[non_exhaustive] variants.
        _ => std::io::ErrorKind::Other,
    };
    std::io::Error::new(kind, error)
}
