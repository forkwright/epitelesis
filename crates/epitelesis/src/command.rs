//! Builder type for command invocations.
//!
//! WHY: A separate builder lets callers assemble the full invocation up front
//! so [`crate::run`] / [`crate::spawn`] receive a single owned value with
//! every field already validated by the type system. The runners stay free of
//! ergonomic concerns and focus on execution semantics.

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

/// Builder describing a subprocess invocation.
///
/// `Command` is intentionally not `Clone`. Each invocation owns its
/// configuration; cloning would invite shared-mutable-builder patterns that
/// undermine the parse-don't-validate boundary. Callers that need to launch
/// the same logical command repeatedly construct a fresh [`Command`] per call.
pub struct Command {
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<OsString>,
    pub(crate) env: HashMap<OsString, OsString>,
    pub(crate) env_remove: Vec<OsString>,
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
            env: HashMap::new(),
            env_remove: Vec::new(),
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
    /// rest. Callers wanting strict isolation should construct fresh
    /// `Command`s and use the runner's process namespace conventions.
    #[must_use]
    pub fn env(mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Self {
        self.env
            .insert(key.as_ref().to_os_string(), value.as_ref().to_os_string());
        self
    }

    /// Remove one inherited environment variable from the child process.
    #[must_use]
    pub fn env_remove(mut self, key: impl AsRef<OsStr>) -> Self {
        self.env_remove.push(key.as_ref().to_os_string());
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
            self.env
                .insert(key.as_ref().to_os_string(), value.as_ref().to_os_string());
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
    #[must_use]
    pub fn timeout(mut self, dur: Duration) -> Self {
        self.timeout = Some(dur);
        self
    }

    /// Run this invocation and return captured output for both zero and
    /// non-zero exits.
    ///
    /// Convenience forwarding method for [`crate::output`].
    pub fn output(self) -> std::io::Result<std::process::Output> {
        crate::output(self)
            .map(into_std_output)
            .map_err(std::io::Error::other)
    }

    /// Run this invocation and return its exit status for both zero and
    /// non-zero exits.
    ///
    /// Convenience forwarding method for [`crate::status`].
    pub fn status(self) -> std::io::Result<std::process::ExitStatus> {
        crate::status(self).map_err(std::io::Error::other)
    }

    /// Spawn this invocation and return the child handle.
    ///
    /// This preserves the child-handle shape needed by streaming callers while
    /// keeping the raw process creation inside the epitelesis substrate.
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

    /// Iterate over configured environment overrides and removals.
    ///
    /// Compatibility helper for migrated call sites that previously inspected
    /// `std::process::Command` in tests.
    pub fn get_envs(&self) -> impl Iterator<Item = (&OsStr, Option<&OsStr>)> {
        self.env
            .iter()
            .map(|(key, value)| (key.as_os_str(), Some(value.as_os_str())))
            .chain(self.env_remove.iter().map(|key| (key.as_os_str(), None)))
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
            .field("env", &self.env)
            .field("env_remove", &self.env_remove)
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
