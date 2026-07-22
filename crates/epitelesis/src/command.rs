//! Typestate command builder and process translation.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;

use crate::error::{InvalidPolicySnafu, Result};
use crate::policy::{
    CapturePolicy, Draft, EnvironmentPolicy, ExecutionPolicy, PolicyViolation, Ready,
    validate_deadline,
};

/// One environment mutation, replayed after the base environment policy.
pub(crate) enum EnvOp {
    Set(OsString, OsString),
    Remove(OsString),
}

impl std::fmt::Debug for EnvOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Set(key, _) => f
                .debug_tuple("Set")
                .field(key)
                .field(&"<redacted>")
                .finish(),
            Self::Remove(key) => f.debug_tuple("Remove").field(key).finish(),
        }
    }
}

/// Builder describing one owned subprocess invocation.
///
/// `Command` is intentionally not `Clone`. A newly constructed
/// `Command<Draft>` cannot be passed to a runner; declaring a bounded deadline
/// or an intentional unbounded reason produces `Command<Ready>`.
pub struct Command<State = Draft> {
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<OsString>,
    pub(crate) environment: EnvironmentPolicy,
    pub(crate) env_ops: Vec<EnvOp>,
    pub(crate) cwd: Option<PathBuf>,
    pub(crate) stdin: Option<Stdio>,
    pub(crate) stdout: Option<Stdio>,
    pub(crate) stderr: Option<Stdio>,
    pub(crate) stdout_capture: CapturePolicy,
    pub(crate) stderr_capture: CapturePolicy,
    state: State,
}

impl Command<Draft> {
    /// Start a non-runnable command with a clean environment and bounded capture.
    #[must_use]
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            environment: EnvironmentPolicy::default(),
            env_ops: Vec::new(),
            cwd: None,
            stdin: None,
            stdout: None,
            stderr: None,
            stdout_capture: CapturePolicy::default(),
            stderr_capture: CapturePolicy::default(),
            state: Draft,
        }
    }

    /// Declare a bounded wall-clock lifetime and make the command runnable.
    pub fn deadline(self, duration: Duration) -> Result<Command<Ready>> {
        validate_deadline(duration)?;
        Ok(self.into_state(Ready::new(ExecutionPolicy::Deadline(duration))))
    }

    /// Intentionally permit an unbounded lifetime for a non-empty reason.
    pub fn unbounded(self, reason: impl Into<String>) -> Result<Command<Ready>> {
        let execution = ExecutionPolicy::Unbounded(crate::policy::NonEmptyReason::new(reason)?);
        Ok(self.into_state(Ready::new(execution)))
    }
}

impl<State> Command<State> {
    fn into_state<Next>(self, state: Next) -> Command<Next> {
        Command {
            program: self.program,
            args: self.args,
            environment: self.environment,
            env_ops: self.env_ops,
            cwd: self.cwd,
            stdin: self.stdin,
            stdout: self.stdout,
            stderr: self.stderr,
            stdout_capture: self.stdout_capture,
            stderr_capture: self.stderr_capture,
            state,
        }
    }

    /// Append one positional argument.
    #[must_use]
    pub fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.args.push(arg.as_ref().to_os_string());
        self
    }

    /// Append positional arguments.
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

    /// Replace the base environment policy.
    #[must_use]
    pub fn environment(mut self, policy: EnvironmentPolicy) -> Self {
        self.environment = policy;
        self
    }

    /// Use a clean base environment.
    #[must_use]
    pub fn clean_environment(self) -> Self {
        self.environment(EnvironmentPolicy::Clean)
    }

    /// Copy only the named keys from the parent environment.
    #[must_use]
    pub fn allow_environment<I, K>(self, keys: I) -> Self
    where
        I: IntoIterator<Item = K>,
        K: AsRef<OsStr>,
    {
        self.environment(EnvironmentPolicy::allowlist(keys))
    }

    /// Inherit the full parent environment for an explicit non-empty reason.
    pub fn inherit_environment(mut self, reason: impl Into<String>) -> Result<Self> {
        self.environment = EnvironmentPolicy::inherit_all(reason)?;
        Ok(self)
    }

    /// Set an environment key after applying the base policy.
    #[must_use]
    pub fn env(mut self, key: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Self {
        self.env_ops.push(EnvOp::Set(
            key.as_ref().to_os_string(),
            value.as_ref().to_os_string(),
        ));
        self
    }

    /// Remove an environment key after applying the base policy.
    #[must_use]
    pub fn env_remove(mut self, key: impl AsRef<OsStr>) -> Self {
        self.env_ops
            .push(EnvOp::Remove(key.as_ref().to_os_string()));
        self
    }

    /// Set several environment keys in iteration order.
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

    /// Set the child working directory.
    #[must_use]
    pub fn cwd(mut self, directory: impl Into<PathBuf>) -> Self {
        self.cwd = Some(directory.into());
        self
    }

    /// Alias for [`Command::cwd`].
    #[must_use]
    pub fn current_dir(self, directory: impl Into<PathBuf>) -> Self {
        self.cwd(directory)
    }

    /// Configure standard input.
    #[must_use]
    pub fn stdin(mut self, stdin: Stdio) -> Self {
        self.stdin = Some(stdin);
        self
    }

    /// Redirect or explicitly pipe standard output.
    #[must_use]
    pub fn stdout(mut self, stdout: Stdio) -> Self {
        self.stdout = Some(stdout);
        self
    }

    /// Redirect or explicitly pipe standard error.
    #[must_use]
    pub fn stderr(mut self, stderr: Stdio) -> Self {
        self.stderr = Some(stderr);
        self
    }

    /// Set the stdout capture policy.
    #[must_use]
    pub fn capture_stdout(mut self, policy: CapturePolicy) -> Self {
        self.stdout_capture = policy;
        self
    }

    /// Set the stderr capture policy.
    #[must_use]
    pub fn capture_stderr(mut self, policy: CapturePolicy) -> Self {
        self.stderr_capture = policy;
        self
    }

    /// Borrow the configured program path.
    #[must_use]
    pub fn program(&self) -> &Path {
        &self.program
    }

    /// Borrow the argument vector.
    #[must_use]
    pub fn arg_list(&self) -> &[OsString] {
        &self.args
    }

    /// Iterate over arguments.
    pub fn get_args(&self) -> impl Iterator<Item = &OsStr> {
        self.args.iter().map(OsString::as_os_str)
    }

    /// Iterate over explicit environment mutations in effective key order.
    pub fn get_envs(&self) -> impl Iterator<Item = (&OsStr, Option<&OsStr>)> {
        let mut effective = std::collections::BTreeMap::new();
        for operation in &self.env_ops {
            match operation {
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
}

impl Command<Ready> {
    /// Borrow the validated execution policy.
    #[must_use]
    pub fn execution_policy(&self) -> &ExecutionPolicy {
        self.state.execution()
    }

    /// Run and capture stdout and stderr.
    pub fn output(self) -> Result<crate::Output> {
        crate::output(self)
    }

    /// Run and return the exit status, including non-zero statuses.
    pub fn status(self) -> Result<std::process::ExitStatus> {
        crate::status(self)
    }

    /// Explicitly transfer stdout/stderr byte and backpressure ownership to the caller.
    ///
    /// Managed streaming rejects non-default capture policies rather than
    /// silently discarding them.
    pub fn streaming(self) -> Result<StreamingCommand> {
        if !self.stdout_capture.is_default() {
            return InvalidPolicySnafu {
                violation: PolicyViolation::CapturePolicyWithStreaming(
                    crate::output::StreamName::Stdout,
                ),
            }
            .fail();
        }
        if !self.stderr_capture.is_default() {
            return InvalidPolicySnafu {
                violation: PolicyViolation::CapturePolicyWithStreaming(
                    crate::output::StreamName::Stderr,
                ),
            }
            .fail();
        }
        Ok(StreamingCommand { command: self })
    }
}

/// Explicit managed-streaming typestate.
///
/// Entering this state disables supervisor capture: the caller must take and
/// drain piped handles, while the supervisor retains deadline, cancellation,
/// process-group termination, and reap ownership. The transition rejects any
/// non-default capture policy.
///
/// ```compile_fail
/// use epitelesis::Command;
/// use std::time::Duration;
///
/// let captured = Command::new("/bin/true").deadline(Duration::from_secs(1))?;
/// let _ = captured.spawn();
/// # Ok::<(), epitelesis::Error>(())
/// ```
#[must_use]
pub struct StreamingCommand {
    pub(crate) command: Command<Ready>,
}

impl StreamingCommand {
    /// Spawn a managed streaming child whose lifecycle remains enforced.
    pub fn spawn(self) -> Result<crate::ManagedChild> {
        crate::spawn_managed(self)
    }
}

impl std::fmt::Debug for StreamingCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("StreamingCommand")
            .field(&self.command)
            .finish()
    }
}

impl<State: std::fmt::Debug> std::fmt::Debug for Command<State> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Command")
            .field("program", &self.program)
            .field("args", &self.args)
            .field("environment", &self.environment)
            .field("env_ops", &self.env_ops)
            .field("cwd", &self.cwd)
            .field("stdin", &self.stdin.as_ref().map(|_| "configured"))
            .field("stdout", &self.stdout.as_ref().map(|_| "configured"))
            .field("stderr", &self.stderr.as_ref().map(|_| "configured"))
            .field("stdout_capture", &self.stdout_capture)
            .field("stderr_capture", &self.stderr_capture)
            .field("state", &self.state)
            .finish()
    }
}

pub(crate) struct PreparedCommand {
    pub(crate) command: StdCommand,
    pub(crate) program: String,
    pub(crate) execution: ExecutionPolicy,
    pub(crate) stdout_capture: CapturePolicy,
    pub(crate) stderr_capture: CapturePolicy,
}

pub(crate) fn prepare(command: Command<Ready>) -> Result<PreparedCommand> {
    validate_backend()?;
    validate_path_policy(&command)?;

    let Command {
        program,
        args,
        environment,
        env_ops,
        cwd,
        stdin,
        stdout,
        stderr,
        stdout_capture,
        stderr_capture,
        state,
    } = command;
    let program_display = program.display().to_string();
    let mut process = StdCommand::new(&program);
    process.args(args);
    process.env_clear();
    match environment {
        EnvironmentPolicy::Clean => {}
        EnvironmentPolicy::Allowlist(keys) => {
            for key in keys {
                if let Some(value) = std::env::var_os(&key) {
                    process.env(key, value);
                }
            }
        }
        EnvironmentPolicy::InheritAll(_) => {
            process.envs(std::env::vars_os());
        }
    }
    for operation in env_ops {
        match operation {
            EnvOp::Set(key, value) => {
                process.env(key, value);
            }
            EnvOp::Remove(key) => {
                process.env_remove(key);
            }
        }
    }
    if let Some(cwd) = cwd {
        process.current_dir(cwd);
    }
    process.stdin(stdin.unwrap_or_else(Stdio::null));
    process.stdout(stdout.unwrap_or_else(Stdio::piped));
    process.stderr(stderr.unwrap_or_else(Stdio::piped));

    #[cfg(all(
        unix,
        not(any(
            target_os = "cygwin",
            target_os = "horizon",
            target_os = "openbsd",
            target_os = "redox",
            target_os = "wasi"
        ))
    ))]
    {
        use std::os::unix::process::CommandExt as _;
        process.process_group(0);
    }

    Ok(PreparedCommand {
        command: process,
        program: program_display,
        execution: state.into_execution(),
        stdout_capture,
        stderr_capture,
    })
}

fn validate_path_policy<State>(command: &Command<State>) -> Result<()> {
    let has_separator = command.program.components().count() > 1;
    if command.program.is_absolute() || has_separator {
        return Ok(());
    }

    let path_key = OsStr::new("PATH");
    let mut path_available =
        command.environment.allows_key(path_key) && std::env::var_os(path_key).is_some();
    for operation in &command.env_ops {
        match operation {
            EnvOp::Set(key, _) if key == path_key => path_available = true,
            EnvOp::Remove(key) if key == path_key => path_available = false,
            _ => {}
        }
    }
    if path_available {
        Ok(())
    } else {
        InvalidPolicySnafu {
            violation: PolicyViolation::BareProgramWithoutPath(
                command.program.as_os_str().to_os_string(),
            ),
        }
        .fail()
    }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "cygwin",
        target_os = "horizon",
        target_os = "openbsd",
        target_os = "redox",
        target_os = "wasi"
    ))
))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "the cfg-paired unsupported implementation returns a typed pre-spawn error"
)]
fn validate_backend() -> Result<()> {
    Ok(())
}

#[cfg(not(all(
    unix,
    not(any(
        target_os = "cygwin",
        target_os = "horizon",
        target_os = "openbsd",
        target_os = "redox",
        target_os = "wasi"
    ))
)))]
fn validate_backend() -> Result<()> {
    crate::error::UnsupportedCapabilitySnafu {
        capability: crate::error::Capability::OwnedProcessGroup,
    }
    .fail()
}
