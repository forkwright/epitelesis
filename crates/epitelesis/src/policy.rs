//! Typed invocation policies.

use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::time::{Duration, Instant};

use crate::error::{InvalidPolicySnafu, Result};
use crate::output::StreamName;

/// Conservative capture bound applied independently to stdout and stderr.
pub const DEFAULT_CAPTURE_LIMIT: usize = 10 * 1024 * 1024;

/// Marker for a command whose lifecycle policy has not yet been declared.
#[derive(Debug)]
pub struct Draft;

/// Marker for a command whose lifecycle policy is valid and runnable.
#[derive(Debug)]
pub struct Ready;

/// A non-empty explanation attached to exceptional policy choices.
#[derive(Clone, Eq, PartialEq)]
pub struct NonEmptyReason(Box<str>);

impl NonEmptyReason {
    /// Validate and retain a human-readable reason.
    pub fn new(reason: impl Into<String>) -> Result<Self> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return InvalidPolicySnafu {
                violation: PolicyViolation::EmptyReason,
            }
            .fail();
        }
        Ok(Self(reason.into_boxed_str()))
    }

    /// Borrow the validated reason.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NonEmptyReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NonEmptyReason").field(&self.0).finish()
    }
}

/// The wall-clock ownership policy for one invocation.
#[derive(Debug)]
pub enum ExecutionPolicy {
    /// Stop and clean up the owned process group when the duration elapses.
    Deadline(Duration),
    /// Intentionally permit the invocation to run without a time bound.
    Unbounded(NonEmptyReason),
}

impl ExecutionPolicy {
    pub(crate) fn deadline(&self, started: Instant) -> Option<Instant> {
        match self {
            Self::Deadline(duration) => started.checked_add(*duration),
            Self::Unbounded(_) => None,
        }
    }

    /// Return the configured deadline duration, if bounded.
    #[must_use]
    pub fn duration(&self) -> Option<Duration> {
        match self {
            Self::Deadline(duration) => Some(*duration),
            Self::Unbounded(_) => None,
        }
    }
}

/// Which portion of the parent environment is visible before explicit ops.
#[derive(Clone, Eq, PartialEq)]
pub enum EnvironmentPolicy {
    /// Begin with an empty environment.
    Clean,
    /// Copy only the named keys that exist in the parent environment.
    Allowlist(BTreeSet<OsString>),
    /// Inherit every parent variable for the stated exceptional reason.
    InheritAll(NonEmptyReason),
}

impl EnvironmentPolicy {
    /// Construct the secure empty-environment policy.
    #[must_use]
    pub fn clean() -> Self {
        Self::Clean
    }

    /// Construct an allowlist from environment keys.
    #[must_use]
    pub fn allowlist<I, K>(keys: I) -> Self
    where
        I: IntoIterator<Item = K>,
        K: AsRef<OsStr>,
    {
        Self::Allowlist(
            keys.into_iter()
                .map(|key| key.as_ref().to_os_string())
                .collect(),
        )
    }

    /// Construct explicit full inheritance with a non-empty reason.
    pub fn inherit_all(reason: impl Into<String>) -> Result<Self> {
        NonEmptyReason::new(reason).map(Self::InheritAll)
    }

    pub(crate) fn allows_key(&self, key: &OsStr) -> bool {
        match self {
            Self::Clean => false,
            Self::Allowlist(keys) => keys.contains(key),
            Self::InheritAll(_) => true,
        }
    }
}

impl Default for EnvironmentPolicy {
    fn default() -> Self {
        Self::Clean
    }
}

impl fmt::Debug for EnvironmentPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clean => f.write_str("Clean"),
            Self::Allowlist(keys) => f.debug_tuple("Allowlist").field(keys).finish(),
            Self::InheritAll(reason) => f.debug_tuple("InheritAll").field(reason).finish(),
        }
    }
}

/// Action taken after a stream reaches its declared byte limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverflowBehavior {
    /// Terminate the invocation and return a typed limit error.
    FailClosed,
    /// Keep draining while retaining only the bounded prefix.
    TruncateAndDrain,
}

/// Capture policy for one output stream.
#[derive(Debug)]
pub enum CapturePolicy {
    /// Retain at most `limit` bytes and apply `overflow` to further bytes.
    Bounded {
        /// Maximum retained byte count.
        limit: usize,
        /// Overflow action.
        overflow: OverflowBehavior,
    },
    /// Retain all bytes for the stated exceptional reason.
    Unbounded(NonEmptyReason),
}

impl CapturePolicy {
    /// Construct bounded, fail-closed capture.
    #[must_use]
    pub fn bounded(limit: usize) -> Self {
        Self::Bounded {
            limit,
            overflow: OverflowBehavior::FailClosed,
        }
    }

    /// Construct bounded prefix capture that continues draining on overflow.
    #[must_use]
    pub fn truncate(limit: usize) -> Self {
        Self::Bounded {
            limit,
            overflow: OverflowBehavior::TruncateAndDrain,
        }
    }

    /// Construct exceptional unbounded capture with a non-empty reason.
    pub fn unbounded(reason: impl Into<String>) -> Result<Self> {
        NonEmptyReason::new(reason).map(Self::Unbounded)
    }

    pub(crate) fn is_unbounded(&self) -> bool {
        matches!(self, Self::Unbounded(_))
    }
}

impl Default for CapturePolicy {
    fn default() -> Self {
        Self::bounded(DEFAULT_CAPTURE_LIMIT)
    }
}

/// A policy violation detected before process creation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PolicyViolation {
    /// An exceptional policy was given only whitespace or an empty reason.
    EmptyReason,
    /// The duration cannot be represented as a monotonic deadline.
    DeadlineOverflow(Duration),
    /// PATH lookup was requested without making PATH visible to the child.
    BareProgramWithoutPath(OsString),
    /// The process identifier could not be represented safely by the backend.
    InvalidProcessId(u32),
    /// Managed streaming does not use a supervisor-owned capture buffer.
    UnboundedCaptureForManaged(StreamName),
}

impl fmt::Display for PolicyViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyReason => f.write_str("exceptional policy reason must not be empty"),
            Self::DeadlineOverflow(duration) => {
                write!(
                    f,
                    "deadline {duration:?} exceeds the monotonic clock domain"
                )
            }
            Self::BareProgramWithoutPath(program) => write!(
                f,
                "bare program {program:?} requires PATH to be explicitly set or allowlisted"
            ),
            Self::InvalidProcessId(id) => write!(f, "process id {id} is not representable"),
            Self::UnboundedCaptureForManaged(stream) => write!(
                f,
                "unbounded {stream:?} capture is unavailable for managed streaming"
            ),
        }
    }
}

pub(crate) fn validate_deadline(duration: Duration) -> Result<()> {
    if duration == Duration::MAX || Instant::now().checked_add(duration).is_none() {
        return InvalidPolicySnafu {
            violation: PolicyViolation::DeadlineOverflow(duration),
        }
        .fail();
    }
    Ok(())
}
