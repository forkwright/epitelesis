//! Integration coverage for the structural synchronous contract.

#![cfg(all(
    unix,
    not(any(
        target_os = "cygwin",
        target_os = "horizon",
        target_os = "openbsd",
        target_os = "redox",
        target_os = "wasi"
    ))
))]

use std::ffi::OsStr;
use std::fmt::Debug;
use std::time::{Duration, Instant};

use epitelesis::{
    CLEANUP_ALLOWANCE, CaptureCompleteness, CapturePolicy, CleanupOutcome, Command,
    DEFAULT_CAPTURE_LIMIT, EnvironmentPolicy, Error, PolicyViolation, StreamName, output, run,
};

trait Must<T> {
    fn must(self, context: &str) -> T;
}

impl<T, E: Debug> Must<T> for Result<T, E> {
    fn must(self, context: &str) -> T {
        match self {
            Ok(value) => value,
            Err(error) => panic!("{context}: {error:?}"),
        }
    }
}

impl<T> Must<T> for Option<T> {
    fn must(self, context: &str) -> T {
        match self {
            Some(value) => value,
            None => panic!("{context}"),
        }
    }
}

trait MustErr<E> {
    fn must_err(self, context: &str) -> E;
}

impl<T: Debug, E> MustErr<E> for Result<T, E> {
    fn must_err(self, context: &str) -> E {
        match self {
            Err(error) => error,
            Ok(value) => panic!("{context}: unexpectedly succeeded with {value:?}"),
        }
    }
}

fn bounded(program: &str) -> epitelesis::Command<epitelesis::Ready> {
    Command::new(program)
        .deadline(Duration::from_secs(10))
        .must("fixture deadline is representable")
}

#[test]
fn deadline_overflow_is_rejected_before_spawn() {
    let error = Command::new("/definitely/not/spawned")
        .deadline(Duration::MAX)
        .must_err("Duration::MAX cannot become an Instant deadline");
    assert!(matches!(
        error,
        Error::InvalidPolicy {
            violation: PolicyViolation::DeadlineOverflow(Duration::MAX),
            ..
        }
    ));
}

#[test]
fn clean_environment_is_the_actual_default() {
    let output = run(bounded("/usr/bin/env")).must("env with no variables exits zero");
    assert!(output.evidence.stdout.captured.is_empty());
    assert_eq!(
        output.evidence.stdout.captured.completeness,
        CaptureCompleteness::Complete
    );
}

#[test]
fn allowlist_and_inherit_have_explicit_parity() {
    let expected_path = std::env::var_os("PATH").must("test process has PATH");
    let allowlisted = run(Command::new("/usr/bin/printenv")
        .arg("PATH")
        .environment(EnvironmentPolicy::allowlist(["PATH"]))
        .deadline(Duration::from_secs(10))
        .must("deadline is valid"))
    .must("allowlisted PATH is visible");
    let inherited = run(Command::new("/usr/bin/printenv")
        .arg("PATH")
        .inherit_environment("fixture compares explicit inheritance")
        .must("reason is non-empty")
        .deadline(Duration::from_secs(10))
        .must("deadline is valid"))
    .must("inherited PATH is visible");
    assert_eq!(
        allowlisted.evidence.stdout.captured.bytes,
        [expected_path.as_encoded_bytes(), b"\n"].concat()
    );
    assert_eq!(
        allowlisted.evidence.stdout.captured.bytes,
        inherited.evidence.stdout.captured.bytes
    );
}

#[test]
fn environment_operations_apply_after_policy_in_call_order() {
    let output = run(Command::new("/usr/bin/printenv")
        .arg("EPITELESIS_ORDER")
        .env("EPITELESIS_ORDER", "first")
        .env_remove("EPITELESIS_ORDER")
        .env("EPITELESIS_ORDER", "final")
        .deadline(Duration::from_secs(10))
        .must("deadline is valid"))
    .must("last set wins");
    assert_eq!(output.evidence.stdout.captured.bytes, b"final\n");
}

#[test]
fn bare_program_requires_explicit_path() {
    let error = run(Command::new("true")
        .deadline(Duration::from_secs(1))
        .must("deadline is valid"))
    .must_err("clean environment cannot resolve a bare name");
    assert!(matches!(
        error,
        Error::InvalidPolicy {
            violation: PolicyViolation::BareProgramWithoutPath(program),
            ..
        } if program == OsStr::new("true")
    ));

    run(Command::new("true")
        .env("PATH", "/usr/bin:/bin")
        .deadline(Duration::from_secs(1))
        .must("deadline is valid"))
    .must("explicit PATH permits bare lookup");
}

#[test]
fn streaming_rejects_non_default_capture_policies() {
    let stdout_error = bounded("/bin/true")
        .capture_stdout(CapturePolicy::bounded(1))
        .streaming()
        .must_err("custom stdout capture cannot become streaming");
    assert!(matches!(
        stdout_error,
        Error::InvalidPolicy {
            violation: PolicyViolation::CapturePolicyWithStreaming(StreamName::Stdout),
            ..
        }
    ));

    let stderr_error = bounded("/bin/true")
        .capture_stderr(CapturePolicy::truncate(DEFAULT_CAPTURE_LIMIT))
        .streaming()
        .must_err("custom stderr overflow behavior cannot become streaming");
    assert!(matches!(
        stderr_error,
        Error::InvalidPolicy {
            violation: PolicyViolation::CapturePolicyWithStreaming(StreamName::Stderr),
            ..
        }
    ));

    let _streaming = bounded("/bin/true")
        .capture_stdout(CapturePolicy::default())
        .capture_stderr(CapturePolicy::default())
        .streaming()
        .must("explicit default capture policies may become streaming");
}

#[test]
fn missing_program_surfaces_spawn_failure() {
    let error = run(bounded("/definitely/does/not/exist/epitelesis-test-binary"))
        .must_err("missing program cannot spawn");
    assert!(matches!(error, Error::SpawnFailed { .. }));
}

#[test]
fn get_envs_reports_effective_last_call_wins_view() {
    let command = Command::new("/bin/true")
        .env("EPITELESIS_A", "kept")
        .env("EPITELESIS_B", "shadowed")
        .env_remove("EPITELESIS_B")
        .env_remove("EPITELESIS_C")
        .env("EPITELESIS_C", "revived");
    let envs = command.get_envs().collect::<Vec<_>>();
    assert_eq!(
        envs,
        vec![
            (OsStr::new("EPITELESIS_A"), Some(OsStr::new("kept"))),
            (OsStr::new("EPITELESIS_B"), None),
            (OsStr::new("EPITELESIS_C"), Some(OsStr::new("revived"))),
        ]
    );
}

#[test]
fn captured_run_closes_piped_stdin_before_supervision() {
    let output = run(Command::new("/bin/cat")
        .stdin(std::process::Stdio::piped())
        .deadline(Duration::from_secs(2))
        .must("deadline is valid"))
    .must("cat observes EOF");
    assert!(output.success());
}

#[test]
fn command_debug_redacts_environment_values() {
    let debug = format!(
        "{:?}",
        Command::new("/bin/true").env("TOKEN", "super-secret-value")
    );
    assert!(debug.contains("TOKEN"));
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("super-secret-value"));
}

#[test]
fn exact_stdout_and_stderr_caps_are_complete() {
    let output = run(Command::new("/bin/sh")
        .args(["-c", "printf 12345; printf abcde >&2"])
        .capture_stdout(CapturePolicy::bounded(5))
        .capture_stderr(CapturePolicy::bounded(5))
        .deadline(Duration::from_secs(10))
        .must("deadline is valid"))
    .must("exact caps are not overflow");
    assert_eq!(output.evidence.stdout.captured.bytes, b"12345");
    assert_eq!(output.evidence.stderr.captured.bytes, b"abcde");
    assert_eq!(
        output.evidence.stdout.captured.completeness,
        CaptureCompleteness::Complete
    );
    assert_eq!(
        output.evidence.stderr.captured.completeness,
        CaptureCompleteness::Complete
    );
}

#[test]
fn cap_plus_one_fails_closed_on_each_stream_with_bounded_peer_bytes() {
    for (script, expected_stream) in [
        ("printf 123456; printf peer >&2", StreamName::Stdout),
        ("printf peer; printf 123456 >&2", StreamName::Stderr),
    ] {
        let error = run(Command::new("/bin/sh")
            .args(["-c", script])
            .capture_stdout(CapturePolicy::bounded(5))
            .capture_stderr(CapturePolicy::bounded(5))
            .deadline(Duration::from_secs(10))
            .must("deadline is valid"))
        .must_err("cap plus one must fail closed");
        match error {
            Error::CaptureLimitExceeded {
                stream, evidence, ..
            } => {
                assert_eq!(stream, expected_stream);
                assert!(evidence.stdout.captured.len() <= 5);
                assert!(evidence.stderr.captured.len() <= 5);
            }
            other => panic!("expected CaptureLimitExceeded, got {other:?}"),
        }
    }
}

#[test]
fn fast_exit_cap_plus_one_stress_never_returns_success() {
    for (script, expected) in [
        ("printf 123456; printf peer >&2", StreamName::Stdout),
        ("printf peer; printf 123456 >&2", StreamName::Stderr),
    ] {
        for _ in 0..64 {
            let result = run(Command::new("/bin/sh")
                .args(["-c", script])
                .capture_stdout(CapturePolicy::bounded(5))
                .capture_stderr(CapturePolicy::bounded(5))
                .deadline(Duration::from_secs(2))
                .must("deadline is valid"));
            assert!(matches!(
                result,
                Err(Error::CaptureLimitExceeded { stream, .. }) if stream == expected
            ));
        }
    }
}

#[test]
fn simultaneous_overflow_is_stdout_first_and_both_buffers_remain_bounded() {
    let error = run(Command::new("/bin/sh")
        .args(["-c", "printf 123456; printf abcdef >&2"])
        .capture_stdout(CapturePolicy::bounded(5))
        .capture_stderr(CapturePolicy::bounded(5))
        .deadline(Duration::from_secs(10))
        .must("deadline is valid"))
    .must_err("both streams overflow");
    match error {
        Error::CaptureLimitExceeded {
            stream, evidence, ..
        } => {
            assert_eq!(stream, StreamName::Stdout);
            assert_eq!(evidence.stdout.captured.len(), 5);
            assert_eq!(evidence.stderr.captured.len(), 5);
        }
        other => panic!("expected CaptureLimitExceeded, got {other:?}"),
    }
}

#[test]
fn truncation_drains_high_volume_output_and_counts_every_discard() {
    const TOTAL: usize = 1024 * 1024;
    const LIMIT: usize = 4096;
    let output = run(Command::new("/usr/bin/head")
        .args(["-c", "1048576", "/dev/zero"])
        .capture_stdout(CapturePolicy::truncate(LIMIT))
        .deadline(Duration::from_secs(10))
        .must("deadline is valid"))
    .must("truncate mode drains without failing");
    assert_eq!(output.evidence.stdout.captured.len(), LIMIT);
    assert_eq!(
        output.evidence.stdout.captured.completeness,
        CaptureCompleteness::Truncated {
            discarded: (TOTAL - LIMIT) as u64
        }
    );
}

#[test]
fn redirected_stream_is_distinct_from_captured_empty() {
    let output = run(Command::new("/bin/true")
        .stdout(std::process::Stdio::null())
        .deadline(Duration::from_secs(10))
        .must("deadline is valid"))
    .must("true exits zero");
    assert_eq!(
        output.evidence.stdout.captured.completeness,
        CaptureCompleteness::Redirected
    );
    assert_eq!(
        output.evidence.stderr.captured.completeness,
        CaptureCompleteness::Complete
    );
}

#[test]
fn timeout_retains_partial_capture_and_secondary_evidence() {
    let error = run(Command::new("/bin/sh")
        .args(["-c", "printf ready; exec /bin/sleep 30"])
        .deadline(Duration::from_millis(500))
        .must("deadline is valid"))
    .must_err("sleep exceeds deadline");
    match error {
        Error::Timeout { evidence, .. } => {
            assert_eq!(evidence.stdout.captured.bytes, b"ready");
            assert!(matches!(&evidence.cleanup, CleanupOutcome::Complete));
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
}

#[cfg(target_os = "linux")]
#[test]
fn sync_timeout_kills_background_descendant_holding_pipes() {
    let directory = tempfile::tempdir().must("scratch directory");
    let leaderfile = directory.path().join("leader.pid");
    let pidfile = directory.path().join("descendant.pid");
    let script = format!(
        "echo $$ > {}; /bin/sleep 30 & echo $! > {}; wait",
        leaderfile.display(),
        pidfile.display()
    );
    let configured_deadline = Duration::from_secs(1);
    let error = run(Command::new("/bin/sh")
        .args(["-c", &script])
        .deadline(configured_deadline)
        .must("deadline is valid"))
    .must_err("background descendant exceeds deadline");
    let elapsed = match error {
        Error::Timeout { evidence, .. } => evidence.elapsed.must("elapsed evidence is known"),
        other => panic!("expected Timeout, got {other:?}"),
    };
    assert!(elapsed >= configured_deadline);
    assert!(elapsed <= configured_deadline + CLEANUP_ALLOWANCE);
    let leader = wait_for_pid(&leaderfile);
    let pid = wait_for_pid(&pidfile);
    wait_until_gone(leader);
    wait_until_gone(pid);
}

#[test]
fn output_preserves_nonzero_payload_without_clone() {
    let output = output(bounded("/bin/false")).must("output preserves nonzero status");
    assert!(!output.success());
    assert!(!output.status().success());
}

#[cfg(target_os = "linux")]
#[test]
fn escaped_session_surfaces_truthful_cleanup_incomplete() {
    use rustix::process::{Pid, Signal, kill_process};

    let directory = tempfile::tempdir().must("scratch directory");
    let pidfile = directory.path().join("escaped.pid");
    let script = format!(
        "/usr/bin/setsid /bin/sh -c 'echo $$ > {}; exec /bin/sleep 30' & exec /bin/sleep 1",
        pidfile.display()
    );
    let leader_lifetime = Duration::from_secs(1);
    let error = run(Command::new("/bin/sh")
        .args(["-c", &script])
        .deadline(Duration::from_secs(10))
        .must("deadline is valid"))
    .must_err("escaped process retains both capture pipes");
    let elapsed = match error {
        Error::CleanupIncomplete {
            evidence: lifecycle,
            ..
        } => {
            let cleanup = match &lifecycle.cleanup {
                CleanupOutcome::Incomplete(cleanup) => cleanup,
                other => panic!("expected incomplete cleanup evidence, got {other:?}"),
            };
            assert_eq!(
                cleanup.unfinished_streams,
                vec![StreamName::Stdout, StreamName::Stderr]
            );
            lifecycle.elapsed.must("elapsed evidence is known")
        }
        other => panic!("expected CleanupIncomplete, got {other:?}"),
    };
    assert!(elapsed >= leader_lifetime);
    assert!(elapsed <= leader_lifetime + CLEANUP_ALLOWANCE);

    let deadline = Instant::now() + Duration::from_secs(2);
    let pid = loop {
        if let Ok(value) = std::fs::read_to_string(&pidfile) {
            break value.trim().parse::<i32>().must("pid is numeric");
        }
        assert!(
            Instant::now() < deadline,
            "escaped child never wrote pidfile"
        );
        std::thread::yield_now();
    };
    if let Some(pid) = Pid::from_raw(pid) {
        let _ = kill_process(pid, Signal::KILL);
    }
}

#[cfg(target_os = "linux")]
fn wait_for_pid(path: &std::path::Path) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Ok(value) = std::fs::read_to_string(path) {
            return value.trim().parse().must("pid is numeric");
        }
        assert!(Instant::now() < deadline, "child never became ready");
        std::thread::yield_now();
    }
}

#[cfg(target_os = "linux")]
fn wait_until_gone(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(3);
    let proc_path = format!("/proc/{pid}");
    while std::path::Path::new(&proc_path).exists() {
        assert!(
            Instant::now() < deadline,
            "descendant {pid} survived cleanup"
        );
        std::thread::yield_now();
    }
}
