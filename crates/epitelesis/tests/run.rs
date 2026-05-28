//! Integration coverage for the synchronous `epitelesis::run` entry point.
//!
//! Tests target portable POSIX commands available on the menos workstation
//! and any standard fleet CI image (`true`, `false`, `printenv`, `sleep`).

// WHY: Integration tests legitimately use expect/unwrap/panic for setup
// failures so the named step surfaces directly in the failure message.
#![expect(
    clippy::expect_used,
    reason = "integration tests assert on the named subprocess step; expect surfaces the failing fixture"
)]

use std::time::Duration;

use epitelesis::{Command, Error, run};

#[test]
fn simple_success_returns_output() {
    let output = run(Command::new("true")).expect("`true` always succeeds");
    assert!(output.success(), "expected zero exit");
    assert!(output.stdout.is_empty(), "`true` writes nothing to stdout");
    assert!(output.stderr.is_empty(), "`true` writes nothing to stderr");
}

#[test]
fn non_zero_exit_returns_typed_error_with_payload() {
    let err = run(Command::new("false")).expect_err("`false` always exits 1");
    match err {
        Error::NonZeroExit {
            program,
            status,
            output,
            ..
        } => {
            assert!(
                program.ends_with("false"),
                "program field carries the invocation"
            );
            assert!(!status.success(), "status reflects failure");
            assert!(
                !output.success(),
                "captured Output also reports the failure"
            );
        }
        other => panic!("expected NonZeroExit, got {other:?}"),
    }
}

#[test]
fn missing_program_surfaces_spawn_failed() {
    let err = run(Command::new(
        "/definitely/does/not/exist/epitelesis-test-binary",
    ))
    .expect_err("missing program must not spawn");
    match err {
        Error::SpawnFailed { program, .. } => {
            assert!(
                program.contains("epitelesis-test-binary"),
                "program field carries the invocation"
            );
        }
        other => panic!("expected SpawnFailed, got {other:?}"),
    }
}

#[test]
fn env_passthrough_reaches_child_process() {
    let output = run(Command::new("printenv")
        .arg("EPITELESIS_TEST_TOKEN")
        .env("EPITELESIS_TEST_TOKEN", "telos-acknowledged"))
    .expect("printenv with the var set must exit 0");
    let captured = output
        .stdout_str()
        .expect("printenv emits utf-8")
        .trim_end();
    assert_eq!(
        captured, "telos-acknowledged",
        "child must observe the env var the builder set"
    );
}

#[test]
fn large_stdout_does_not_deadlock_against_full_pipe_buffer() {
    // WHY: regression for the pipe-deadlock (kanon#908). A child that writes
    // more than the ~64KB OS pipe buffer blocks on `write` until a reader
    // drains it; reading only after `wait()` deadlocks forever. 1 MiB is well
    // past the buffer, so the run must still complete and capture every byte.
    // The timeout bounds the test: a regression surfaces as a clean Timeout
    // error here instead of hanging the suite indefinitely.
    const SIZE: usize = 1024 * 1024;
    let output = run(Command::new("head")
        .args(["-c", "1048576", "/dev/zero"])
        .timeout(Duration::from_secs(30)))
    .expect("draining concurrently, head must complete well within the timeout");
    assert!(output.success(), "expected zero exit");
    assert_eq!(
        output.stdout.len(),
        SIZE,
        "every byte of the large stdout must be captured without truncation"
    );
}

#[test]
fn timeout_kills_long_running_child() {
    let started = std::time::Instant::now();
    let err = run(Command::new("sleep")
        .arg("30")
        .timeout(Duration::from_millis(150)))
    .expect_err("sleep 30 must be killed before completion");
    let elapsed = started.elapsed();

    match err {
        Error::Timeout {
            program, duration, ..
        } => {
            assert!(program.ends_with("sleep"), "program reflects invocation");
            assert_eq!(duration, Duration::from_millis(150));
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(5),
        "wait_with_timeout must not block for the full sleep ({elapsed:?})"
    );
}
