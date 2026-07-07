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

use std::io::ErrorKind;
use std::process::Stdio;
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
fn env_remove_after_env_removes_the_var() {
    // WHY: builder call order is the contract — an `env_remove` after `env`
    // for the same key must remove it, exactly like std::process::Command.
    let err = run(Command::new("printenv")
        .arg("EPITELESIS_ORDER_TOKEN")
        .env("EPITELESIS_ORDER_TOKEN", "should-be-removed")
        .env_remove("EPITELESIS_ORDER_TOKEN"))
    .expect_err("printenv on an absent var exits 1");
    assert!(
        matches!(err, Error::NonZeroExit { .. }),
        "the later env_remove must win over the earlier env, got {err:?}"
    );
}

#[test]
fn env_after_env_remove_resets_the_var() {
    let output = run(Command::new("printenv")
        .arg("EPITELESIS_ORDER_TOKEN")
        .env_remove("EPITELESIS_ORDER_TOKEN")
        .env("EPITELESIS_ORDER_TOKEN", "restored"))
    .expect("the later env must win over the earlier env_remove");
    assert_eq!(
        output
            .stdout_str()
            .expect("printenv emits utf-8")
            .trim_end(),
        "restored"
    );
}

#[test]
fn get_envs_reports_effective_configuration() {
    // WHY: get_envs must resolve conflicting env/env_remove calls to the
    // effective (last-call-wins) view instead of listing both entries.
    let cmd = Command::new("true")
        .env("EPITELESIS_A", "kept")
        .env("EPITELESIS_B", "shadowed")
        .env_remove("EPITELESIS_B")
        .env_remove("EPITELESIS_C")
        .env("EPITELESIS_C", "revived");
    let envs: Vec<_> = cmd.get_envs().collect();
    assert_eq!(
        envs,
        vec![
            ("EPITELESIS_A".as_ref(), Some("kept".as_ref())),
            ("EPITELESIS_B".as_ref(), None),
            ("EPITELESIS_C".as_ref(), Some("revived".as_ref())),
        ],
        "one entry per key, later builder call wins, sorted by key"
    );
}

#[test]
fn piped_stdin_is_closed_so_a_reading_child_sees_eof() {
    // WHY: run() never exposes the stdin handle, so a caller-piped stdin
    // must be closed at spawn — otherwise `cat` never sees EOF and stalls
    // until the timeout instead of completing immediately.
    let started = std::time::Instant::now();
    let output = run(Command::new("cat")
        .stdin(Stdio::piped())
        .timeout(Duration::from_secs(30)))
    .expect("cat on a closed stdin exits 0 immediately");
    assert!(output.success(), "expected zero exit");
    assert!(output.stdout.is_empty(), "no input, no output");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "cat must see EOF at once, not wait out the timeout"
    );
}

#[test]
fn stdout_override_is_honored() {
    let output = run(Command::new("echo")
        .arg("discarded")
        .stdout(Stdio::null())
        .timeout(Duration::from_secs(10)))
    .expect("echo must exit 0");
    assert!(output.success(), "expected zero exit");
    assert!(
        output.stdout.is_empty(),
        "stdout redirected to null must not be captured"
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

#[test]
fn timeout_error_carries_partial_output() {
    // WHY: a killed child's last words say where it stalled — the Timeout
    // payload must preserve whatever stdout/stderr was captured before the
    // deadline instead of discarding it.
    let err = run(Command::new("sh")
        .args([
            "-c",
            "printf early-stdout; printf early-stderr >&2; sleep 30",
        ])
        .timeout(Duration::from_millis(500)))
    .expect_err("the trailing sleep must exceed the timeout");
    match err {
        Error::Timeout { stdout, stderr, .. } => {
            assert_eq!(
                stdout, b"early-stdout",
                "stdout written before the deadline must survive the kill"
            );
            assert_eq!(
                stderr, b"early-stderr",
                "stderr written before the deadline must survive the kill"
            );
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
}

#[test]
fn oversized_timeout_runs_to_completion_without_panicking() {
    // WHY: regression for `Instant::now() + timeout` overflow — an
    // absurd-but-legal Duration must degrade to an unbounded wait.
    let output = run(Command::new("true").timeout(Duration::MAX))
        .expect("true must complete normally under an oversized timeout");
    assert!(output.success(), "expected zero exit");
}

#[test]
fn builder_output_preserves_io_error_kind() {
    // WHY: migrated std::process call sites match on io::Error::kind();
    // collapsing every failure to ErrorKind::Other breaks that matching.
    let err = Command::new("/definitely/does/not/exist/epitelesis-test-binary")
        .output()
        .expect_err("missing program must fail");
    assert_eq!(
        err.kind(),
        ErrorKind::NotFound,
        "spawn failure must surface the underlying kind, got {err:?}"
    );
}

#[test]
fn builder_status_maps_timeout_to_timed_out_kind() {
    let err = Command::new("sleep")
        .arg("30")
        .timeout(Duration::from_millis(150))
        .status()
        .expect_err("sleep 30 must be killed before completion");
    assert_eq!(
        err.kind(),
        ErrorKind::TimedOut,
        "timeout must surface as ErrorKind::TimedOut, got {err:?}"
    );
}
