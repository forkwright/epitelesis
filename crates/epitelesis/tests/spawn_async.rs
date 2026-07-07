//! Integration coverage for `epitelesis::spawn` (async, gated by the `async`
//! feature). Mirrors the sync surface so we catch divergence between the two
//! runners.

#![cfg(feature = "async")]
// WHY: Integration tests legitimately use expect/unwrap/panic for setup
// failures so the named step surfaces directly in the failure message.
#![expect(
    clippy::expect_used,
    reason = "integration tests assert on the named subprocess step; expect surfaces the failing fixture"
)]

use std::process::Stdio;
use std::time::Duration;

use epitelesis::{Command, Error, spawn};

#[tokio::test]
async fn async_simple_success_returns_output() {
    let output = spawn(Command::new("true"))
        .await
        .expect("`true` always succeeds");
    assert!(output.success());
}

#[tokio::test]
async fn async_non_zero_exit_returns_typed_error() {
    let err = spawn(Command::new("false"))
        .await
        .expect_err("`false` always exits 1");
    assert!(matches!(err, Error::NonZeroExit { .. }));
}

#[tokio::test]
async fn async_timeout_kills_long_running_child() {
    let started = std::time::Instant::now();
    let err = spawn(
        Command::new("sleep")
            .arg("30")
            .timeout(Duration::from_millis(150)),
    )
    .await
    .expect_err("sleep 30 must be killed before completion");
    assert!(matches!(err, Error::Timeout { .. }));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "tokio::time::timeout must fire well before the child completes"
    );
}

#[tokio::test]
async fn async_env_passthrough_reaches_child_process() {
    // WHY: async/sync parity — the builder's env must apply identically on
    // the tokio path.
    let output = spawn(
        Command::new("printenv")
            .arg("EPITELESIS_ASYNC_TOKEN")
            .env("EPITELESIS_ASYNC_TOKEN", "telos-acknowledged"),
    )
    .await
    .expect("printenv with the var set must exit 0");
    assert_eq!(
        output
            .stdout_str()
            .expect("printenv emits utf-8")
            .trim_end(),
        "telos-acknowledged"
    );
}

#[tokio::test]
async fn async_env_remove_after_env_removes_the_var() {
    // WHY: regression for the async path silently dropping env_remove — the
    // builder contract must hold identically on both runners.
    let err = spawn(
        Command::new("printenv")
            .arg("EPITELESIS_ASYNC_ORDER")
            .env("EPITELESIS_ASYNC_ORDER", "should-be-removed")
            .env_remove("EPITELESIS_ASYNC_ORDER"),
    )
    .await
    .expect_err("printenv on an absent var exits 1");
    assert!(
        matches!(err, Error::NonZeroExit { .. }),
        "the later env_remove must win over the earlier env, got {err:?}"
    );
}

#[tokio::test]
async fn async_env_after_env_remove_resets_the_var() {
    let output = spawn(
        Command::new("printenv")
            .arg("EPITELESIS_ASYNC_ORDER")
            .env_remove("EPITELESIS_ASYNC_ORDER")
            .env("EPITELESIS_ASYNC_ORDER", "restored"),
    )
    .await
    .expect("the later env must win over the earlier env_remove");
    assert_eq!(
        output
            .stdout_str()
            .expect("printenv emits utf-8")
            .trim_end(),
        "restored"
    );
}

#[tokio::test]
async fn async_stdout_override_is_honored() {
    // WHY: regression for the async path hardcoding piped stdio — a caller
    // redirecting stdout away from the pipe must get the same behaviour as
    // the sync runner (nothing captured).
    let output = spawn(
        Command::new("echo")
            .arg("discarded")
            .stdout(Stdio::null())
            .timeout(Duration::from_secs(10)),
    )
    .await
    .expect("echo must exit 0");
    assert!(output.success(), "expected zero exit");
    assert!(
        output.stdout.is_empty(),
        "stdout redirected to null must not be captured"
    );
}

#[tokio::test]
async fn async_piped_stdin_is_closed_so_a_reading_child_sees_eof() {
    let started = std::time::Instant::now();
    let output = spawn(
        Command::new("cat")
            .stdin(Stdio::piped())
            .timeout(Duration::from_secs(30)),
    )
    .await
    .expect("cat on a closed stdin exits 0 immediately");
    assert!(output.success(), "expected zero exit");
    assert!(output.stdout.is_empty(), "no input, no output");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "cat must see EOF at once, not wait out the timeout"
    );
}

#[tokio::test]
async fn async_timeout_error_carries_partial_output() {
    // WHY: parity with the sync runner — partial output captured before the
    // deadline must survive into the Timeout payload.
    let err = spawn(
        Command::new("sh")
            .args([
                "-c",
                "printf early-stdout; printf early-stderr >&2; sleep 30",
            ])
            .timeout(Duration::from_millis(500)),
    )
    .await
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

// WHY: regression for the `Span::enter()` guard held across `.await`: on a
// single worker thread, a suspended spawn() future must NOT leave its span
// installed as the thread-local current span, or every other task polled on
// that thread has its events attributed to `epitelesis.spawn`. The probe
// task interleaves with two concurrent spawns on a current_thread runtime
// and asserts it never observes their span as current.
#[tokio::test(flavor = "current_thread")]
async fn concurrent_spawn_does_not_leak_span_onto_other_tasks() {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::level_filters::LevelFilter::TRACE)
        .with_writer(std::io::sink)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let probe = async {
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let name = tracing::Span::current()
                .metadata()
                .map(|metadata| metadata.name().to_owned());
            assert_ne!(
                name.as_deref(),
                Some("epitelesis.spawn"),
                "a suspended spawn() future leaked its span onto the worker thread"
            );
        }
    };

    let first = spawn(
        Command::new("sleep")
            .arg("0.3")
            .timeout(Duration::from_secs(10)),
    );
    let second = spawn(
        Command::new("sleep")
            .arg("0.3")
            .timeout(Duration::from_secs(10)),
    );

    let (first, second, ()) = tokio::join!(first, second, probe);
    first.expect("first concurrent sleep must succeed");
    second.expect("second concurrent sleep must succeed");
}
