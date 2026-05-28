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
