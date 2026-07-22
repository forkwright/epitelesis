//! Async adapter lifecycle coverage.

#![cfg(feature = "async")]
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
use std::fmt::Debug;
use std::time::{Duration, Instant};

use epitelesis::{
    CLEANUP_ALLOWANCE, CaptureCompleteness, CapturePolicy, Command, EnvironmentPolicy, Error, spawn,
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

#[tokio::test]
async fn async_success_and_timeout_match_sync_contract() {
    let output = spawn(
        Command::new("/bin/true")
            .deadline(Duration::from_secs(2))
            .must("deadline is valid"),
    )
    .await
    .must("true exits zero");
    assert!(output.success());

    let error = spawn(
        Command::new("/bin/sleep")
            .arg("30")
            .deadline(Duration::from_millis(100))
            .must("deadline is valid"),
    )
    .await
    .must_err("sleep exceeds deadline");
    assert!(matches!(error, Error::Timeout { .. }));
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn dropping_async_future_cancels_background_descendant() {
    let directory = tempfile::tempdir().must("scratch directory");
    let leaderfile = directory.path().join("leader.pid");
    let pidfile = directory.path().join("descendant.pid");
    let script = format!(
        "echo $$ > {}; /bin/sleep 30 & echo $! > {}; wait",
        leaderfile.display(),
        pidfile.display()
    );
    let task = tokio::spawn(spawn(
        Command::new("/bin/sh")
            .args(["-c", &script])
            .deadline(Duration::from_secs(30))
            .must("deadline is valid"),
    ));
    let leader = wait_for_pid(&leaderfile).await;
    let pid = wait_for_pid(&pidfile).await;
    task.abort();
    let _ = task.await;
    wait_until_gone(leader).await;
    wait_until_gone(pid).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn async_timeout_kills_background_descendant_holding_pipes() {
    let directory = tempfile::tempdir().must("scratch directory");
    let leaderfile = directory.path().join("leader.pid");
    let pidfile = directory.path().join("descendant.pid");
    let script = format!(
        "echo $$ > {}; /bin/sleep 30 & echo $! > {}; wait",
        leaderfile.display(),
        pidfile.display()
    );
    let configured_deadline = Duration::from_secs(1);
    let task = tokio::spawn(spawn(
        Command::new("/bin/sh")
            .args(["-c", &script])
            .deadline(configured_deadline)
            .must("deadline is valid"),
    ));
    let leader = wait_for_pid(&leaderfile).await;
    let pid = wait_for_pid(&pidfile).await;
    let error = task
        .await
        .must("timeout task joins")
        .must_err("background descendant exceeds deadline");
    let elapsed = match error {
        Error::Timeout { evidence, .. } => evidence.elapsed.must("elapsed evidence is known"),
        other => panic!("expected Timeout, got {other:?}"),
    };
    assert!(elapsed >= configured_deadline);
    assert!(elapsed <= configured_deadline + CLEANUP_ALLOWANCE);
    wait_until_gone(leader).await;
    wait_until_gone(pid).await;
}

#[tokio::test]
async fn async_fast_exit_overflow_and_truncation_match_sync() {
    for (script, expected) in [
        (
            "printf 123456; printf peer >&2",
            epitelesis::StreamName::Stdout,
        ),
        (
            "printf peer; printf 123456 >&2",
            epitelesis::StreamName::Stderr,
        ),
    ] {
        for _ in 0..32 {
            let error = spawn(
                Command::new("/bin/sh")
                    .args(["-c", script])
                    .capture_stdout(CapturePolicy::bounded(5))
                    .capture_stderr(CapturePolicy::bounded(5))
                    .deadline(Duration::from_secs(2))
                    .must("deadline is valid"),
            )
            .await
            .must_err("cap plus one cannot return success");
            assert!(matches!(
                error,
                Error::CaptureLimitExceeded { stream, .. } if stream == expected
            ));
        }
    }

    let output = spawn(
        Command::new("/bin/sh")
            .args(["-c", "printf 123456"])
            .capture_stdout(CapturePolicy::truncate(5))
            .deadline(Duration::from_secs(2))
            .must("deadline is valid"),
    )
    .await
    .must("truncation drains to EOF");
    assert_eq!(output.evidence.stdout.captured.bytes, b"12345");
    assert_eq!(
        output.evidence.stdout.captured.completeness,
        CaptureCompleteness::Truncated { discarded: 1 }
    );
}

#[tokio::test]
async fn async_environment_and_nonzero_match_sync() {
    let clean = spawn(
        Command::new("/usr/bin/env")
            .deadline(Duration::from_secs(2))
            .must("deadline is valid"),
    )
    .await
    .must("clean env exits zero");
    assert!(clean.evidence.stdout.captured.is_empty());

    let allowlisted = spawn(
        Command::new("/usr/bin/printenv")
            .arg("PATH")
            .environment(EnvironmentPolicy::allowlist(["PATH"]))
            .deadline(Duration::from_secs(2))
            .must("deadline is valid"),
    )
    .await
    .must("allowlisted env exits zero");
    assert!(!allowlisted.evidence.stdout.captured.is_empty());

    let error = spawn(
        Command::new("/bin/false")
            .deadline(Duration::from_secs(2))
            .must("deadline is valid"),
    )
    .await
    .must_err("nonzero remains typed");
    assert!(matches!(error, Error::NonZeroExit { .. }));
}

#[tokio::test]
async fn async_missing_program_stdio_override_and_piped_stdin_match_sync() {
    let missing = spawn(
        Command::new("/definitely/does/not/exist/epitelesis-test-binary")
            .deadline(Duration::from_secs(2))
            .must("deadline is valid"),
    )
    .await
    .must_err("missing program cannot spawn");
    assert!(matches!(missing, Error::SpawnFailed { .. }));

    let redirected = spawn(
        Command::new("/bin/sh")
            .args(["-c", "printf discarded"])
            .stdout(std::process::Stdio::null())
            .deadline(Duration::from_secs(2))
            .must("deadline is valid"),
    )
    .await
    .must("redirected command exits");
    assert_eq!(
        redirected.evidence.stdout.captured.completeness,
        CaptureCompleteness::Redirected
    );

    let eof = spawn(
        Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .deadline(Duration::from_secs(2))
            .must("deadline is valid"),
    )
    .await
    .must("capturing supervisor closes stdin");
    assert!(eof.success());
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_async_supervisors_do_not_leak_tracing_spans() {
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
            assert_ne!(name.as_deref(), Some("epitelesis.supervise"));
        }
    };
    let first = spawn(
        Command::new("/bin/sleep")
            .arg("0.3")
            .deadline(Duration::from_secs(2))
            .must("deadline is valid"),
    );
    let second = spawn(
        Command::new("/bin/sleep")
            .arg("0.3")
            .deadline(Duration::from_secs(2))
            .must("deadline is valid"),
    );
    let (first, second, ()) = tokio::join!(first, second, probe);
    first.must("first concurrent supervisor exits");
    second.must("second concurrent supervisor exits");
}

#[cfg(target_os = "linux")]
async fn wait_for_pid(path: &std::path::Path) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Ok(value) = std::fs::read_to_string(path) {
            return value.trim().parse().must("pid is numeric");
        }
        assert!(Instant::now() < deadline, "child never became ready");
        tokio::task::yield_now().await;
    }
}

#[cfg(target_os = "linux")]
async fn wait_until_gone(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(3);
    let proc_path = format!("/proc/{pid}");
    while std::path::Path::new(&proc_path).exists() {
        assert!(
            Instant::now() < deadline,
            "descendant {pid} survived cleanup"
        );
        tokio::task::yield_now().await;
    }
}
