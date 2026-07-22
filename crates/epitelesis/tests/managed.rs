//! Managed streaming child lifecycle coverage.

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
use std::io::Read as _;
use std::time::{Duration, Instant};

use epitelesis::{CLEANUP_ALLOWANCE, Command, Error, ManagedPoll, spawn_managed};

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

#[test]
fn managed_child_streams_and_waits() {
    let mut child = spawn_managed(
        Command::new("/bin/sh")
            .args(["-c", "printf managed"])
            .deadline(Duration::from_secs(5))
            .must("deadline is valid")
            .streaming(),
    )
    .must("managed child spawns");
    let mut stdout = child.take_stdout().must("stdout is piped");
    let mut bytes = Vec::new();
    stdout.read_to_end(&mut bytes).must("stdout drains");
    let output = child.wait().must("managed child exits");
    assert!(output.success());
    assert!(output.status().success());
    assert_eq!(bytes, b"managed");
}

#[cfg(target_os = "linux")]
#[test]
fn managed_cancel_kills_background_descendant() {
    let directory = tempfile::tempdir().must("scratch directory");
    let pidfile = directory.path().join("descendant.pid");
    let script = format!("/bin/sleep 30 & echo $! > {}; wait", pidfile.display());
    let child = spawn_managed(
        Command::new("/bin/sh")
            .args(["-c", &script])
            .deadline(Duration::from_secs(30))
            .must("deadline is valid")
            .streaming(),
    )
    .must("managed child spawns");
    let pid = wait_for_pid(&pidfile);
    child.cancel().must("cancel settles and reaps");
    wait_until_gone(pid);
}

#[cfg(target_os = "linux")]
#[test]
fn managed_drop_kills_background_descendant() {
    let directory = tempfile::tempdir().must("scratch directory");
    let pidfile = directory.path().join("descendant.pid");
    let script = format!("/bin/sleep 30 & echo $! > {}; wait", pidfile.display());
    let child = spawn_managed(
        Command::new("/bin/sh")
            .args(["-c", &script])
            .deadline(Duration::from_secs(30))
            .must("deadline is valid")
            .streaming(),
    )
    .must("managed child spawns");
    let pid = wait_for_pid(&pidfile);
    drop(child);
    wait_until_gone(pid);
}

#[test]
fn managed_wait_closes_retained_piped_stdin() {
    let mut child = spawn_managed(
        Command::new("/bin/sh")
            .args(["-c", "cat >/dev/null; printf eof"])
            .stdin(std::process::Stdio::piped())
            .deadline(Duration::from_secs(2))
            .must("deadline is valid")
            .streaming(),
    )
    .must("managed child spawns");
    let mut stdout = child.take_stdout().must("stdout is piped");
    let output = child.wait().must("wait closes retained stdin");
    let mut bytes = Vec::new();
    stdout.read_to_end(&mut bytes).must("stdout reaches EOF");
    assert!(output.success());
    assert_eq!(bytes, b"eof");
}

#[test]
fn managed_deadline_is_automatic_and_evidence_is_aggregate() {
    let configured_deadline = Duration::from_millis(100);
    let child = spawn_managed(
        Command::new("/bin/sleep")
            .arg("30")
            .deadline(configured_deadline)
            .must("deadline is valid")
            .streaming(),
    )
    .must("managed child spawns");
    match child.wait() {
        Err(Error::Timeout {
            deadline, evidence, ..
        }) => {
            assert_eq!(deadline, configured_deadline);
            let elapsed = evidence.elapsed.must("elapsed evidence is known");
            assert!(elapsed >= deadline);
            assert!(elapsed <= deadline + CLEANUP_ALLOWANCE);
            assert!(evidence.leader_status.is_some());
        }
        other => panic!("expected managed timeout, got {other:?}"),
    }
}

#[test]
fn managed_poll_distinguishes_running_success_and_terminal_error() {
    let mut running = spawn_managed(
        Command::new("/bin/sleep")
            .arg("30")
            .deadline(Duration::from_secs(30))
            .must("deadline is valid")
            .streaming(),
    )
    .must("sleep spawns");
    assert!(matches!(running.poll(), ManagedPoll::Running));
    running.cancel().must("running child cancels");

    let mut success = spawn_managed(
        Command::new("/bin/true")
            .deadline(Duration::from_secs(2))
            .must("deadline is valid")
            .streaming(),
    )
    .must("true spawns");
    wait_for_terminal_poll(&mut success, true);
    assert!(success.wait().must("success remains available").success());

    let mut failed = spawn_managed(
        Command::new("/bin/false")
            .deadline(Duration::from_secs(2))
            .must("deadline is valid")
            .streaming(),
    )
    .must("false spawns");
    wait_for_terminal_poll(&mut failed, false);
    assert!(matches!(failed.wait(), Err(Error::NonZeroExit { .. })));
}

fn wait_for_terminal_poll(child: &mut epitelesis::ManagedChild, success: bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match child.poll() {
            ManagedPoll::Running => {
                assert!(Instant::now() < deadline, "poll never became terminal");
                std::thread::yield_now();
            }
            ManagedPoll::Exited(output) => {
                assert!(success);
                assert!(output.success());
                return;
            }
            ManagedPoll::Failed(error) => {
                assert!(!success);
                assert!(matches!(error, Error::NonZeroExit { .. }));
                return;
            }
            _ => panic!("unknown managed poll state"),
        }
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
