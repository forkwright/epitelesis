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

#[cfg(target_os = "linux")]
mod support;

use std::fmt::Debug;
use std::io::Read as _;
use std::time::{Duration, Instant};

use epitelesis::{
    CLEANUP_ALLOWANCE, CleanupOutcome, Command, Error, ForwardedSignal, GroupSignalOutcome,
    ManagedPoll, spawn_managed,
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

#[test]
fn managed_child_streams_and_waits() {
    let mut child = spawn_managed(
        Command::new("/bin/sh")
            .args(["-c", "printf managed"])
            .deadline(Duration::from_secs(5))
            .must("deadline is valid")
            .streaming()
            .must("streaming policy is valid"),
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
            .streaming()
            .must("streaming policy is valid"),
    )
    .must("managed child spawns");
    let pid = wait_for_pid(&pidfile);
    child.cancel().must("cancel settles and reaps");
    assert!(
        wait_until_gone(pid),
        "cancel must remove the descendant from procfs"
    );
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
            .streaming()
            .must("streaming policy is valid"),
    )
    .must("managed child spawns");
    let pid = wait_for_pid(&pidfile);
    drop(child);
    assert!(
        wait_until_gone(pid),
        "drop must remove the descendant from procfs"
    );
}

#[test]
fn managed_wait_closes_retained_piped_stdin() {
    let mut child = spawn_managed(
        Command::new("/bin/sh")
            .args(["-c", "cat >/dev/null; printf eof"])
            .stdin(std::process::Stdio::piped())
            .deadline(Duration::from_secs(2))
            .must("deadline is valid")
            .streaming()
            .must("streaming policy is valid"),
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
fn managed_signal_request_forwards_without_exposing_the_process_group() {
    let child = spawn_managed(
        Command::new("/bin/sh")
            .args(["-c", "while :; do sleep 1; done"])
            .deadline(Duration::from_secs(30))
            .must("deadline is valid")
            .streaming()
            .must("streaming policy is valid"),
    )
    .must("managed child spawns");
    child
        .signal_requester()
        .forward(ForwardedSignal::Terminate)
        .must("supervisor accepts a forwarded SIGTERM request");

    match child.wait() {
        Err(Error::NonZeroExit { evidence, .. }) => {
            let forwarded = evidence
                .forwarded_signal
                .as_ref()
                .must("forwarded signal is retained in lifecycle evidence");
            assert_eq!(forwarded.signal, ForwardedSignal::Terminate);
            assert!(matches!(forwarded.outcome, GroupSignalOutcome::Sent));
            assert!(forwarded.escalation.is_none());
        }
        other => panic!("expected the forwarded signal to terminate the child, got {other:?}"),
    }
}

/// A forwarded catchable signal gets a grace interval, then the same owned
/// supervisor escalates the complete process group before returning. The
/// descendant deliberately ignores TERM, so leader reap alone would make
/// this test pass while leaving a live writer behind.
#[cfg(target_os = "linux")]
#[test]
fn managed_signal_waits_for_ignoring_descendant_before_settlement() {
    let directory = tempfile::tempdir().must("scratch directory");
    let pidfile = directory.path().join("ignoring-descendant.pid");
    let script = format!(
        "sh -c 'trap \"\" TERM; echo $$ > {}; while :; do sleep 1; done' & wait",
        pidfile.display()
    );
    let child = spawn_managed(
        Command::new("/bin/sh")
            .args(["-c", &script])
            .deadline(Duration::from_secs(30))
            .must("deadline is valid")
            .streaming()
            .must("streaming policy is valid"),
    )
    .must("managed child spawns");
    let pid = wait_for_pid(&pidfile);
    let leader = child.id();
    let started = Instant::now();
    child
        .signal_requester()
        .forward(ForwardedSignal::Terminate)
        .must("supervisor accepts a forwarded SIGTERM request");

    // A waitable leader must continue reserving its PGID while an ignoring
    // descendant remains and the supervisor can still escalate the group.
    let leader_wait_deadline = started + Duration::from_millis(750);
    loop {
        let stat = std::fs::read_to_string(format!("/proc/{leader}/stat"))
            .must("leader PID remains reserved throughout forwarding grace");
        let state = stat
            .rsplit_once(") ")
            .and_then(|(_, rest)| rest.split_whitespace().next());
        if state == Some("Z") {
            break;
        }
        assert!(
            Instant::now() < leader_wait_deadline,
            "leader should exit during forwarding grace"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    match child.wait() {
        Err(Error::NonZeroExit { evidence, .. }) => {
            let forwarded = evidence
                .forwarded_signal
                .as_ref()
                .must("forwarded signal evidence is retained");
            assert!(matches!(forwarded.outcome, GroupSignalOutcome::Sent));
            assert!(
                matches!(forwarded.escalation, Some(GroupSignalOutcome::Sent)),
                "an ignoring descendant requires SIGKILL escalation: {forwarded:?}"
            );
            assert!(matches!(evidence.cleanup, CleanupOutcome::Complete));
        }
        other => panic!("expected forwarded signal to terminate managed group, got {other:?}"),
    }
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "settlement must include the forwarded-signal grace interval"
    );
    assert!(
        wait_until_gone(pid),
        "forwarded signal cleanup must remove the descendant from procfs"
    );
}

#[test]
fn managed_deadline_is_automatic_and_evidence_is_aggregate() {
    let configured_deadline = Duration::from_millis(100);
    let child = spawn_managed(
        Command::new("/bin/sleep")
            .arg("30")
            .deadline(configured_deadline)
            .must("deadline is valid")
            .streaming()
            .must("streaming policy is valid"),
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
            .streaming()
            .must("streaming policy is valid"),
    )
    .must("sleep spawns");
    assert!(matches!(running.poll(), ManagedPoll::Running));
    running.cancel().must("running child cancels");

    let mut success = spawn_managed(
        Command::new("/bin/true")
            .deadline(Duration::from_secs(2))
            .must("deadline is valid")
            .streaming()
            .must("streaming policy is valid"),
    )
    .must("true spawns");
    wait_for_terminal_poll(&mut success, true);
    assert!(success.wait().must("success remains available").success());

    let mut failed = spawn_managed(
        Command::new("/bin/false")
            .deadline(Duration::from_secs(2))
            .must("deadline is valid")
            .streaming()
            .must("streaming policy is valid"),
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
        if let Some(pid) = support::read_complete_pid(path).must("pid is numeric") {
            return pid;
        }
        assert!(Instant::now() < deadline, "child never became ready");
        std::thread::yield_now();
    }
}

#[cfg(target_os = "linux")]
fn wait_until_gone(pid: u32) -> bool {
    let deadline = Instant::now() + Duration::from_secs(3);
    let proc_path = format!("/proc/{pid}");
    while std::path::Path::new(&proc_path).exists() {
        assert!(
            Instant::now() < deadline,
            "descendant {pid} survived cleanup"
        );
        std::thread::yield_now();
    }
    !std::path::Path::new(&proc_path).exists()
}

#[test]
fn typed_error_exposes_execution_evidence_without_treating_refusal_as_execution() {
    let refused = Command::new("/bin/true")
        .unbounded("")
        .err()
        .must("an unbounded lifetime requires a reason before spawn");
    assert!(refused.evidence().is_none());
    let failed = Command::new("/bin/sh")
        .args(["-c", "printf diagnostic >&2; exit 7"])
        .deadline(Duration::from_secs(5))
        .and_then(epitelesis::run)
        .err()
        .must("command exit remains an error");
    let evidence = failed
        .evidence()
        .must("execution retains aggregate evidence");
    assert_eq!(
        evidence.leader_status.and_then(|status| status.code()),
        Some(7)
    );
    assert_eq!(evidence.stderr.captured.bytes, b"diagnostic");
}
