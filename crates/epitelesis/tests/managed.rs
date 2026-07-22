//! Managed streaming child lifecycle coverage.

use std::fmt::Debug;
use std::io::Read as _;
use std::time::{Duration, Instant};

use epitelesis::{Command, spawn_managed};

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
            .must("deadline is valid"),
    )
    .must("managed child spawns");
    let mut stdout = child.take_stdout().must("stdout is piped");
    let mut bytes = Vec::new();
    stdout.read_to_end(&mut bytes).must("stdout drains");
    let status = child.wait().must("managed child exits");
    assert!(status.success());
    assert_eq!(bytes, b"managed");
}

#[cfg(target_os = "linux")]
#[test]
fn managed_cancel_kills_background_descendant() {
    let directory = tempfile::tempdir().must("scratch directory");
    let pidfile = directory.path().join("descendant.pid");
    let script = format!("/bin/sleep 30 & echo $! > {}; wait", pidfile.display());
    let mut child = spawn_managed(
        Command::new("/bin/sh")
            .args(["-c", &script])
            .deadline(Duration::from_secs(30))
            .must("deadline is valid"),
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
            .must("deadline is valid"),
    )
    .must("managed child spawns");
    let pid = wait_for_pid(&pidfile);
    drop(child);
    wait_until_gone(pid);
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
