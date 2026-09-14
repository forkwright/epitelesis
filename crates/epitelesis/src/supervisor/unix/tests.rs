//! Unix supervisor unit coverage.

use super::*;

use super::*;

#[test]
fn primary_precedence_is_stable() {
    let mut observations = Observations {
        cancelled: true,
        deadline_elapsed: true,
        leader_waitable: true,
        observe_error: Some(std::io::Error::other("observe")),
        reap_failures: Vec::new(),
    };
    assert!(matches!(
        choose_primary(&mut observations, Some(7), Some(9)),
        Primary::Limit {
            stream: StreamName::Stdout,
            limit: 7
        }
    ));
    assert!(matches!(
        choose_primary(&mut observations, None, Some(9)),
        Primary::Limit {
            stream: StreamName::Stderr,
            limit: 9
        }
    ));
    assert!(matches!(
        choose_primary(&mut observations, None, None),
        Primary::Cancelled
    ));
}

#[test]
fn settlement_matrix_uses_classification_and_keeps_peer_bytes() {
    let outcomes = [
        None,
        Some(CaptureFailure::Read {
            kind: std::io::ErrorKind::Other,
            message: "fixture".to_owned(),
        }),
        Some(CaptureFailure::Allocation),
    ];
    for stdout_failure in &outcomes {
        for stderr_failure in &outcomes {
            let evidence = fixture_evidence(stdout_failure.clone(), stderr_failure.clone());
            let result = classify(
                Primary::Exit,
                "fixture".to_owned(),
                Some(Duration::from_secs(1)),
                evidence,
            );
            if stdout_failure.is_none() && stderr_failure.is_none() {
                let output = match result {
                    Ok(output) => output,
                    Err(error) => panic!("success matrix cell failed: {error:?}"),
                };
                assert_eq!(output.evidence.stdout.captured.bytes, b"out");
                assert_eq!(output.evidence.stderr.captured.bytes, b"err");
            } else {
                let evidence = match result {
                    Err(crate::Error::CaptureFailed { evidence, .. }) => evidence,
                    other => panic!("capture matrix cell misclassified: {other:?}"),
                };
                assert_eq!(evidence.stdout.stream, StreamName::Stdout);
                assert_eq!(evidence.stderr.stream, StreamName::Stderr);
                assert_eq!(evidence.stdout.captured.bytes, b"out");
                assert_eq!(evidence.stderr.captured.bytes, b"err");
                let expected_first = if stdout_failure.is_some() {
                    StreamName::Stdout
                } else {
                    StreamName::Stderr
                };
                assert_eq!(evidence.first_capture_failure(), Some(expected_first));
            }
        }
    }
}

#[test]
fn unbounded_read_failure_triggers_cleanup_with_incomplete_peer() {
    use std::os::fd::{AsFd, BorrowedFd};

    struct FaultingPipe(std::fs::File);
    impl Read for FaultingPipe {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("injected read failure"))
        }
    }
    impl AsFd for FaultingPipe {
        fn as_fd(&self) -> BorrowedFd<'_> {
            self.0.as_fd()
        }
    }

    struct HoldingPipe(std::fs::File);
    impl Read for HoldingPipe {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        }
    }
    impl AsFd for HoldingPipe {
        fn as_fd(&self) -> BorrowedFd<'_> {
            self.0.as_fd()
        }
    }

    let stdout_file = match std::fs::File::open("/dev/null") {
        Ok(file) => file,
        Err(error) => panic!("stdout fixture failed: {error}"),
    };
    let stderr_file = match std::fs::File::open("/dev/null") {
        Ok(file) => file,
        Err(error) => panic!("stderr fixture failed: {error}"),
    };
    let policy = match CapturePolicy::unbounded("fault-injection fixture") {
        Ok(policy) => policy,
        Err(error) => panic!("unbounded fixture policy failed: {error:?}"),
    };
    let mut stdout =
        CaptureStream::new(StreamName::Stdout, Some(FaultingPipe(stdout_file)), &policy);
    let mut stderr = CaptureStream::new(
        StreamName::Stderr,
        Some(HoldingPipe(stderr_file)),
        &CapturePolicy::bounded(8),
    );
    stdout.pump(1);
    stderr.pump(1);

    assert!(stdout.failed());
    assert!(!stderr.terminal());
    assert!(settlement_triggered(
        &Observations::default(),
        false,
        stdout.failed()
    ));
    let stdout = stdout.into_report();
    let stderr = stderr.into_report();
    assert!(stdout.failure.is_some());
    assert!(matches!(
        stdout.captured.completeness,
        crate::CaptureCompleteness::Incomplete { .. }
    ));
    assert!(matches!(
        stderr.captured.completeness,
        crate::CaptureCompleteness::Incomplete { .. }
    ));
}

fn fixture_evidence(
    stdout_failure: Option<CaptureFailure>,
    stderr_failure: Option<CaptureFailure>,
) -> Box<LifecycleEvidence> {
    let status = match std::process::Command::new("/bin/true").status() {
        Ok(status) => status,
        Err(error) => panic!("true fixture failed: {error}"),
    };
    Box::new(LifecycleEvidence {
        leader_status: Some(status),
        elapsed: Some(Duration::from_millis(1)),
        signal: GroupSignalOutcome::Sent,
        forwarded_signal: None,
        reap: LeaderReapOutcome {
            disposition: LeaderReapDisposition::Reaped,
            failures: Vec::new(),
        },
        stdout: CaptureReport {
            stream: StreamName::Stdout,
            captured: if stdout_failure.is_some() {
                CapturedStream::incomplete(b"out".to_vec(), 0)
            } else {
                CapturedStream::settled(b"out".to_vec(), 0)
            },
            failure: stdout_failure,
        },
        stderr: CaptureReport {
            stream: StreamName::Stderr,
            captured: if stderr_failure.is_some() {
                CapturedStream::incomplete(b"err".to_vec(), 0)
            } else {
                CapturedStream::settled(b"err".to_vec(), 0)
            },
            failure: stderr_failure,
        },
        cleanup: CleanupOutcome::Complete,
    })
}
