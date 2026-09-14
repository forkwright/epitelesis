//! Bounded capture storage and nonblocking pipe pumping for the Unix supervisor.

use std::io::Read;
use std::process::{ChildStderr, ChildStdout};
use std::thread;
use std::time::Duration;

use rustix::event::{Nsecs, PollFd, PollFlags, Timespec, poll};
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};

use crate::error::CaptureFailure;
use crate::output::{CaptureReport, CapturedStream, StreamName};
use crate::policy::{CapturePolicy, OverflowBehavior};

const READ_CHUNK: usize = 8 * 1024;
const CHUNKS_PER_STREAM_TURN: usize = 8;

pub(super) struct CaptureStream<Pipe> {
    name: StreamName,
    pipe: Option<Pipe>,
    storage: CaptureStorage,
    discarded: u64,
    overflow: Option<usize>,
    failure: Option<CaptureFailure>,
    eof: bool,
    redirected: bool,
}

enum CaptureStorage {
    Bounded {
        bytes: Vec<u8>,
        limit: usize,
        overflow: OverflowBehavior,
    },
    Unbounded(Vec<u8>),
}

impl<Pipe: Read + std::os::fd::AsFd> CaptureStream<Pipe> {
    pub(super) fn new(name: StreamName, pipe: Option<Pipe>, policy: &CapturePolicy) -> Self {
        let redirected = pipe.is_none();
        let storage = match policy {
            CapturePolicy::Bounded { limit, overflow } => CaptureStorage::Bounded {
                bytes: Vec::new(),
                limit: *limit,
                overflow: *overflow,
            },
            CapturePolicy::Unbounded(_) => CaptureStorage::Unbounded(Vec::new()),
        };
        Self {
            name,
            pipe,
            storage,
            discarded: 0,
            overflow: None,
            failure: None,
            eof: false,
            redirected,
        }
    }

    pub(super) fn make_nonblocking(&mut self) {
        let result = self.pipe.as_ref().map(|pipe| {
            fcntl_getfl(pipe)
                .and_then(|flags| fcntl_setfl(pipe, flags | OFlags::NONBLOCK))
                .map_err(std::io::Error::from)
        });
        if let Some(Err(error)) = result {
            self.record_failure(&error);
        }
    }

    pub(super) fn pump(&mut self, chunk_budget: usize) {
        for _ in 0..chunk_budget {
            let mut chunk = [0_u8; READ_CHUNK];
            let read = match self.pipe.as_mut() {
                Some(pipe) => pipe.read(&mut chunk),
                None => return,
            };
            match read {
                Ok(0) => {
                    self.eof = true;
                    self.pipe = None;
                    return;
                }
                Ok(count) => match chunk.get(..count) {
                    Some(captured) => self.retain(captured),
                    None => {
                        self.record_failure(&std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "pipe reported a read length beyond its supplied buffer",
                        ));
                        return;
                    }
                },
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return,
                Err(error) => {
                    self.record_failure(&error);
                    return;
                }
            }
        }
    }

    fn retain(&mut self, chunk: &[u8]) {
        let mut allocation_failed = false;
        match &mut self.storage {
            CaptureStorage::Unbounded(bytes) => {
                if bytes.try_reserve(chunk.len()).is_ok() {
                    bytes.extend_from_slice(chunk);
                } else {
                    self.discarded = self.discarded.saturating_add(usize_to_u64(chunk.len()));
                    allocation_failed = true;
                }
            }
            CaptureStorage::Bounded {
                bytes,
                limit,
                overflow,
            } => {
                let retained = chunk.len().min(limit.saturating_sub(bytes.len()));
                let discarded = chunk.len() - retained;
                self.discarded = self.discarded.saturating_add(usize_to_u64(discarded));
                if discarded > 0 && *overflow == OverflowBehavior::FailClosed {
                    self.overflow.get_or_insert(*limit);
                }
                if retained > 0 {
                    if bytes.try_reserve(retained).is_ok() {
                        bytes.extend(chunk.iter().take(retained).copied());
                    } else {
                        self.discarded = self.discarded.saturating_add(usize_to_u64(retained));
                        allocation_failed = true;
                    }
                }
            }
        }
        if allocation_failed {
            self.failure.get_or_insert(CaptureFailure::Allocation);
            self.pipe = None;
        }
    }

    fn record_failure(&mut self, error: &std::io::Error) {
        self.failure.get_or_insert(CaptureFailure::Read {
            kind: error.kind(),
            message: error.to_string(),
        });
        self.pipe = None;
    }

    pub(super) fn failed(&self) -> bool {
        self.failure.is_some()
    }

    pub(super) fn overflow(&self) -> Option<usize> {
        self.overflow
    }

    pub(super) fn terminal(&self) -> bool {
        self.redirected || self.eof || self.failure.is_some()
    }

    pub(super) fn into_report(self) -> CaptureReport {
        let (CaptureStorage::Bounded { bytes, .. } | CaptureStorage::Unbounded(bytes)) =
            self.storage;
        let captured = if self.redirected {
            CapturedStream::redirected()
        } else if self.eof {
            CapturedStream::settled(bytes, self.discarded)
        } else {
            CapturedStream::incomplete(bytes, self.discarded)
        };
        CaptureReport {
            stream: self.name,
            captured,
            failure: self.failure,
        }
    }
}

pub(super) fn poll_streams(
    stdout: &CaptureStream<ChildStdout>,
    stderr: &CaptureStream<ChildStderr>,
    timeout: Duration,
) -> std::io::Result<(bool, bool)> {
    let events = PollFlags::IN | PollFlags::HUP | PollFlags::ERR;
    // The supervisor event quantum is below one second, so this also fits
    // 32-bit c_long on supported Unix targets.
    let nanoseconds = Nsecs::try_from(timeout.subsec_nanos()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "poll timeout nanoseconds exceed the platform timespec field",
        )
    })?;
    let timespec = Timespec {
        tv_sec: 0,
        tv_nsec: nanoseconds,
    };
    match (stdout.pipe.as_ref(), stderr.pipe.as_ref()) {
        (Some(stdout_pipe), Some(stderr_pipe)) => {
            let mut descriptors = [
                PollFd::new(stdout_pipe, events),
                PollFd::new(stderr_pipe, events),
            ];
            poll(&mut descriptors, Some(&timespec)).map_err(std::io::Error::from)?;
            Ok((
                poll_ready(descriptors.first()),
                poll_ready(descriptors.get(1)),
            ))
        }
        (Some(stdout_pipe), None) => {
            let mut descriptors = [PollFd::new(stdout_pipe, events)];
            poll(&mut descriptors, Some(&timespec)).map_err(std::io::Error::from)?;
            Ok((poll_ready(descriptors.first()), false))
        }
        (None, Some(stderr_pipe)) => {
            let mut descriptors = [PollFd::new(stderr_pipe, events)];
            poll(&mut descriptors, Some(&timespec)).map_err(std::io::Error::from)?;
            Ok((false, poll_ready(descriptors.first())))
        }
        (None, None) => {
            thread::sleep(timeout);
            Ok((false, false))
        }
    }
}

pub(super) fn pump_fair(
    stdout: &mut CaptureStream<ChildStdout>,
    stderr: &mut CaptureStream<ChildStderr>,
    stdout_ready: bool,
    stderr_ready: bool,
    stdout_first: &mut bool,
) {
    if *stdout_first {
        if stdout_ready {
            stdout.pump(CHUNKS_PER_STREAM_TURN);
        }
        if stderr_ready {
            stderr.pump(CHUNKS_PER_STREAM_TURN);
        }
    } else {
        if stderr_ready {
            stderr.pump(CHUNKS_PER_STREAM_TURN);
        }
        if stdout_ready {
            stdout.pump(CHUNKS_PER_STREAM_TURN);
        }
    }
    *stdout_first = !*stdout_first;
}

fn poll_ready(descriptor: Option<&PollFd<'_>>) -> bool {
    descriptor.is_some_and(|descriptor| !descriptor.revents().is_empty())
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
