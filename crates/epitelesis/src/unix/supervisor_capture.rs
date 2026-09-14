//! Bounded capture storage and nonblocking pipe pumping for the Unix supervisor.

use std::io::Read;
use std::os::fd::AsFd;

use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};

use crate::error::CaptureFailure;
use crate::output::{CaptureReport, CapturedStream, StreamName};
use crate::policy::{CapturePolicy, OverflowBehavior};

const READ_CHUNK: usize = 8 * 1024;

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
                .map_err(errno_to_io)
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
                Ok(count) => self.retain(&chunk[..count]),
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
                        bytes.extend_from_slice(&chunk[..retained]);
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

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn errno_to_io(error: rustix::io::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(error.raw_os_error())
}
