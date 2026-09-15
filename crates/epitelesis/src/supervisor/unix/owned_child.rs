//! Reap and process-group containment ownership for the Unix supervisor.

use std::process::{Child, ExitStatus};
use std::sync::mpsc;
use std::thread;

use rustix::process::{Pid, Signal, WaitId, WaitIdOptions, kill_process_group, waitid};
use snafu::IntoError as _;

use crate::error::{FailureEvidence, ReaperStartFailedSnafu, Result};

pub(super) fn start_background_reaper(program: &str) -> Result<mpsc::SyncSender<Child>> {
    let (sender, receiver) = mpsc::sync_channel::<Child>(1);
    let _reaper = thread::Builder::new()
        .name("epitelesis-background-reaper".to_owned())
        .spawn(move || match receiver.recv() {
            Ok(mut child) => {
                if let Err(error) = child.wait() {
                    tracing::error!(
                        %error,
                        "background reaper could not reap the transferred process leader"
                    );
                }
            }
            Err(error) => {
                tracing::debug!(
                    %error,
                    "background reaper stopped before receiving process ownership"
                );
            }
        })
        .map_err(|source| {
            ReaperStartFailedSnafu {
                program: program.to_owned(),
            }
            .into_error(source)
        })?;
    Ok(sender)
}

pub(super) struct OwnedChild {
    child: Option<Child>,
    pgid: Pid,
    reaper: Option<mpsc::SyncSender<Child>>,
}

impl OwnedChild {
    pub(super) fn new(child: Child, reaper: mpsc::SyncSender<Child>) -> Self {
        let pgid = Pid::from_child(&child);
        Self {
            child: Some(child),
            pgid,
            reaper: Some(reaper),
        }
    }

    pub(super) fn id(&self) -> u32 {
        self.child.as_ref().map_or(0, Child::id)
    }

    pub(super) fn child_mut(&mut self) -> Option<&mut Child> {
        self.child.as_mut()
    }

    pub(super) fn has_child(&self) -> bool {
        self.child.is_some()
    }

    pub(super) fn signal_group(&self, signal: Signal) -> std::io::Result<()> {
        if !self.has_child() {
            return Err(std::io::Error::from(rustix::io::Errno::SRCH));
        }
        kill_process_group(self.pgid, signal).map_err(std::io::Error::from)
    }

    pub(super) fn leader_waitable(&self) -> std::io::Result<bool> {
        if self.child.is_none() {
            return Ok(true);
        }
        let options = WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT;
        waitid(WaitId::Pid(self.pgid), options)
            .map(|status| status.is_some())
            .map_err(std::io::Error::from)
    }

    pub(super) fn reap_waitable(&mut self) -> std::io::Result<ExitStatus> {
        match self.child.as_mut() {
            Some(child) => {
                let status = child.wait()?;
                self.child = None;
                Ok(status)
            }
            None => Err(std::io::Error::other("process leader was already reaped")),
        }
    }

    pub(super) fn handoff_to_background_reaper(
        &mut self,
    ) -> std::result::Result<(), FailureEvidence> {
        let Some(child) = self.child.take() else {
            return Ok(());
        };
        let Some(sender) = self.reaper.take() else {
            self.child = Some(child);
            return Err(FailureEvidence::from_io(
                "transfer child to background reaper",
                &std::io::Error::other("fallback reaper was unavailable"),
            ));
        };
        if let Err(error) = sender.send(child) {
            self.child = Some(error.0);
            return Err(FailureEvidence::from_io(
                "transfer child to background reaper",
                &std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "background reaper stopped before ownership transfer",
                ),
            ));
        }
        Ok(())
    }

    /// Prove that no living process can still use resources scoped to
    /// this child-led process group. This is intentionally stricter than
    /// checking whether the leader was reaped: background descendants can
    /// survive their shell leader. Linux `/proc` is the supported source
    /// for that proof. Other supported Unix systems use the kernel's
    /// non-signalling group-existence probe and conservatively wait for
    /// zombies to be reaped. Only ESRCH proves that the group is gone.
    pub(super) fn has_living_process_group_member(
        &self,
    ) -> std::result::Result<bool, FailureEvidence> {
        #[cfg(target_os = "linux")]
        {
            let entries = std::fs::read_dir("/proc").map_err(|source| {
                FailureEvidence::from_io("inspect process group liveness", &source)
            })?;
            for entry in entries {
                let entry = entry.map_err(|source| {
                    FailureEvidence::from_io("inspect process group liveness", &source)
                })?;
                if entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.parse::<u32>().ok())
                    .is_none()
                {
                    continue;
                }
                let path = entry.path().join("stat");
                let stat = match std::fs::read_to_string(path) {
                    Ok(stat) => stat,
                    Err(source) if proc_stat_read_proves_departure(&source) => continue,
                    Err(source) => {
                        return Err(FailureEvidence::from_io(
                            "inspect process group liveness",
                            &source,
                        ));
                    }
                };
                let Some((_, fields)) = stat.rsplit_once(") ") else {
                    return Err(FailureEvidence::from_io(
                        "inspect process group liveness",
                        &std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "malformed proc stat",
                        ),
                    ));
                };
                let fields = fields.split_whitespace().collect::<Vec<_>>();
                let state = fields.first().copied().ok_or_else(|| {
                    FailureEvidence::from_io(
                        "inspect process group liveness",
                        &std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "proc stat omitted state",
                        ),
                    )
                })?;
                let group = fields
                    .get(2)
                    .and_then(|field| field.parse::<u32>().ok())
                    .ok_or_else(|| {
                        FailureEvidence::from_io(
                            "inspect process group liveness",
                            &std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "proc stat omitted process group",
                            ),
                        )
                    })?;
                let same_group = u32::try_from(self.pgid.as_raw_pid())
                    .ok()
                    .is_some_and(|pgid| group == pgid);
                if same_group && state != "Z" {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        #[cfg(not(target_os = "linux"))]
        {
            match rustix::process::test_kill_process_group(self.pgid) {
                Ok(()) => Ok(true),
                Err(rustix::io::Errno::SRCH) => Ok(false),
                Err(error) => Err(FailureEvidence::from_io(
                    "inspect process group liveness",
                    &std::io::Error::from(error),
                )),
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn proc_stat_read_proves_departure(source: &std::io::Error) -> bool {
    source.kind() == std::io::ErrorKind::NotFound
        || source.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::process::Stdio;
    use std::time::Duration;

    use super::{OwnedChild, proc_stat_read_proves_departure, start_background_reaper};

    #[test]
    fn proc_stat_read_after_reap_proves_departure() -> std::io::Result<()> {
        let mut prepared = crate::command::prepare(
            crate::Command::new("/bin/sh")
                .args(["-c", "read -r _"])
                .stdin(Stdio::piped())
                .deadline(Duration::from_secs(1))
                .map_err(std::io::Error::other)?,
        )
        .map_err(std::io::Error::other)?;
        let reaper = start_background_reaper(&prepared.program).map_err(std::io::Error::other)?;
        let mut child = OwnedChild::new(prepared.command.spawn()?, reaper);
        let mut stat = std::fs::File::open(format!("/proc/{}/stat", child.id()))?;
        child
            .child_mut()
            .ok_or_else(|| std::io::Error::other("fixture child must remain owned"))?
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("fixture shell stdin must be piped"))?
            .write_all(b"\n")?;
        child.reap_waitable()?;

        let mut contents = String::new();
        let source = stat.read_to_string(&mut contents).err().ok_or_else(|| {
            std::io::Error::other("held stat descriptor unexpectedly read a departed process")
        })?;
        assert_eq!(
            source.raw_os_error(),
            Some(rustix::io::Errno::SRCH.raw_os_error()),
            "a held proc stat descriptor must report ESRCH after the child is reaped"
        );
        assert!(
            proc_stat_read_proves_departure(&source),
            "ESRCH proves departure"
        );
        Ok(())
    }

    #[test]
    fn proc_stat_missing_path_proves_departure_but_other_errors_do_not() -> std::io::Result<()> {
        let source = std::fs::File::open("/proc/epitelesis-missing-process/stat")
            .err()
            .ok_or_else(|| {
                std::io::Error::other("missing fixture proc path unexpectedly opened")
            })?;
        assert_eq!(
            source.kind(),
            std::io::ErrorKind::NotFound,
            "fixture process must be absent"
        );
        assert!(
            proc_stat_read_proves_departure(&source),
            "a missing proc entry proves departure"
        );

        for errno in [rustix::io::Errno::PERM, rustix::io::Errno::IO] {
            let source = std::io::Error::from_raw_os_error(errno.raw_os_error());
            assert!(
                !proc_stat_read_proves_departure(&source),
                "{errno} must remain an inspection failure"
            );
        }
        Ok(())
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.child.is_none() {
            return;
        }
        if let Err(error) = kill_process_group(self.pgid, Signal::KILL) {
            tracing::error!(
                %error,
                "owned child dropped before process-group termination completed"
            );
        }
        if let Err(failure) = self.handoff_to_background_reaper() {
            tracing::error!(
                ?failure,
                "owned child dropped before background reaper handoff completed"
            );
        }
    }
}
