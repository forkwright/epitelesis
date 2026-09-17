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
    /// zombies to be reaped.
    ///
    /// WHY this walks every `/proc` entry, not only this group's members:
    /// pgid membership is not discoverable any other way without the same
    /// problem this comment describes. That means every read here races
    /// every *other* process's exit on the box, not just our own group's,
    /// and can also hit a process this reader was never going to be able
    /// to attribute either way (unreadable under `hidepid`, or carrying
    /// `comm` bytes this reader cannot decode). None of those outcomes can
    /// mean a *living member of our group*: proof requires a confident
    /// `pgrp` match, so anything this reader cannot confidently attribute
    /// to `pgid` is "not that member" (`stat_outcome`, which cannot itself
    /// fail) rather than failing the whole scan. Process churn elsewhere on
    /// the machine can no longer turn a clean exit into a reported
    /// lifecycle failure. Only a structural failure that prevents scanning
    /// `/proc` at all (its own `read_dir`, or the entry iterator) still
    /// fails here, so a real unresolvable group is still reported.
    pub(super) fn has_living_process_group_member(
        &self,
    ) -> std::result::Result<bool, FailureEvidence> {
        #[cfg(target_os = "linux")]
        {
            let Some(pgid) = u32::try_from(self.pgid.as_raw_pid()).ok() else {
                return Ok(false);
            };
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
                let stat = std::fs::read_to_string(entry.path().join("stat"));
                if stat_outcome(pgid, stat).is_living_member() {
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

/// One `/proc/<pid>/stat` read's contribution to group-liveness proof.
#[cfg(target_os = "linux")]
#[derive(Debug, Eq, PartialEq)]
enum StatOutcome {
    /// Not proven a living member of the owned group: gone, unreadable or
    /// unparseable for a reason a process's own exit, permissions, or
    /// naming can cause, a zombie, or a member of a different group.
    NotLivingMember,
    /// Proven a living (non-zombie) member of the owned group.
    LivingMember,
}

#[cfg(target_os = "linux")]
impl StatOutcome {
    fn is_living_member(&self) -> bool {
        matches!(self, Self::LivingMember)
    }
}

/// Classify one already-read (or failed-to-read) `/proc/<pid>/stat` entry.
///
/// This never fails: proof of group membership needs a confident `pgrp`
/// match, so anything short of that — a read error, an undecodable or
/// short/malformed stat line, a zombie, or a different `pgrp` — carries the
/// same weight: this one entry does not prove `pgid` still has a living
/// member. A race with that *other* process's own exit, a `hidepid` mount
/// restricting what this reader may see, or content this reader was never
/// going to be able to interpret (an arbitrary `comm`) can produce every
/// failure shape below without that entry ever having been a member of our
/// group. A structural failure that prevents scanning `/proc` at all is the
/// caller's concern, not this per-entry classification's.
#[cfg(target_os = "linux")]
fn stat_outcome(pgid: u32, stat: std::io::Result<String>) -> StatOutcome {
    let Ok(stat) = stat else {
        return StatOutcome::NotLivingMember;
    };
    let Some((_, fields)) = stat.rsplit_once(") ") else {
        return StatOutcome::NotLivingMember;
    };
    let fields = fields.split_whitespace().collect::<Vec<_>>();
    let (Some(state), Some(group)) = (
        fields.first().copied(),
        fields.get(2).and_then(|field| field.parse::<u32>().ok()),
    ) else {
        return StatOutcome::NotLivingMember;
    };
    if group == pgid && state != "Z" {
        StatOutcome::LivingMember
    } else {
        StatOutcome::NotLivingMember
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{StatOutcome, stat_outcome};

    const PGID: u32 = 4242;

    fn stat_line(pid: u32, comm: &str, state: &str, pgrp: u32) -> String {
        // Real `/proc/<pid>/stat` layout: `pid (comm) state ppid pgrp ...`.
        format!("{pid} ({comm}) {state} 1 {pgrp} 0 0 0")
    }

    #[test]
    fn living_same_group_member_is_detected() {
        let stat = stat_line(99, "git", "R", PGID);
        assert_eq!(stat_outcome(PGID, Ok(stat)), StatOutcome::LivingMember);
    }

    #[test]
    fn zombie_same_group_member_is_not_living() {
        let stat = stat_line(99, "git", "Z", PGID);
        assert_eq!(stat_outcome(PGID, Ok(stat)), StatOutcome::NotLivingMember);
    }

    #[test]
    fn different_group_member_is_not_living() {
        let stat = stat_line(99, "unrelated", "R", PGID + 1);
        assert_eq!(stat_outcome(PGID, Ok(stat)), StatOutcome::NotLivingMember);
    }

    /// This is the primary regression case: a process elsewhere on the
    /// machine exits between the directory listing and the stat read.
    /// Before the fix, this `Err` aborted the whole scan and was recorded
    /// as a permanent lifecycle failure even when the owned group had
    /// already cleanly settled.
    #[test]
    fn transient_not_found_is_not_a_scan_failure() {
        let source = std::io::Error::from(std::io::ErrorKind::NotFound);
        assert_eq!(
            stat_outcome(PGID, Err(source)),
            StatOutcome::NotLivingMember
        );
    }

    #[test]
    fn transient_esrch_is_not_a_scan_failure() {
        let source = std::io::Error::from(rustix::io::Errno::SRCH);
        assert_eq!(
            stat_outcome(PGID, Err(source)),
            StatOutcome::NotLivingMember
        );
    }

    #[test]
    fn non_utf8_comm_is_not_a_scan_failure() {
        let source = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "stream did not contain valid UTF-8",
        );
        assert_eq!(
            stat_outcome(PGID, Err(source)),
            StatOutcome::NotLivingMember
        );
    }

    /// A `hidepid`-style permission refusal on an unrelated process is not
    /// a scan failure either: it says nothing about our own group, which
    /// runs under the same uid and remains readable.
    #[test]
    fn permission_denied_on_an_unrelated_process_is_not_a_scan_failure() {
        let source = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            stat_outcome(PGID, Err(source)),
            StatOutcome::NotLivingMember
        );
    }

    /// Live-reproduced regression case (kanon#10257,
    /// `fleet_ci::tests::policy_skew_and_candidate_refusal_diagnostics_bind_complete_safe_provenance`
    /// under concurrent full-gate load): a stat line that reads
    /// successfully but does not carry a parseable `pgrp` field. Before the
    /// fix this raised `FailureEvidence { kind: InvalidData, message: "proc
    /// stat omitted process group" }` and failed a leader that had exited 0
    /// and been cleanly reaped.
    #[test]
    fn short_read_missing_pgrp_field_is_not_a_scan_failure() {
        assert_eq!(
            stat_outcome(PGID, Ok("99 (git) R 1".to_owned())),
            StatOutcome::NotLivingMember
        );
    }

    #[test]
    fn malformed_stat_line_is_not_a_scan_failure() {
        assert_eq!(
            stat_outcome(PGID, Ok("not a stat line".to_owned())),
            StatOutcome::NotLivingMember
        );
    }
}
