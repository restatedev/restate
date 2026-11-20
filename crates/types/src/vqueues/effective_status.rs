// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The **effective status** of a vqueue entry: what the entry is doing now, as
//! one value that also tells its stage.
//!
//! An entry stores a [`Stage`] and a [`Status`]. The stored status is what the
//! entry returns to when it leaves the stage: pausing, suspending and running
//! keep it. So on its own it says little: an entry paused while backing off
//! stores `backing-off`, like one that is backing off in the inbox.
//!
//! The effective status reads the stage first, and the stored status only where
//! the stage leaves a choice:
//!
//! | stage     | stored status | effective status |
//! |-----------|---------------|------------------|
//! | inbox     | new           | `new`            |
//! | inbox     | scheduled     | `scheduled`      |
//! | inbox     | backing-off   | `backing-off`    |
//! | inbox     | started       | `ready`          |
//! | inbox     | yielded       | `yielded`        |
//! | running   | any           | `running`        |
//! | suspended | any           | `suspended`      |
//! | paused    | any           | `paused`         |
//! | finished  | succeeded     | `succeeded`      |
//! | finished  | failed        | `failed`         |
//! | finished  | killed        | `killed`         |
//! | finished  | cancelled     | `cancelled`      |
//!
//! Two rules hold for every value, and tests guard them:
//!
//! - **It comes from a stored stage and status.** Each value is what
//!   [`EffectiveStatus::new`] makes of some stored pairs, and of nothing else.
//!   So whatever keeps those pairs, such as an index with a lane per pair or a
//!   gauge with a bucket per pair, answers for effective statuses too: the
//!   entries of `ready` are those of the pair (inbox, started).
//! - **Its byte sorts like its name.** A key that leads with the byte is in the
//!   order SQL sorts the column's strings: `backing-off` < `cancelled` < … <
//!   `yielded`.
//!
//! Every effective status belongs to exactly one stage, so a condition on the
//! stage, the effective status or both is a set of effective statuses:
//! `stage = 'inbox'` is the inbox's five, and `status = 'failed'` is only
//! finished entries.
//!
//! No code path writes the other combinations: an inbox entry with an outcome,
//! a finished entry without one, or a stage or status this binary doesn't know
//! (decoded as `Unknown`, e.g. after a rollback). They have no effective
//! status.

use std::fmt;
use std::str::FromStr;

use super::{ParseError, Stage, Status};

/// What a vqueue entry is doing now. See the [module documentation](self).
///
/// It is built only from an entry's stage and stored status
/// ([`Self::new`]), from its name ([`FromStr`]), or from its byte
/// ([`Self::from_byte`]). Values order like their names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectiveStatus(Inner);

/// Bytes follow the names' order, `0x10` apart, so a new name fits between
/// its neighbours without renumbering them. Zero is no value.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::FromRepr, strum::VariantArray,
)]
#[repr(u8)]
enum Inner {
    /// Inbox: ran, and waits to retry after an attempt failed.
    BackingOff = 0x10,
    /// Finished: cancelled.
    Cancelled = 0x20,
    /// Finished: failed.
    Failed = 0x30,
    /// Finished: killed.
    Killed = 0x40,
    /// Inbox: never ran.
    New = 0x50,
    /// Paused, manually or after errors, until resumed.
    Paused = 0x60,
    /// Inbox: ran, and is ready to run again, e.g. woken up from a suspension.
    Ready = 0x70,
    /// Running.
    Running = 0x80,
    /// Inbox: waits for the time it's scheduled to run at.
    Scheduled = 0x90,
    /// Finished: succeeded.
    Succeeded = 0xA0,
    /// Suspended: waits for a signal to wake up.
    Suspended = 0xB0,
    /// Inbox: ran, and was put back by the scheduler to run again later.
    Yielded = 0xC0,
}

impl EffectiveStatus {
    /// Every effective status, in order.
    pub const ALL: [Self; 12] = [
        Self(Inner::BackingOff),
        Self(Inner::Cancelled),
        Self(Inner::Failed),
        Self(Inner::Killed),
        Self(Inner::New),
        Self(Inner::Paused),
        Self(Inner::Ready),
        Self(Inner::Running),
        Self(Inner::Scheduled),
        Self(Inner::Succeeded),
        Self(Inner::Suspended),
        Self(Inner::Yielded),
    ];

    const INBOX: [Self; 5] = [
        Self(Inner::BackingOff),
        Self(Inner::New),
        Self(Inner::Ready),
        Self(Inner::Scheduled),
        Self(Inner::Yielded),
    ];
    const RUNNING: [Self; 1] = [Self(Inner::Running)];
    const SUSPENDED: [Self; 1] = [Self(Inner::Suspended)];
    const PAUSED: [Self; 1] = [Self(Inner::Paused)];
    const FINISHED: [Self; 4] = [
        Self(Inner::Cancelled),
        Self(Inner::Failed),
        Self(Inner::Killed),
        Self(Inner::Succeeded),
    ];

    /// The effective status of an entry in `stage` with the stored `status`,
    /// or `None` for a combination no code path writes.
    pub const fn new(stage: Stage, status: Status) -> Option<Self> {
        let inner = match (stage, status) {
            (Stage::Inbox, Status::New) => Inner::New,
            (Stage::Inbox, Status::Scheduled) => Inner::Scheduled,
            (Stage::Inbox, Status::BackingOff) => Inner::BackingOff,
            (Stage::Inbox, Status::Started) => Inner::Ready,
            (Stage::Inbox, Status::Yielded) => Inner::Yielded,
            (Stage::Running, _) => Inner::Running,
            (Stage::Suspended, _) => Inner::Suspended,
            (Stage::Paused, _) => Inner::Paused,
            (Stage::Finished, Status::Succeeded) => Inner::Succeeded,
            (Stage::Finished, Status::Failed) => Inner::Failed,
            (Stage::Finished, Status::Killed) => Inner::Killed,
            (Stage::Finished, Status::Cancelled) => Inner::Cancelled,
            (Stage::Unknown, _)
            | (
                Stage::Inbox,
                Status::Unknown
                | Status::Succeeded
                | Status::Failed
                | Status::Killed
                | Status::Cancelled,
            )
            | (
                Stage::Finished,
                Status::Unknown
                | Status::New
                | Status::Scheduled
                | Status::Started
                | Status::BackingOff
                | Status::Yielded,
            ) => return None,
        };
        Some(Self(inner))
    }

    /// The stage of every entry with this effective status.
    pub const fn stage(self) -> Stage {
        match self.0 {
            Inner::BackingOff | Inner::New | Inner::Ready | Inner::Scheduled | Inner::Yielded => {
                Stage::Inbox
            }
            Inner::Running => Stage::Running,
            Inner::Suspended => Stage::Suspended,
            Inner::Paused => Stage::Paused,
            Inner::Cancelled | Inner::Failed | Inner::Killed | Inner::Succeeded => Stage::Finished,
        }
    }

    /// The effective statuses of the entries in `stage`, in order.
    pub fn of_stage(stage: Stage) -> &'static [Self] {
        match stage {
            Stage::Inbox => &Self::INBOX,
            Stage::Running => &Self::RUNNING,
            Stage::Suspended => &Self::SUSPENDED,
            Stage::Paused => &Self::PAUSED,
            Stage::Finished => &Self::FINISHED,
            Stage::Unknown => &[],
        }
    }

    /// Whether the entry is done: its stage is Finished, with an outcome.
    pub const fn is_completed(self) -> bool {
        matches!(self.stage(), Stage::Finished)
    }

    pub const fn as_str(self) -> &'static str {
        match self.0 {
            Inner::BackingOff => "backing-off",
            Inner::Cancelled => "cancelled",
            Inner::Failed => "failed",
            Inner::Killed => "killed",
            Inner::New => "new",
            Inner::Paused => "paused",
            Inner::Ready => "ready",
            Inner::Running => "running",
            Inner::Scheduled => "scheduled",
            Inner::Succeeded => "succeeded",
            Inner::Suspended => "suspended",
            Inner::Yielded => "yielded",
        }
    }

    /// Its byte, e.g. for a key: bytes sort like names. Never zero.
    pub const fn to_byte(self) -> u8 {
        self.0 as u8
    }

    /// The effective status with the byte `byte`, if any.
    pub fn from_byte(byte: u8) -> Option<Self> {
        Inner::from_repr(byte).map(Self)
    }
}

impl fmt::Display for EffectiveStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for EffectiveStatus {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|status| status.as_str() == s)
            .ok_or(ParseError::UnknownEffectiveStatus)
    }
}

#[cfg(test)]
mod tests {
    use strum::VariantArray;

    use super::*;

    /// Every variant, from the derive: a new one is in it without anyone
    /// listing it.
    fn variants() -> Vec<EffectiveStatus> {
        Inner::VARIANTS
            .iter()
            .copied()
            .map(EffectiveStatus)
            .collect()
    }

    /// Bytes, `Ord` and names sort alike, so a key leading with the byte is
    /// in SQL's order of the strings; names and bytes round-trip; `ALL` and
    /// `of_stage` list every variant, in order.
    #[test]
    fn values_sort_like_their_names() {
        let mut by_name = variants();
        by_name.sort_by_key(|status| status.as_str());
        let mut by_byte = variants();
        by_byte.sort_by_key(|status| status.to_byte());
        let mut by_ord = variants();
        by_ord.sort();
        assert_eq!(by_byte, by_name, "bytes must sort like names");
        assert_eq!(by_ord, by_name, "`Ord` must sort like names");
        assert_eq!(EffectiveStatus::ALL.as_slice(), by_name);

        for status in variants() {
            assert_eq!(status.as_str().parse(), Ok(status));
            assert_eq!(status.to_string(), status.as_str());
            assert_ne!(status.to_byte(), 0);
            assert_eq!(EffectiveStatus::from_byte(status.to_byte()), Some(status));
            assert_eq!(status.is_completed(), status.stage() == Stage::Finished);
        }
        for stage in Stage::VARIANTS {
            let in_stage: Vec<_> = by_name
                .iter()
                .copied()
                .filter(|status| status.stage() == *stage)
                .collect();
            assert_eq!(EffectiveStatus::of_stage(*stage), in_stage, "{stage:?}");
        }
        assert!("started".parse::<EffectiveStatus>().is_err());
    }

    /// Every variant is what `new` makes of some stored stage and status, in
    /// that stage, so lanes and gauges kept per stored pair answer for it.
    /// `new` has none exactly where no code path writes the pair.
    #[test]
    fn values_come_from_stored_stage_and_status() {
        let mut made = Vec::new();
        for stage in Stage::VARIANTS {
            for status in Status::VARIANTS {
                let effective = EffectiveStatus::new(*stage, *status);
                let written = match stage {
                    Stage::Unknown => false,
                    Stage::Inbox => matches!(
                        status,
                        Status::New
                            | Status::Scheduled
                            | Status::Started
                            | Status::BackingOff
                            | Status::Yielded
                    ),
                    Stage::Running | Stage::Suspended | Stage::Paused => true,
                    Stage::Finished => matches!(
                        status,
                        Status::Succeeded | Status::Failed | Status::Killed | Status::Cancelled
                    ),
                };
                assert_eq!(effective.is_some(), written, "{stage:?} {status:?}");
                if let Some(effective) = effective {
                    assert_eq!(effective.stage(), *stage, "{stage:?} {status:?}");
                    made.push(effective);
                }
            }
        }
        for status in variants() {
            assert!(made.contains(&status), "no stored pair makes {status}");
        }
        assert_eq!(
            EffectiveStatus::new(Stage::Inbox, Status::Started).map(EffectiveStatus::as_str),
            Some("ready")
        );
        assert_eq!(
            EffectiveStatus::new(Stage::Paused, Status::BackingOff).map(EffectiveStatus::as_str),
            Some("paused")
        );
    }
}
