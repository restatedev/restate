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
//! Every value belongs to exactly one stage, so a condition on the stage, the
//! effective status or both is a set of effective statuses: `stage = 'running'`
//! is `running` alone, and `status = 'failed'` is only finished entries.
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
//! ## The `unknown` sentinel
//!
//! As [`Stage::Unknown`] and [`Status::Unknown`], `unknown` is no value: it is
//! what [`EffectiveStatus::new`] returns for a stored pair that makes none of
//! the values above, so that showing an entry never fails. That is a stage or
//! status this binary doesn't know (decoded as `Unknown`, e.g. after a
//! rollback), or a combination no code path writes: an inbox entry with an
//! outcome, or a finished entry without one. A row shows it, but nothing builds
//! it from a name or a byte, and it isn't among [`EffectiveStatus::VALUES`], so
//! a query condition on `'unknown'` names no value and selects nothing.

use std::fmt;
use std::str::FromStr;

use super::{ParseError, Stage, Status};

/// What a vqueue entry is doing now. See the [module documentation](self).
///
/// It is built from an entry's stage and stored status ([`Self::new`]), from
/// its name ([`FromStr`]), or from its byte ([`Self::from_byte`]); the last two
/// never build the `unknown` sentinel. Values order like their names, after the
/// sentinel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectiveStatus(Inner);

/// Bytes of the values follow their names' order, `0x10` apart, so a new name
/// fits between its neighbours without renumbering them. Zero is the
/// sentinel, as for [`Stage`] and [`Status`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::FromRepr, strum::VariantArray,
)]
#[repr(u8)]
enum Inner {
    /// The sentinel for a stored pair that makes no value.
    Unknown = 0,
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
    /// Every effective status, in order: every variant but the `unknown`
    /// sentinel, so the names a query can select by.
    pub const VALUES: [Self; 12] = [
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

    /// The effective status of an entry in `stage` with the stored `status`:
    /// the `unknown` sentinel for a pair that makes no value.
    pub const fn new(stage: Stage, status: Status) -> Self {
        Self(match (stage, status) {
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
            ) => Inner::Unknown,
        })
    }

    /// The stage of every entry with this effective status; [`Stage::Unknown`]
    /// for the sentinel.
    pub const fn stage(self) -> Stage {
        match self.0 {
            Inner::BackingOff | Inner::New | Inner::Ready | Inner::Scheduled | Inner::Yielded => {
                Stage::Inbox
            }
            Inner::Running => Stage::Running,
            Inner::Suspended => Stage::Suspended,
            Inner::Paused => Stage::Paused,
            Inner::Cancelled | Inner::Failed | Inner::Killed | Inner::Succeeded => Stage::Finished,
            Inner::Unknown => Stage::Unknown,
        }
    }

    /// Whether the entry is done: it finished with an outcome.
    pub const fn is_completed(self) -> bool {
        matches!(self.stage(), Stage::Finished)
    }

    /// Its name, or none for the `unknown` sentinel, which is no value.
    pub const fn name(self) -> Option<&'static str> {
        match self.0 {
            Inner::Unknown => None,
            _ => Some(self.as_str()),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self.0 {
            Inner::Unknown => "unknown",
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

    /// Its byte, e.g. for a key: the values' bytes sort like their names.
    pub const fn to_byte(self) -> u8 {
        self.0 as u8
    }

    /// The value with the byte `byte`, if any; never the sentinel.
    pub fn from_byte(byte: u8) -> Option<Self> {
        Inner::from_repr(byte)
            .filter(|inner| *inner != Inner::Unknown)
            .map(Self)
    }
}

impl fmt::Display for EffectiveStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for EffectiveStatus {
    type Err = ParseError;

    /// The value named `s`; `"unknown"` names none.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::VALUES
            .into_iter()
            .find(|status| status.as_str() == s)
            .ok_or(ParseError::UnknownEffectiveStatus)
    }
}

#[cfg(test)]
mod tests {
    use strum::VariantArray;

    use super::*;

    const UNKNOWN: EffectiveStatus = EffectiveStatus(Inner::Unknown);

    /// Every value, from the derive: a new one is in it without anyone listing
    /// it.
    fn values() -> Vec<EffectiveStatus> {
        Inner::VARIANTS
            .iter()
            .copied()
            .filter(|inner| *inner != Inner::Unknown)
            .map(EffectiveStatus)
            .collect()
    }

    /// The values' bytes, `Ord` and names sort alike, so a key leading with the
    /// byte is in SQL's order of the strings; names and bytes round-trip;
    /// `VALUES` lists every value, in order. The sentinel is byte zero, first,
    /// and comes back from neither its name nor its byte.
    #[test]
    fn values_sort_like_their_names() {
        let mut by_name = values();
        by_name.sort_by_key(|status| status.as_str());
        let mut by_byte = values();
        by_byte.sort_by_key(|status| status.to_byte());
        let mut by_ord = values();
        by_ord.sort();
        assert_eq!(by_byte, by_name, "bytes must sort like names");
        assert_eq!(by_ord, by_name, "`Ord` must sort like names");
        assert_eq!(EffectiveStatus::VALUES.as_slice(), by_name);

        for status in values() {
            assert_eq!(status.as_str().parse(), Ok(status));
            assert_eq!(status.to_string(), status.as_str());
            assert_ne!(status.to_byte(), 0);
            assert_eq!(EffectiveStatus::from_byte(status.to_byte()), Some(status));
            assert_eq!(status.is_completed(), status.stage() == Stage::Finished);
        }
        assert_eq!((UNKNOWN.to_byte(), UNKNOWN.as_str()), (0, "unknown"));
        assert!(by_name.iter().all(|status| UNKNOWN < *status));
        assert!("unknown".parse::<EffectiveStatus>().is_err());
        assert_eq!(EffectiveStatus::from_byte(0), None);
        assert!("started".parse::<EffectiveStatus>().is_err());
    }

    /// Every value is what `new` makes of some stored stage and status, and
    /// only in its own stage, so lanes and gauges kept per stored pair answer
    /// for it. `new` makes the sentinel exactly where no code path writes the
    /// pair.
    #[test]
    fn values_come_from_stored_stage_and_status() {
        let mut made = Vec::new();
        for &stage in Stage::VARIANTS {
            for &status in Status::VARIANTS {
                let effective = EffectiveStatus::new(stage, status);
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
                assert_eq!(effective != UNKNOWN, written, "{stage:?} {status:?}");
                if written {
                    assert_eq!(effective.stage(), stage, "{stage:?} {status:?}");
                    made.push(effective);
                }
            }
        }
        for status in values() {
            assert!(made.contains(&status), "no stored pair makes {status}");
        }
        assert_eq!(
            EffectiveStatus::new(Stage::Inbox, Status::Started).as_str(),
            "ready"
        );
        assert_eq!(
            EffectiveStatus::new(Stage::Paused, Status::BackingOff).as_str(),
            "paused"
        );
    }
}
