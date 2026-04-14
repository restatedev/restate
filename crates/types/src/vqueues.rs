// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod entry_id;
mod seq;
mod vqueue_id;

pub use entry_id::{EntryId, EntryIdDisplay, EntryKind};
pub use seq::Seq;
pub use vqueue_id::{VQueueId, VQueueIdRef};

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    Ord,
    PartialOrd,
    PartialEq,
    Eq,
    Hash,
    bilrost::Enumeration,
    strum::FromRepr,
)]
#[repr(u8)]
pub enum EffectivePriority {
    /// Exclusively for wake-ups that hold tokens already. All other wake-ups will
    /// continue to run with their original priority.
    ///
    /// This is crucial to ensure that when we release our token back to the pool that it gets
    /// picked up again by the scheduler and we can re-acquire it.
    TokenHeld = 0,
    /// High priority
    Started = 1,
    /// System high priority (new)
    System = 2,
    /// User-defined high-priority
    UserHigh = 3,
    /// User-defined low priority
    #[default]
    UserDefault = 4,
}

impl EffectivePriority {
    pub const NUM_PRIORITIES: usize = 5;

    /// Whether this entry has never been started or not
    pub fn is_new(&self) -> bool {
        *self >= EffectivePriority::System
    }

    pub fn token_held(&self) -> bool {
        *self == EffectivePriority::TokenHeld
    }

    pub fn has_started(&self) -> bool {
        *self <= EffectivePriority::Started
    }
}

/// Priorities for entries in the vqueue when inserting new entries
#[derive(Debug, Default, Clone, Copy, Ord, PartialOrd, PartialEq, Eq)]
#[repr(u8)]
pub enum NewEntryPriority {
    /// System high priority
    System = 2,
    /// Default priority
    UserHigh = 3,
    #[default]
    UserDefault = 4,
}

impl From<NewEntryPriority> for EffectivePriority {
    #[inline(always)]
    fn from(value: NewEntryPriority) -> Self {
        match value {
            NewEntryPriority::System => EffectivePriority::System,
            NewEntryPriority::UserHigh => EffectivePriority::UserHigh,
            NewEntryPriority::UserDefault => EffectivePriority::UserDefault,
        }
    }
}

/// Errors returned when parsing encoded vqueue entry identifiers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("bad length")]
    Length,
    #[error("unknown entry kind: {0}")]
    UnknownEntryKind(u8),
}
