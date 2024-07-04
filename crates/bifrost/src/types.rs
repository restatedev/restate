// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Remove after fleshing the code out.
#![allow(dead_code)]

use crate::loglet::LogletOffset;
use restate_types::logs::{Lsn, SequenceNumber};
use serde::{Deserialize, Serialize};

// Only implemented for LSNs
pub(crate) trait LsnExt: SequenceNumber {
    /// Converts a loglet offset into the virtual address (LSN).
    #[track_caller]
    fn offset_by<S: SequenceNumber>(self, offset: S) -> Self {
        // This assumes that this will not overflow. That's not guaranteed to always be the
        // case but it should be extremely rare that it'd be okay to just wrap in this case.
        //
        // We subtract from OLDEST because loglets might start offsets from a non-zero value.
        // 1 is the oldest valid offset within a loglet, 0 is an invalid offset.
        debug_assert!(offset != S::INVALID);
        let self_raw: u64 = self.into();
        let offset_raw: u64 = offset.into();

        Self::from(std::cmp::max(
            Self::OLDEST.into(),
            self_raw
                .wrapping_add(offset_raw)
                .saturating_sub(S::OLDEST.into()),
        ))
    }

    /// Convert an LSN back to a loglet offset given a base_lsn.
    #[track_caller]
    fn into_offset(self, base_lsn: Lsn) -> LogletOffset {
        let base_lsn_raw: u64 = base_lsn.into();
        let self_raw: u64 = self.into();
        let oldest_offset: u64 = LogletOffset::OLDEST.into();

        if self_raw < base_lsn_raw {
            return LogletOffset::OLDEST;
        }
        // We must first add the oldest_offset before subtracting base_lsn_raw because self_raw
        // can be 0.
        LogletOffset(
            self_raw
                .saturating_sub(base_lsn_raw)
                .saturating_add(oldest_offset),
        )
    }
}

impl LsnExt for Lsn {}

/// Details about why a log was sealed
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum SealReason {
    /// Log was sealed to perform a repartitioning operation (split or unsplit).
    /// The reader/writer need to figure out where to read/write next.
    Resharding,
    Other(String),
}

#[derive(Debug, Clone, Default)]
pub struct FindTailAttributes {
    // Ensure that we are reading the most recent metadata. This should be used when
    // linearizable metadata reads are required.
    // TODO: consistent_read: bool,
}

/// Represents the state of the tail of the loglet.
#[derive(Clone, Debug)]
pub enum TailState<Offset = Lsn> {
    /// Loglet is open for appends
    Open(Offset),
    /// Loglet is sealed. This offset if the durable tail.
    Sealed(Offset),
}

impl<Offset: SequenceNumber> TailState<Offset> {
    pub fn map<F, T>(self, f: F) -> TailState<T>
    where
        F: FnOnce(Offset) -> T,
    {
        match self {
            TailState::Open(offset) => TailState::Open(f(offset)),
            TailState::Sealed(offset) => TailState::Sealed(f(offset)),
        }
    }

    pub fn is_sealed(&self) -> bool {
        matches!(self, TailState::Sealed(_))
    }

    pub fn offset(&self) -> Offset {
        match self {
            TailState::Open(offset) | TailState::Sealed(offset) => *offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::loglet::LogletOffset;
    use crate::types::LsnExt;
    use restate_types::logs::{Lsn, SequenceNumber};

    #[test]
    fn lsn_to_offset_oldest() {
        let lsn = Lsn::OLDEST;
        let offset = lsn.into_offset(Lsn::OLDEST);
        assert_eq!(offset, LogletOffset::OLDEST);

        // should work if base_lsn is even zero
        let lsn = Lsn::from(1);
        let offset = lsn.into_offset(Lsn::from(0));
        // offset would be 2 -- lsn(0), lsn(1) -->  offset(1), offset(2)
        assert_eq!(offset, LogletOffset::from(2));
    }

    #[test]
    fn lsn_to_offset() {
        // validate we still return oldest if lsn is smaller than base_lsn.
        let lsn = Lsn::INVALID;
        let offset = lsn.into_offset(Lsn::from(100));
        assert_eq!(offset, LogletOffset::OLDEST);

        let lsn = Lsn::OLDEST;
        let offset = lsn.into_offset(Lsn::from(100));
        assert_eq!(offset, LogletOffset::OLDEST);

        let lsn = Lsn::from(99);
        let offset = lsn.into_offset(Lsn::from(100));
        assert_eq!(offset, LogletOffset::OLDEST);

        // base_lsn -> oldest
        let lsn = Lsn::from(100);
        let offset = lsn.into_offset(Lsn::from(100));
        assert_eq!(offset, LogletOffset::OLDEST);

        let lsn = Lsn::from(101);
        let offset = lsn.into_offset(Lsn::from(100));
        assert_eq!(offset, LogletOffset::OLDEST.next());

        // validate we are saturating correctly
        let lsn = Lsn::MAX;
        let offset = lsn.into_offset(Lsn::OLDEST);
        assert_eq!(offset, LogletOffset::MAX);
    }

    #[test]
    #[should_panic]
    fn invalid_offset_cannot_be_offsetted() {
        // not acceptable.
        let offset = LogletOffset::INVALID;
        let base_lsn = Lsn::OLDEST;
        assert_eq!(Lsn::OLDEST, base_lsn.offset_by(offset));
    }

    #[test]
    fn offset_to_lsn() {
        let offset = LogletOffset::OLDEST;
        let base_lsn = Lsn::OLDEST;
        assert_eq!(Lsn::OLDEST, base_lsn.offset_by(offset));

        let offset = LogletOffset::from(10);
        let base_lsn = Lsn::OLDEST;
        assert_eq!(Lsn::from(10), base_lsn.offset_by(offset));

        let offset = LogletOffset::from(10);
        let base_lsn = Lsn::from(100);
        assert_eq!(Lsn::from(109), base_lsn.offset_by(offset));
    }
}
