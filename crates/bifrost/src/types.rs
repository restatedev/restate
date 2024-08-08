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

// Only implemented for LSNs
pub(crate) trait LsnExt: SequenceNumber {
    /// Converts a loglet offset into the virtual address (LSN).
    ///
    /// # Panics
    ///
    /// On conversion overflow this function will panic.
    #[track_caller]
    fn offset_by<S: SequenceNumber>(self, offset: S) -> Self {
        // We subtract from OLDEST because loglets might start offsets from a non-zero value.
        // 1 is the oldest valid offset within a loglet, 0 is an invalid offset.
        debug_assert!(offset >= S::OLDEST);
        let self_raw: u64 = self.into();
        let offset_raw: u64 = offset.into();

        let offset_from_zero = offset_raw
            .checked_sub(S::OLDEST.into())
            .expect("offset is => OLDEST offset");

        Self::from(
            self_raw
                .checked_add(offset_from_zero)
                .expect("offset to lsn conversion to not overflow"),
        )
    }

    /// Convert an LSN back to a loglet offset given a base_lsn.
    ///
    /// # Panics
    ///
    /// On conversion overflow this function will panic. This also panics if
    /// base_lsn is `Lsn::INVALID`.
    #[track_caller]
    fn into_offset(self, base_lsn: Lsn) -> LogletOffset {
        assert!(base_lsn > Lsn::INVALID);
        let base_lsn_raw: u64 = base_lsn.into();
        let self_raw: u64 = self.into();
        let oldest_offset: u64 = LogletOffset::OLDEST.into();
        assert!(self_raw >= base_lsn_raw);

        LogletOffset(
            (self_raw - base_lsn_raw)
                .checked_add(oldest_offset)
                .expect("offset+base_lsn within LSN bounds"),
        )
    }
}

impl LsnExt for Lsn {}

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
    pub fn new(sealed: bool, offset: Offset) -> Self {
        if sealed {
            TailState::Sealed(offset)
        } else {
            TailState::Open(offset)
        }
    }

    /// Combines two TailStates together
    ///
    /// Only applies updates to the value according to the following rules:
    ///   - Offsets can only move forward.
    ///   - Tail cannot be unsealed once sealed.
    ///
    /// Returns true if the state was updated
    pub fn combine(&mut self, sealed: bool, offset: Offset) -> bool {
        let old_offset = self.offset();
        let is_already_sealed = self.is_sealed();

        let new_offset = std::cmp::max(self.offset(), offset);
        let new_sealed = self.is_sealed() || sealed;
        if new_sealed != is_already_sealed || new_offset > old_offset {
            *self = TailState::new(new_sealed, new_offset);
            true
        } else {
            false
        }
    }

    /// Applies a seal on the tail state without changing the tail offset
    /// Returns true if the state was updated
    pub fn seal(&mut self) -> bool {
        if self.is_sealed() {
            false
        } else {
            *self = TailState::new(true, self.offset());
            true
        }
    }
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

    #[inline(always)]
    pub fn is_sealed(&self) -> bool {
        matches!(self, TailState::Sealed(_))
    }

    #[inline(always)]
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
    fn lsn_to_offset() {
        let lsn = Lsn::OLDEST;
        let offset = lsn.into_offset(Lsn::OLDEST);
        assert_eq!(offset, LogletOffset::OLDEST);

        // INVALID cannot be used as base_lsn
        let lsn = Lsn::INVALID;
        assert!(std::panic::catch_unwind(|| lsn.into_offset(Lsn::from(100))).is_err());

        // base_lsn > lsn
        let lsn = Lsn::OLDEST;
        assert!(std::panic::catch_unwind(|| lsn.into_offset(Lsn::from(100))).is_err());

        // base_lsn -> oldest
        let lsn = Lsn::from(100);
        let offset = lsn.into_offset(Lsn::from(100));
        assert_eq!(offset, LogletOffset::OLDEST);

        let lsn = Lsn::from(101);
        let offset = lsn.into_offset(Lsn::from(100));
        assert_eq!(offset, LogletOffset::OLDEST.next());
    }

    #[test]
    #[should_panic]
    fn invalid_offset_cannot_be_offsetted() {
        // not acceptable. offset must be > oldest
        let offset = LogletOffset::INVALID;
        let base_lsn = Lsn::OLDEST;
        base_lsn.offset_by(offset);
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

        // validate we panic on overflow
        let offset = LogletOffset::MAX;
        assert!(std::panic::catch_unwind(|| base_lsn.offset_by(offset)).is_err());
    }
}
