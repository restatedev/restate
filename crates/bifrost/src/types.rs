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

pub(crate) trait LsnExt: SequenceNumber {
    /// Converts a loglet offset into the virtual address (LSN).
    fn offset_by<S: SequenceNumber>(self, offset: S) -> Self {
        // This assumes that this will not overflow. That's not guaranteed to always be the
        // case but it should be extremely rare that it'd be okay to just wrap in this case.
        //
        // We subtract from OLDEST because loglets start their offset from 1, not 0.
        // 1 is the oldest valid offset within a loglet, 0 is an invalid offset.
        debug_assert!(offset != S::INVALID);
        let self_raw: u64 = self.into();
        let offset_raw: u64 = offset.into();

        Self::from(self_raw.wrapping_add(offset_raw) - S::OLDEST.into())
    }

    /// Convert an LSN back to a loglet offset given a base_lsn.
    fn into_offset(self, base_lsn: Lsn) -> LogletOffset {
        let base_lsn_raw: u64 = base_lsn.into();
        let self_raw: u64 = self.into();
        let oldest_offset: u64 = LogletOffset::OLDEST.into();
        // We must first add the oldest_offset before subtracting base_lsn_raw because self_raw
        // can be 0.
        LogletOffset((self_raw + oldest_offset).saturating_sub(base_lsn_raw))
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

#[cfg(test)]
mod tests {
    use crate::loglet::LogletOffset;
    use crate::types::LsnExt;
    use restate_types::logs::{Lsn, SequenceNumber};

    #[test]
    fn convert_invalid_lsn_into_invalid_offset() {
        let lsn = Lsn::INVALID;

        let offset = lsn.into_offset(Lsn::OLDEST);

        assert_eq!(offset, LogletOffset::INVALID);
    }
}
