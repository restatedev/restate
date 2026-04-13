// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use bytes::{Buf, BufMut};

use restate_clock::RoughTimestamp;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryKey, Stage};
use restate_types::vqueues::{self, EntryId, EntryKind, Seq, VQueueId};

use crate::keys::{KeyDecode, KeyEncode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct HasLock(bool);

impl HasLock {
    pub const fn new(has_lock: bool) -> Self {
        Self(has_lock)
    }

    pub const fn serialized_length_fixed() -> usize {
        1
    }
}

impl Ord for HasLock {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse the ordering so that locks come first
        self.0.cmp(&other.0).reverse()
    }
}

impl PartialOrd for HasLock {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl KeyEncode for VQueueId {
    #[inline]
    fn encode<B: BufMut>(&self, target: &mut B) {
        self.encode_raw_bytes(target);
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        Self::serialized_length_fixed()
    }
}

impl KeyDecode for VQueueId {
    #[inline]
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(VQueueId::from_raw_bytes(source))
    }
}

impl KeyEncode for Stage {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_u8(*self as u8);
    }

    fn serialized_length(&self) -> usize {
        std::mem::size_of::<u8>()
    }
}

impl KeyDecode for Stage {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let i: u8 = source.get_u8();
        Self::from_repr(i)
            .ok_or_else(|| StorageError::Generic(anyhow::anyhow!("Wrong value for Stage: {}", i)))
    }
}

impl KeyEncode for HasLock {
    fn encode<B: BufMut>(&self, target: &mut B) {
        // If we have the lock, the byte value is 0;
        target.put_u8(if self.0 { 0 } else { 1 });
    }

    fn serialized_length(&self) -> usize {
        1
    }
}

impl KeyDecode for HasLock {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let raw = source.get_u8();
        if raw == 0 {
            Ok(Self(true))
        } else {
            Ok(Self(false))
        }
    }
}

impl KeyEncode for EntryId {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_slice(&self.to_bytes());
    }

    fn serialized_length(&self) -> usize {
        EntryId::serialized_length_fixed()
    }
}

impl KeyDecode for EntryId {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let kind_byte = source.get_u8();
        let kind = EntryKind::from_repr(kind_byte);
        let Some(kind) = kind else {
            return Err(StorageError::Generic(
                vqueues::ParseError::UnknownEntryKind(kind_byte).into(),
            ));
        };

        let mut dst = [0u8; EntryId::REMAINDER_LEN];
        source.copy_to_slice(&mut dst);
        Ok(EntryId::new(kind, dst))
    }
}

// RoughTimestamp is encoded as a u64 in big-endian order to enable forward compatibility
// with potential future higher-precision restate-epoch based timestamp.
impl KeyEncode for RoughTimestamp {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_u64(self.as_u32() as u64);
    }

    fn serialized_length(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl KeyDecode for RoughTimestamp {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let raw = source.get_u64();
        Ok(Self::new(
            raw.try_into()
                .context("RoughTimestamp needs to fit into u32")?,
        ))
    }
}

impl KeyEncode for Seq {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_u64(self.as_u64());
    }

    fn serialized_length(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl KeyDecode for Seq {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(Self::new(source.get_u64()))
    }
}

/// We don't encode ever encode EntryKey but we need to be able to decode it using the same
/// byte representation as the inbox key.
impl KeyDecode for EntryKey {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let has_lock = HasLock::decode(source)?;
        let run_at = RoughTimestamp::decode(source)?;
        let seq = Seq::decode(source)?;
        let entry_id = EntryId::decode(source)?;
        Ok(Self::new(has_lock.0, run_at, seq, entry_id))
    }
}
