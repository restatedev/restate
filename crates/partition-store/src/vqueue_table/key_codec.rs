// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

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

impl Deref for HasLock {
    type Target = bool;

    fn deref(&self) -> &Self::Target {
        &self.0
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

// The on-disk value is milliseconds-since-restate-epoch, quantized to whole
// seconds by the current seconds-precision `RoughTimestamp`. Storing in the
// millisecond domain keeps the format forward-compatible with a future
// higher-precision ms timestamp using the same u64 slot — no migration, and
// sort order is preserved across the boundary.
impl KeyEncode for RoughTimestamp {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_u64(self.as_u32() as u64 * 1_000);
    }

    fn serialized_length(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

impl KeyDecode for RoughTimestamp {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let raw_ms = source.get_u64();
        // Floor to seconds. Clamp so that forward-written ms values beyond
        // `RoughTimestamp`'s range saturate at MAX instead of truncating
        // on the `as u32` cast.
        let secs = (raw_ms / 1_000).min(u32::MAX as u64) as u32;
        Ok(Self::new(secs))
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

impl KeyDecode for EntryKey {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let has_lock = HasLock::decode(source)?;
        let run_at = RoughTimestamp::decode(source)?;
        let seq = Seq::decode(source)?;
        let entry_id = EntryId::decode(source)?;
        Ok(Self::new(has_lock.0, run_at, seq, entry_id))
    }
}

impl KeyEncode for EntryKey {
    fn encode<B: BufMut>(&self, target: &mut B) {
        HasLock::new(self.has_lock()).encode(target);
        RoughTimestamp::encode(&self.run_at(), target);
        Seq::encode(&self.seq(), target);
        EntryId::encode(self.entry_id(), target);
    }

    fn serialized_length(&self) -> usize {
        Self::serialized_length_fixed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rough_timestamp_ms_codec() {
        // Round-trip preserves seconds across the representable range, and
        // encoded bytes are always an exact multiple of 1_000 (i.e. the
        // encoder only ever emits whole-second ms values, so the `*1_000` /
        // `/1_000` pair is lossless for every `RoughTimestamp`).
        for &secs in &[0u32, 1, 100, 1_000, u32::MAX / 2, u32::MAX - 1] {
            let mut buf = Vec::new();
            RoughTimestamp::new(secs).encode(&mut buf);

            let raw = u64::from_be_bytes(buf.as_slice().try_into().unwrap());
            assert_eq!(
                raw % 1_000,
                0,
                "encoded value must be a multiple of 1_000 (got {raw} for {secs}s)"
            );
            assert_eq!(
                raw,
                secs as u64 * 1_000,
                "encoded ms must equal seconds * 1_000"
            );

            let mut slice = buf.as_slice();
            let decoded = RoughTimestamp::decode(&mut slice).expect("decode ok");
            assert_eq!(decoded.as_u32(), secs, "round-trip failed for {secs}s");
        }

        // Forward-compat: sub-second ms values floor to the correct second.
        let mut sub_second = Vec::new();
        sub_second.put_u64(5_999);
        let mut slice = sub_second.as_slice();
        assert_eq!(
            RoughTimestamp::decode(&mut slice).unwrap().as_u32(),
            5,
            "ms value should floor to whole seconds on decode"
        );

        // Forward-compat: ms values beyond RoughTimestamp::MAX clamp to MAX.
        let mut over_max = Vec::new();
        over_max.put_u64(u64::MAX);
        let mut slice = over_max.as_slice();
        assert_eq!(
            RoughTimestamp::decode(&mut slice).unwrap(),
            RoughTimestamp::MAX,
        );
    }
}
