// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Debug, Formatter};
use std::num::NonZeroU16;

use bytes::{Buf, BufMut};

use restate_clock::UniqueTimestamp;
use restate_clock::time::MillisSinceEpoch;
use restate_types::ServiceName;
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, StateMutationId};
use restate_types::vqueue::VQueueId;

use crate::StorageError;

use super::{RunAt, Seq, Stage};

#[derive(Debug, Clone, Eq, PartialEq, bilrost::Message, bilrost::Oneof)]
pub enum EntryId {
    #[bilrost(empty)]
    Unknown,
    /// Invocation UUID (u128 in big-endian byte order)
    #[bilrost(tag(1), encoding(plainbytes))]
    Invocation(
        // Using u128 would have added an extra unnecessary 8 bytes due to alignment
        // requirements (u128 is 0x10 aligned and it forces the struct to be 0x10 aligned)
        [u8; 16],
    ),
    #[bilrost(tag(2), encoding(plainbytes))]
    StateMutation([u8; 16]),
}

impl EntryId {
    pub const fn kind(&self) -> EntryKind {
        match self {
            EntryId::Unknown => panic!("cannot get kind of unknown entry id"),
            EntryId::Invocation(_) => EntryKind::Invocation,
            EntryId::StateMutation(_) => EntryKind::StateMutation,
        }
    }

    pub fn display(&self, partition_key: PartitionKey) -> EntryIdDisplay<'_> {
        EntryIdDisplay {
            partition_key,
            id: self,
        }
    }
}

pub struct EntryIdDisplay<'a> {
    partition_key: PartitionKey,
    id: &'a EntryId,
}

impl<'a> std::fmt::Display for EntryIdDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.id {
            EntryId::Unknown => f.write_str("Unknown"),
            EntryId::Invocation(remainder) => std::fmt::Display::fmt(
                &InvocationId::from_parts(
                    self.partition_key,
                    InvocationUuid::from_bytes(*remainder),
                ),
                f,
            ),
            EntryId::StateMutation(remainder) => std::fmt::Display::fmt(
                &StateMutationId::from_partition_key_and_bytes(self.partition_key, *remainder),
                f,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::FromRepr)]
#[repr(u8)]
pub enum EntryKind {
    Invocation = b'i',    // 0x69
    StateMutation = b's', // 0x73
}

impl EntryKind {
    pub const fn serialized_length_fixed() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl From<&InvocationId> for EntryId {
    #[inline]
    fn from(id: &InvocationId) -> Self {
        Self::Invocation(id.invocation_uuid().to_bytes())
    }
}

impl From<InvocationId> for EntryId {
    #[inline]
    fn from(id: InvocationId) -> Self {
        Self::Invocation(id.invocation_uuid().to_bytes())
    }
}

impl From<StateMutationId> for EntryId {
    fn from(mutation_id: StateMutationId) -> Self {
        Self::StateMutation(mutation_id.to_remainder_bytes())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, bilrost::Message)]
pub struct EntryKey(#[bilrost(encoding(plainbytes))] [u8; EntryKey::ENCODED_LEN]);

impl EntryKey {
    pub const ENCODED_LEN: usize = 16;
    // First byte high bit stores lock marker in inverted form:
    //   0 => has_lock=true, 1 => has_lock=false.
    // This keeps lexicographic ordering aligned with queue priority.
    const HAS_LOCK_MASK: u8 = 0b1000_0000;
    const RUN_AT_MASK: u64 = (1u64 << 63) - 1;

    ///  Creates a new entry key from the given components.
    ///
    /// The timestamp (run_at) is encoded in 63 bits.
    ///
    /// # Panics if `run_at` is larger than the maximum allowed value. Normal timestamps
    /// are safe to use.
    pub fn new(has_lock: bool, run_at: impl Into<RunAt>, seq: Seq) -> Self {
        let run_at = run_at.into();
        assert!(run_at.as_u64() < (1u64 << 63), "run_at must fit in 63 bits");
        // We want:
        //   has_lock=true  => smaller key
        //   has_lock=false => larger key
        //
        // So encode:
        //   true  -> top bit = 0
        //   false -> top bit = 1
        let lock_bit: u64 = if has_lock { 0 } else { 1 };

        let hi = (lock_bit << 63) | run_at.as_u64();

        let mut out = [0u8; Self::ENCODED_LEN];
        out[..8].copy_from_slice(&hi.to_be_bytes());
        out[8..].copy_from_slice(&seq.to_be_bytes());
        Self(out)
    }

    pub fn split(&self) -> (bool, RunAt, Seq) {
        (self.has_lock(), self.run_at(), self.seq())
    }

    // 16 bytes
    #[inline]
    pub const fn serialized_length_fixed() -> usize {
        Self::ENCODED_LEN
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; Self::serialized_length_fixed()] {
        &self.0
    }

    pub fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_slice(&self.0);
    }

    pub fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        if source.remaining() < Self::serialized_length_fixed() {
            return Err(StorageError::Generic(anyhow::anyhow!(
                "Not enough bytes to decode an EntryKey"
            )));
        }

        let mut buf = [0u8; Self::ENCODED_LEN];
        source.copy_to_slice(&mut buf);
        Ok(Self::from_bytes(buf))
    }

    #[inline]
    pub fn from_bytes(bytes: [u8; Self::serialized_length_fixed()]) -> Self {
        Self(bytes)
    }

    pub fn acquire_lock(&mut self) {
        self.0[0] &= !Self::HAS_LOCK_MASK;
    }

    pub fn release_lock(&mut self) {
        self.0[0] |= Self::HAS_LOCK_MASK;
    }

    pub fn set_run_at(&mut self, run_at: impl Into<RunAt>) {
        let run_at = run_at.into();
        assert!(run_at.as_u64() < (1u64 << 63), "run_at must fit in 63 bits");

        let hi = u64::from_be_bytes(self.0[..8].try_into().unwrap());
        let lock_bit = hi & !Self::RUN_AT_MASK;
        let updated_hi = lock_bit | run_at.as_u64();
        self.0[..8].copy_from_slice(&updated_hi.to_be_bytes());
    }

    /// Returns whether this key represents an entry that currently holds a lock.
    #[inline]
    pub fn has_lock(&self) -> bool {
        (self.0[0] & Self::HAS_LOCK_MASK) == 0
    }

    /// Returns the encoded run-at timestamp.
    #[inline]
    pub fn run_at(&self) -> RunAt {
        // The timestamp is stored in the low 63 bits of the first 8 bytes.
        // This accessor extracts those bits directly.
        let hi = u64::from_be_bytes(self.0[..8].try_into().unwrap());
        RunAt::from_raw(hi & Self::RUN_AT_MASK)
    }

    /// Returns the sequence field from the trailing 8 bytes.
    #[inline]
    pub fn seq(&self) -> Seq {
        u64::from_be_bytes(self.0[8..].try_into().unwrap())
    }
}

impl std::fmt::Debug for EntryKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EntryKey(has_lock:{}, run_at: {}, seq: {})",
            self.has_lock(),
            self.run_at(),
            self.seq(),
        )
    }
}

impl std::fmt::Display for EntryKey {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct EntryStatistics {
    /// Creation timestamp of the entry.
    #[bilrost(tag(1))]
    pub created_at: UniqueTimestamp,
    /// Timestamp of the last stage transition.
    ///
    /// This is always initialized to `created_at` and updated on every stage move.
    #[bilrost(tag(2))]
    pub transitioned_at: UniqueTimestamp,
    /// How many times did we move this entry to the run queue?
    /// '0` means that it's never been started.
    #[bilrost(tag(3))]
    pub num_attempts: u32,
    #[bilrost(tag(4))]
    pub num_parks: u32,
    #[bilrost(tag(5))]
    pub num_yields: u32,
    /// Timestamp of the first attempt to run this entry
    #[bilrost(tag(6))]
    pub first_attempt_at: Option<UniqueTimestamp>,
    /// Timestamp of the last attempt to run this entry
    #[bilrost(tag(7))]
    pub latest_attempt_at: Option<UniqueTimestamp>,
    /// Earliest timestamp at which the first run can realistically start.
    ///
    /// This is computed once at enqueue-time as
    /// `max(created_at, original_run_at)`.
    ///
    /// We clamp to `created_at` when `original_run_at` is in the past to avoid
    /// inflating the first-attempt wait time.
    #[bilrost(tag(8))]
    pub first_runnable_at: MillisSinceEpoch,
    // todo:
    // pub time_spent_running: u32,
    // pub time_spent_parked: u32,
    // pub time_spent_ready_in_inbox: u32,
    // pub time_spent_waiting_for_retry: u32,
    // pub last_updated_at: MillisSinceEpoch,
}

impl EntryStatistics {
    pub fn new(created_at: UniqueTimestamp, original_run_at: RunAt) -> Self {
        let created_at_ms = created_at.to_unix_millis();
        let original_run_at_ms = original_run_at.as_millis();
        let first_runnable_at = if original_run_at_ms < created_at_ms {
            created_at_ms
        } else {
            original_run_at_ms
        };

        Self {
            created_at,
            transitioned_at: created_at,
            num_attempts: 0,
            num_parks: 0,
            num_yields: 0,
            first_attempt_at: None,
            latest_attempt_at: None,
            first_runnable_at,
        }
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct EntryValue {
    #[bilrost(tag(1))]
    pub id: EntryId,
    /// When was the first run attempt of this entry scheduled?
    #[bilrost(tag(2))]
    pub original_run_at: RunAt,
    #[bilrost(tag(3))]
    pub stats: EntryStatistics,
    #[bilrost(tag(4))]
    pub metadata: EntryMetadata,
}

impl EntryValue {
    #[inline]
    pub fn kind(&self) -> EntryKind {
        self.id.kind()
    }

    pub const fn weight(&self) -> NonZeroU16 {
        // The reasoning here is to give queues that need to resume invocations more
        // priority than queues that need to start new ones.
        // Those weights are provisional and can be changed any time, do not make any
        // hard assumptions about them.
        let weight = if self.stats.num_attempts == 0 { 2 } else { 1 };

        // Safety: All values are positive numbers as shown in the match above.
        unsafe { NonZeroU16::new_unchecked(weight) }
    }
}

/// Metadata for an invocation going through vqueues.
#[derive(Debug, Clone, Eq, PartialEq, bilrost::Message)]
pub struct InvocationEntryMetadata {
    #[bilrost(tag(1))]
    // todo: maybe add "deployment_id?" or other metadata needed to identify the deployment
    pub service_name: ServiceName,
}

#[derive(Debug, Clone, Eq, PartialEq, bilrost::Message, bilrost::Oneof, derive_more::From)]
pub enum EntryMetadata {
    #[bilrost(empty)]
    Empty,
    #[bilrost(tag(1))]
    Invocation(InvocationEntryMetadata),
}

pub trait AsEntryStateHeader {
    fn stage(&self) -> Stage;
    fn vqueue_id(&self) -> &VQueueId;
    fn current_entry_key(&self) -> &EntryKey;
}

pub trait AsEntryState: AsEntryStateHeader {
    type State;

    fn state(&self) -> &Self::State;
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_clock::time::MillisSinceEpoch;

    #[test]
    fn roundtrip() {
        let k = EntryKey::new(true, MillisSinceEpoch::new(1_744_000_000_123), 42);

        let enc = *k.as_bytes();
        let dec = EntryKey::from_bytes(enc);
        assert_eq!(k, dec);
    }

    #[test]
    fn entry_key_ordering_is_has_lock_then_run_at_then_seq() {
        let a = EntryKey::new(true, MillisSinceEpoch::new(1_744_000_000_000), 1);
        let b = EntryKey::new(true, MillisSinceEpoch::new(1_744_000_000_000), 2);
        let c = EntryKey::new(true, MillisSinceEpoch::new(1_744_000_001_000), 1);
        let d = EntryKey::new(false, MillisSinceEpoch::new(0), 1);

        assert!(a < b, "seq should break ties");
        assert!(b < c, "run_at should sort before seq");
        assert!(c < d, "has_lock=true should sort before has_lock=false");
    }

    #[test]
    fn entry_key_codec_roundtrip_and_accessors() {
        let key = EntryKey::new(true, MillisSinceEpoch::new(1_744_000_000_123), 42);

        let mut encoded = Vec::new();
        key.encode(&mut encoded);
        let decoded = EntryKey::decode(&mut encoded.as_slice()).unwrap();

        assert_eq!(decoded, key);
        assert!(decoded.has_lock());
        assert_eq!(decoded.run_at(), MillisSinceEpoch::new(1_744_000_000_000));
        assert_eq!(decoded.seq(), 42);

        let unlocked = EntryKey::new(false, MillisSinceEpoch::new(1_744_000_000_999), 7);
        assert!(!unlocked.has_lock());
        assert_eq!(unlocked.run_at(), MillisSinceEpoch::new(1_744_000_000_000));
        assert_eq!(unlocked.seq(), 7);
    }

    #[test]
    fn run_at_large_timestamps_are_clamped_to_supported_range() {
        let key = EntryKey::new(true, MillisSinceEpoch::new(u64::MAX), 1);
        let expected = RunAt::from(MillisSinceEpoch::new(u64::MAX));
        assert_eq!(key.run_at(), expected);
    }

    #[test]
    fn set_run_at_updates_timestamp_and_preserves_other_fields() {
        let mut locked = EntryKey::new(true, MillisSinceEpoch::new(1_744_000_000_000), 7);
        locked.set_run_at(MillisSinceEpoch::new(1_744_000_200_000));
        assert!(locked.has_lock());
        assert_eq!(locked.run_at(), MillisSinceEpoch::new(1_744_000_200_000));
        assert_eq!(locked.seq(), 7);

        let mut unlocked = EntryKey::new(false, MillisSinceEpoch::new(1_744_000_000_000), 9);
        unlocked.set_run_at(MillisSinceEpoch::new(1_744_000_300_000));
        assert!(!unlocked.has_lock());
        assert_eq!(unlocked.run_at(), MillisSinceEpoch::new(1_744_000_300_000));
        assert_eq!(unlocked.seq(), 9);
    }

    #[test]
    fn set_run_at_large_timestamps_are_clamped_to_supported_range() {
        let mut key = EntryKey::new(true, MillisSinceEpoch::new(1_744_000_000_000), 1);
        key.set_run_at(MillisSinceEpoch::new(u64::MAX));
        let expected = RunAt::from(MillisSinceEpoch::new(u64::MAX));
        assert_eq!(key.run_at(), expected);
    }

    #[test]
    fn acquire_and_release_lock_toggle_lock_bit() {
        let mut key = EntryKey::new(false, MillisSinceEpoch::new(1_744_000_000_000), 7);
        assert!(!key.has_lock());

        key.acquire_lock();
        key.acquire_lock();
        assert!(key.has_lock());
        assert_eq!(key.run_at(), MillisSinceEpoch::new(1_744_000_000_000));
        assert_eq!(key.seq(), 7);

        key.release_lock();
        key.release_lock();
        assert!(!key.has_lock());
        assert_eq!(key.run_at(), MillisSinceEpoch::new(1_744_000_000_000));
        assert_eq!(key.seq(), 7);

        key.acquire_lock();
        assert!(key.has_lock());
        key.release_lock();
        assert!(!key.has_lock());
    }
}
