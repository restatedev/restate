// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU16;

use restate_clock::time::MillisSinceEpoch;
use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::{EntryId, EntryKind, Seq, VQueueId};

use super::{EntryState, Stage};

/// EntryKey uniquely identifies a vqueue entry within a vqueue stage.
///
/// The entry encodes the following information:
/// - HasLock: whether the entry holds a lock or not (serialized as inverted)
/// - RunAt: A rough timestamp for when the entry should be allowed to run
/// - Seq: A secondary ordering key that's inherited from the original entry's canonical
///   entry identity. The sequence number should be a monotonically increasing value unique
///   within a vqueue. Uniqueness is not critical between entries arriving from different
///   services but more critical for strands of entries where their relative ordering
///   needs to be preserved (e.g. sequential invocations on the same virtual object key
///   requested by the same caller/thread).
/// - EntryId: The entry identifier bytes (kind + 16-byte identifier), stored after the
///   ordering fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bilrost::Message)]
pub struct EntryKey {
    #[bilrost(tag(1))]
    has_lock: bool,
    #[bilrost(tag(2))]
    run_at: RoughTimestamp,
    #[bilrost(tag(3))]
    seq: Seq,
    #[bilrost(tag(4))]
    entry_id: EntryId,
}

// Custom implementation to match the ordering in storage. Entries with has_lock come
// before those without.
impl Ord for EntryKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.has_lock
            .cmp(&other.has_lock)
            // Reverse the ordering so that locks come first
            .reverse()
            .then_with(|| self.run_at.cmp(&other.run_at))
            .then_with(|| self.seq.cmp(&other.seq))
            .then_with(|| self.entry_id.cmp(&other.entry_id))
    }
}

impl PartialOrd for EntryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl EntryKey {
    ///  Creates a new entry key from the given components.
    ///
    /// The timestamp (run_at) is encoded in 63 bits.
    ///
    /// The sequence number is encoded in 7 bytes (56 bits).
    ///
    /// # Panics if `run_at` is larger than the maximum allowed value. Normal timestamps
    /// are safe to use.
    pub fn new(
        has_lock: bool,
        run_at: impl Into<RoughTimestamp>,
        seq: impl Into<Seq>,
        entry_id: impl Into<EntryId>,
    ) -> Self {
        let entry_id = entry_id.into();
        assert_ne!(
            entry_id.kind(),
            EntryKind::Unknown,
            "entry id kind must be known"
        );
        Self {
            has_lock,
            run_at: run_at.into(),
            seq: seq.into(),
            entry_id,
        }
    }

    #[inline]
    pub const fn kind(&self) -> EntryKind {
        self.entry_id.kind()
    }

    #[inline]
    pub const fn seq(&self) -> Seq {
        self.seq
    }

    /// Returns the encoded run-at timestamp.
    #[inline]
    pub const fn run_at(&self) -> RoughTimestamp {
        self.run_at
    }

    #[inline]
    pub const fn entry_id(&self) -> &EntryId {
        &self.entry_id
    }

    /// Returns whether this key represents an entry that currently holds a lock.
    #[inline]
    pub fn has_lock(&self) -> bool {
        self.has_lock
    }

    #[inline]
    #[must_use]
    pub fn acquire_lock(self) -> Self {
        Self {
            has_lock: true,
            ..self
        }
    }

    #[inline]
    #[must_use]
    pub fn release_lock(self) -> Self {
        Self {
            has_lock: false,
            ..self
        }
    }

    /// If the input run_at is None, the current run_at remains unchanged
    #[inline]
    #[must_use]
    pub fn set_run_at(self, run_at: Option<RoughTimestamp>) -> Self {
        Self {
            run_at: run_at.unwrap_or_else(|| self.run_at),
            ..self
        }
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
    pub num_paused: u32,
    #[bilrost(tag(5))]
    pub num_suspensions: u32,
    #[bilrost(tag(6))]
    pub num_yields: u32,
    /// Timestamp of the first attempt to run this entry
    #[bilrost(tag(7))]
    pub first_attempt_at: Option<UniqueTimestamp>,
    /// Timestamp of the last attempt to run this entry
    #[bilrost(tag(8))]
    pub latest_attempt_at: Option<UniqueTimestamp>,
    /// Earliest timestamp at which the first run can realistically start.
    ///
    /// This is computed once at enqueue-time as
    /// `max(created_at, original_run_at)`.
    ///
    /// We clamp to `created_at` when `original_run_at` is in the past to avoid
    /// inflating the first-attempt wait time.
    #[bilrost(tag(9))]
    pub first_runnable_at: MillisSinceEpoch,
    // todo:
    // pub time_spent_running: u32,
    // pub time_spent_parked: u32,
    // pub time_spent_ready_in_inbox: u32,
    // pub time_spent_waiting_for_retry: u32,
    // pub last_updated_at: MillisSinceEpoch,
}

impl EntryStatistics {
    pub fn new(created_at: UniqueTimestamp, original_run_at: RoughTimestamp) -> Self {
        let first_runnable_at = created_at
            .to_unix_millis()
            .max(original_run_at.as_unix_millis());

        Self {
            created_at,
            transitioned_at: created_at,
            num_attempts: 0,
            num_paused: 0,
            num_suspensions: 0,
            num_yields: 0,
            first_attempt_at: None,
            latest_attempt_at: None,
            first_runnable_at,
        }
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct EntryValue {
    /// If unset, then the item has never been run.
    /// When set, it's the timestamp of the first run attempt.
    #[bilrost(tag(1))]
    pub first_started_at: Option<UniqueTimestamp>,
    /// Earliest timestamp at which the first run can realistically start.
    ///
    /// This is computed once at enqueue-time as
    /// `max(created_at, original_run_at)`.
    ///
    /// We clamp to `created_at` when `original_run_at` is in the past to avoid
    /// inflating the first-attempt wait time.
    #[bilrost(tag(2))]
    pub first_runnable_at: RoughTimestamp,
    #[bilrost(tag(3))]
    pub metadata: EntryMetadata,
}

impl EntryValue {
    pub const fn weight(&self) -> NonZeroU16 {
        // The reasoning here is to give queues that need to resume invocations more
        // priority than queues that need to start new ones.
        // Those weights are provisional and can be changed any time, do not make any
        // hard assumptions about them.
        let weight = if self.first_started_at.is_none() {
            2
        } else {
            1
        };

        // Safety: All values are positive numbers as shown in the match above.
        unsafe { NonZeroU16::new_unchecked(weight) }
    }
}

#[derive(Debug, Clone, Eq, Default, PartialEq, bilrost::Message)]
pub struct EntryMetadataRef<'a> {
    // todo: maybe add "deployment_id?" or other metadata needed to identify the deployment
    // or maybe service revision.
    #[bilrost(tag(1))]
    deployment: Option<&'a str>,
}

impl<'a> From<&'a EntryMetadata> for EntryMetadataRef<'a> {
    #[inline]
    fn from(value: &'a EntryMetadata) -> Self {
        Self {
            deployment: value.deployment.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Eq, Default, PartialEq, bilrost::Message)]
pub struct EntryMetadata {
    // todo: maybe add "deployment_id?" or other metadata needed to identify the deployment
    // or maybe service revision.
    #[bilrost(tag(1))]
    deployment: Option<String>,
}

pub trait EntryStateHeader {
    fn vqueue_id(&self) -> &VQueueId;
    fn entry_id(&self) -> &EntryId;
    fn entry_key(&self) -> &EntryKey;
    fn kind(&self) -> EntryKind;
    fn metadata(&self) -> &EntryMetadata;
    fn stage(&self) -> Stage;
    fn has_lock(&self) -> bool;
    fn next_run_at(&self) -> RoughTimestamp;
    fn seq(&self) -> Seq;
    fn stats(&self) -> &EntryStatistics;
    fn display_entry_id(&self) -> impl std::fmt::Display + '_;
}

pub trait LazyEntryState: EntryStateHeader {
    fn header(&self) -> &impl EntryStateHeader;
    fn into_header(self) -> impl EntryStateHeader + Send + Sync + 'static;

    fn decode_state_owned<E>(&self) -> Option<E>
    where
        E: EntryState + bilrost::OwnedMessage + Send + Sized + 'static;

    fn decode_state_borrowed<'b, E>(&'b self) -> Option<E>
    where
        E: EntryState + bilrost::BorrowedMessage<'b> + Sized + Send;
}

pub trait IdentifiesEntry {
    type State: EntryState;
    const KIND: EntryKind;

    fn partition_key(&self) -> PartitionKey;
    fn to_entry_id(&self) -> EntryId;
}

impl<T: IdentifiesEntry> IdentifiesEntry for &T {
    type State = T::State;
    const KIND: EntryKind = T::KIND;

    fn partition_key(&self) -> PartitionKey {
        T::partition_key(self)
    }

    fn to_entry_id(&self) -> EntryId {
        T::to_entry_id(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_clock::time::MillisSinceEpoch;

    #[test]
    fn entry_key_ordering_is_has_lock_then_run_at_then_seq() {
        use std::cmp::Ordering;

        let a = EntryKey::new(
            true,
            MillisSinceEpoch::new(1_744_000_000_000),
            1,
            EntryId::new(EntryKind::StateMutation, [99; 16]),
        );
        let b = EntryKey::new(
            true,
            MillisSinceEpoch::new(1_744_000_000_000),
            2,
            EntryId::new(EntryKind::Invocation, [1; 16]),
        );
        let c = EntryKey::new(
            true,
            MillisSinceEpoch::new(1_744_000_001_000),
            1,
            EntryId::new(EntryKind::Invocation, [1; 16]),
        );
        let d = EntryKey::new(
            false,
            MillisSinceEpoch::new(0),
            1,
            EntryId::new(EntryKind::Invocation, [1; 16]),
        );

        assert_eq!(a.cmp(&b), Ordering::Less, "seq should break ties");
        assert_eq!(b.cmp(&c), Ordering::Less, "run_at should sort before seq");
        assert_eq!(
            c.cmp(&d),
            Ordering::Less,
            "has_lock=true should sort before has_lock=false"
        );
    }
}
