// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::RoughTimestamp;
use restate_types::vqueues::{CanonicalEntryId, EntryId, EntryKind, Seq, VQueueId, VQueueIdRef};

use super::stats::EntryStatistics;
use super::{EntryKey, EntryMetadata, EntryMetadataRef, Stage};

#[derive(
    Debug,
    strum::Display,
    strum::EnumCount,
    strum::FromRepr,
    strum::VariantArray,
    Clone,
    Copy,
    Eq,
    PartialEq,
    bilrost::Enumeration,
)]
#[repr(u8)]
#[strum(serialize_all = "kebab-case")]
pub enum Status {
    #[bilrost(0)]
    Unknown = 0,
    #[bilrost(1)]
    New = 1,
    #[bilrost(2)]
    Scheduled = 2,
    /// Invocation has started running with at least one attempt.
    #[bilrost(3)]
    Started = 3,
    /// Invocation has previously started but has been placed back on the inbox stage
    /// due to an attempt error.
    #[bilrost(4)]
    BackingOff = 4,
    /// Invocation has previously started but has been placed back on the inbox stage.
    /// This does not mean that the invocation attempt has failed, it just means that
    /// it has been evicted from the run queue and will be resumed later.
    #[bilrost(5)]
    Yielded = 5,
    ///
    /// -- Terminal states, invocation cannot transition back to any of the previous
    /// statuses
    ///
    #[bilrost(6)]
    Killed = 6,
    #[bilrost(7)]
    Cancelled = 7,
    #[bilrost(8)]
    Failed = 8,
    #[bilrost(9)]
    Succeeded = 9,
}

/// Borrowing version of [`RawStatusHeader`].
///
/// NOTE: keep in-sync with [`RawStatusHeader`]
#[derive(Clone, bilrost::Message)]
pub struct RawStatusHeaderRef<'a> {
    #[bilrost(tag(1))]
    pub qid: VQueueIdRef<'a>,
    #[bilrost(tag(2))]
    pub stage: Stage,
    #[bilrost(tag(3))]
    pub has_lock: bool,
    #[bilrost(tag(4))]
    pub next_run_at: RoughTimestamp,
    #[bilrost(tag(5))]
    pub seq: Seq,
    #[bilrost(tag(6))]
    pub metadata: EntryMetadataRef<'a>,
    // Not borrowed because it's full of numeric values
    #[bilrost(tag(7))]
    pub stats: EntryStatistics,
    #[bilrost(tag(8))]
    pub status: Status,
    /// See [`TrackedIds`]. Omitted when empty.
    #[bilrost(tag(9), encoding(plainbytes))]
    pub tracked: &'a [u8],
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct RawStatusHeader {
    #[bilrost(tag(1))]
    pub qid: VQueueId,
    #[bilrost(tag(2))]
    pub stage: Stage,
    #[bilrost(tag(3))]
    pub has_lock: bool,
    #[bilrost(tag(4))]
    pub next_run_at: RoughTimestamp,
    #[bilrost(tag(5))]
    pub seq: Seq,
    /// Entry metadata is lightweight metadata and/or resource information that
    /// is copied from entry state to the vqueue's inbox entry on each transition.
    #[bilrost(tag(6))]
    pub metadata: EntryMetadata,
    #[bilrost(tag(7))]
    pub stats: EntryStatistics,
    #[bilrost(tag(8))]
    pub status: Status,
    /// See [`TrackedIds`]. Omitted when empty.
    #[bilrost(tag(9), encoding(plainbytes))]
    pub tracked: Vec<u8>,
}

impl<'a> RawStatusHeaderRef<'a> {
    /// Decodes a stored, length-delimited header, rejecting malformed tracking IDs.
    pub fn decode_stored(value: &mut &'a [u8]) -> crate::Result<Self> {
        let header = <Self as bilrost::BorrowedMessage>::decode_borrowed_length_delimited(value)?;
        TrackedIds::from_bytes(header.tracked).validate()?;
        Ok(header)
    }
}

impl RawStatusHeader {
    /// Decodes a stored, length-delimited header, rejecting malformed tracking IDs.
    pub fn decode_stored(mut value: &[u8]) -> crate::Result<Self> {
        let header = <Self as bilrost::OwnedMessage>::decode_length_delimited(&mut value)?;
        TrackedIds::from_bytes(&header.tracked).validate()?;
        Ok(header)
    }
}

/// Physical IDs of the index and statistic instances that already reflect an
/// entry status while they are still being built, as the partition store's
/// registry assigns them. An instance that is complete (Ready) reflects every
/// entry and is not listed. Only the partition store writes this list.
///
/// Stored as sorted, distinct big-endian `u32`s. For example, an entry written
/// while instances 64 and 70 were being built stores `[0, 0, 0, 64, 0, 0, 0, 70]`;
/// once both are complete, its next write stores nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrackedIds<'a>(&'a [u8]);

impl<'a> TrackedIds<'a> {
    pub const EMPTY: Self = Self(&[]);

    /// Wraps stored bytes; [`Self::ids`] validates them.
    pub const fn from_bytes(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &'a [u8] {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn validate(&self) -> crate::Result<()> {
        self.ids()
            .map(|_| ())
            .ok_or(crate::StorageError::DataIntegrityError)
    }

    /// Whether `id` is listed. Expects validated bytes (see [`Self::ids`]).
    pub fn contains(&self, id: u32) -> bool {
        let (chunks, _) = self.0.as_chunks::<4>();
        chunks
            .binary_search_by_key(&id, |chunk| u32::from_be_bytes(*chunk))
            .is_ok()
    }

    /// The IDs in increasing order, or `None` if the bytes are not a sorted list
    /// of distinct big-endian `u32`s.
    pub fn ids(&self) -> Option<impl Iterator<Item = u32> + 'a> {
        let (chunks, rest) = self.0.as_chunks::<4>();
        let ids = chunks.iter().map(|chunk| u32::from_be_bytes(*chunk));
        (rest.is_empty() && ids.clone().is_sorted_by(|a, b| a < b)).then_some(ids)
    }
}

/// Owned vqueue entry status header.
#[derive(Debug, Clone)]
pub struct OwnedEntryStatusHeader {
    qid: VQueueId,
    stage: Stage,
    entry_key: EntryKey,
    metadata: EntryMetadata,
    status: Status,
    stats: EntryStatistics,
    tracked: Vec<u8>,
}

impl OwnedEntryStatusHeader {
    pub fn new(
        qid: VQueueId,
        stage: Stage,
        entry_key: EntryKey,
        metadata: EntryMetadata,
        stats: EntryStatistics,
        status: Status,
    ) -> Self {
        Self {
            qid,
            stage,
            entry_key,
            metadata,
            status,
            stats,
            tracked: Vec::new(),
        }
    }

    /// The stored [`TrackedIds`] bytes.
    pub fn with_tracked(mut self, tracked: Vec<u8>) -> Self {
        self.tracked = tracked;
        self
    }
}

impl EntryStatusHeader for OwnedEntryStatusHeader {
    #[inline]
    fn canonical_entry_id(&self) -> CanonicalEntryId {
        self.entry_key
            .to_canonical_entry_id(self.qid.partition_key())
    }

    #[inline]
    fn vqueue_id(&self) -> &VQueueId {
        &self.qid
    }

    #[inline]
    fn entry_id(&self) -> &EntryId {
        self.entry_key.entry_id()
    }

    #[inline]
    fn entry_key(&self) -> &EntryKey {
        &self.entry_key
    }

    #[inline]
    fn kind(&self) -> EntryKind {
        self.entry_key.kind()
    }

    #[inline]
    fn stage(&self) -> Stage {
        self.stage
    }

    #[inline]
    fn seq(&self) -> Seq {
        self.entry_key.seq()
    }

    #[inline]
    fn has_lock(&self) -> bool {
        self.entry_key.has_lock()
    }

    #[inline]
    fn next_run_at(&self) -> RoughTimestamp {
        self.entry_key.run_at()
    }

    #[inline]
    fn stats(&self) -> &EntryStatistics {
        &self.stats
    }

    #[inline]
    fn metadata(&self) -> &EntryMetadata {
        &self.metadata
    }

    #[inline]
    fn display_entry_id(&self) -> impl std::fmt::Display + '_ {
        self.entry_id().display(self.qid.partition_key())
    }

    #[inline]
    fn status(&self) -> Status {
        self.status
    }

    #[inline]
    fn tracked(&self) -> TrackedIds<'_> {
        TrackedIds::from_bytes(&self.tracked)
    }
}

mod bilrost_encoding {
    use bilrost::encoding::{DistinguishedProxiable, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind, Enumeration};

    use super::Status;

    impl Proxiable for Status {
        type Proxy = u32;

        fn encode_proxy(&self) -> Self::Proxy {
            <Status as Enumeration>::to_number(self)
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = <Status as Enumeration>::try_from_number(proxy).unwrap_or(Status::Unknown);
            Ok(())
        }
    }

    impl DistinguishedProxiable for Status {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonicity::Canonical)
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Fixed)
        to encode proxied type (Status)
        with encoding (bilrost::encoding::Fixed)
        including distinguished
    );
}

pub trait EntryStatusHeader: std::fmt::Debug {
    fn vqueue_id(&self) -> &VQueueId;
    fn status(&self) -> Status;
    fn entry_id(&self) -> &EntryId;
    fn entry_key(&self) -> &EntryKey;
    fn kind(&self) -> EntryKind;
    fn metadata(&self) -> &EntryMetadata;
    fn stage(&self) -> Stage;
    fn has_lock(&self) -> bool;
    fn next_run_at(&self) -> RoughTimestamp;
    fn seq(&self) -> Seq;
    fn stats(&self) -> &EntryStatistics;
    fn canonical_entry_id(&self) -> CanonicalEntryId;
    fn display_entry_id(&self) -> impl std::fmt::Display + '_;
    /// The tracking IDs stored with this header. A header that was not read from
    /// storage has none.
    fn tracked(&self) -> TrackedIds<'_>;
    /// Returns new if this entry has not started yet.
    fn has_started(&self) -> bool {
        self.stats().num_attempts > 0
    }
    /// Returns true if this entry is in the terminal state and cannot transition
    /// out of it.
    fn is_terminal(&self) -> bool {
        if matches!(self.stage(), Stage::Finished) {
            return true;
        }
        false
    }
}

/// For future support for extra state storage for entries.
pub trait LazyEntryStatus: EntryStatusHeader {
    fn header(&self) -> &impl EntryStatusHeader;
    fn into_header(self) -> impl EntryStatusHeader + Send + Sync + 'static;

    fn decode_state_owned<E>(&self) -> Option<E>
    where
        E: EntryStatusExtra + bilrost::OwnedMessage + Send + Sized + 'static;

    fn decode_state_borrowed<'b, E>(&'b self) -> Option<E>
    where
        E: EntryStatusExtra + bilrost::BorrowedMessage<'b> + Sized + Send;
}

/// A marker trait for types that can be used as entry extra state values.
pub trait EntryStatusExtra {}

#[cfg(test)]
mod tests {
    use bilrost::{Message, OwnedMessage};

    use super::*;

    #[test]
    fn tracked_ids_are_optional_sorted_and_round_trip() {
        let header = |tracked: Vec<u8>| RawStatusHeader {
            qid: VQueueId::custom(7, "q"),
            stage: Stage::Inbox,
            has_lock: false,
            next_run_at: RoughTimestamp::new(10),
            seq: Seq::new(1),
            metadata: EntryMetadata::default(),
            stats: EntryStatistics::new(
                restate_clock::UniqueTimestamp::try_from_unix_millis(
                    restate_clock::time::MillisSinceEpoch::new(1_744_010_000_000),
                )
                .unwrap(),
                RoughTimestamp::new(10),
            ),
            status: Status::New,
            tracked,
        };
        let untracked = header(vec![]).encode_to_vec();
        let tracked = header(vec![0, 0, 0, 64, 0, 1, 0, 70]).encode_to_vec();
        assert_eq!(
            tracked.len(),
            untracked.len() + 10,
            "an_empty_list_is_omitted_and_ids_take_four_bytes_each"
        );
        let decoded = RawStatusHeader::decode(tracked.as_slice()).unwrap();
        assert_eq!(
            TrackedIds::from_bytes(&decoded.tracked)
                .ids()
                .unwrap()
                .collect::<Vec<_>>(),
            [64, 65_606]
        );
        for malformed in [
            &[0, 0, 0, 64, 0][..],
            &[0, 0, 0, 70, 0, 0, 0, 64],
            &[0, 0, 0, 64, 0, 0, 0, 64],
        ] {
            assert!(
                TrackedIds::from_bytes(malformed).ids().is_none(),
                "{malformed:?}"
            );
        }
        assert_eq!(TrackedIds::EMPTY.ids().unwrap().count(), 0);
    }

    #[test]
    fn fixed_encoding_round_trips_status() {
        #[derive(Debug, PartialEq, bilrost::Message)]
        struct EncodedStatus {
            #[bilrost(tag(1), encoding(fixed))]
            value: Status,
        }

        let value = EncodedStatus {
            value: Status::Succeeded,
        };
        let encoded = value.encode_to_bytes();

        assert_eq!(encoded.as_ref(), &[0x06, 9, 0, 0, 0]);
        assert_eq!(EncodedStatus::decode(encoded).unwrap(), value);
    }
}
