// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Cursor;

use restate_clock::RoughTimestamp;
use restate_storage_api::vqueue_table::{
    EntryKey, EntryMetadata, EntryMetadataRef, EntryStateHeader, EntryStatistics, LazyEntryState,
    Stage,
};
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey};
use restate_types::vqueues::{EntryId, EntryKind, Seq, VQueueId, VQueueIdRef};
use rocksdb::DBPinnableSlice;

use crate::TableKind;
use crate::keys::{KeyKind, define_table_key};

// `qe` | PKEY | ENTRY_ID
define_table_key!(
    TableKind::VQueue,
    KeyKind::VQueueEntryState,
    EntryStateKey(
        partition_key: PartitionKey,
        id: EntryId,
    )
);

static_assertions::const_assert_eq!(EntryKind::serialized_length_fixed(), 1);

impl EntryStateKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            + EntryId::serialized_length_fixed()
    }
}

impl From<&InvocationId> for EntryStateKey {
    #[inline]
    fn from(id: &InvocationId) -> Self {
        EntryStateKey {
            partition_key: WithPartitionKey::partition_key(id),
            id: EntryId::from(id),
        }
    }
}

/// Borrowing version of [`StateHeaderRaw`].
///
/// NOTE: keep in-sync with [`StateHeaderRaw`]
#[derive(Clone, bilrost::Message)]
pub struct StateHeaderRawRef<'a> {
    #[bilrost(tag(1))]
    pub(super) qid: VQueueIdRef<'a>,
    /// Unknown is an invalid state, this will be set to None when the invocation
    /// leaves the queue.
    #[bilrost(tag(2))]
    pub(super) stage: Stage,
    #[bilrost(tag(3))]
    pub(super) has_lock: bool,
    #[bilrost(tag(4))]
    pub(super) next_run_at: RoughTimestamp,
    #[bilrost(tag(5))]
    pub(super) seq: Seq,
    #[bilrost(tag(6))]
    pub(super) metadata: EntryMetadataRef<'a>,
    // Not borrowed because it's full of numeric values
    #[bilrost(tag(7))]
    pub(super) stats: EntryStatistics,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct StateHeaderRaw {
    #[bilrost(tag(1))]
    qid: VQueueId,
    #[bilrost(tag(2))]
    stage: Stage,
    #[bilrost(tag(3))]
    has_lock: bool,
    #[bilrost(tag(4))]
    next_run_at: RoughTimestamp,
    #[bilrost(tag(5))]
    seq: Seq,
    /// Entry metadata is lightweight metadata and/or resource information that
    /// is copied from entry state to the vqueue's inbox entry on each transition.
    #[bilrost(tag(6))]
    metadata: EntryMetadata,
    #[bilrost(tag(7))]
    stats: EntryStatistics,
}

pub struct OwnedEntryStateHeader {
    qid: VQueueId,
    stage: Stage,
    entry_key: EntryKey,
    metadata: EntryMetadata,
    stats: EntryStatistics,
}

impl OwnedEntryStateHeader {
    pub(crate) fn new(entry_id: EntryId, header: StateHeaderRaw) -> Self {
        Self {
            qid: header.qid,
            stage: header.stage,
            entry_key: EntryKey::new(header.has_lock, header.next_run_at, header.seq, entry_id),
            metadata: header.metadata,
            stats: header.stats,
        }
    }
}

impl EntryStateHeader for OwnedEntryStateHeader {
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
}

pub struct LazyEntryStateHolder<'a> {
    header: OwnedEntryStateHeader,
    state_bytes: Cursor<DBPinnableSlice<'a>>,
}

impl<'a> LazyEntryStateHolder<'a> {
    pub(crate) fn new(
        entry_id: EntryId,
        header: StateHeaderRaw,
        state_bytes: Cursor<DBPinnableSlice<'a>>,
    ) -> Self {
        Self {
            header: OwnedEntryStateHeader::new(entry_id, header),
            state_bytes,
        }
    }
}

impl<'a> EntryStateHeader for LazyEntryStateHolder<'a> {
    #[inline]
    fn vqueue_id(&self) -> &VQueueId {
        self.header.vqueue_id()
    }

    #[inline]
    fn entry_id(&self) -> &EntryId {
        self.header.entry_id()
    }

    #[inline]
    fn entry_key(&self) -> &EntryKey {
        self.header.entry_key()
    }

    #[inline]
    fn kind(&self) -> EntryKind {
        self.header.kind()
    }

    #[inline]
    fn stage(&self) -> Stage {
        self.header.stage()
    }

    #[inline]
    fn seq(&self) -> Seq {
        self.header.seq()
    }

    #[inline]
    fn has_lock(&self) -> bool {
        self.header.has_lock()
    }

    #[inline]
    fn next_run_at(&self) -> RoughTimestamp {
        self.header.next_run_at()
    }

    #[inline]
    fn stats(&self) -> &EntryStatistics {
        &self.header.stats()
    }

    #[inline]
    fn metadata(&self) -> &EntryMetadata {
        self.header.metadata()
    }

    #[inline]
    fn display_entry_id(&self) -> impl std::fmt::Display + '_ {
        self.entry_id().display(self.header.qid.partition_key())
    }
}

impl<'a> LazyEntryState for LazyEntryStateHolder<'a> {
    fn header(&self) -> &impl EntryStateHeader {
        &self.header
    }

    fn into_header(self) -> impl EntryStateHeader + Send + Sync + 'static {
        self.header
    }

    fn decode_state_owned<E>(&self) -> Option<E>
    where
        E: bilrost::OwnedMessage + Send + Sized + 'static,
    {
        let buf = self.state_bytes.get_ref();
        E::decode_length_delimited(&mut buf.as_ref()).ok()
    }

    fn decode_state_borrowed<'b, E>(&'b self) -> Option<E>
    where
        E: bilrost::BorrowedMessage<'b> + Send,
    {
        let buf = self.state_bytes.get_ref();
        E::decode_borrowed_length_delimited(&mut buf.as_ref()).ok()
    }
}
