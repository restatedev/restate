// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::BaseEntryId;
use restate_types::vqueues::{CanonicalEntryId, Seq, Stage, VQueueId};

use super::metadata::VQueueMeta;
use super::{
    EntryContext, EntryId, EntryKey, EntryStateRef, EntryStatusHeader, EntryValue, StoredEntryRef,
};
use crate::Result;

/// Whether updating a vqueue retained or purged its metadata.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VQueueDisposition {
    Retained,
    Purged,
}

pub trait WriteVQueueTable {
    /// Initializes a new vqueue
    fn create_vqueue(&mut self, qid: &VQueueId, meta: &VQueueMeta);

    /// Update VQueueMeta with a set of differential updates.
    /// The `meta` **must** match the vqueue metadata on disk prior to the update,
    /// then it gets updated in place. Obsolete metadata is deleted atomically
    /// with the update and reported through the returned disposition.
    fn update_vqueue(
        &mut self,
        qid: &VQueueId,
        meta: &mut VQueueMeta,
        update: &super::metadata::Update,
    ) -> VQueueDisposition;

    /// Deletes a vqueue's metadata record.
    ///
    /// Must only be used on obsolete vqueues (see
    /// [`super::metadata::VQueueMeta::is_obsolete`]); the metadata merge
    /// operator cannot apply updates to a deleted vqueue.
    fn delete_vqueue(&mut self, qid: &VQueueId);

    /// Places an entry onto an inbox stage
    fn put_vqueue_inbox(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
        value: &EntryValue,
    );

    /// Returns and entry from the vqueue
    fn get_vqueue_inbox(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
    ) -> Result<Option<EntryValue>>;

    /// Deletes entry from inbox unconditionally
    fn delete_vqueue_inbox(&mut self, qid: &VQueueId, stage: Stage, key: &EntryKey);

    // Left intentionally for future reference if/when we need to store extra state
    // fn put_vqueue_entry_state<E>(
    //     &mut self,
    //     qid: &VQueueId,
    //     stage: Stage,
    //     entry_key: &EntryKey,
    //     stats: EntryStatistics,
    //     meta: &EntryMetadata,
    //     status: Status,
    //     extra_state: &E,
    // ) where
    //     E: EntryState + bilrost::Message + bilrost::encoding::RawMessage,
    //     (): bilrost::encoding::EmptyState<(), E>;

    /// Creates a previously absent entry status. The caller must establish absence,
    /// including earlier writes in this transaction; this operation does not read storage.
    fn create_vqueue_entry_status(&mut self, context: &EntryContext<'_>, after: EntryStateRef<'_>);

    /// Updates an existing entry, keeping its queue, target, and base identity fixed.
    /// `before` must describe the actual previous state, including earlier writes in
    /// this transaction. This operation does not read or compare against storage.
    ///
    /// # Panics
    /// Panics if the before and after entry IDs differ.
    fn update_vqueue_entry_status(
        &mut self,
        context: &EntryContext<'_>,
        before: StoredEntryRef<'_>,
        after: EntryStateRef<'_>,
    );

    /// Deletes an existing status by base identity, without checking its sequence.
    /// `before` must describe the actual previous state, including earlier writes in
    /// this transaction. This operation does not read or compare against storage.
    fn delete_vqueue_entry_status(
        &mut self,
        context: &EntryContext<'_>,
        before: StoredEntryRef<'_>,
    );

    /// Stores a vqueue entry input payload
    fn put_vqueue_input_payload<E>(
        &mut self,
        qid: &VQueueId,
        seq: impl Into<Seq>,
        id: &EntryId,
        item: E,
    ) where
        E: bilrost::Message;

    /// Deletes a vqueue item.
    fn delete_vqueue_input_payload(&mut self, qid: &VQueueId, id: &CanonicalEntryId);
}

pub trait ReadVQueueTable {
    /// Get vqueue's metadata
    fn get_vqueue(
        &self,
        qid: &VQueueId,
    ) -> impl Future<Output = Result<Option<super::metadata::VQueueMeta>>>;

    /// Get the current entry state (header information only) by base identity.
    /// This lookup does not check a sequence number.
    fn get_vqueue_entry_status(
        &self,
        id: &BaseEntryId,
    ) -> impl Future<Output = Result<Option<impl EntryStatusHeader + 'static + use<Self>>>>;

    /// Finds the key of the state mutation in the inbox of `qid` that has the same position
    /// (`has_lock`, `run_at` and `seq`) as `key`, ignoring the entry id.
    ///
    /// Before v1.8.0, every replica generated a random entry id for a state mutation (#5416),
    /// so a scheduler decision can carry an id that doesn't exist on this replica. The position
    /// is the same on all replicas, so this lets the replica find its own copy of the entry.
    fn find_inbox_state_mutation_key(
        &self,
        qid: &VQueueId,
        key: &EntryKey,
    ) -> impl Future<Output = Result<Option<EntryKey>>>;

    // /// Get the entry state for a vqueue entry by id
    // fn get_vqueue_entry_status_lazy<'a>(
    //     &'a self,
    //     partition_key: PartitionKey,
    //     entry_id: &EntryId,
    // ) -> impl Future<Output = Result<Option<impl LazyEntryStatus + 'a>>>;

    // Left intentionally for future reference
    // fn get_entry_state<I>(
    //     &self,
    //     id: I,
    // ) -> impl Future<Output = Result<Option<(impl EntryStateHeader + 'static, I::State)>>>
    // where
    //     I: IdentifiesEntry,
    //     I::State: EntryState
    //         + bilrost::OwnedMessage
    //         + bilrost::encoding::RawMessageDecoder
    //         + Sized
    //         + Send
    //         + 'static,
    //     (): bilrost::encoding::EmptyState<(), I::State>;

    /// Gets a vqueue input payload identified by its qid, the entry id, its kind and the associated.
    /// sequence number.
    fn get_vqueue_input_payload<E>(
        &self,
        qid: &VQueueId,
        seq: impl Into<Seq>,
        id: &EntryId,
    ) -> impl Future<Output = Result<Option<E>>>
    where
        E: bilrost::OwnedMessage;
}

pub trait ScanVQueueTable {
    fn scan_active_vqueues(
        &self,
        on_item: impl FnMut(VQueueId, super::metadata::VQueueMeta),
    ) -> Result<()>;
}
