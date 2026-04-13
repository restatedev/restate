// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::VQueueId;

use super::metadata::{VQueueMeta, VQueueMetaRef};
use super::{
    EntryId, EntryKey, EntryMetadata, EntryState, EntryStateHeader, EntryStatistics, EntryValue,
    IdentifiesEntry, LazyEntryState,
};
use crate::Result;

/// Stages in the inbox/vqueue
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    bilrost::Enumeration,
    strum::FromRepr,
    strum::Display,
)]
#[repr(u8)]
#[strum(serialize_all = "kebab-case")]
pub enum Stage {
    #[bilrost(0)]
    Unknown = 0,
    /// Holds entries that are waiting for the scheduler to pick and move to `Run`
    #[bilrost(1)]
    Inbox = b'i',
    /// Holds entries that are currently assumed to be running
    #[bilrost(2)]
    Running = b'r',
    /// Holds entries that are suspended and will be woken up upon receiving a signal.
    #[bilrost(3)]
    Suspended = b's',
    /// Holds entries that are paused, either manually or due to errors.
    /// Paused entries can be resumed by moving them back to `Inbox`
    #[bilrost(4)]
    Paused = b'p',
    /// Items that are completed/finished/terminated. This is a terminal stage and
    /// items in this stage are allowed to be deleted/purged/archived either immediately
    /// or delayed.
    #[bilrost(5)]
    Finished = b'f',
}
impl Stage {
    pub const fn serialized_length_fixed() -> usize {
        std::mem::size_of::<Self>()
    }
}

pub trait WriteVQueueTable {
    /// Initializes a new vqueue
    fn create_vqueue(&mut self, qid: &VQueueId, meta: &VQueueMeta);

    /// Update VQueueMeta with a set of differential updates.
    fn update_vqueue(&mut self, qid: &VQueueId, update: &super::metadata::Update);

    /// Places an entry onto an inbox stage
    fn put_vqueue_entry(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
        value: &EntryValue,
    );

    /// Returns and entry from the vqueue
    fn get_vqueue_entry(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
    ) -> Result<Option<EntryValue>>;

    /// Deletes entry from inbox unconditionally
    fn delete_vqueue_entry(&mut self, qid: &VQueueId, stage: Stage, key: &EntryKey);

    /// Adds a vqueue to the list of active vqueues
    ///
    /// A vqueue is considered active when it's of interest to the scheduler.
    ///
    /// The scheduler cares about vqueues that have entries that are already running or that are waiting
    /// to run. With some special rules to consider when the queue is paused. When the vqueue is
    /// paused, the scheduler will only be interested in its "running" entries and not in its
    /// waiting entries. Therefore, it will remain to be "active" as long as it has running
    /// entries. Once running entries are moved to waiting or completed, the vqueue is be
    /// considered dormant until it's unpaused.
    ///
    /// A vqueue that is "not" active does not mean it's "empty". It could be paused or
    /// only contains entries in parked or completed states. As such, it's not considered
    /// by the scheduler and it's considered "dormant".
    fn mark_vqueue_as_active(&mut self, qid: &VQueueId);

    /// Removes the vqueue from the list of active vqueues
    ///
    /// A dormant vqueue is not necessarily `empty`. It's a vqueue _might_ have items (or not)
    /// in parked or completed states, or it might have waiting items in its inbox but the
    /// vqueue is paused and would not be visible to the scheduler.
    fn mark_vqueue_as_dormant(&mut self, qid: &VQueueId);

    /// Updates a vqueue's entry's state
    fn put_vqueue_entry_state<E>(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        entry_key: &EntryKey,
        metadata: &EntryMetadata,
        stats: EntryStatistics,
        state: &E,
    ) where
        E: EntryState + bilrost::Message + bilrost::encoding::RawMessage,
        (): bilrost::encoding::EmptyState<(), E>;

    fn delete_vqueue_entry_state(&mut self, partition_key: PartitionKey, id: &EntryId);

    // todo: DELETT..
    /// Stores a vqueue item for later use.
    fn put_item<E>(&mut self, qid: &VQueueId, created_at: UniqueTimestamp, id: &EntryId, item: E)
    where
        E: bilrost::Message;

    /// Deletes a vqueue item.
    fn delete_item(&mut self, qid: &VQueueId, created_at: UniqueTimestamp, id: &EntryId);
}

pub trait ReadVQueueTable {
    /// Get vqueue's metadata
    fn get_vqueue<'a>(
        &'a self,
        qid: &VQueueId,
    ) -> impl Future<Output = Result<Option<super::metadata::VQueueMeta>>>;

    /// Get the entry state (header information only) for a vqueue entry by id
    fn get_entry_state_header(
        &self,
        partition_key: PartitionKey,
        id: &EntryId,
    ) -> impl Future<Output = Result<Option<impl EntryStateHeader + 'static>>>;

    /// Get the entry state for a vqueue entry by id
    fn get_entry_state_lazy<'a>(
        &'a self,
        partition_key: PartitionKey,
        entry_id: &EntryId,
    ) -> impl Future<Output = Result<Option<impl LazyEntryState + 'a>>>;

    fn get_entry_state<I>(
        &self,
        id: I,
    ) -> impl Future<Output = Result<Option<(impl EntryStateHeader + 'static, I::State)>>>
    where
        I: IdentifiesEntry,
        I::State: EntryState
            + bilrost::OwnedMessage
            + bilrost::encoding::RawMessageDecoder
            + Sized
            + Send
            + 'static,
        (): bilrost::encoding::EmptyState<(), I::State>;

    /// Gets a vqueue item identified by its qid, the entry id, its kind and the creation timestamp.
    fn get_item<E>(
        &self,
        qid: &VQueueId,
        created_at: UniqueTimestamp,
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

pub trait ScanVQueueMetaTable {
    /// Used for data-fusion queries
    fn for_each_vqueue_meta<
        F: for<'a> FnMut((&'a VQueueId, &'a VQueueMetaRef<'a>)) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}
