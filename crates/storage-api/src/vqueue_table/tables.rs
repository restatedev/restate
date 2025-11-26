// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueue::VQueueId;

use super::{AsEntryState, AsEntryStateHeader, EntryCard, EntryId, EntryKind, EntryStateKind};
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
    Run = b'r',
    /// Holds entries that are paused, suspended. Such entries move back to `Inbox` when
    /// woken up.
    #[bilrost(3)]
    Park = b'p',
}

pub trait WriteVQueueTable {
    /// Update VQueueMeta with a set of differential updates.
    fn update_vqueue(&mut self, qid: &VQueueId, updates: &super::metadata::VQueueMetaUpdates);

    /// Places an entry onto an inbox stage
    fn put_inbox_entry(&mut self, qid: &VQueueId, stage: Stage, card: &EntryCard);

    /// Deletes an entry from an inbox stage
    fn delete_inbox_entry(&mut self, qid: &VQueueId, stage: Stage, card: &EntryCard);

    /// Adds a vqueue to the list of active vqueues
    fn mark_queue_as_active(&mut self, qid: &VQueueId);

    /// Removes the vqueue from the list of active vqueues
    fn mark_queue_as_empty(&mut self, qid: &VQueueId);

    /// Updates a vqueue's entry's state
    fn put_vqueue_entry_state<E>(
        &mut self,
        qid: &VQueueId,
        card: &EntryCard,
        stage: Stage,
        state: E,
    ) where
        E: EntryStateKind + bilrost::Message + bilrost::encoding::RawMessage,
        (): bilrost::encoding::EmptyState<(), E>;

    /// Stores a vqueue item for later use.
    fn put_item<E>(
        &mut self,
        qid: &VQueueId,
        created_at: UniqueTimestamp,
        kind: EntryKind,
        id: &EntryId,
        item: E,
    ) where
        E: bilrost::Message;

    /// Deletes a vqueue item.
    fn delete_item(
        &mut self,
        qid: &VQueueId,
        created_at: UniqueTimestamp,
        kind: EntryKind,
        id: &EntryId,
    );
}

pub trait ReadVQueueTable {
    /// Get vqueue's metadata
    fn get_vqueue(
        &mut self,
        qid: &VQueueId,
    ) -> impl Future<Output = Result<Option<super::metadata::VQueueMeta>>> + Send;

    /// Get the entry state (header information only) for a vqueue entry by id
    fn get_entry_state_header(
        &mut self,
        kind: EntryKind,
        partition_key: PartitionKey,
        id: &EntryId,
    ) -> impl Future<Output = Result<Option<impl AsEntryStateHeader + 'static + Send>>>;

    /// Get the entry state for a vqueue entry by id
    fn get_entry_state<E>(
        &mut self,
        kind: EntryKind,
        partition_key: PartitionKey,
        id: &EntryId,
    ) -> impl Future<Output = Result<Option<impl AsEntryState<State = E> + 'static + Send>>>
    where
        E: EntryStateKind
            + bilrost::OwnedMessage
            + bilrost::encoding::RawMessageDecoder
            + Sized
            + 'static,
        (): bilrost::encoding::EmptyState<(), E>;

    /// Gets a vqueue item identified by its qid, the entry id, its kind and the creation timestamp.
    fn get_item<E>(
        &mut self,
        qid: &VQueueId,
        created_at: UniqueTimestamp,
        kind: EntryKind,
        id: &EntryId,
    ) -> impl Future<Output = Result<Option<E>>>
    where
        E: bilrost::OwnedMessage;

    // Commented for future reference
    // fn with_entry_state<'a, E, F, O>(
    //     &mut self,
    //     partition_key: PartitionKey,
    //     id: &EntryId,
    //     f: F,
    // ) -> impl Future<Output = Result<Option<O>>>
    // where
    //     F: FnOnce(&'a (dyn AsEntryState<State = E> + 'a)) -> O,
    //     O: 'static,
    //     E: EntryStateKind
    //         + bilrost::BorrowedMessage<'a>
    //         + bilrost::encoding::RawMessageBorrowDecoder<'a>
    //         + 'a,
    //     (): bilrost::encoding::EmptyState<(), E>;
}

pub trait ScanVQueueTable {
    fn scan_active_vqueues(
        &self,
        on_item: impl FnMut(VQueueId, super::metadata::VQueueMeta),
    ) -> Result<()>;
}
