// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod cache;
mod metric_definitions;
pub mod scheduler;
mod vqueue_config;

pub use cache::{VQueuesMeta, VQueuesMetaMut};
pub use metric_definitions::describe_metrics;
pub use scheduler::SchedulerService;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::VQueueMetaUpdates;
use restate_storage_api::vqueue_table::{
    AsEntryStateHeader, EntryCard, EntryId, EntryKind, ReadVQueueTable, Stage, VisibleAt,
    WriteVQueueTable, metadata,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueue::{EffectivePriority, NewEntryPriority, VQueueId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventDetails<Item> {
    /// Entry is being enqueued for the first time
    Enqueued(Item),
    /// Scheduler assignment has been confirmed
    RunAttemptConfirmed { item_hash: u64 },
    /// We cannot accept the run attempt request, notify the scheduler
    RunAttemptRejected { item_hash: u64 },
    /// Entry has been removed from the waiting inbox
    Removed { item_hash: u64 },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct VQueueEvent<Item> {
    pub qid: VQueueId,
    pub details: EventDetails<Item>,
}

impl<Item> VQueueEvent<Item> {
    pub const fn new(qid: VQueueId, details: EventDetails<Item>) -> Self {
        Self { qid, details }
    }
}

/// VQueues Mutations
///
/// given an operation, describe the storage changes that need to be made and emit effects
/// to allow the scheduler to cache.
pub struct VQueues<'a, A, S> {
    qid: VQueueId,
    storage: &'a mut S,
    cache: &'a mut VQueuesMetaMut,
    // action collector is only available if we have a scheduler to notify
    action_collector: Option<&'a mut Vec<A>>,
}

impl<'a, A, S> VQueues<'a, A, S>
where
    A: From<VQueueEvent<EntryCard>> + 'static,
    S: WriteVQueueTable + ReadVQueueTable,
{
    pub const fn new(
        qid: VQueueId,
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaMut,
        action_collector: Option<&'a mut Vec<A>>,
    ) -> Self {
        Self {
            qid,
            storage,
            cache,
            action_collector,
        }
    }

    /// The entry has completed execution and it needs to be removed from the vqueue.
    ///
    /// Does nothing if the entry was not found in the previous stage.
    pub async fn end_by_id(
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaMut,
        action_collector: Option<&'a mut Vec<A>>,
        at: UniqueTimestamp,
        kind: EntryKind,
        partition_key: PartitionKey,
        id: &EntryId,
    ) -> Result<bool, StorageError> {
        // find the entry
        let entry_state = storage
            .get_entry_state_header(kind, partition_key, id)
            .await?;

        let Some(entry_state) = entry_state else {
            return Ok(false);
        };

        let mut inbox = Self::new(entry_state.vqueue_id(), storage, cache, action_collector);
        inbox
            .end(at, entry_state.stage(), &entry_state.current_entry_card())
            .await?;

        Ok(true)
    }

    pub async fn enqueue_new<E>(
        &mut self,
        created_at: UniqueTimestamp,
        visible_at: VisibleAt,
        priority: NewEntryPriority,
        kind: EntryKind,
        id: impl Into<EntryId>,
        item: Option<E>,
    ) -> Result<(), StorageError>
    where
        E: bilrost::Message,
    {
        let visible_at = match visible_at {
            VisibleAt::Now => VisibleAt::At(created_at),
            VisibleAt::At(ts) => VisibleAt::At(ts),
        };
        let card = EntryCard::new(priority, visible_at, created_at, kind, id.into());
        // todo: Perform enqueue validations:
        // - Can admit? (bounds check, is_sealed, etc.)
        let mut updates = VQueueMetaUpdates::default();
        updates.push(
            created_at,
            metadata::Action::EnqueueNew {
                priority: card.priority,
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = self
            .cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(&self.qid, &updates);

        // We need to add the entry into the inbox vqueue.
        // todo: add to the general vqueue prefix (that includes all entries regardless of stage)
        self.storage.put_inbox_entry(&self.qid, Stage::Inbox, &card);

        self.storage
            .put_vqueue_entry_state(&self.qid, &card, Stage::Inbox, ());

        // store the vqueue item for later usage
        if let Some(item) = item {
            self.storage
                .put_item(&self.qid, card.created_at, card.kind, &card.id, item);
        }

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(&self.qid);
        }

        if let Some(collector) = self.action_collector.as_deref_mut()
            && is_active_now
        {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            let inbox_event = VQueueEvent::new(self.qid, EventDetails::Enqueued(card));
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }

    /// Moves a vqueue item from [`Stage::Inbox`] to [`Stage::Run`] and returns the modified
    /// [`EntryCard`] to identify the updated vqueue item.
    pub async fn attempt_to_run(
        &mut self,
        at: UniqueTimestamp,
        card: &EntryCard,
        updated_run_token_bucket_zero_time: Option<f64>,
    ) -> Result<EntryCard, StorageError> {
        // Remove from inbox and move to ready
        self.storage
            .delete_inbox_entry(&self.qid, Stage::Inbox, card);

        let mut updates = VQueueMetaUpdates::default();
        updates.push(
            at,
            metadata::Action::StartAttempt {
                visible_at: card.visible_at,
                priority: card.priority,
                updated_start_tb_zero_time: updated_run_token_bucket_zero_time,
            },
        );

        let (was_active_before, is_active_now) = self
            .cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;
        self.storage.update_vqueue(&self.qid, &updates);

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(&self.qid);
        }

        let mut modified_card = card.clone();
        modified_card.priority = EffectivePriority::TokenHeld;

        self.storage
            .put_inbox_entry(&self.qid, Stage::Run, &modified_card);

        // update the entry state
        self.storage
            .put_vqueue_entry_state(&self.qid, &modified_card, Stage::Run, ());

        if let Some(collector) = self.action_collector.as_deref_mut()
            && is_active_now
        {
            let inbox_event = VQueueEvent::new(
                self.qid,
                EventDetails::RunAttemptConfirmed {
                    item_hash: card.unique_hash(),
                },
            );
            collector.push(A::from(inbox_event));
        }

        Ok(modified_card)
    }

    /// Wake up moves the inbox entry from (parked) back into the inbox stage.
    pub async fn wake_up(
        &mut self,
        at: UniqueTimestamp,
        card: &EntryCard,
    ) -> Result<(), StorageError> {
        self.storage
            .delete_inbox_entry(&self.qid, Stage::Park, card);

        let mut updates = VQueueMetaUpdates::default();
        updates.push(
            at,
            metadata::Action::WakeUp {
                priority: card.priority,
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = self
            .cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(&self.qid, &updates);

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(&self.qid);
        }

        let mut modified_card = card.clone();
        if card.priority.has_started() {
            // we need to do this to ensure that inbox entries of started entries follow the
            // creation time and not their visible_at time.
            modified_card.visible_at = VisibleAt::Now;
        }

        // We add the entry back into the waiting inbox
        self.storage
            .put_inbox_entry(&self.qid, Stage::Inbox, &modified_card);

        self.storage
            .put_vqueue_entry_state(&self.qid, &modified_card, Stage::Inbox, ());

        if let Some(collector) = self.action_collector.as_deref_mut() {
            let inbox_event = VQueueEvent::new(self.qid, EventDetails::Enqueued(modified_card));
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }

    /// Park an entry
    ///
    /// If `should_release_concurrency_token` is true, the parked entry will release its token
    pub async fn park(
        &mut self,
        at: UniqueTimestamp,
        card: &EntryCard,
        previous_stage: Stage,
        should_release_concurrency_token: bool,
    ) -> Result<(), StorageError> {
        self.storage
            .delete_inbox_entry(&self.qid, previous_stage, card);

        let mut updates = VQueueMetaUpdates::default();

        updates.push(
            at,
            metadata::Action::Park {
                should_release_concurrency_token,
                priority: card.priority,
                was_running: matches!(previous_stage, Stage::Run),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = self
            .cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(&self.qid, &updates);

        if was_active_before != is_active_now {
            assert!(!is_active_now);
            self.storage.mark_vqueue_as_dormant(&self.qid);
        }

        let mut modified_card = card.clone();
        if should_release_concurrency_token && card.priority.token_held() {
            // adjust the priority to reflect releasing the token
            modified_card.priority = EffectivePriority::Started;
        }

        self.storage
            .put_inbox_entry(&self.qid, Stage::Park, &modified_card);
        self.storage
            .put_vqueue_entry_state(&self.qid, &modified_card, Stage::Park, ());

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            let inbox_event = VQueueEvent::new(
                self.qid,
                EventDetails::Removed {
                    item_hash: card.unique_hash(),
                },
            );
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }

    /// Movement of a running entry back to the waiting inbox happens on failover of pp.
    pub async fn yield_running(
        &mut self,
        at: UniqueTimestamp,
        card: &EntryCard,
    ) -> Result<(), StorageError> {
        // Remove from running and move to waiting
        self.storage.delete_inbox_entry(&self.qid, Stage::Run, card);

        // Not sure about that. we need to treat it similar to enqueue though, but it was already
        // running, probably needs its own metadata update action.
        let mut updates = VQueueMetaUpdates::default();
        updates.push(
            at,
            metadata::Action::YieldRunning {
                priority: card.priority,
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = self
            .cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;
        self.storage.update_vqueue(&self.qid, &updates);

        if was_active_before != is_active_now {
            assert!(!is_active_now);
            self.storage.mark_vqueue_as_dormant(&self.qid);
        }

        // We add the entry back into the waiting inbox
        self.storage.put_inbox_entry(&self.qid, Stage::Inbox, card);

        self.storage
            .put_vqueue_entry_state(&self.qid, card, Stage::Inbox, ());

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            let inbox_event = VQueueEvent::new(self.qid, EventDetails::Enqueued(card.clone()));
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }

    /// The entry has completed execution and it needs to be removed from the vqueue.
    ///
    /// Does nothing if the entry was not found in the previous stage.
    pub async fn end(
        &mut self,
        at: UniqueTimestamp,
        previous_stage: Stage,
        card: &EntryCard,
    ) -> Result<(), StorageError> {
        // Remove from the current stage
        self.storage
            .delete_inbox_entry(&self.qid, previous_stage, card);

        self.storage
            .delete_item(&self.qid, card.created_at, card.kind, &card.id);

        let mut updates = VQueueMetaUpdates::default();
        updates.push(
            at,
            metadata::Action::Complete {
                priority: card.priority,
                was_waiting: matches!(previous_stage, Stage::Inbox),
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = self
            .cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;
        self.storage.update_vqueue(&self.qid, &updates);

        if was_active_before != is_active_now {
            assert!(!is_active_now);
            self.storage.mark_vqueue_as_dormant(&self.qid);
        }

        if let Some(collector) = self.action_collector.as_deref_mut() {
            let inbox_event = VQueueEvent::new(
                self.qid,
                EventDetails::Removed {
                    item_hash: card.unique_hash(),
                },
            );
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }
}
