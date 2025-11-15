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
pub mod scheduler;
mod vqueue_config;

pub use cache::{VQueuesMeta, VQueuesMetaMut};
pub use scheduler::SchedulerService;

use tracing::{info, instrument, trace, warn};

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::{VQueueMetaUpdates, VQueueStatus};
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
    /// Scheduler should consider this card has running
    RunAttemptPermitted { item_hash: u64 },
    /// We cannot accept the run attempt request, notify the scheduler
    RunAttemptRejected { item_hash: u64 },
    /// Entry has been removed from the waiting inbox
    Removed { item_hash: u64 },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct InboxEvent<Item> {
    pub qid: VQueueId,
    pub ts: UniqueTimestamp,
    pub details: EventDetails<Item>,
}

impl<Item> InboxEvent<Item> {
    pub const fn new(qid: VQueueId, at: UniqueTimestamp, details: EventDetails<Item>) -> Self {
        Self {
            qid,
            ts: at,
            details,
        }
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
    A: From<InboxEvent<EntryCard>> + 'static,
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
    #[instrument(skip_all)]
    pub async fn end_by_id(
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaMut,
        action_collector: Option<&'a mut Vec<A>>,
        at: UniqueTimestamp,
        kind: EntryKind,
        partition_key: PartitionKey,
        id: &EntryId,
    ) -> Result<(), StorageError> {
        // find the entry
        let entry_state = storage
            .get_entry_state_header(kind, partition_key, id)
            .await?;

        let Some(entry_state) = entry_state else {
            warn!("Inbox entry's state was not found for id: {id:?}");
            return Ok(());
        };

        let mut inbox = Self::new(entry_state.vqueue_id(), storage, cache, action_collector);
        inbox
            .end(at, entry_state.stage(), &entry_state.current_entry_card())
            .await?;

        Ok(())
    }

    // todo:  accept any futher information about the entry (when to expire, or enqueue conditions)
    #[instrument(skip_all)]
    pub async fn enqueue_new(
        &mut self,
        created_at: UniqueTimestamp,
        visible_at: VisibleAt,
        priority: NewEntryPriority,
        kind: EntryKind,
        id: impl Into<EntryId>,
    ) -> Result<(), StorageError> {
        let visible_at = match visible_at {
            VisibleAt::Now => VisibleAt::At(created_at),
            VisibleAt::At(ts) => VisibleAt::At(ts),
        };
        let card = EntryCard::new(priority, visible_at, created_at, kind, id.into());
        info!("New inbox entry {card:?}");
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
        let (status_before, status_after) = self
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

        // do not keep putting the same queue if it's already known to be active
        if status_before == VQueueStatus::Empty
            && matches!(status_after, VQueueStatus::Active | VQueueStatus::Paused)
        {
            // Components we need. PKEY, QID. We have all.
            info!("Marking vqueue {:?} as active", self.qid);
            self.storage.mark_queue_as_active(&self.qid);
        }

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            let inbox_event = InboxEvent::new(self.qid, created_at, EventDetails::Enqueued(card));
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn attempt_to_run(
        &mut self,
        at: UniqueTimestamp,
        card: &EntryCard,
    ) -> Result<(), StorageError> {
        info!("Inbox entry {}->{}, {card:?}", Stage::Inbox, Stage::Run);
        // Remove from inbox and move to ready
        self.storage
            .delete_inbox_entry(&self.qid, Stage::Inbox, card);

        let mut updates = VQueueMetaUpdates::default();
        updates.push(
            at,
            metadata::Action::StartAttempt {
                visible_at: card.visible_at,
                priority: card.priority,
            },
        );

        self.cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;
        self.storage.update_vqueue(&self.qid, &updates);

        let mut modified_card = card.clone();
        modified_card.priority = EffectivePriority::TokenHeld;

        self.storage
            .put_inbox_entry(&self.qid, Stage::Run, &modified_card);

        // update the entry state
        self.storage
            .put_vqueue_entry_state(&self.qid, &modified_card, Stage::Run, ());

        info!("Inbox entry is ready to run: {card:?}");

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            let inbox_event = InboxEvent::new(
                self.qid,
                at,
                EventDetails::RunAttemptPermitted {
                    item_hash: card.unique_hash(),
                },
            );
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }

    /// Wake up moves the inbox entry from (parked) back into the inbox stage.
    #[instrument(skip_all)]
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
        self.cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(&self.qid, &updates);

        // We add the entry back into the waiting inbox
        self.storage.put_inbox_entry(&self.qid, Stage::Inbox, card);

        self.storage
            .put_vqueue_entry_state(&self.qid, card, Stage::Inbox, ());

        trace!("Inbox entry is woken up and was added back to waiting inbox: {card:?}");

        if let Some(collector) = self.action_collector.as_deref_mut() {
            let inbox_event = InboxEvent::new(self.qid, at, EventDetails::Enqueued(card.clone()));
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }

    /// To park an entry, the entry must be in the running inbox index.
    ///
    /// If `release_concurrency_token` is true, the parked entry will release its token
    ///
    /// The most important detail is when moving the entry back into the park, we take special
    /// care of items that have been holding concurrency tokens while being parked.
    /// we upgrade their priority to P0 and set visible_at = now, to avoid potential deadlocks
    /// on wake-up.
    #[instrument(skip_all)]
    pub async fn park(
        &mut self,
        at: UniqueTimestamp,
        card: &EntryCard,
        previous_stage: Stage,
        should_release_concurrency_token: bool,
    ) -> Result<(), StorageError> {
        // Remove from inbox and move to ready
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
        self.cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(&self.qid, &updates);

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
            let inbox_event = InboxEvent::new(
                self.qid,
                at,
                EventDetails::Removed {
                    item_hash: card.unique_hash(),
                },
            );
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }

    /// Movement of a running entry back to the waiting inbox happens on failover of pp.
    #[instrument(skip_all)]
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
        self.cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;
        self.storage.update_vqueue(&self.qid, &updates);

        // We add the entry back into the waiting inbox
        self.storage.put_inbox_entry(&self.qid, Stage::Inbox, card);

        self.storage
            .put_vqueue_entry_state(&self.qid, card, Stage::Inbox, ());

        info!("Inbox entry was running and has yielded back to the waiting inbox: {card:?}");

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            let inbox_event = InboxEvent::new(self.qid, at, EventDetails::Enqueued(card.clone()));
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }

    /// The entry has completed execution and it needs to be removed from the vqueue.
    ///
    /// Does nothing if the entry was not found in the previous stage.
    #[instrument(skip_all)]
    pub async fn end(
        &mut self,
        at: UniqueTimestamp,
        previous_stage: Stage,
        card: &EntryCard,
    ) -> Result<(), StorageError> {
        // Remove from the current stage
        self.storage
            .delete_inbox_entry(&self.qid, previous_stage, card);

        let mut updates = VQueueMetaUpdates::default();
        updates.push(
            at,
            metadata::Action::Complete {
                priority: card.priority,
                was_waiting: matches!(previous_stage, Stage::Inbox),
            },
        );

        // Update cache
        let (status_before, status_after) = self
            .cache
            .apply_updates(self.storage, &self.qid, &updates)
            .await?;
        self.storage.update_vqueue(&self.qid, &updates);

        if status_before != VQueueStatus::Empty && status_after == VQueueStatus::Empty {
            // Components we need. PKEY, QID. We have all.
            info!("Marking vqueue {:?} as empty", self.qid);
            self.storage.mark_queue_as_empty(&self.qid);
        }

        info!("Inbox entry was in {previous_stage:?} and is now marked as ended: {card:?}");

        if let Some(collector) = self.action_collector.as_deref_mut() {
            let inbox_event = InboxEvent::new(
                self.qid,
                at,
                EventDetails::Removed {
                    item_hash: card.unique_hash(),
                },
            );
            collector.push(A::from(inbox_event));
        }

        Ok(())
    }
}
