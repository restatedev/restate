// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
mod util;

// Re-exports
pub use cache::{VQueuesMeta, VQueuesMetaCache};
pub use metric_definitions::describe_metrics;
pub use scheduler::{
    ResourceManager, SchedulerService, SchedulingStatus, ThrottleScope, VQueueSchedulerStatus,
};
pub use util::*;

use restate_limiter::LimitKey;
use restate_storage_api::StorageError;
use restate_storage_api::lock_table::{AcquiredBy, LockState, WriteLockTable};
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{
    AsEntryStateHeader, EntryCard, EntryId, EntryKind, ReadVQueueTable, Stage, VisibleAt,
    WriteVQueueTable, metadata,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey};
use restate_types::invocation::InvocationTarget;
use restate_types::logs::Lsn;
use restate_types::vqueue::{EffectivePriority, NewEntryPriority, VQueueId};
use restate_types::{LockName, Scope};
use restate_util_string::{ReString, format_restring};

use self::cache::VQueueCacheKey;

// Token bucket used for throttling over all vqueues
type GlobalTokenBucket<C = gardal::TokioClock> =
    gardal::TokenBucket<gardal::PaddedAtomicSharedStorage, C>;

#[derive(Debug, Clone)]
pub enum EventDetails<Item> {
    MetadataUpdated(metadata::Update),
    // Queue has been added to the list of active
    VQueueBecameActive(VQueueMeta),
    // Queue has been marked as dormant
    VQueueBecameDormant,
    /// Entry is being enqueued for the first time
    Enqueued(Item),
    /// Scheduler assignment has been confirmed
    RunAttemptConfirmed {
        item_hash: u64,
    },
    /// Entry has been removed from the inbox
    Removed {
        item_hash: u64,
    },
    LockReleased {
        scope: Option<Scope>,
        lock_name: LockName,
    },
}

#[derive(Debug, Clone)]
pub struct VQueueEvent<Item> {
    pub qid: VQueueId,
    pub updates: Vec<EventDetails<Item>>,
}

impl<Item> VQueueEvent<Item> {
    pub const fn new(qid: VQueueId) -> Self {
        Self {
            qid,
            updates: Vec::new(),
        }
    }

    pub fn push(&mut self, details: EventDetails<Item>) {
        self.updates.push(details);
    }
}

/// VQueue Mutations
///
/// given an operation, describe the storage changes that need to be made and emit effects
/// to allow the scheduler to cache.
pub struct VQueue<'a, A, S> {
    storage: &'a mut S,
    cache: &'a mut VQueuesMetaCache,
    cache_key: VQueueCacheKey,
    // action collector is only available if we have a scheduler to notify
    action_collector: Option<&'a mut Vec<A>>,
}

impl<'a, A, S> VQueue<'a, A, S>
where
    A: From<VQueueEvent<EntryCard>> + 'static,
    S: WriteVQueueTable + ReadVQueueTable + WriteLockTable,
{
    /// Determines the vqueue id from the invocation id, invocation target, and limit key.
    #[inline]
    pub fn infer_vqueue_id_from_invocation(
        partition_key: PartitionKey,
        invocation_target: &InvocationTarget,
        limit_key: &LimitKey<ReString>,
    ) -> VQueueId {
        util::infer_vqueue_id_from_invocation(partition_key, invocation_target, limit_key)
    }
    /// The entry has completed execution and it needs to be removed from the vqueue.
    ///
    /// Does nothing if the entry was not found in the previous stage.
    ///
    /// Returns true if the entry was found and was ended correctly, false otherwise.
    pub async fn end_by_id(
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
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

        let inbox = Self::get(entry_state.vqueue_id(), storage, cache, action_collector).await?;
        let Some(mut inbox) = inbox else {
            return Ok(false);
        };
        inbox.end(at, entry_state.stage(), &entry_state.current_entry_card())
    }

    /// Get access to the vqueue if it exists, otherwise this returns None.
    pub async fn get(
        qid: &VQueueId,
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
        action_collector: Option<&'a mut Vec<A>>,
    ) -> Result<Option<Self>, StorageError> {
        let Some(cache_key) = cache.load(storage, qid).await? else {
            return Ok(None);
        };

        Ok(Some(Self {
            storage,
            cache_key,
            cache,
            action_collector,
        }))
    }

    pub async fn vqueue_from_invocation_target(
        partition_key: PartitionKey,
        invocation_target: &InvocationTarget,
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
        action_collector: Option<&'a mut Vec<A>>,
        limit_key: &LimitKey<ReString>,
    ) -> Result<Self, StorageError> {
        let qid =
            Self::infer_vqueue_id_from_invocation(partition_key, invocation_target, limit_key);
        let cache_key = match cache.load(storage, &qid).await? {
            Some(key) => key,
            None => {
                let meta = VQueueMeta::new(
                    invocation_target.scope(),
                    limit_key.clone(),
                    invocation_target.lock_name(),
                );
                storage.create_vqueue(&qid, &meta);
                cache.insert(qid.clone(), meta)
            }
        };

        Ok(Self {
            storage,
            cache_key,
            cache,
            action_collector,
        })
    }

    pub async fn get_or_create_vqueue(
        qid: &VQueueId,
        storage: &'a mut S,
        cache: &'a mut VQueuesMetaCache,
        action_collector: Option<&'a mut Vec<A>>,
        scope: &Option<Scope>,
        limit_key: &LimitKey<ReString>,
        lock_name: &Option<LockName>,
    ) -> Result<Self, StorageError> {
        let cache_key = match cache.load(storage, qid).await? {
            None => {
                // Note: we don't send an event (i.e. vqueue created) because we only care
                // about notifying the scheduler only when the queue becomes active.
                let limit_key = limit_key.to_cheap_cloneable();
                let meta = VQueueMeta::new(scope.clone(), limit_key, lock_name.clone());
                storage.create_vqueue(qid, &meta);
                cache.insert(qid.clone(), meta)
            }
            Some(key) => key,
        };

        Ok(Self {
            storage,
            cache_key,
            cache,
            action_collector,
        })
    }

    pub fn enqueue_new<E>(
        &mut self,
        created_at: UniqueTimestamp,
        visible_at: VisibleAt,
        priority: NewEntryPriority,
        kind: EntryKind,
        id: impl Into<EntryId>,
        item: Option<E>,
    ) -> Result<EntryCard, StorageError>
    where
        E: bilrost::Message,
    {
        let visible_at = match visible_at {
            VisibleAt::Now => VisibleAt::At(created_at.to_unix_millis()),
            VisibleAt::At(ts) => VisibleAt::At(ts),
        };
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        let mut event = VQueueEvent::new(meta.vqueue_id().clone());
        let card = EntryCard::new(priority, visible_at, created_at, kind, id.into());
        // todo: Perform enqueue validations:
        // - Can admit? (bounds check, is_sealed, etc.)
        let update = metadata::Update::new(
            created_at,
            metadata::Action::EnqueueNew {
                priority: card.priority,
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update)?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        // We need to add the entry into the inbox vqueue.
        // todo: add to the general vqueue prefix (that includes all entries regardless of stage)
        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Inbox, &card);

        self.storage
            .put_vqueue_entry_state(meta.vqueue_id(), &card, Stage::Inbox, ());

        // store the vqueue item for later usage
        if let Some(item) = item {
            self.storage
                .put_item(meta.vqueue_id(), card.created_at, card.kind, &card.id, item);
        }

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
            event.push(EventDetails::VQueueBecameActive(meta.meta().clone()));
            event.push(EventDetails::Enqueued(card.clone()));
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::Enqueued(card.clone()));
        } else {
            unreachable!("Cannot enqueue an item on a dormant vqueue");
        }

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            collector.push(A::from(event));
        }

        Ok(card)
    }

    /// Moves a vqueue item from [`Stage::Inbox`] to [`Stage::Run`] and returns the modified
    /// [`Option<EntryCard>`] to identify the updated vqueue item.
    ///
    /// The returned [`Option<EntryCard>`] is `None` if the vqueue item was not found in the inbox.
    pub fn attempt_to_run(
        &mut self,
        at: UniqueTimestamp,
        card: &EntryCard,
    ) -> Result<Option<EntryCard>, StorageError> {
        let meta = self.cache.get_mut(self.cache_key).unwrap();

        // Remove from inbox and move to ready
        if !self
            .storage
            .pop_inbox_entry(meta.vqueue_id(), Stage::Inbox, card)?
        {
            // We don't do work if the inbox entry was not found
            return Ok(None);
        }

        let mut event = VQueueEvent::new(meta.vqueue_id().clone());
        let mut modified_card = card.clone();

        let update = metadata::Update::new(
            at,
            metadata::Action::StartAttempt {
                visible_at: card.visible_at,
                priority: card.priority,
            },
        );

        let (was_active_before, is_active_now) = meta.apply_update(&update)?;
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        // Do we need to hold the lock or is this running an entry that doesn't need a lock (or)
        // already has the lock held?
        if !card.has_lock()
            && let Some(lock_name) = meta.meta().lock_name()
        {
            let acquired_by = match card.kind {
                EntryKind::Unknown => AcquiredBy::Other(ReString::from_static("unknown")),
                EntryKind::Invocation => AcquiredBy::InvocationId(InvocationId::from_parts(
                    meta.vqueue_id().partition_key(),
                    InvocationUuid::from_slice(card.id.as_bytes()).unwrap(),
                )),
                EntryKind::StateMutation => {
                    // State mutation uses the LSN as their canonical identifier
                    let raw = u128::from_be_bytes(card.id.to_bytes());
                    // fallback to Lsn::INVALID if the raw value is too large
                    let lsn = Lsn::from(u64::try_from(raw).unwrap_or_default());
                    AcquiredBy::Other(format_restring!("State mutation @ {lsn}"))
                }
            };

            // acquire lock
            let lock_state = LockState {
                acquired_at: at,
                acquired_by,
            };

            self.storage
                .acquire_lock(meta.meta().scope(), lock_name, &lock_state);
            // todo: make this "lock held" instead
            // modified_card.has_lock = true;
            modified_card.priority = EffectivePriority::TokenHeld;
        }

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
            event.push(EventDetails::VQueueBecameActive(meta.meta().clone()));
            event.push(EventDetails::RunAttemptConfirmed {
                item_hash: card.unique_hash(),
            });
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::RunAttemptConfirmed {
                item_hash: card.unique_hash(),
            });
        } else {
            unreachable!("Cannot run an item on a dormant vqueue");
        }

        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Run, &modified_card);

        // update the entry state
        self.storage
            .put_vqueue_entry_state(meta.vqueue_id(), &modified_card, Stage::Run, ());

        if let Some(collector) = self.action_collector.as_deref_mut() {
            collector.push(A::from(event));
        }

        Ok(Some(modified_card))
    }

    /// Wake up moves the inbox entry from (parked) back into the inbox stage.
    ///
    /// Returns true if the entry was found in the parked inbox and resumed correctly, false otherwise.
    pub fn wake_up(&mut self, at: UniqueTimestamp, card: &EntryCard) -> Result<bool, StorageError> {
        let meta = self.cache.get_mut(self.cache_key).unwrap();

        if !self
            .storage
            .pop_inbox_entry(meta.vqueue_id(), Stage::Park, card)?
        {
            return Ok(false);
        }

        let mut event = VQueueEvent::new(meta.vqueue_id().clone());
        let update = metadata::Update::new(
            at,
            metadata::Action::WakeUp {
                priority: card.priority,
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update)?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        let mut modified_card = card.clone();
        if card.priority.has_started() {
            // we need to do this to ensure that inbox entries of started entries follow the
            // creation time and not their visible_at time.
            modified_card.visible_at = VisibleAt::Now;
        }

        if was_active_before != is_active_now {
            assert!(is_active_now);
            self.storage.mark_vqueue_as_active(meta.vqueue_id());
            event.push(EventDetails::VQueueBecameActive(meta.meta().clone()));
            event.push(EventDetails::Enqueued(modified_card.clone()));
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::Enqueued(modified_card.clone()));
        } else {
            unreachable!("Cannot enqueue an item on a dormant vqueue");
        }

        // We add the entry back into the waiting inbox
        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Inbox, &modified_card);

        self.storage
            .put_vqueue_entry_state(meta.vqueue_id(), &modified_card, Stage::Inbox, ());

        if let Some(collector) = self.action_collector.as_deref_mut() {
            collector.push(A::from(event));
        }

        Ok(true)
    }

    /// Park an entry
    ///
    /// Returns `true` if the entry was found in inbox and parked correctly, `false` otherwise.
    pub fn park(
        &mut self,
        at: UniqueTimestamp,
        card: &EntryCard,
        previous_stage: Stage,
    ) -> Result<bool, StorageError> {
        let meta = self.cache.get_mut(self.cache_key).unwrap();

        if !self
            .storage
            .pop_inbox_entry(meta.vqueue_id(), previous_stage, card)?
        {
            return Ok(false);
        }

        let mut event = VQueueEvent::new(meta.vqueue_id().clone());

        let update = metadata::Update::new(
            at,
            metadata::Action::Park {
                priority: card.priority,
                previous_stage,
            },
        );

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update)?;

        // Update vqueue meta in storage
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if was_active_before != is_active_now {
            assert!(!is_active_now);
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
            event.push(EventDetails::Removed {
                item_hash: card.unique_hash(),
            });
            event.push(EventDetails::VQueueBecameDormant);
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::Removed {
                item_hash: card.unique_hash(),
            });
        } else {
            unreachable!("Cannot remove an item from a dormant vqueue");
        }

        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Park, card);
        self.storage
            .put_vqueue_entry_state(meta.vqueue_id(), card, Stage::Park, ());

        if let Some(collector) = self.action_collector.as_deref_mut() {
            collector.push(A::from(event));
        }

        Ok(true)
    }

    /// Movement of a running entry back to the waiting inbox happens on failover of pp.
    ///
    /// Returns `true` if the entry was found in running inbox and yielded correctly, `false` otherwise.
    pub fn yield_running(
        &mut self,
        at: UniqueTimestamp,
        card: EntryCard,
    ) -> Result<bool, StorageError> {
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        // Remove from running and move to waiting
        if !self
            .storage
            .pop_inbox_entry(meta.vqueue_id(), Stage::Run, &card)?
        {
            return Ok(false);
        }

        // Not sure about that. we need to treat it similar to enqueue though, but it was already
        // running, probably needs its own metadata update action.
        let update = metadata::Update::new(at, metadata::Action::YieldRunning);

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update)?;
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if was_active_before != is_active_now {
            unreachable!("Yielding should never toggle vqueue's active state");
        }

        // We add the entry back into the waiting inbox
        self.storage
            .put_inbox_entry(meta.vqueue_id(), Stage::Inbox, &card);

        self.storage
            .put_vqueue_entry_state(meta.vqueue_id(), &card, Stage::Inbox, ());

        if let Some(collector) = self.action_collector.as_deref_mut() {
            // Let the scheduler know about the new entry to keep its head-of-line cache of the vqueue
            // as fresh as possible.
            let mut event = VQueueEvent::new(meta.vqueue_id().clone());
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::Enqueued(card));
            collector.push(A::from(event));
        }

        Ok(true)
    }

    /// The entry has completed execution and it needs to be removed from the vqueue.
    ///
    /// Does nothing if the entry was not found in the previous stage.
    ///
    /// Returns `true` if the entry was found in `previous_stage` inbox and was ended correctly, `false` otherwise.
    pub fn end(
        &mut self,
        at: UniqueTimestamp,
        previous_stage: Stage,
        card: &EntryCard,
    ) -> Result<bool, StorageError> {
        let meta = self.cache.get_mut(self.cache_key).unwrap();
        // Remove from the current stage
        if !self
            .storage
            .pop_inbox_entry(meta.vqueue_id(), previous_stage, card)?
        {
            return Ok(false);
        }

        self.storage
            .delete_item(meta.vqueue_id(), card.created_at, card.kind, &card.id);

        let mut event = VQueueEvent::new(meta.vqueue_id().clone());
        let update = metadata::Update::new(
            at,
            metadata::Action::Complete {
                priority: card.priority,
                previous_stage,
            },
        );

        if card.has_lock()
            && let Some(lock_name) = meta.meta().lock_name()
        {
            self.storage.release_lock(meta.meta().scope(), lock_name);
            event.push(EventDetails::LockReleased {
                scope: meta.meta().scope().clone(),
                lock_name: lock_name.clone(),
            });
        }

        // todo(asoli): We need to discuss whether entry_state should outlive the item's
        // lifecycle in the queue or not. For now, we'll remove it.
        self.storage
            .delete_vqueue_entry_state(meta.vqueue_id(), card.kind, &card.id);

        // Update cache
        let (was_active_before, is_active_now) = meta.apply_update(&update)?;
        self.storage.update_vqueue(meta.vqueue_id(), &update);

        if was_active_before != is_active_now {
            assert!(!is_active_now);
            self.storage.mark_vqueue_as_dormant(meta.vqueue_id());
            event.push(EventDetails::Removed {
                item_hash: card.unique_hash(),
            });
            event.push(EventDetails::VQueueBecameDormant);
        } else if is_active_now {
            event.push(EventDetails::MetadataUpdated(update));
            event.push(EventDetails::Removed {
                item_hash: card.unique_hash(),
            });
        } else {
            unreachable!("Cannot remove an item from a dormant vqueue");
        }

        if let Some(collector) = self.action_collector.as_deref_mut() {
            collector.push(A::from(event));
        }

        Ok(true)
    }
}
