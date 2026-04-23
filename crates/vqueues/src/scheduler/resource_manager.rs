// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod invoker;
mod invoker_memory;
mod invoker_throttle;
mod locks;
mod permit;
mod user_limiter;

pub use self::permit::{PermitBuilder, ReservedResources};

use std::collections::VecDeque;
use std::task::Poll;

use smallvec::SmallVec;
use tokio::sync::mpsc;
use tracing::trace;

use restate_futures_util::concurrency::Concurrency;
use restate_memory::{MemoryPool, NonZeroByteCount};
use restate_storage_api::StorageError;
use restate_storage_api::lock_table::LoadLocks;
use restate_storage_api::vqueue_table::{EntryKey, EntryMetadata};
use restate_types::vqueues::EntryKind;
use restate_types::{LockName, Scope};
use restate_worker_api::ResourceKind;

use self::invoker::InvokerConcurrencyLimiter;
use self::invoker_memory::InvokerMemoryLimiter;
use self::invoker_throttle::InvokerThrottlingLimiter;
use self::locks::Locks;
use self::permit::{ProvisionalPermit, UserPermitKind};
use self::user_limiter::UserLimiter;
use super::VQueueHandle;
use super::eligible::EligibilityTracker;
use super::queue_meta::VQueueMetaLite;
use crate::GlobalTokenBucket;

// A set of queues waiting on a resource
type Waiters = VecDeque<VQueueHandle>;

pub struct ResourceManager {
    // Resources
    locks: Locks,
    /// Limiter for invoker global capacity
    invoker_concurrency: InvokerConcurrencyLimiter,
    invoker_throttling: InvokerThrottlingLimiter,
    invoker_memory: InvokerMemoryLimiter,
    user_limiter: UserLimiter,
    rx: mpsc::UnboundedReceiver<ResourceManagerUpdate>,
    // We need to keep this alive to:
    // - Keep the receiver alive even if we don't have any resource permits handed out
    tx: mpsc::UnboundedSender<ResourceManagerUpdate>,
}

#[allow(dead_code)]
enum ResourceManagerUpdate {
    PermitReleased(SmallVec<[UserPermitKind; 1]>),
    RulesUpdated(user_limiter::RuleUpdate),
}

pub(super) enum AcquireOutcome {
    Acquired(PermitBuilder),
    BlockedOn(ResourceKind),
}

impl ResourceManager {
    pub async fn create<S: LoadLocks + Send + Sync + 'static>(
        storage: S,
        concurrency_limiter: Concurrency,
        global_throttling: Option<GlobalTokenBucket>,
        memory_pool: MemoryPool,
        initial_invocation_memory: NonZeroByteCount,
    ) -> Result<Self, StorageError> {
        let locks = Locks::create(storage).await?;

        let (_tx, rx) = mpsc::unbounded_channel();

        Ok(Self {
            invoker_concurrency: InvokerConcurrencyLimiter::new(concurrency_limiter),
            invoker_throttling: InvokerThrottlingLimiter::new(global_throttling),
            invoker_memory: InvokerMemoryLimiter::new(memory_pool, initial_invocation_memory),
            user_limiter: UserLimiter::create(),
            locks,
            rx,
            tx: _tx,
        })
    }

    /// Removes the vqueue from the resource it's blocked on
    pub(super) fn remove_vqueue(&mut self, handle: VQueueHandle, blocked_resource: &ResourceKind) {
        match blocked_resource {
            ResourceKind::Lock { scope, lock_name } => {
                self.locks.remove_from_waiters(handle, scope, lock_name);
            }
            ResourceKind::InvokerConcurrency => {
                self.invoker_concurrency.remove_from_waiters(handle);
            }
            ResourceKind::InvokerThrottling => {
                self.invoker_throttling.remove_from_waiters(handle);
            }
            ResourceKind::InvokerMemory => {
                self.invoker_memory.remove_from_waiters(handle);
            }
            ResourceKind::DeploymentConcurrency => todo!(),
            ResourceKind::LimitKeyConcurrency {
                scope,
                limit_key,
                blocked_level,
                ..
            } => {
                self.user_limiter
                    .remove_from_waiters(handle, scope, limit_key, *blocked_level);
            }
        }
    }

    /// returns true if queues were woken up
    pub(super) fn release_lock(
        &mut self,
        eligible: &mut EligibilityTracker,
        scope: &Option<Scope>,
        lock_name: &LockName,
    ) -> bool {
        trace!("[release_lock] scope: {scope:?}, lock_name: {lock_name}");

        let Some(queues) = self.locks.release_lock(scope, lock_name) else {
            return false;
        };

        let mut wake_up = false;
        for queue in queues {
            // notify the scheduler that those queues should be woken up.
            wake_up |= eligible.wake_up_queue(queue);
        }
        wake_up
    }

    /// Reverts will release the lock if the user permit has one
    pub(super) fn revert_permit_builder(
        &mut self,
        eligible: &mut EligibilityTracker,
        builder: PermitBuilder,
    ) -> bool {
        let Some(permit) = builder.into_user_permit() else {
            return false;
        };

        let mut wake_up = false;
        // Release the lock if we have one held
        if let Some(lock) = permit.lock
            && let Some(queues) = self.locks.release_lock(&lock.scope, &lock.lock_name)
        {
            wake_up |= eligible.wake_up_queues(queues);
        }

        for resource in permit.resources {
            match resource {
                UserPermitKind::LimitKeyConcurrency(scope, limit_key) => {
                    let woken = self
                        .user_limiter
                        .release_action_concurrency(&scope, &limit_key);
                    wake_up |= eligible.wake_up_queues(woken);
                }
            }
        }

        wake_up
    }

    pub(super) fn poll_acquire_permit(
        &mut self,
        cx: &mut std::task::Context<'_>,
        vqueue: VQueueHandle,
        meta: &VQueueMetaLite,
        key: &EntryKey,
        _metadata: &EntryMetadata,
        current_permit: &mut PermitBuilder,
    ) -> AcquireOutcome {
        if !current_permit.has_user_permit() {
            // we need to acquire user permit first

            // When failing short on resources, we register the queue into the first resource
            // we failed to acquire.
            //
            // Note that it's safe to *check* for resources first and then acquire all of them
            // in one go because we are the sole consumer of resources. We achieve this through
            // via the `ProvisionalPermit` type.
            let mut provisional = ProvisionalPermit::default();

            // if the entry holds a lock already, we don't need to acquire a new one.
            if let Some(lock_name) = meta.lock_name()
                && !key.has_lock()
            {
                // needs to acquire a lock
                if !self.locks.is_locked(meta.scope(), lock_name) {
                    provisional.set_lock(meta.scope().clone(), lock_name.clone());
                } else {
                    self.locks.add_to_waiters(vqueue, meta.scope(), lock_name);
                    return AcquireOutcome::BlockedOn(ResourceKind::Lock {
                        scope: meta.scope().clone(),
                        lock_name: lock_name.clone(),
                    });
                }
            }

            // unscoped entries cannot acquire user limits
            if let Some(scope) = meta.scope() {
                let capacity = self
                    .user_limiter
                    .check_concurrency_capacity(scope, meta.limit_key());
                if let Some((blocked_level, blocked_rule)) = capacity.narrowest_blocked() {
                    trace!(
                        %scope,
                        limit_key = %meta.limit_key(),
                        blocked_at = %blocked_level,
                        details = %capacity.display(&self.user_limiter),
                        "User concurrency limit reached",
                    );
                    self.user_limiter.add_to_waiters(
                        vqueue,
                        scope,
                        meta.limit_key(),
                        blocked_level,
                    );
                    return AcquireOutcome::BlockedOn(ResourceKind::LimitKeyConcurrency {
                        scope: scope.clone(),
                        limit_key: meta.limit_key().clone(),
                        blocked_level,
                        blocked_rule,
                    });
                }

                // Stage the permit — counters are incremented in secure()
                provisional.add_permit(UserPermitKind::LimitKeyConcurrency(
                    scope.clone(),
                    meta.limit_key().clone(),
                ));
            }

            // All user requirements are satisfied.
            current_permit.set_user_permit(provisional.secure(self));
        }

        // System permit
        match key.kind() {
            EntryKind::Unknown => unreachable!("Cannot acquire system permit for unknown entry"),
            EntryKind::Invocation => {
                // this is invocation. It needs an invoker permit + invoker throttling token
                // Do we have one?
                if !current_permit.has_invoker_permit() {
                    // poll for one or die trying
                    let Some(invoker_permit) = self.invoker_concurrency.poll_acquire(cx, vqueue)
                    else {
                        return AcquireOutcome::BlockedOn(ResourceKind::InvokerConcurrency);
                    };
                    current_permit.set_invoker_permit(invoker_permit);
                }

                // If we have the concurrency permit, let's see if we need to wait for throttling
                if !current_permit.has_invoker_throttling_token() {
                    // poll for one or die trying
                    let Some(throttling_token) = self.invoker_throttling.poll_acquire(cx, vqueue)
                    else {
                        return AcquireOutcome::BlockedOn(ResourceKind::InvokerThrottling);
                    };
                    current_permit.set_throttling_permit(throttling_token);
                }
            }
            EntryKind::StateMutation => {
                // I don't need a system permit here.
            }
        }

        AcquireOutcome::Acquired(current_permit.take())
    }

    pub(super) fn poll_resources(
        &mut self,
        cx: &mut std::task::Context<'_>,
        eligible: &mut EligibilityTracker,
    ) {
        // check if we have global resource that can move forward
        // drain as many updates as possible
        while let Poll::Ready(Some(update)) = self.rx.poll_recv(cx) {
            match update {
                ResourceManagerUpdate::PermitReleased(resources) => {
                    for resource in resources {
                        match resource {
                            UserPermitKind::LimitKeyConcurrency(scope, limit_key) => {
                                let woken = self
                                    .user_limiter
                                    .release_action_concurrency(&scope, &limit_key);
                                eligible.wake_up_queues(woken);
                            }
                        }
                    }
                }
                ResourceManagerUpdate::RulesUpdated(update) => {
                    let woken = self.user_limiter.apply_rule_update(update);
                    eligible.wake_up_queues(woken);
                }
            }
        }

        while let Poll::Ready(Some(queue)) = self.invoker_concurrency.poll_head(cx) {
            // wake up this vqueue and shift all other waiters to need poll so
            // they can get a chance to be added to the ready ring if they are eligible and
            // still valid.
            tracing::trace!(
                "waking up vqueue {queue:?} because invoker concurrency permit was acquired"
            );
            eligible.wake_up_queue(queue);
        }
    }
}
