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
mod limiter;
mod locks;
mod permit;

use self::invoker_memory::InvokerMemoryLimiter;
use self::limiter::UserLimiter;
pub use self::permit::{PermitBuilder, ReservedResources};

use std::collections::VecDeque;
use std::task::Poll;

use smallvec::SmallVec;
use tokio::sync::mpsc;

use restate_futures_util::concurrency::Concurrency;
use restate_memory::{MemoryPool, NonZeroByteCount};
use restate_storage_api::StorageError;
use restate_storage_api::lock_table::LoadLocks;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{EntryKind, VQueueEntry};
use restate_types::{LockName, Scope};

use self::invoker::InvokerConcurrencyLimiter;
use self::invoker_throttle::InvokerThrottlingLimiter;
use self::locks::Locks;
use self::permit::{ProvisionalPermit, UserPermitKind};
use super::VQueueHandle;
use super::eligible::EligibilityTracker;
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
    #[allow(dead_code)]
    user_limiter: UserLimiter,
    // todo: counters.
    rx: mpsc::UnboundedReceiver<ResourceManagerUpdate>,
    // We need to keep this alive to:
    // - Keep the receiver alive even if we don't have any resource permits handed out
    tx: mpsc::UnboundedSender<ResourceManagerUpdate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceKind {
    /// Waiting to acquire a lock of a VO.
    Lock {
        scope: Option<Scope>,
        lock_name: LockName,
    },
    /// Waiting to acquire invoker concurrency capacity
    InvokerConcurrency,
    /// Waiting to acquire invoker being throttled
    InvokerThrottling,
    /// Invoker needs to allocate memory for an invocation
    InvokerMemory,
    /// Waiting for deployment-level concurrency tokens to be available
    DeploymentConcurrency,
    /// Waiting for user-defined concurrency to be acquired
    LimitKeyConcurrency,
}

enum ResourceManagerUpdate {
    PermitReleased(SmallVec<[UserPermitKind; 1]>),
    // todo: RulesUpdated
}

pub(super) enum AcquireOutcome {
    Acquired(ReservedResources),
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
            user_limiter: UserLimiter::default(),
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
            ResourceKind::LimitKeyConcurrency => todo!(),
        }
    }

    /// returns true if queues were woken up
    pub(super) fn release_lock(
        &mut self,
        eligible: &mut EligibilityTracker,
        scope: &Option<Scope>,
        lock_name: &LockName,
    ) -> bool {
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

    pub(super) fn poll_acquire_permit<E: VQueueEntry>(
        &mut self,
        cx: &mut std::task::Context<'_>,
        vqueue: VQueueHandle,
        meta: &VQueueMeta,
        entry: &E,
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
                && !entry.has_lock()
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

            // All user requirements are satisfied.
            current_permit.set_user_permit(provisional.secure(self));
        }

        // System permit
        match entry.kind() {
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
            EntryKind::StateMutation | EntryKind::Unknown => {
                // I don't need a system permit here.
            }
        }

        AcquireOutcome::Acquired(current_permit.take(self))
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
                ResourceManagerUpdate::PermitReleased(permits) => {
                    // 1. reclaim resources
                    // 2. collect the vqueues that need to be woken up
                    for permit in permits {
                        match permit {
                            UserPermitKind::LimitKeyConcurrency(_limit_key) => {
                                todo!("pending implementation of hierarchical counters")
                            }
                        }
                    }
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
