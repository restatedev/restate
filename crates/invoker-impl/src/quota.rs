// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

use metrics::{Counter, counter, gauge};

use crate::{
    InvokerId,
    metric_definitions::{
        ID_LOOKUP, INVOKER_CONCURRENCY_LIMIT, INVOKER_CONCURRENCY_SLOTS_ACQUIRED,
        INVOKER_CONCURRENCY_SLOTS_RELEASED,
    },
};

#[derive(Debug)]
enum InvokerConcurrencyQuotaInner {
    Unlimited,
    Limited { available_slots: Arc<AtomicUsize> },
}

#[derive(Debug)]
pub(super) struct InvokerConcurrencyQuota {
    inner: InvokerConcurrencyQuotaInner,
    acquired_counter: Counter,
    released_counter: Counter,
}

impl InvokerConcurrencyQuota {
    pub(super) fn new(invoker_id: impl Into<InvokerId>, quota: Option<NonZeroUsize>) -> Self {
        let invoker_id = invoker_id.into();
        let acquired_counter =
            counter!(INVOKER_CONCURRENCY_SLOTS_ACQUIRED, "invoker_id" => ID_LOOKUP.get(invoker_id));
        let released_counter =
            counter!(INVOKER_CONCURRENCY_SLOTS_RELEASED, "invoker_id" => ID_LOOKUP.get(invoker_id));

        let inner = match quota {
            Some(available_slots) => {
                gauge!(INVOKER_CONCURRENCY_LIMIT, "invoker_id" => ID_LOOKUP.get(invoker_id))
                    .set(available_slots.get() as f64);

                InvokerConcurrencyQuotaInner::Limited {
                    available_slots: Arc::new(AtomicUsize::new(available_slots.get())),
                }
            }
            None => {
                gauge!(INVOKER_CONCURRENCY_LIMIT, "invoker_id" => ID_LOOKUP.get(invoker_id))
                    .set(f64::INFINITY);

                InvokerConcurrencyQuotaInner::Unlimited
            }
        };

        Self {
            inner,
            acquired_counter,
            released_counter,
        }
    }

    pub(super) fn is_slot_available(&self) -> bool {
        match &self.inner {
            InvokerConcurrencyQuotaInner::Unlimited => true,
            InvokerConcurrencyQuotaInner::Limited { available_slots } => {
                // Acquire: ensures we observe slot releases (`fetch_add` with `Release`)
                // performed by `ConcurrencySlot::drop`, which may run on a different
                // thread/task (e.g. when an invocation task is dropped).
                available_slots.load(Ordering::Acquire) > 0
            }
        }
    }

    pub(super) fn acquire_slot(&mut self) -> ConcurrencySlot {
        assert!(self.is_slot_available());

        match &mut self.inner {
            InvokerConcurrencyQuotaInner::Unlimited => ConcurrencySlot {
                count: 0,
                available_slots: Weak::new(),
                released_counter: self.released_counter.clone(),
            },
            InvokerConcurrencyQuotaInner::Limited { available_slots } => {
                // Relaxed: `acquire_slot` requires `&mut self`, so no concurrent acquires
                // are possible. The only concurrent modification is `ConcurrencySlot::drop`
                // (fetch_add with Release), which we synchronize with via the Acquire load
                // in `is_slot_available` above.
                let prev = available_slots.fetch_sub(1, Ordering::Relaxed);

                // sanity check: This should never happen since we assert that a slot is available
                // while holding an exclusive reference
                assert!(prev != 0, "Called acquire_slot with no slots available");

                self.acquired_counter.increment(1);
                ConcurrencySlot {
                    count: 1,
                    available_slots: Arc::downgrade(available_slots),
                    released_counter: self.released_counter.clone(),
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn available_slots(&self) -> usize {
        match &self.inner {
            InvokerConcurrencyQuotaInner::Unlimited => usize::MAX,
            InvokerConcurrencyQuotaInner::Limited { available_slots } => {
                available_slots.load(Ordering::Acquire)
            }
        }
    }
}

#[derive(derive_more::Debug)]
#[debug("ConcurrencySlot({count})")]
pub struct ConcurrencySlot {
    count: usize,
    available_slots: Weak<AtomicUsize>,
    released_counter: Counter,
}

impl ConcurrencySlot {
    #[cfg(test)]
    pub fn empty() -> Self {
        Self {
            count: 0,
            available_slots: Weak::new(),
            released_counter: Counter::noop(),
        }
    }
}

impl Drop for ConcurrencySlot {
    fn drop(&mut self) {
        if self.count == 0 {
            return;
        }

        // to make sure metrics don't diverge, we increment the counter even if the
        // available slots atomic no longer exists
        self.released_counter.increment(1);
        if let Some(store) = self.available_slots.upgrade() {
            // Release: pairs with the Acquire load in `is_slot_available` to ensure
            // the slot release is visible to the invoker loop when it checks for
            // available capacity.
            store.fetch_add(self.count, Ordering::Release);
        }
    }
}
