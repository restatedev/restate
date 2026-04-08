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
        Arc,
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

/// Bundles the available-slots atomic with the released-slots counter
/// so they can be passed around as a single unit.
#[derive(Debug)]
struct LimitedSlots {
    available_slots: AtomicUsize,
    released_counter: Counter,
}

#[derive(Debug)]
enum InvokerConcurrencyQuotaInner {
    Unlimited,
    Limited {
        slots: Arc<LimitedSlots>,
        acquired_counter: Counter,
    },
}

#[derive(Debug)]
pub(super) struct InvokerConcurrencyQuota {
    inner: InvokerConcurrencyQuotaInner,
}

impl InvokerConcurrencyQuota {
    pub(super) fn new(invoker_id: impl Into<InvokerId>, quota: Option<NonZeroUsize>) -> Self {
        let partition_id_str = ID_LOOKUP.get(invoker_id.into());

        let inner = match quota {
            Some(available_slots) => {
                gauge!(INVOKER_CONCURRENCY_LIMIT, "invoker_id" => partition_id_str, "partition_id" => partition_id_str)
                    .set(available_slots.get() as f64);

                let acquired_counter = counter!(INVOKER_CONCURRENCY_SLOTS_ACQUIRED, "invoker_id" => partition_id_str, "partition_id" => partition_id_str);
                let released_counter = counter!(INVOKER_CONCURRENCY_SLOTS_RELEASED, "invoker_id" => partition_id_str, "partition_id" => partition_id_str);

                InvokerConcurrencyQuotaInner::Limited {
                    slots: Arc::new(LimitedSlots {
                        available_slots: AtomicUsize::new(available_slots.get()),
                        released_counter,
                    }),
                    acquired_counter,
                }
            }
            None => {
                gauge!(INVOKER_CONCURRENCY_LIMIT, "invoker_id" => partition_id_str, "partition_id" => partition_id_str)
                    .set(f64::INFINITY);

                InvokerConcurrencyQuotaInner::Unlimited
            }
        };

        Self { inner }
    }

    pub(super) fn is_slot_available(&self) -> bool {
        match &self.inner {
            InvokerConcurrencyQuotaInner::Unlimited => true,
            InvokerConcurrencyQuotaInner::Limited { slots, .. } => {
                slots.available_slots.load(Ordering::Relaxed) > 0
            }
        }
    }

    /// Acquires a concurrency slot from the quota.
    ///
    /// # Panics
    /// Panics if no slot is available. Callers must check [`is_slot_available`](Self::is_slot_available)
    /// before calling this method.
    pub(super) fn acquire_slot(&mut self) -> ConcurrencySlot {
        assert!(self.is_slot_available());

        match &mut self.inner {
            InvokerConcurrencyQuotaInner::Unlimited => ConcurrencySlot { inner: None },
            InvokerConcurrencyQuotaInner::Limited {
                slots,
                acquired_counter,
            } => {
                let prev = slots.available_slots.fetch_sub(1, Ordering::Relaxed);

                // sanity check: This should never happen since we assert that a slot is available
                // while holding an exclusive reference
                assert!(prev != 0, "Called acquire_slot with no slots available");

                acquired_counter.increment(1);
                ConcurrencySlot {
                    inner: Some(Arc::clone(slots)),
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn available_slots(&self) -> usize {
        match &self.inner {
            InvokerConcurrencyQuotaInner::Unlimited => usize::MAX,
            InvokerConcurrencyQuotaInner::Limited { slots, .. } => {
                slots.available_slots.load(Ordering::Relaxed)
            }
        }
    }
}

/// An acquired concurrency slot.
///
/// # Important
/// Must only be dropped inside the Invoker's event loop. Slot availability
/// drives the `select!` arms, and dropping a slot externally won't wake the
/// loop to observe the change.
#[derive(derive_more::Debug)]
#[debug("ConcurrencySlot({})", inner.is_some())]
pub struct ConcurrencySlot {
    inner: Option<Arc<LimitedSlots>>,
}

impl ConcurrencySlot {
    #[cfg(test)]
    pub fn empty() -> Self {
        Self { inner: None }
    }
}

impl Drop for ConcurrencySlot {
    fn drop(&mut self) {
        if let Some(store) = self.inner.take() {
            store.released_counter.increment(1);
            store.available_slots.fetch_add(1, Ordering::Relaxed);
        }
    }
}
