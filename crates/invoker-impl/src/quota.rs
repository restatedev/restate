// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use metrics::gauge;

use crate::{
    InvokerId,
    metric_definitions::{ID_LOOKUP, INVOKER_AVAILABLE_SLOTS, INVOKER_CONCURRENCY_LIMIT},
};

#[derive(Debug)]
enum InvokerConcurrencyQuotaInner {
    Unlimited,
    Limited { available_slots: usize },
}

#[derive(Debug)]
pub(super) struct InvokerConcurrencyQuota {
    inner: InvokerConcurrencyQuotaInner,
    invoker_id: InvokerId,
}

impl InvokerConcurrencyQuota {
    pub(super) fn new(invoker_id: impl Into<InvokerId>, quota: Option<NonZeroUsize>) -> Self {
        let invoker_id = invoker_id.into();
        let inner = match quota {
            Some(available_slots) => {
                gauge!(INVOKER_CONCURRENCY_LIMIT, "invoker_id" => ID_LOOKUP.get(invoker_id))
                    .set(available_slots.get() as f64);
                gauge!(INVOKER_AVAILABLE_SLOTS, "invoker_id" => ID_LOOKUP.get(invoker_id))
                    .set(available_slots.get() as f64);
                InvokerConcurrencyQuotaInner::Limited {
                    available_slots: available_slots.get(),
                }
            }
            None => {
                gauge!(INVOKER_CONCURRENCY_LIMIT, "invoker_id" => ID_LOOKUP.get(invoker_id))
                    .set(f64::INFINITY);
                gauge!(INVOKER_AVAILABLE_SLOTS, "invoker_id" => ID_LOOKUP.get(invoker_id))
                    .set(f64::INFINITY);
                InvokerConcurrencyQuotaInner::Unlimited
            }
        };

        Self { inner, invoker_id }
    }

    pub(super) fn is_slot_available(&self) -> bool {
        match &self.inner {
            InvokerConcurrencyQuotaInner::Unlimited => true,
            InvokerConcurrencyQuotaInner::Limited { available_slots } => *available_slots > 0,
        }
    }

    pub(super) fn unreserve_slot(&mut self) {
        match &mut self.inner {
            InvokerConcurrencyQuotaInner::Unlimited => {}
            InvokerConcurrencyQuotaInner::Limited { available_slots } => {
                *available_slots += 1;
                gauge!(
                    INVOKER_AVAILABLE_SLOTS,
                    "invoker_id" =>
                    ID_LOOKUP.get(self.invoker_id)
                )
                .set(*available_slots as f64);
            }
        }
    }

    pub(super) fn reserve_slot(&mut self) {
        assert!(self.is_slot_available());
        match &mut self.inner {
            InvokerConcurrencyQuotaInner::Unlimited => {}
            InvokerConcurrencyQuotaInner::Limited { available_slots } => {
                *available_slots -= 1;
                gauge!(
                    INVOKER_AVAILABLE_SLOTS,
                    "invoker_id" =>
                    ID_LOOKUP.get(self.invoker_id)
                )
                .set(*available_slots as f64);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn available_slots(&self) -> usize {
        match self.inner {
            InvokerConcurrencyQuotaInner::Unlimited => usize::MAX,
            InvokerConcurrencyQuotaInner::Limited { available_slots } => available_slots,
        }
    }
}
