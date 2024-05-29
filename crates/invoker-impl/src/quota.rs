// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics::gauge;

use crate::metric_definitions::INVOKER_AVAILABLE_SLOTS;

#[derive(Debug)]
pub(super) enum InvokerConcurrencyQuota {
    Unlimited,
    Limited { available_slots: usize },
}

impl InvokerConcurrencyQuota {
    pub(super) fn new(quota: Option<usize>) -> Self {
        match quota {
            Some(available_slots) => Self::Limited { available_slots },
            None => Self::Unlimited,
        }
    }

    pub(super) fn is_slot_available(&self) -> bool {
        match self {
            Self::Unlimited => true,
            Self::Limited { available_slots } => {
                gauge!(INVOKER_AVAILABLE_SLOTS).set(*available_slots as f64);
                *available_slots > 0
            }
        }
    }

    pub(super) fn unreserve_slot(&mut self) {
        match self {
            Self::Unlimited => {}
            Self::Limited { available_slots } => *available_slots += 1,
        }
    }

    pub(super) fn reserve_slot(&mut self) {
        assert!(self.is_slot_available());
        match self {
            Self::Unlimited => {}
            Self::Limited { available_slots } => *available_slots -= 1,
        }
    }
}
