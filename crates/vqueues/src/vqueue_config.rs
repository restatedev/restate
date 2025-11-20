// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use std::num::NonZeroU32;

use hashbrown::HashMap;

use restate_types::rate::Rate;
use restate_types::vqueue::VQueueParent;

static UNLIMITED: VQueueConfig = const { VQueueConfig::new() };
static SINGLETON: VQueueConfig = const {
    VQueueConfig {
        is_paused: false,
        concurrency: Some(NonZeroU32::new(1).unwrap()),
        capacity: None,
        rate_limit: None,
    }
};

#[derive(Default, Clone)]
pub struct ConfigPool {
    vqueues: HashMap<VQueueParent, VQueueConfig>,
}

impl ConfigPool {
    #[inline]
    pub fn find(&self, parent: &VQueueParent) -> &VQueueConfig {
        if parent == &VQueueParent::SYSTEM_UNLIMITED {
            &UNLIMITED
        } else if parent == &VQueueParent::SYSTEM_SINGLETON {
            &SINGLETON
        } else {
            // it's safe to fallback to singleton instead of unlimited if this parent
            // is unknown
            self.vqueues.get(parent).unwrap_or(&SINGLETON)
        }
    }

    #[inline]
    pub fn insert(&mut self, parent: VQueueParent, config: VQueueConfig) {
        self.vqueues.insert(parent, config);
    }

    #[inline]
    pub fn concurrency(&self, parent: &VQueueParent) -> Option<NonZeroU32> {
        let parent = self.vqueues.get(parent)?;
        parent.concurrency()
    }

    #[inline]
    pub fn capacity(&self, parent: &VQueueParent) -> Option<NonZeroU32> {
        let parent = self.vqueues.get(parent)?;
        parent.capacity()
    }

    #[inline]
    pub fn rate_limit(&self, parent: &VQueueParent) -> Option<Rate> {
        let parent = self.vqueues.get(parent)?;
        parent.rate_limit()
    }

    #[inline]
    pub fn is_paused(&self, parent: &VQueueParent) -> bool {
        self.vqueues
            .get(parent)
            .map(|c| c.is_paused())
            .unwrap_or(false)
    }
}

#[derive(Default, Clone)]
pub struct VQueueConfig {
    is_paused: bool,
    concurrency: Option<NonZeroU32>,
    capacity: Option<NonZeroU32>,
    rate_limit: Option<Rate>,
}

impl VQueueConfig {
    pub const fn new() -> Self {
        Self {
            concurrency: None,
            capacity: None,
            rate_limit: None,
            is_paused: false,
        }
    }

    #[inline]
    pub const fn concurrency(&self) -> Option<NonZeroU32> {
        self.concurrency
    }

    #[inline]
    pub const fn capacity(&self) -> Option<NonZeroU32> {
        self.capacity
    }

    #[inline]
    pub const fn rate_limit(&self) -> Option<Rate> {
        self.rate_limit
    }

    #[inline]
    pub const fn is_paused(&self) -> bool {
        self.is_paused
    }
}
