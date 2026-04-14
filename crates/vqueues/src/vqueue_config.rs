// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;

use restate_types::vqueue::VQueueParent;

const UNLIMITED: VQueueConfig = const { VQueueConfig::new() };

#[derive(Default, Clone)]
pub struct ConfigPool {}

impl ConfigPool {
    #[inline]
    pub fn find(&self, _parent: &VQueueParent) -> &VQueueConfig {
        &UNLIMITED
    }
}

#[derive(Default, Clone)]
pub struct VQueueConfig {
    is_paused: bool,
}

impl VQueueConfig {
    pub const UNLIMITED: Self = UNLIMITED;

    pub const fn new() -> Self {
        Self { is_paused: false }
    }

    #[inline]
    pub const fn concurrency(&self) -> Option<NonZeroU32> {
        // hardcoded, forcing all vqueues to act as VOs. This is removed in the next commit
        Some(NonZeroU32::new(1).unwrap())
    }

    #[inline]
    pub const fn is_paused(&self) -> bool {
        self.is_paused
    }

    pub fn start_rate_limit(&self) -> Option<&gardal::Limit> {
        None
    }
}
