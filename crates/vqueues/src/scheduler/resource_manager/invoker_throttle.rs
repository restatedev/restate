// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use std::pin::Pin;

use tokio::time::Sleep;

use super::Waiters;
use super::permit::ThrottlingToken;
use crate::GlobalTokenBucket;
use crate::scheduler::VQueueHandle;

pub struct InvokerThrottlingLimiter {
    // the global throttling token bucket (shared with other resource managers on this node)
    token_bucket: Option<GlobalTokenBucket>,
    // vqueues currently blocked on invoker capacity
    waiters: Waiters,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl InvokerThrottlingLimiter {
    pub fn new(token_bucket: Option<GlobalTokenBucket>) -> Self {
        Self {
            token_bucket,
            waiters: Default::default(),
            sleep: None,
        }
    }

    pub fn poll_acquire(
        &mut self,
        _cx: &mut std::task::Context<'_>,
        _vqueue: VQueueHandle,
    ) -> Option<ThrottlingToken> {
        // todo: Implementation comes in a later stage
        Some(ThrottlingToken)
    }

    pub fn remove_from_waiters(&mut self, vqueue: VQueueHandle) {
        self.waiters.retain(|h| *h != vqueue);
    }
}
