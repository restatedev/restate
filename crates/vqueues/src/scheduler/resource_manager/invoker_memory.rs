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

use restate_memory::{MemoryPool, PollMemoryPool};
use restate_serde_util::NonZeroByteCount;

use crate::scheduler::VQueueHandle;

use super::Waiters;

pub struct InvokerMemoryLimiter {
    /// Memory to reserve per invocation from the memory pool
    // todo make dynamically configurable via configuration
    initial_invocation_memory: NonZeroByteCount,
    memory_limiter: PollMemoryPool,
    waiters: Waiters,
}
impl InvokerMemoryLimiter {
    pub fn new(memory_pool: MemoryPool, initial_invocation_memory: NonZeroByteCount) -> Self {
        Self {
            initial_invocation_memory,
            memory_limiter: PollMemoryPool::new(memory_pool),
            waiters: Default::default(),
        }
    }

    pub(crate) fn remove_from_waiters(&mut self, vqueue: VQueueHandle) {
        self.waiters.retain(|h| *h != vqueue);
    }
}

impl InvokerMemoryLimiter {
    pub fn poll_reserve(&mut self) -> Option<NonZeroByteCount> {
        todo!()
        // // Reserve memory from the invoker's pool. If unavailable,
        // // drop the concurrency permit (it will be re-acquired on the
        // // next poll) and signal blocked-on-memory.
        // let Poll::Ready(memory_lease) =
        //     memory_limiter.poll_reserve(cx, self.initial_invocation_memory.as_usize()) else {
        //
        // };
    }
}
