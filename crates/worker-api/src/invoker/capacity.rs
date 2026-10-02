// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use metrics::{Counter, counter, gauge};

use restate_futures_util::concurrency::{Concurrency, PermitObserver};
use restate_memory::{MemoryPool, NonZeroByteCount};
use restate_types::config::{DEFAULT_PER_INVOCATION_INITIAL_MEMORY, ThrottlingOptions};

use crate::metric_definitions::{
    INVOKER_CONCURRENCY_LIMIT, INVOKER_CONCURRENCY_SLOTS_ACQUIRED,
    INVOKER_CONCURRENCY_SLOTS_RELEASED, describe_metrics,
};

/// Publishes acquisitions and releases of invoker concurrency slots as metrics.
struct ConcurrencyMetrics {
    acquired: Counter,
    released: Counter,
}

impl PermitObserver for ConcurrencyMetrics {
    fn on_acquire(&self, permits: u32) {
        self.acquired.increment(u64::from(permits));
    }

    fn on_release(&self, permits: u32) {
        self.released.increment(u64::from(permits));
    }
}

pub type TokenBucket<C = gardal::TokioClock> = gardal::SharedTokenBucket<C>;

#[derive(Clone)]
pub struct InvokerCapacity {
    pub concurrency: Concurrency,
    pub invocation_token_bucket: Option<TokenBucket>,
    pub action_token_bucket: Option<TokenBucket>,
    pub memory_pool: MemoryPool,
    /// Outbound initial memory in bytes reserved from the memory pool per invocation.
    pub initial_invocation_memory: NonZeroByteCount,
}

impl InvokerCapacity {
    pub const fn new_unlimited() -> Self {
        Self {
            concurrency: Concurrency::new_unlimited(),
            invocation_token_bucket: None,
            action_token_bucket: None,
            memory_pool: MemoryPool::unlimited(),
            initial_invocation_memory: DEFAULT_PER_INVOCATION_INITIAL_MEMORY,
        }
    }

    pub fn new(
        concurrency: Option<NonZeroUsize>,
        invocation_throttling: Option<&ThrottlingOptions>,
        action_throttling: Option<&ThrottlingOptions>,
        memory_pool: MemoryPool,
        initial_invocation_memory: NonZeroByteCount,
    ) -> Self {
        describe_metrics();
        gauge!(INVOKER_CONCURRENCY_LIMIT)
            .set(concurrency.map_or(f64::INFINITY, |limit| limit.get() as f64));

        Self {
            concurrency: Concurrency::with_observer(
                concurrency,
                ConcurrencyMetrics {
                    acquired: counter!(INVOKER_CONCURRENCY_SLOTS_ACQUIRED),
                    released: counter!(INVOKER_CONCURRENCY_SLOTS_RELEASED),
                },
            ),
            invocation_token_bucket: invocation_throttling.map(|opts| {
                TokenBucket::new(gardal::Limit::from(opts.clone()), gardal::TokioClock)
            }),
            action_token_bucket: action_throttling.map(|opts| {
                TokenBucket::new(gardal::Limit::from(opts.clone()), gardal::TokioClock)
            }),
            memory_pool,
            initial_invocation_memory,
        }
    }
}
