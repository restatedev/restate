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

use restate_futures_util::concurrency::Concurrency;
use restate_memory::MemoryPool;
use restate_types::config::ThrottlingOptions;

pub type TokenBucket<C = gardal::TokioClock> = gardal::SharedTokenBucket<C>;

/// Size of the inbound seed lease (~2 HTTP/2 frames).
pub const INBOUND_SEED_SIZE: usize = 64 * 1024;

/// Size of the outbound seed lease. Zero for now; will be non-zero once
/// outbound budget is pushed into the storage layer.
pub const OUTBOUND_SEED_SIZE: usize = 0;

/// Combined seed size reserved from the memory pool per invocation.
pub const SEED_SIZE: usize = INBOUND_SEED_SIZE + OUTBOUND_SEED_SIZE;

#[derive(Clone)]
pub struct InvokerCapacity {
    pub concurrency: Concurrency,
    pub invocation_token_bucket: Option<TokenBucket>,
    pub action_token_bucket: Option<TokenBucket>,
    pub memory_pool: MemoryPool,
}

impl InvokerCapacity {
    pub const fn new_unlimited() -> Self {
        Self {
            concurrency: Concurrency::new_unlimited(),
            invocation_token_bucket: None,
            action_token_bucket: None,
            memory_pool: MemoryPool::unlimited(),
        }
    }

    pub fn new(
        concurrency: Option<NonZeroUsize>,
        invocation_throttling: Option<&ThrottlingOptions>,
        action_throttling: Option<&ThrottlingOptions>,
        memory_pool: MemoryPool,
    ) -> Self {
        Self {
            concurrency: Concurrency::new(concurrency),
            invocation_token_bucket: invocation_throttling.map(|opts| {
                TokenBucket::new(gardal::Limit::from(opts.clone()), gardal::TokioClock)
            }),
            action_token_bucket: action_throttling.map(|opts| {
                TokenBucket::new(gardal::Limit::from(opts.clone()), gardal::TokioClock)
            }),
            memory_pool,
        }
    }
}
