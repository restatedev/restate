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

use restate_futures_util::concurrency::Concurrency;
use restate_types::config::ThrottlingOptions;

pub type TokenBucket<C = gardal::TokioClock> =
    gardal::TokenBucket<gardal::PaddedAtomicSharedStorage, C>;

/// Marker type used to identify invoker's capacity tokens
pub struct InvokerToken;

#[derive(Clone)]
pub struct InvokerCapacity {
    pub concurrency: Concurrency<InvokerToken>,
    pub invocation_token_bucket: Option<TokenBucket>,
    pub action_token_bucket: Option<TokenBucket>,
}

impl InvokerCapacity {
    pub const fn new_unlimited() -> Self {
        Self {
            concurrency: Concurrency::new_unlimited(),
            invocation_token_bucket: None,
            action_token_bucket: None,
        }
    }

    pub fn new(
        concurrency: Option<NonZeroUsize>,
        invocation_throttling: Option<&ThrottlingOptions>,
        action_throttling: Option<&ThrottlingOptions>,
    ) -> Self {
        Self {
            concurrency: Concurrency::new(concurrency),
            invocation_token_bucket: invocation_throttling.map(|opts| {
                let limit = gardal::Limit::from(opts.clone());
                let capacity = limit.burst();
                let bucket = TokenBucket::from_parts(limit, gardal::TokioClock::default());
                bucket.add_tokens(capacity.get());
                bucket
            }),
            action_token_bucket: action_throttling.map(|opts| {
                let limit = gardal::Limit::from(opts.clone());
                let capacity = limit.burst();
                let bucket = TokenBucket::from_parts(limit, gardal::TokioClock::default());
                bucket.add_tokens(capacity.get());
                bucket
            }),
        }
    }
}
