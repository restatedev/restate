// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub struct InvokerCapacity {
    inner: Arc<Inner>,
}

// hierarchical semaphores
struct Inner {
    // map: HashMap<ServiceId, Semaphore>,
    used: AtomicU64,
    concurrency_limit: Option<NonZeroU32>,
}

impl InvokerCapacity {
    pub fn new(concurrency_limit: Option<NonZeroU32>) -> Self {
        // let simple = Semaphore::new(concurrency_limit.unwrap_or(0));
        Self {
            inner: Arc::new(Inner {
                // simple,
                used: AtomicU64::new(0),
                concurrency_limit,
            }),
        }
    }

    pub fn try_acquire_for(&self, service_id: &str) -> Option<Token> {
        Some(Token {
            inner: Arc::downgrade(&self.inner),
        })
    }

    pub fn reserve_slot(&self) -> Token {
        // todo: garbage implementation
        self.inner
            .used
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Token {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl Inner {
    pub fn release_one(&self) {
        self.used.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct Token {
    inner: Weak<Inner>,
}

impl Drop for Token {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.release_one();
        }
    }
}
