// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(unused)]

use std::fmt;
use std::hash::Hash;
use std::sync::LazyLock;

use dashmap::{DashMap, Entry};
use metrics::{Unit, describe_counter};

// ---------------------------------------------------------------------------
// Metric label interning utilities
//
// These caches convert runtime values into `&'static str` for use as metric
// labels, avoiding per-emission allocations. Values are leaked and never freed.
//
// **Only use for low-cardinality dimensions** (node IDs, partition IDs,
// service names, deployment IDs, HTTP status codes). High-cardinality keys
// (request IDs, trace IDs, user-supplied strings) will leak unbounded memory.
// ---------------------------------------------------------------------------

/// Intern cache that maps values to their `Display` representation as leaked
/// `&'static str`. Cache hit = hash lookup, no formatting. Cache miss = format
/// once, leak, store.
pub struct LazyIntern<K: Hash + Eq> {
    cache: LazyLock<DashMap<K, &'static str>>,
}

impl<K: Hash + Eq + fmt::Display + Clone> LazyIntern<K> {
    pub const fn new() -> Self {
        Self {
            cache: LazyLock::new(DashMap::new),
        }
    }

    #[inline]
    pub fn get(&self, key: &K) -> &'static str {
        if let Some(entry) = self.cache.get(key) {
            return entry.value();
        }
        match self.cache.entry(key.clone()) {
            Entry::Occupied(entry) => entry.get(),
            Entry::Vacant(entry) => {
                let s: &'static str = entry.key().to_string().leak();
                entry.insert(s).value()
            }
        }
    }
}

// value of label `kind` in TC_SPAWN are defined in [`crate::TaskKind`].
pub const TC_SPAWN: &str = "restate.task_center.spawned.total";
pub const TC_FINISHED: &str = "restate.task_center.finished.total";

// values of label `status` in METRICS
pub const STATUS_COMPLETED: &str = "completed";
pub const STATUS_FAILED: &str = "failed";

pub fn describe_metrics() {
    describe_counter!(
        TC_SPAWN,
        Unit::Count,
        "Total tasks spawned by the task center"
    );

    describe_counter!(
        TC_FINISHED,
        Unit::Count,
        "Number of tasks that finished with 'status'"
    );
}
