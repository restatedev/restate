// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use dashmap::{DashMap, Entry};
/// Optional to have but adds description/help message to the metrics emitted to
/// the metrics' sink.
use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

/// Lazily initialized cache that maps "partition" ids to their string representation for metric labels,
/// avoiding fresh string allocations whenever a partition id is used as a metric dimension.
pub(crate) static ID_LOOKUP: LazyLock<IdLookup> = LazyLock::new(IdLookup::new);

pub(crate) struct IdLookup {
    index: Vec<&'static str>,
    extra: DashMap<u16, &'static str>,
}

impl IdLookup {
    const SIZE: u16 = 512;

    fn new() -> Self {
        let mut index = Vec::with_capacity(Self::SIZE as usize);
        for i in 0..Self::SIZE {
            let s: &'static str = i.to_string().leak();
            index.push(s);
        }

        Self {
            index,
            extra: DashMap::new(),
        }
    }

    #[inline]
    pub fn get(&'static self, id: impl Into<u16>) -> &'static str {
        let id = id.into();
        if id < Self::SIZE {
            // fast access
            return self.index[id as usize];
        }

        // We are running more that SIZE partitions
        // it's a slower path but still better than doing a .to_string()
        // on each metric
        let entry = self.extra.entry(id);

        match entry {
            Entry::Occupied(entry) => entry.get(),
            Entry::Vacant(entry) => {
                let s: &'static str = id.to_string().leak();
                entry.insert(s).value()
            }
        }
    }
}

pub const INVOKER_ENQUEUE: &str = "restate.invoker.enqueue.total";
pub const INVOKER_INVOCATION_TASKS: &str = "restate.invoker.invocation_tasks.total";
pub const INVOKER_AVAILABLE_SLOTS: &str = "restate.invoker.available_slots";
pub const INVOKER_CONCURRENCY_LIMIT: &str = "restate.invoker.concurrency_limit";
pub const INVOKER_TASK_DURATION: &str = "restate.invoker.task_duration.seconds";

pub const TASK_OP_STARTED: &str = "started";
pub const TASK_OP_SUSPENDED: &str = "suspended";
pub const TASK_OP_FAILED: &str = "failed";
pub const TASK_OP_COMPLETED: &str = "completed";

pub(crate) fn describe_metrics() {
    describe_counter!(
        INVOKER_ENQUEUE,
        Unit::Count,
        "Number of invocations that were added to the queue"
    );

    describe_gauge!(
        INVOKER_CONCURRENCY_LIMIT,
        Unit::Count,
        "Concurrency limit (slots) for invoker tasks"
    );

    describe_counter!(
        INVOKER_INVOCATION_TASKS,
        Unit::Count,
        "Invocation task operation"
    );

    describe_gauge!(
        INVOKER_AVAILABLE_SLOTS,
        Unit::Count,
        "Number of available slots to create new tasks"
    );

    describe_histogram!(
        INVOKER_TASK_DURATION,
        Unit::Seconds,
        "Time taken to complete an invoker task"
    );
}
