// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::hash::Hash;
use std::sync::LazyLock;

use bytestring::ByteString;
use dashmap::{DashMap, Entry};
use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};

use restate_types::identifiers::DeploymentId;

/// Lazily initialized cache that maps "partition" ids to their string representation for metric labels,
/// avoiding fresh string allocations whenever a partition id is used as a metric dimension.
pub(crate) static ID_LOOKUP: LazyLock<IdLookup> = LazyLock::new(IdLookup::new);
pub(crate) static STR_LOOKUP: LazyIntern<ByteString> = LazyIntern::new();
pub(crate) static UUID_LOOKUP: LazyIntern<DeploymentId> = LazyIntern::new();
static STATUS_CODE_LOOKUP: LazyIntern<u16> = LazyIntern::new();

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

/// Lazily initialized intern cache that maps values to their `Display` string representation
/// as `&'static str`. Cache hit = hash comparison only, no formatting. Cache miss = format
/// once via `Display`, leak, store. Bounded by the number of distinct values seen at runtime.
pub(crate) struct LazyIntern<K: Hash + Eq> {
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

        let entry = self.cache.entry(key.clone());
        match entry {
            Entry::Occupied(entry) => entry.get(),
            Entry::Vacant(entry) => {
                let s: &'static str = entry.key().to_string().leak();
                entry.insert(s).value()
            }
        }
    }
}

/// Cached interned label values for zero-allocation metric emissions.
/// Built incrementally: `partition_id` and `service_name` set at task start,
/// `deployment_id` set when `PinnedDeployment` is received (empty until then).
/// When `deployment_id` is empty, it is omitted from emitted labels to avoid
/// cardinality splits in Prometheus.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ServiceMetrics {
    pub partition_id: &'static str,
    pub service_name: &'static str,
    pub deployment_id: &'static str,
}

impl ServiceMetrics {
    pub fn new(partition_id: &'static str, service_name: &'static str) -> Self {
        Self {
            partition_id,
            service_name,
            deployment_id: "",
        }
    }

    pub fn gauge(&self, name: &'static str) -> metrics::Gauge {
        if self.deployment_id.is_empty() {
            gauge!(name,
                "partition_id" => self.partition_id,
                "service_name" => self.service_name,
            )
        } else {
            gauge!(name,
                "partition_id" => self.partition_id,
                "service_name" => self.service_name,
                "deployment_id" => self.deployment_id,
            )
        }
    }

    pub fn counter(&self, name: &'static str) -> metrics::Counter {
        if self.deployment_id.is_empty() {
            counter!(name,
                "partition_id" => self.partition_id,
                "service_name" => self.service_name,
            )
        } else {
            counter!(name,
                "partition_id" => self.partition_id,
                "service_name" => self.service_name,
                "deployment_id" => self.deployment_id,
            )
        }
    }

    pub fn histogram(&self, name: &'static str) -> metrics::Histogram {
        if self.deployment_id.is_empty() {
            histogram!(name,
                "partition_id" => self.partition_id,
                "service_name" => self.service_name,
            )
        } else {
            histogram!(name,
                "partition_id" => self.partition_id,
                "service_name" => self.service_name,
                "deployment_id" => self.deployment_id,
            )
        }
    }

    pub fn http_status_counter(&self, status_code: u16) -> metrics::Counter {
        let code = STATUS_CODE_LOOKUP.get(&status_code);
        if self.deployment_id.is_empty() {
            counter!(INVOKER_HTTP_STATUS_CODE,
                "partition_id" => self.partition_id,
                "service_name" => self.service_name,
                "status_code" => code,
            )
        } else {
            counter!(INVOKER_HTTP_STATUS_CODE,
                "partition_id" => self.partition_id,
                "service_name" => self.service_name,
                "deployment_id" => self.deployment_id,
                "status_code" => code,
            )
        }
    }

    pub fn task(&self, status: &'static str) -> TaskMetrics {
        TaskMetrics {
            partition_id: self.partition_id,
            service_name: self.service_name,
            status,
        }
    }

    #[cfg(test)]
    pub const EMPTY: Self = Self {
        partition_id: "",
        service_name: "",
        deployment_id: "",
    };
}

/// Task lifecycle metrics: partition + service + status (no deployment_id).
#[derive(Debug, Clone, Copy)]
pub(crate) struct TaskMetrics {
    partition_id: &'static str,
    service_name: &'static str,
    status: &'static str,
}

impl TaskMetrics {
    pub fn counter(&self) -> metrics::Counter {
        counter!(INVOKER_INVOCATION_TASKS,
            "status" => self.status,
            "partition_id" => self.partition_id,
            "service_name" => self.service_name,
        )
    }

    pub fn failed_counter(&self, transient: bool) -> metrics::Counter {
        counter!(INVOKER_INVOCATION_TASKS,
            "status" => self.status,
            "transient" => if transient { "true" } else { "false" },
            "partition_id" => self.partition_id,
            "service_name" => self.service_name,
        )
    }
}

pub const INVOKER_ENQUEUE: &str = "restate.invoker.enqueue.total";
pub const INVOKER_INVOCATION_TASKS: &str = "restate.invoker.invocation_tasks.total";
pub const INVOKER_CONCURRENCY_SLOTS_ACQUIRED: &str = "restate.invoker.concurrency_slots.acquired";
pub const INVOKER_CONCURRENCY_SLOTS_RELEASED: &str = "restate.invoker.concurrency_slots.released";
pub const INVOKER_CONCURRENCY_LIMIT: &str = "restate.invoker.concurrency_limit";
pub const INVOKER_TASK_DURATION: &str = "restate.invoker.task_duration.seconds";
pub const INVOKER_EAGER_STATE_TRUNCATED: &str = "restate.invoker.eager_state_truncated.total";
pub const INVOKER_THROTTLE_BALANCE: &str = "restate.invoker.throttle_balance";
pub const INVOKER_QUEUE_DURATION: &str = "restate.invoker.queue_duration.seconds";
pub const INVOKER_ACTIVE_INVOCATIONS: &str = "restate.invoker.active_invocations";
pub const INVOKER_HTTP_REQUEST_DURATION: &str = "restate.invoker.http_request_duration.seconds";
pub const INVOKER_HTTP_TOTAL_DURATION: &str = "restate.invoker.http_total_duration.seconds";
pub const INVOKER_HTTP_STATUS_CODE: &str = "restate.invoker.http_status_code.total";

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

    describe_counter!(
        INVOKER_CONCURRENCY_SLOTS_ACQUIRED,
        Unit::Count,
        "Number of concurrency slots acquired"
    );

    describe_counter!(
        INVOKER_CONCURRENCY_SLOTS_RELEASED,
        Unit::Count,
        "Number of concurrency slots released"
    );

    describe_histogram!(
        INVOKER_TASK_DURATION,
        Unit::Seconds,
        "Time taken to complete an invoker task"
    );

    describe_counter!(
        INVOKER_EAGER_STATE_TRUNCATED,
        Unit::Count,
        "Number of invocations where eager state was truncated due to size limit"
    );

    describe_gauge!(
        INVOKER_THROTTLE_BALANCE,
        Unit::Count,
        "Invocation token bucket balance, recorded on acquire/release. Negative = throttled."
    );

    describe_histogram!(
        INVOKER_QUEUE_DURATION,
        Unit::Seconds,
        "Wall-clock time from invocation enqueue to task start"
    );

    describe_gauge!(
        INVOKER_ACTIVE_INVOCATIONS,
        Unit::Count,
        "Current in-flight invocations per service/deployment"
    );

    describe_histogram!(
        INVOKER_HTTP_REQUEST_DURATION,
        Unit::Seconds,
        "Time-to-first-byte (TTFB): HTTP request send to response headers received"
    );

    describe_histogram!(
        INVOKER_HTTP_TOTAL_DURATION,
        Unit::Seconds,
        "Full HTTP request duration including response body streaming"
    );

    describe_counter!(
        INVOKER_HTTP_STATUS_CODE,
        Unit::Count,
        "Count of non-200 HTTP status codes returned by deployments"
    );
}
