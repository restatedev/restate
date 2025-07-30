// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use metrics::gauge;
use tokio::runtime::RuntimeMetrics;

use restate_types::SharedString;

use super::Handle;

pub trait TaskCenterMonitoring {
    fn default_runtime_metrics(&self) -> RuntimeMetrics;

    fn managed_runtime_metrics(&self) -> Vec<(SharedString, RuntimeMetrics)>;

    /// How long has the task-center been running?
    fn age(&self) -> Duration;

    /// Submit telemetry for all runtimes to metrics recorder
    fn submit_metrics(&self);
}

impl TaskCenterMonitoring for Handle {
    fn default_runtime_metrics(&self) -> RuntimeMetrics {
        self.inner.default_runtime_handle.metrics()
    }

    fn managed_runtime_metrics(&self) -> Vec<(SharedString, RuntimeMetrics)> {
        let guard = self.inner.managed_runtimes.lock();
        guard
            .iter()
            .map(|(k, v)| (k.clone(), v.runtime_handle().metrics()))
            .collect()
    }

    /// How long has the task-center been running?
    fn age(&self) -> Duration {
        self.inner.start_time.elapsed()
    }

    /// Submit telemetry for all runtimes to metrics recorder
    fn submit_metrics(&self) {
        submit_runtime_metrics("default", self.default_runtime_metrics());

        // Partition processor runtimes
        let processor_runtimes = self.managed_runtime_metrics();
        for (task_name, metrics) in processor_runtimes {
            submit_runtime_metrics(task_name, metrics);
        }
    }
}

fn submit_runtime_metrics(runtime: impl Into<SharedString>, stats: RuntimeMetrics) {
    let runtime = runtime.into();
    gauge!("restate.tokio.num_workers", "runtime" => runtime.clone())
        .set(stats.num_workers() as f64);
    gauge!("restate.tokio.blocking_threads", "runtime" => runtime.clone())
        .set(stats.num_blocking_threads() as f64);
    gauge!("restate.tokio.blocking_queue_depth", "runtime" => runtime.clone())
        .set(stats.blocking_queue_depth() as f64);
    gauge!("restate.tokio.num_alive_tasks", "runtime" => runtime.clone())
        .set(stats.num_alive_tasks() as f64);
    gauge!("restate.tokio.io_driver_ready_count", "runtime" => runtime.clone())
        .set(stats.io_driver_ready_count() as f64);
    gauge!("restate.tokio.remote_schedule_count", "runtime" => runtime.clone())
        .set(stats.remote_schedule_count() as f64);
    // per worker stats
    for idx in 0..stats.num_workers() {
        gauge!("restate.tokio.worker_overflow_count", "runtime" => runtime.clone(), "worker" =>
            idx.to_string())
        .set(stats.worker_overflow_count(idx) as f64);
        gauge!("restate.tokio.worker_poll_count", "runtime" => runtime.clone(), "worker" => idx.to_string())
            .set(stats.worker_poll_count(idx) as f64);
        gauge!("restate.tokio.worker_park_count", "runtime" => runtime.clone(), "worker" => idx.to_string())
            .set(stats.worker_park_count(idx) as f64);
        gauge!("restate.tokio.worker_noop_count", "runtime" => runtime.clone(), "worker" => idx.to_string())
            .set(stats.worker_noop_count(idx) as f64);
        gauge!("restate.tokio.worker_steal_count", "runtime" => runtime.clone(), "worker" => idx.to_string())
            .set(stats.worker_steal_count(idx) as f64);
        gauge!("restate.tokio.worker_total_busy_duration_seconds", "runtime" => runtime.clone(), "worker" => idx.to_string())
            .set(stats.worker_total_busy_duration(idx).as_secs_f64());
        gauge!("restate.tokio.worker_mean_poll_time", "runtime" => runtime.clone(), "worker" => idx.to_string())
            .set(stats.worker_mean_poll_time(idx).as_secs_f64());
    }
}
