// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, OnceLock, RwLock};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;

use metrics_exporter_prometheus::formatting;
use restate_types::{PlainNodeId, config::CommonOptions};
use tokio::task::AbortHandle;
use tokio::time::MissedTickBehavior;
use tracing::{debug, trace};

use crate::GLOBAL_PROMETHEUS;

#[derive(Clone, Debug, Default)]
pub struct Prometheus {
    handle: Option<PrometheusHandle>,
    upkeep_task: Option<AbortHandle>,
    global_labels: Arc<RwLock<Vec<String>>>,
    node_id_initialized: OnceLock<bool>,
}

impl Prometheus {
    /// Creates and installs a global records unless prometheus is explicitly disabled in
    /// configuration.
    ///
    /// Note that this *does not* start the upkeep task, the caller should call
    /// `start_upkeep_task()` from within a tokio runtime.
    pub fn install(opts: &CommonOptions) -> Self {
        if opts.disable_prometheus {
            return Self {
                handle: None,
                upkeep_task: None,
                global_labels: Arc::new(RwLock::new(vec![])),
                node_id_initialized: OnceLock::new(),
            };
        }
        let builder = PrometheusBuilder::default()
            // Remove a metric from registry if it was not updated for that duration
            .idle_timeout(
                MetricKindMask::HISTOGRAM,
                opts.histogram_inactivity_timeout.map(Into::into),
            )
            .add_global_label("cluster_name", opts.cluster_name())
            .add_global_label("node_name", opts.node_name());
        let recorder = builder.build_recorder();
        let prometheus_handle = recorder.handle();

        // We do not expect this to fail except due to atomic CAS failure
        // which should never happen in practice.
        metrics::set_global_recorder(recorder)
            .expect("no global metrics recorder should be installed");
        let prometheus = Self {
            handle: Some(prometheus_handle),
            upkeep_task: None,
            global_labels: Arc::new(RwLock::new(vec![
                format!(
                    "cluster_name=\"{}\"",
                    formatting::sanitize_label_value(opts.cluster_name())
                ),
                format!(
                    "node_name=\"{}\"",
                    formatting::sanitize_label_value(opts.node_name())
                ),
            ])),
            node_id_initialized: OnceLock::new(),
        };

        GLOBAL_PROMETHEUS
            .set(prometheus.clone())
            .expect("no global metrics recorder should be installed");

        prometheus
    }

    pub fn handle(&self) -> Option<&PrometheusHandle> {
        self.handle.as_ref()
    }

    /// Obtain a copy of global labels for metric lines that get generated outside of Prometheus
    pub fn global_labels(&self) -> Vec<String> {
        self.global_labels.read().unwrap().clone()
    }

    /// Since the node_id may not be known when the metrics recorder is installed early on node
    /// start, this method allows us to inject it when it becomes available without delaying metrics
    /// startup.
    pub fn set_node_id(&self, node_id: PlainNodeId) {
        self.node_id_initialized.get_or_init(|| {
            let node_id_str = node_id.to_string();
            if let Some(recorder) = &self.handle {
                self.global_labels.write().unwrap().push(format!(
                    "node_id=\"{}\"",
                    formatting::sanitize_label_value(&node_id_str)
                ));
                recorder.add_global_label("node_id", node_id_str);
            }
            true
        });
    }

    /// Starts the upkeep task. Should typically be run once, but it'll abort
    /// current task if it's already running.
    pub fn start_upkeep_task(&mut self) {
        // aborts current task if any
        self.stop_upkeep_task();
        if let Some(prometheus_handle) = self.handle.clone() {
            self.upkeep_task = Some(
                tokio::task::Builder::new()
                    .name("prometheus-upkeep")
                    .spawn(async move {
                        debug!("Prometheus metrics upkeep loop started");

                        let mut update_interval =
                            tokio::time::interval(std::time::Duration::from_secs(5));
                        update_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

                        loop {
                            update_interval.tick().await;
                            trace!("Performing Prometheus metrics upkeep...");
                            prometheus_handle.run_upkeep();
                        }
                    })
                    .expect("No tokio runtime")
                    .abort_handle(),
            );
        }
    }

    /// Stops the upkeep task if it's running.
    pub fn stop_upkeep_task(&mut self) {
        if let Some(upkeep_task) = self.upkeep_task.take() {
            upkeep_task.abort();
        }
    }
}

impl Drop for Prometheus {
    fn drop(&mut self) {
        self.stop_upkeep_task();
    }
}
