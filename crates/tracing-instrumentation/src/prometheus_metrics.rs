// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indexmap::IndexMap;
use metrics_exporter_prometheus::formatting;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::task::AbortHandle;
use tokio::time::MissedTickBehavior;
use tracing::{debug, trace};

use restate_types::config::CommonOptions;

#[derive(Default)]
pub struct Prometheus {
    handle: Option<PrometheusHandle>,
    upkeep_task: Option<AbortHandle>,
    global_labels: IndexMap<String, String>,
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
                global_labels: IndexMap::default(),
            };
        }
        let builder = PrometheusBuilder::default()
            .add_global_label("cluster_name", opts.cluster_name())
            .add_global_label("node_name", opts.node_name())
            .set_quantiles(&[0.5, 0.9, 0.99, 1.0])
            .expect("valid quantiles");

        let recorder = builder.build_recorder();
        let prometheus_handle = recorder.handle();

        // We do not expect this to fail except due to atomic CAS failure
        // which should never happen in practice.
        metrics::set_global_recorder(recorder)
            .expect("no global metrics recorder should be installed");
        Self {
            handle: Some(prometheus_handle),
            upkeep_task: None,
            global_labels: IndexMap::from([
                (
                    "cluster_name".to_owned(),
                    formatting::sanitize_label_value(opts.cluster_name()),
                ),
                (
                    "node_name".to_owned(),
                    formatting::sanitize_label_value(opts.node_name()),
                ),
            ]),
        }
    }

    pub fn handle(&self) -> Option<&PrometheusHandle> {
        self.handle.as_ref()
    }

    pub fn global_labels(&self) -> &IndexMap<String, String> {
        &self.global_labels
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
