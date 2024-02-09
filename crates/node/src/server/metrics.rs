// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_tracing_context::TracingContextLayer;
use metrics_util::{layers::Layer, MetricKindMask};

use crate::server::Options;

/// The set of labels that are allowed to be extracted from tracing context to be used in metrics.
/// Be mindful when adding new labels, the number of time series(es) is directly propotional
/// to cardinality of the chosen labels. Avoid using labels with potential high cardinality
/// as much as possible (e.g. `restate.invocation.id`)
static ALLOWED_LABELS: &[&str] = &[
    "partition_id",
    "rpc.method",
    "rpc.service",
    "command",
    "service",
];

pub(crate) fn install_global_prometheus_recorder(opts: &Options) -> PrometheusHandle {
    let builder = PrometheusBuilder::default()
        // Remove a metric from registry if it was not updated for that duration
        .idle_timeout(
            MetricKindMask::HISTOGRAM,
            opts.histogram_inactivity_timeout.map(Into::into),
        );
    let recorder = builder.build_recorder();
    let prometheus_handle = recorder.handle();
    let recorder = TracingContextLayer::only_allow(ALLOWED_LABELS).layer(recorder);

    // We do not expect this to fail except due to atomic CAS failure
    // which should never happen in practice.
    metrics::set_global_recorder(recorder).expect("no global metrics recorder should be installed");
    prometheus_handle
}
