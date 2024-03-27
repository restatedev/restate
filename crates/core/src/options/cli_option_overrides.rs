// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use humantime::Duration;
use serde::Serialize;
use serde_with::serde_as;

use restate_types::net::{AdvertisedAddress, BindAddress};
use restate_types::nodes_config::Role;
use restate_types::PlainNodeId;

use super::LogFormat;

#[serde_as]
#[derive(Debug, clap::Parser, Serialize, Default)]
/// A subset of CommonOptions that can be parsed via the CLI. This **must** remain
/// parse-compatible with CommonOptions.
pub struct CommonOptionCliOverride {
    /// Defines the roles which this Restate node should run, by default the node
    /// starts with all roles.
    #[clap(long, alias = "role")]
    #[serde(skip_serializing_if = "Option::is_none")]
    roles: Option<Vec<Role>>,

    /// Unique name for this node in the cluster. The node must not change unless
    /// it's started with empty local store. It defaults to the node hostname.
    #[clap(long, env = "RESTATE_NODE_NAME")]
    #[serde(skip_serializing_if = "Option::is_none")]
    node_name: Option<String>,

    /// If set, the node insists on acquiring this node ID.
    #[clap(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<PlainNodeId>,

    /// A unique identifier for the cluster. All nodes in the same cluster should
    /// have the same.
    #[clap(long, env = "RESTATE_CLUSTER_NAME")]
    #[serde(skip_serializing_if = "Option::is_none")]
    cluster_name: Option<String>,

    /// If true, then a new cluster is bootstrapped. This node *must* be has an admin
    /// role and a new nodes configuration will be created that includes this node.
    #[clap(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    bootstrap_cluster: Option<bool>,

    /// Address of the metadata store server to bootstrap the node from.
    #[clap(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_store_address: Option<AdvertisedAddress>,

    /// Address to bind for the Node server. e.g. `0.0.0.0:5122`
    #[clap(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bind_address: Option<BindAddress>,

    /// Address that other nodes will use to connect to this node. Defaults to use bind_address if
    /// unset. e.g. `http://127.0.0.1:5122/`
    #[clap(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advertise_address: Option<AdvertisedAddress>,

    /// This timeout is used when shutting down the various Restate components to drain all the internal queues.
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[clap(long)]
    shutdown_grace_period: Option<Duration>,

    /// # Tracing Endpoint
    ///
    /// Specify the tracing endpoint to send traces to.
    /// Traces will be exported using [OTLP gRPC](https://opentelemetry.io/docs/specs/otlp/#otlpgrpc)
    /// through [opentelemetry_otlp](https://docs.rs/opentelemetry-otlp/0.12.0/opentelemetry_otlp/).
    #[clap(long, env = "RESTATE_TRACING_ENDPOINT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    tracing_endpoint: Option<String>,

    /// # Distributed Tracing JSON Export Path
    ///
    /// If set, an exporter will be configured to write traces to files using the Jaeger JSON format.
    /// Each trace file will start with the `trace` prefix.
    ///
    /// If unset, no traces will be written to file.
    ///
    /// It can be used to export traces in a structured format without configuring a Jaeger agent.
    ///
    /// To inspect the traces, open the Jaeger UI and use the Upload JSON feature to load and inspect them.
    #[clap(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    tracing_json_path: Option<PathBuf>,

    /// # Tracing Filter
    ///
    /// Distributed tracing exporter filter.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    #[clap(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    tracing_filter: Option<String>,
    /// # Logging Filter
    ///
    /// Log filter configuration. Can be overridden by the `RUST_LOG` environment variable.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    #[clap(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_filter: Option<String>,

    /// # Logging format
    ///
    /// Format to use when logging.
    #[clap(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_format: Option<LogFormat>,

    /// # Disable ANSI in log output
    ///
    /// Disable ANSI terminal codes for logs. This is useful when the log collector doesn't support processing ANSI terminal codes.
    #[clap(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_disable_ansi_codes: Option<bool>,
}
