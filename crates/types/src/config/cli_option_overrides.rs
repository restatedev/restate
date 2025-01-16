// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU64;
use std::path::PathBuf;

use humantime::Duration;
use serde::Serialize;
use serde_with::{serde_as, skip_serializing_none};

use crate::locality::NodeLocation;
use crate::net::{AdvertisedAddress, BindAddress};
use crate::nodes_config::Role;
use crate::PlainNodeId;

use super::LogFormat;

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, clap::Parser, Serialize, Default)]
/// A subset of CommonOptions that can be parsed via the CLI. This **must** remain
/// parse-compatible with CommonOptions.
#[serde(rename_all = "kebab-case")]
pub struct CommonOptionCliOverride {
    /// Defines the roles which this Restate node should run, by default the node
    /// starts with all roles.
    ///
    /// Roles can be comma-separated list without spaces (`--roles=worker,admin,log-server`),
    /// or a repeated option like `--roles=worker --roles=admin`.
    #[clap(long, alias = "role", global = true, value_delimiter=',', num_args = 0..)]
    pub roles: Option<Vec<Role>>,

    /// Unique name for this node in the cluster. The node must not change unless
    /// it's started with empty local store. It defaults to the node hostname.
    #[clap(long, env = "RESTATE_NODE_NAME", global = true)]
    pub node_name: Option<String>,

    /// Node location
    ///
    /// Setting the location allows Restate to form a tree-like cluster topology.
    /// The value is written in the format of "<region>[.zone]" to assign this node
    /// to a specific region, or to a zone within a region.
    ///
    /// The value of region and zone is arbitrary but whitespace and `.` are disallowed.
    ///
    ///
    /// NOTE: It's _strongly_ recommended to not change the node's location string after
    /// its initial registration. Changing the location may result in data loss or data
    /// inconsistency if `log-server` is enabled on this node.
    ///
    /// When this value is not set, the node is considered to be in the _default_ location.
    /// The _default_ location means that the node is not assigned to any specific region or zone.
    ///
    /// ## Examples
    /// - `us-west` -- the node is in the `us-west` region.
    /// - `us-west.a1` -- the node is in the `us-west` region and in the `a1` zone.
    /// - `` -- [default] the node is in the default location
    #[clap(long, alias = "node-location", global = true)]
    pub location: Option<NodeLocation>,

    /// If set, the node insists on acquiring this node ID.
    #[clap(long, global = true)]
    pub force_node_id: Option<PlainNodeId>,

    /// A unique identifier for the cluster. All nodes in the same cluster should
    /// have the same.
    #[clap(long, env = "RESTATE_CLUSTER_NAME", global = true)]
    pub cluster_name: Option<String>,

    /// If true, then a new cluster is bootstrapped. This node *must* have an admin
    /// role and a new nodes configuration will be created that includes this node.
    #[clap(long, global = true)]
    pub allow_bootstrap: Option<bool>,

    /// The working directory which this Restate node should use for relative paths. The default is
    /// `restate-data` under the current working directory.
    #[clap(long, global = true)]
    pub base_dir: Option<PathBuf>,

    /// Address of the metadata store server to bootstrap the node from.
    #[clap(long, global = true)]
    pub metadata_store_address: Option<AdvertisedAddress>,

    /// Address to bind for the Node server. e.g. `0.0.0.0:5122`
    #[clap(long, global = true)]
    pub bind_address: Option<BindAddress>,

    /// Address that other nodes will use to connect to this node. Defaults to use bind_address if
    /// unset. e.g. `http://127.0.0.1:5122/`
    #[clap(long, global = true)]
    pub advertise_address: Option<AdvertisedAddress>,

    /// # Partitions
    ///
    /// Number of partitions that will be provisioned during cluster bootstrap,
    /// partitions used to process messages.
    ///
    /// NOTE: This config entry only impacts the initial number of partitions, the
    /// value of this entry is ignored for bootstrapped nodes/clusters.
    ///
    /// Cannot be higher than `4611686018427387903` (You should almost never need as many partitions anyway)
    #[clap(long, global = true)]
    pub bootstrap_num_partitions: Option<NonZeroU64>,

    /// # Automatically provision number of configured partitions
    ///
    /// If this option is set to `false`, then one needs to manually write a partition table to
    /// the metadata store. Without a partition table, the cluster will not start.
    #[clap(long, global = true)]
    pub auto_provision_partitions: Option<bool>,

    /// This timeout is used when shutting down the various Restate components to drain all the internal queues.
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[clap(long, global = true)]
    pub shutdown_timeout: Option<Duration>,

    /// Tracing Endpoint
    ///
    /// Specify the tracing endpoint to send traces to.
    /// Traces will be exported using [OTLP gRPC](https://opentelemetry.io/docs/specs/otlp/#otlpgrpc)
    /// through [opentelemetry_otlp](https://docs.rs/opentelemetry-otlp/0.12.0/opentelemetry_otlp/).
    #[clap(long, env = "RESTATE_TRACING_ENDPOINT", global = true)]
    pub tracing_endpoint: Option<String>,

    /// Runtime Tracing Endpoint
    ///
    /// Overrides [`Self::tracing_endpoint`] for runtime traces
    ///
    /// Specify the tracing endpoint to send runtime traces to.
    /// Traces will be exported using [OTLP gRPC](https://opentelemetry.io/docs/specs/otlp/#otlpgrpc)
    /// through [opentelemetry_otlp](https://docs.rs/opentelemetry-otlp/0.12.0/opentelemetry_otlp/).
    ///
    /// To configure the sampling, please refer to the [opentelemetry autoconfigure docs](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#sampler).
    #[clap(long, env = "RESTATE_TRACING_RUNTIME_ENDPOINT", global = true)]
    pub tracing_runtime_endpoint: Option<String>,

    /// Services Tracing Endpoint
    ///
    /// Overrides [`Self::tracing_endpoint`] for services traces
    ///
    /// Specify the tracing endpoint to send services traces to.
    /// Traces will be exported using [OTLP gRPC](https://opentelemetry.io/docs/specs/otlp/#otlpgrpc)
    /// through [opentelemetry_otlp](https://docs.rs/opentelemetry-otlp/0.12.0/opentelemetry_otlp/).
    ///
    /// To configure the sampling, please refer to the [opentelemetry autoconfigure docs](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#sampler).
    #[clap(long, env = "RESTATE_TRACING_SERVICES_ENDPOINT", global = true)]
    pub tracing_services_endpoint: Option<String>,

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
    #[clap(long, global = true)]
    pub tracing_json_path: Option<PathBuf>,

    /// # Tracing Filter
    ///
    /// Distributed tracing exporter filter.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    #[clap(long, global = true)]
    pub tracing_filter: Option<String>,

    /// # Logging Filter
    ///
    /// Log filter configuration. Can be overridden by the `RUST_LOG` environment variable.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    #[clap(long, global = true)]
    pub log_filter: Option<String>,

    /// # Logging format
    ///
    /// Format to use when logging.
    #[clap(long, global = true)]
    pub log_format: Option<LogFormat>,

    /// # Disable ANSI in log output
    ///
    /// Disable ANSI terminal codes for logs. This is useful when the log collector doesn't support processing ANSI terminal codes.
    #[clap(long, global = true)]
    pub log_disable_ansi_codes: Option<bool>,
}
