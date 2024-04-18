// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::str::FromStr;

use enumset::EnumSet;
use humantime::Duration;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::net::{AdvertisedAddress, BindAddress};
use crate::nodes_config::Role;
use crate::PlainNodeId;

use super::{AwsOptions, HttpOptions, RocksDbOptions};

const DEFAULT_STORAGE_DIRECTORY: &str = "restate-data";

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct CommonOptions {
    /// Defines the roles which this Restate node should run, by default the node
    /// starts with all roles.
    #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
    pub roles: EnumSet<Role>,

    /// # Node Name
    ///
    /// Unique name for this node in the cluster. The node must not change unless
    /// it's started with empty local store. It defaults to the node's hostname.
    node_name: Option<String>,

    /// If set, the node insists on acquiring this node ID.
    pub force_node_id: Option<PlainNodeId>,

    /// # Cluster Name
    ///
    /// A unique identifier for the cluster. All nodes in the same cluster should
    /// have the same.
    cluster_name: String,

    /// If true, then a new cluster is bootstrapped. This node *must* have an admin
    /// role and a new nodes configuration will be created that includes this node.
    pub allow_bootstrap: bool,

    /// The working directory which this Restate node should use for relative paths. The default is
    /// `restate-data` under the current working directory.
    #[builder(setter(strip_option))]
    base_dir: Option<PathBuf>,

    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    /// Address of the metadata store server to bootstrap the node from.
    pub metadata_store_address: AdvertisedAddress,

    /// Address to bind for the Node server. Default is `0.0.0.0:5122`
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub bind_address: BindAddress,

    /// Address that other nodes will use to connect to this node. Default is `http://127.0.0.1:5122/`
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub advertised_address: AdvertisedAddress,

    /// # Shutdown grace timeout
    ///
    /// This timeout is used when shutting down the various Restate components to drain all the internal queues.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub shutdown_timeout: Duration,

    /// # Default async runtime thread pool
    ///
    /// Size of the default thread pool used to perform internal tasks.
    /// If not set, it defaults to the number of CPU cores.
    #[builder(setter(strip_option))]
    pub default_thread_pool_size: Option<usize>,

    /// # Tracing Endpoint
    ///
    /// Specify the tracing endpoint to send traces to.
    /// Traces will be exported using [OTLP gRPC](https://opentelemetry.io/docs/specs/otlp/#otlpgrpc)
    /// through [opentelemetry_otlp](https://docs.rs/opentelemetry-otlp/0.12.0/opentelemetry_otlp/).
    ///
    /// To configure the sampling, please refer to the [opentelemetry autoconfigure docs](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#sampler).
    pub tracing_endpoint: Option<String>,

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
    pub tracing_json_path: Option<String>,

    /// # Tracing Filter
    ///
    /// Distributed tracing exporter filter.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    pub tracing_filter: String,
    /// # Logging Filter
    ///
    /// Log filter configuration. Can be overridden by the `RUST_LOG` environment variable.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    pub log_filter: String,

    /// # Logging format
    ///
    /// Format to use when logging.
    pub log_format: LogFormat,

    /// # Disable ANSI in log output
    ///
    /// Disable ANSI terminal codes for logs. This is useful when the log collector doesn't support processing ANSI terminal codes.
    pub log_disable_ansi_codes: bool,

    /// Timeout for idle histograms.
    ///
    /// The duration after which a histogram is considered idle and will be removed from
    /// metric responses to save memory. Unsetting means that histograms will never be removed.
    #[serde(with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub histogram_inactivity_timeout: Option<humantime::Duration>,

    #[serde(flatten)]
    pub service_client: ServiceClientOptions,

    /// Disable prometheus metric recording and reporting. Default is `false`.
    pub disable_prometheus: bool,

    /// # Total memory limit for rocksdb caches and memtables. This includes memory
    /// for uncompressed block cache and all memtables by all open databases.
    ///
    /// The memory size used for rocksdb caches. Default is 4GB.
    pub rocksdb_total_memory_limit: u64,

    /// # Rocksdb total memtable size limit
    ///
    /// The memory size used across all memtables. This limits how much memory
    /// memtables can eat up from the value in rocksdb_total_memory_limit. When
    /// set to 0, memtables can take all available memory up to the value specified
    /// in rocksdb_total_memory_limit.
    ///
    /// Default is 0
    pub rocksdb_total_memtables_size_limit: u64,

    /// # Rocksdb Background Threads
    ///
    /// The number of threads to reserve to Rocksdb background tasks. Defaults to the number of
    /// cores on the machine.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_bg_threads: Option<NonZeroU32>,

    /// # Rocksdb High Priority Background Threads
    ///
    /// The number of threads to reserve to high priority Rocksdb background tasks.
    /// Defaults to 2.
    pub rocksdb_high_priority_bg_threads: NonZeroU32,

    /// RocksDb base settings and memory limits that get applied on every database
    #[serde(flatten)]
    pub rocksdb: RocksDbOptions,
}

static HOSTNAME: Lazy<String> = Lazy::new(|| {
    hostname::get()
        .map(|h| h.into_string().expect("hostname is valid unicode"))
        .unwrap_or("INVALID_HOSTANAME".to_owned())
});

impl CommonOptions {
    pub fn shutdown_grace_period(&self) -> std::time::Duration {
        self.shutdown_timeout.into()
    }
    // todo: It's imperative that the node doesn't change its name after start. Move this to a
    // Once lock to ensure it doesn't change over time, even if the physical hostname changes.
    pub fn node_name(&self) -> &str {
        self.node_name.as_ref().unwrap_or(&HOSTNAME)
    }

    // same as node_name
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }
    pub fn base_dir(&self) -> PathBuf {
        self.base_dir.clone().unwrap_or_else(|| {
            std::env::current_dir()
                .unwrap()
                .join(DEFAULT_STORAGE_DIRECTORY)
        })
    }

    pub fn rocksdb_bg_threads(&self) -> NonZeroU32 {
        self.rocksdb_bg_threads.unwrap_or(
            std::thread::available_parallelism()
                .unwrap_or(NonZeroUsize::new(3).unwrap())
                .try_into()
                .expect("number of cpu cores fits in u32"),
        )
    }
}

impl Default for CommonOptions {
    fn default() -> Self {
        Self {
            roles: EnumSet::all(),
            node_name: None,
            force_node_id: None,
            cluster_name: "localcluster".to_owned(),
            // boot strap the cluster by default. This is very likely to change in the future to be
            // false by default. For now, this is true to make the converged deployment backward
            // compatible and easy for users.
            allow_bootstrap: true,
            base_dir: None,
            metadata_store_address: "http://127.0.0.1:5123"
                .parse()
                .expect("valid metadata store address"),
            bind_address: "0.0.0.0:5122".parse().unwrap(),
            advertised_address: AdvertisedAddress::from_str("http://127.0.0.1:5122/").unwrap(),
            histogram_inactivity_timeout: None,
            disable_prometheus: false,
            service_client: Default::default(),
            shutdown_timeout: std::time::Duration::from_secs(60).into(),
            tracing_endpoint: None,
            tracing_json_path: None,
            tracing_filter: "info".to_owned(),
            log_filter: "warn,restate=info".to_string(),
            log_format: Default::default(),
            log_disable_ansi_codes: false,
            default_thread_pool_size: None,
            rocksdb_total_memtables_size_limit: 0,
            rocksdb_total_memory_limit: 4_000_000_000, // 4GB
            rocksdb_bg_threads: None,
            rocksdb_high_priority_bg_threads: NonZeroU32::new(2).unwrap(),
            rocksdb: Default::default(),
        }
    }
}

/// # Service Client options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(rename = "ServiceClientOptions", default)
)]
#[builder(default)]
#[derive(Default)]
#[serde(rename_all = "kebab-case")]
pub struct ServiceClientOptions {
    #[serde(flatten)]
    pub http: HttpOptions,
    #[serde(flatten)]
    pub lambda: AwsOptions,

    /// # Request identity private key PEM file
    ///
    /// A path to a file, such as "/var/secrets/key.pem", which contains exactly one ed25519 private
    /// key in PEM format. Such a file can be generated with `openssl genpkey -algorithm ed25519`.
    /// If provided, this key will be used to attach JWTs to requests from this client which
    /// SDKs may optionally verify, proving that the caller is a particular Restate instance.
    ///
    /// This file is currently only read on client creation, but this may change in future.
    /// Parsed public keys will be logged at INFO level in the same format that SDKs expect.
    pub request_identity_private_key_pem_file: Option<PathBuf>,
}

/// # Log format
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[derive(Debug, Clone, Copy, Hash, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum LogFormat {
    /// # Pretty
    ///
    /// Enables verbose logging. Not recommended in production.
    #[default]
    Pretty,
    /// # Compact
    ///
    /// Enables compact logging.
    Compact,
    /// # Json
    ///
    /// Enables json logging. You can use a json log collector to ingest these logs and further process them.
    Json,
}
