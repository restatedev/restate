// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::IpAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::bail;
use enumset::EnumSet;
use paste::paste;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_serde_util::SerdeableHeaderHashMap;
use restate_util_bytecount::NonZeroByteCount;
use restate_util_time::{FriendlyDuration, NonZeroFriendlyDuration};
use tracing::warn;

use super::{
    CPU_COUNT, DEFAULT_MESSAGE_SIZE_LIMIT, GossipOptions, InvalidConfigurationError,
    ObjectStoreOptions, PerfStatsLevel, RocksDbOptions,
};
use crate::PlainNodeId;
use crate::config::dynamodb_store::DynamoDbOptions;
use crate::config::{DeprecatedServiceClientOptions, NetworkingOptions};
use crate::locality::NodeLocation;
use crate::net::address::{AdvertisedAddress, ListenerPort};
use crate::net::address::{BindAddress, FabricPort, TokioConsolePort};
use crate::net::listener::AddressBook;
use crate::nodes_config::Role;
use crate::replication::ReplicationProperty;
use crate::retries::RetryPolicy;

const MIN_ROCKSDB_MEMORY: NonZeroByteCount =
    NonZeroByteCount::new(NonZeroUsize::new(256 * 1024 * 1024).unwrap());

const MIN_MEMTABLE_TOTAL_BUDGET: NonZeroByteCount =
    NonZeroByteCount::new(NonZeroUsize::new(32 * 1024 * 1024).unwrap());

const DEFAULT_STORAGE_DIRECTORY: &str = "restate-data";

static HOSTNAME: LazyLock<String> = LazyLock::new(|| {
    hostname::get()
        .map(|h| h.into_string().expect("hostname is valid unicode"))
        .unwrap_or_else(|_| "INVALID_HOSTANAME".to_owned())
});

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[cfg_attr(feature = "clap", clap(rename_all = "kebab-case"))]
pub enum ListenMode {
    /// Exclusively listen on unix domain sockets
    ///
    /// If set, all services will listen exclusively on unix sockets, each service
    /// will create a socket file under the data directory.
    Unix,
    /// Exclusively listen on TCP sockets
    Tcp,
    /// [default] Listen on both Unix and TCP sockets
    #[default]
    All,
}

impl ListenMode {
    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    pub fn is_tcp_enabled(&self) -> bool {
        matches!(self, Self::Tcp | Self::All)
    }

    pub fn is_uds_enabled(&self) -> bool {
        matches!(self, Self::Unix | Self::All)
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct ListenerOptions<P: ListenerPort + 'static> {
    /// Use random ports instead of the default port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) use_random_ports: Option<bool>,

    /// Listen on unix-sockets, TCP sockets, or both.
    ///
    /// The default is to listen on both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) listen_mode: Option<ListenMode>,

    /// Hostname to advertise for this service
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) advertised_host: Option<String>,

    /// Local interface IP address to listen on
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bind_ip: Option<IpAddr>,

    /// Network port to listen on
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bind_port: Option<u16>,

    /// The combination of `bind-ip` and `bind-port` that will be used to bind
    ///
    /// This has precedence over `bind-ip` and `bind-port`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bind_address: Option<BindAddress<P>>,

    /// Address that other nodes will use to connect to this service.
    ///
    /// The full prefix that will be used to advertise this service publicly.
    /// For example, if this is set to `https://my-host` then others will use this
    /// as base URL to connect to this service.
    ///
    /// If unset, the advertised address will be inferred from public address of this node
    /// or it'll use the value supplied in `advertised-host` if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    advertised_address: Option<AdvertisedAddress<P>>,
}

impl<P: ListenerPort + 'static> ListenerOptions<P> {
    /// Assumes the input is some "common" base that we want to use if our current
    /// value is not set.
    pub fn merge<O: ListenerPort>(&mut self, other: &ListenerOptions<O>) {
        // Notes:
        // - We don't inherit the advertised address.
        // - We don't inherit the port
        if self.use_random_ports.is_none() && other.use_random_ports.is_some() {
            self.use_random_ports = other.use_random_ports;
        }

        if self.listen_mode.is_none() && other.listen_mode.is_some() {
            self.listen_mode = other.listen_mode;
        }

        if self.bind_ip.is_none() && other.bind_ip.is_some() {
            self.bind_ip = other.bind_ip;
        }

        if self.advertised_host.is_none() && self.advertised_address.is_none() {
            self.advertised_host.clone_from(&other.advertised_host);
        }
    }

    pub fn listen_mode(&self) -> ListenMode {
        self.listen_mode.unwrap_or_default()
    }

    pub fn bind_address(&self) -> BindAddress<P> {
        self.bind_address.clone().unwrap_or_else(|| {
            BindAddress::from_parts(
                self.bind_ip,
                self.bind_port,
                self.use_random_ports.unwrap_or(false),
            )
        })
    }

    pub fn advertised_address(&self, address_book: &AddressBook) -> AdvertisedAddress<P> {
        self.advertised_address.clone().unwrap_or_else(|| {
            address_book.guess_advertised_address(self.advertised_host.as_deref())
        })
    }
}

impl<P: ListenerPort> Default for ListenerOptions<P> {
    fn default() -> Self {
        Self {
            use_random_ports: None,
            listen_mode: None,
            advertised_host: None,
            bind_ip: None,
            bind_port: None,
            bind_address: None,
            advertised_address: None,
        }
    }
}

/// TLS mode for fabric inter-node communication.
///
/// The modes are ordered for safe rolling enablement (and rollback): certificate
/// distribution is decoupled from advertising TLS, which is decoupled from
/// requiring it. Roll the cluster forward one step at a time:
/// `off` → `allow` → `prefer` → `require`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum TlsMode {
    /// TLS is disabled. Certificates are not loaded and the node behaves as if
    /// `[tls]` were absent. Allows staging the TLS configuration on all nodes
    /// before activating it.
    #[default]
    Off,
    /// Certificates are loaded; both TLS and plaintext connections are
    /// accepted, but the node still advertises a plaintext (`http://`)
    /// address. Peers that have not loaded TLS configuration yet can still
    /// connect to it — and it can be dialed by every node in the cluster.
    Allow,
    /// Both TLS and plaintext connections are accepted, and the node
    /// advertises an `https://` address so peers connect with TLS. Only move
    /// here once all nodes are at least in `allow` mode.
    Prefer,
    /// Only TLS connections are accepted; plaintext is rejected. Only move
    /// here once all nodes are in `prefer` mode.
    Require,
}

impl TlsMode {
    /// Certificates are loaded and the TLS acceptor/connector machinery is active.
    pub fn is_enabled(&self) -> bool {
        !matches!(self, TlsMode::Off)
    }

    /// The node advertises an `https://` fabric address.
    pub fn advertises_tls(&self) -> bool {
        matches!(self, TlsMode::Prefer | TlsMode::Require)
    }

    /// Plaintext connections are accepted alongside TLS.
    pub fn accepts_plaintext(&self) -> bool {
        !matches!(self, TlsMode::Require)
    }
}

/// TLS configuration for fabric inter-node communication.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub struct FabricTlsOptions {
    /// TLS enforcement mode: `off`, `allow`, `prefer`, or `require`.
    /// Default: `off`. See [`TlsMode`] for the rolling-enablement sequence.
    #[serde(default)]
    pub mode: TlsMode,

    /// Path to the PEM-encoded server certificate.
    pub cert_file: PathBuf,

    /// Path to the PEM-encoded private key.
    pub key_file: PathBuf,

    /// Paths to PEM-encoded CA certificates for verifying peer certificates.
    pub ca_files: Vec<PathBuf>,

    /// Require clients to present a valid certificate (mTLS). Default: `false`.
    #[serde(default = "default_require_client_auth")]
    pub require_client_auth: bool,

    /// How often to reload certificates from disk. Default: `1h`.
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval: NonZeroFriendlyDuration,

    /// Allowed subject names on peer certificates. After mTLS authentication
    /// succeeds, the peer certificate's Subject Common Name (CN) and Subject
    /// Alternative Names (DNS names and URIs) are checked against these patterns.
    /// Supports `*` glob wildcards (e.g., `spiffe://domain/*`, `restate-*`).
    ///
    /// Required when `require-client-auth` is `true`. Use `["*"]` to explicitly
    /// allow any authenticated peer (CA-only trust). An empty list is a
    /// configuration error to prevent accidental fail-open.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_subject_names: Vec<String>,
}

impl FabricTlsOptions {
    pub fn validate(&self) -> Result<(), anyhow::Error> {
        if self.require_client_auth && self.allowed_subject_names.is_empty() {
            anyhow::bail!(
                "[tls] require-client-auth is true but allowed-subject-names is empty. \
                 Specify allowed patterns (e.g., [\"spiffe://domain/*\"]) or set [\"*\"] \
                 to explicitly allow any authenticated peer."
            );
        }
        Ok(())
    }

    pub fn validate_advertised_address(
        &self,
        address: &AdvertisedAddress<FabricPort>,
    ) -> anyhow::Result<()> {
        match self.mode {
            TlsMode::Off | TlsMode::Allow => Ok(()),
            TlsMode::Prefer => {
                let Some(uri) = address.uri() else {
                    warn!(
                        "Tls mode is set to prefer, while the advertised address is a unix socket. TLS is currently not supported for unix sockets."
                    );
                    return Ok(());
                };
                if uri.scheme() != Some(&http::uri::Scheme::HTTPS) {
                    warn!(
                        "Advertised address '{address}' is not HTTPS, but TLS is in 'prefer' mode. Nodes will attempt to connect to this node in plaintext instead."
                    );
                }
                Ok(())
            }
            TlsMode::Require => {
                let Some(uri) = address.uri() else {
                    bail!(
                        "Tls mode is set to required, while the advertised address is a unix socket. TLS is currently not supported for unix sockets."
                    );
                };
                if uri.scheme() != Some(&http::uri::Scheme::HTTPS) {
                    bail!(
                        "Advertised address '{address}' is not an HTTPS address, but TLS is in 'require' mode. Please either advertise an HTTPS address in the config or loosen the TLS mode."
                    );
                }
                Ok(())
            }
        }
    }
}

fn default_require_client_auth() -> bool {
    false
}

fn default_refresh_interval() -> NonZeroFriendlyDuration {
    NonZeroFriendlyDuration::from_secs_unchecked(3600)
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct CommonOptions {
    /// Defines the roles which this Restate node should run, by default the node
    /// starts with all roles.
    #[cfg_attr(
        feature = "schemars",
        schemars(schema_with = "schema::enumset_role_schema")
    )]
    pub roles: EnumSet<Role>,

    /// # Node Name
    ///
    /// Unique name for this node in the cluster. The node must not change unless
    /// it's started with empty local store. It defaults to the node's hostname.
    pub(super) node_name: Option<String>,

    #[serde(flatten)]
    pub(super) fabric_listener_options: ListenerOptions<FabricPort>,

    /// # TLS Configuration
    ///
    /// Optional TLS/mTLS configuration for inter-node fabric communication.
    /// When set, the fabric port uses TLS for both inbound and outbound connections.
    /// Without this section, fabric communication remains plaintext (default behavior).
    ///
    /// Since v1.7.3
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    tls: Option<FabricTlsOptions>,

    /// # Node Location
    ///
    /// Setting the location allows Restate to form a tree-like cluster topology.
    /// The value is written in the format of "region[.zone]" to assign this node
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
    /// Examples
    /// - `us-west` -- the node is in the `us-west` region.
    /// - `us-west.a1` -- the node is in the `us-west` region and in the `a1` zone.
    /// - `` -- [default] the node is in the default location
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    #[builder(setter(strip_option))]
    location: Option<NodeLocation>,

    /// If set, the node insists on acquiring this node ID.
    pub force_node_id: Option<PlainNodeId>,

    /// # Cluster name
    ///
    /// A unique identifier for the cluster. All nodes in the same cluster should
    /// have the same.
    cluster_name: String,

    /// # Auto cluster provisioning
    ///
    /// If true, then this node is allowed to automatically provision as a new cluster.
    /// This node *must* have an admin role and a new nodes configuration will be created that includes this node.
    ///
    /// auto-provision is allowed by default in development mode and is disabled if restate-server runs with `--production` flag
    /// to prevent cluster nodes from forming their own clusters, rather than forming a single cluster.
    ///
    /// Use `restatectl` to provision the cluster/node if automatic provisioning is disabled.
    ///
    /// This can also be explicitly disabled by setting this value to false.
    ///
    /// Default: true
    pub auto_provision: bool,

    /// The working directory which this Restate node should use for relative paths. The default is
    /// `restate-data` under the current working directory.
    #[builder(setter(strip_option))]
    pub(super) base_dir: Option<PathBuf>,

    pub metadata_client: MetadataClientOptions,

    /// # Partitions
    ///
    /// Number of partitions that will be provisioned during initial cluster provisioning.
    /// partitions are the logical shards used to process messages.
    ///
    /// Cannot be higher than `65535` (You should almost never need as many partitions anyway)
    ///
    /// NOTE 1: This config entry only impacts the initial number of partitions, the
    /// value of this entry is ignored for provisioned nodes/clusters.
    ///
    /// Default: 24
    pub default_num_partitions: u16,

    /// # Default replication factor
    ///
    /// Configures the global default replication factor to be used by the the system.
    ///
    /// Note that this value only impacts the cluster initial provisioning and will not be respected after
    /// the cluster has been provisioned.
    ///
    /// To update existing clusters use the `restatectl` utility.
    #[serde_as(as = "crate::replication::ReplicationPropertyFromTo")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub default_replication: ReplicationProperty,

    /// # Shutdown grace timeout
    ///
    /// This timeout is used when shutting down the various Restate components to drain all the internal queues.
    pub shutdown_timeout: NonZeroFriendlyDuration,

    /// # Default async runtime thread pool
    ///
    /// Size of the default thread pool used to perform internal tasks.
    /// If not set, it defaults to the number of CPU cores.
    #[builder(setter(strip_option))]
    default_thread_pool_size: Option<u32>,

    /// # Default async runtime thread stack size
    ///
    /// Stack size of the worker threads of the default async runtime.
    /// If not set, it defaults to tokio's default stack size.
    ///
    /// Since v1.7.1
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(strip_option))]
    pub default_thread_stack_size: Option<NonZeroByteCount>,

    #[serde(flatten)]
    pub tracing: TracingOptions,

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

    /// Address to bind for the tokio-console tracing subscriber. If unset and restate-server is
    /// built with tokio-console support, it'll listen on `[::]:6669`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub tokio_console_bind_address: Option<BindAddress<TokioConsolePort>>,

    #[serde(
        flatten,
        // can't use `with=prefix_tokio_console` since it clashes with Schemars
        serialize_with = "prefix_tokio_console::serialize",
        deserialize_with = "prefix_tokio_console::deserialize"
    )]
    tokio_console_listener_options: ListenerOptions<TokioConsolePort>,

    // todo: remove in Restate v1.8
    #[serde(flatten, skip_serializing)]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[deprecated(since = "1.7.0", note = "Moved to `worker.invoker.service-client`")]
    pub(crate) service_client: DeprecatedServiceClientOptions,

    /// Disable prometheus metric recording and reporting. Default is `false`.
    pub disable_prometheus: bool,

    /// Storage high priority thread pool
    ///
    /// This configures the restate-managed storage thread pool for performing
    /// high-priority or latency-sensitive storage tasks when the IO operation cannot
    /// be performed on in-memory caches.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_high_priority_bg_threads: Option<NonZeroUsize>,

    /// Storage low priority thread pool
    ///
    /// This configures the restate-managed storage thread pool for performing
    /// low-priority or latency-insensitive storage tasks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_low_priority_bg_threads: Option<NonZeroUsize>,

    /// # Total memory limit for this process
    ///
    /// This is intended to be determined automatically on Linux based on the cgroup limit,
    /// and is used to emit warning logs if other memory limits are set too close to it.
    #[serde_as(as = "Option<NonZeroByteCount>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub process_total_memory_size: Option<NonZeroUsize>,

    /// # Rocksdb global disk write rate limiter
    ///
    /// This lets Rocksdb calibrates its IO operations to make the best use out of
    /// the available IO bandwidth of the underlying storage device. Rocksdb will
    /// auto-tune the rate according to the actual background IO workload and will
    /// use this value as an upper bound.
    ///
    /// You can use a tool like `fio` to measure the actual IO bandwidth of your storage
    /// device (use block size of 64k, direct IO, and iodepth of 32 across 4 jobs to get a
    /// reasonable estimate).
    ///
    /// For instance, consider the output of the following command:
    ///
    /// ```text
    /// fio --name=c --directory=/restate-data --rw=write --bs=1m --size=8g --numjobs=4 --direct=1 --group_reporting
    /// ...
    ///   WRITE: bw=601MiB/s (630MB/s), 601MiB/s-601MiB/s (630MB/s-630MB/s), io=32.0GiB (34.4GB), run=54560-54560msec
    /// ```
    ///
    /// The default value assumes a fast NVMe with bandwidth of 7GiB (per second).
    pub rocksdb_max_write_rate_per_second: NonZeroByteCount,

    /// # Total memory limit for rocksdb caches and memtables.
    ///
    /// This includes memory for uncompressed block cache and all memtables by all open databases.
    ///
    /// The minimum supported is 256 MiB. Any value below this will be sanitized automatically to 256 MiB.
    rocksdb_total_memory_size: NonZeroByteCount,

    /// # Rocksdb total memtable size ratio
    ///
    /// The memory size used across all memtables (ratio between 0.1 to 1.0). This
    /// limits how much memory memtables can eat up from the value in rocksdb-total-memory-limit.
    ///
    /// The remaining memory will be dedicated to the block cache.
    ///
    /// This value will be sanitized to 1.0 if outside the valid bounds.
    rocksdb_total_memtables_ratio: f32,

    /// # Rocksdb Low Priority Background Threads
    ///
    /// The number of threads to reserve to lower priority Rocksdb background tasks.
    ///
    /// Defaults to the remaining CPU cores not used by high-priority rocksdb threads
    ///
    /// Since v1.7.0 (renamed from `rocksdb-bg-threads`)
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_low_priority_threads: Option<NonZeroU32>,

    /// # Rocksdb High Priority Background Threads
    ///
    /// The number of threads to reserve to high priority Rocksdb background tasks.
    ///
    /// Defaults to 1/4 of the number of CPU cores.
    ///
    /// Since v1.7.0 (renamed from `rocksdb-high-priority-bg-threads`)
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_high_priority_threads: Option<NonZeroU32>,

    /// # Rocksdb performance statistics level
    ///
    /// Defines the level of PerfContext used internally by rocksdb. Default is `enable-count`
    /// which should be sufficient for most users. Note that higher levels incur a CPU cost and
    /// might slow down the critical path.
    pub rocksdb_perf_level: PerfStatsLevel,

    /// RocksDb base settings and memory limits that get applied on every database
    #[serde(flatten)]
    pub rocksdb: RocksDbOptions,

    /// # Metadata update interval
    ///
    /// The idle time after which the node will check for metadata updates from metadata store.
    /// This helps the node detect if it has been operating with stale metadata for extended period
    /// of time, primarily because it didn't interact with other peers in the cluster during that
    /// period.
    pub metadata_update_interval: NonZeroFriendlyDuration,

    /// # Timeout for metadata peer-to-peer fetching
    ///
    /// When a node detects that a new metadata version exists, it'll attempt to fetch it from
    /// its peers. After this timeout duration has passed, the node will attempt to fetch the
    /// metadata from metadata store as well. This is to ensure that the nodes converge quickly
    /// while reducing the load on the metadata store.
    pub metadata_fetch_from_peer_timeout: NonZeroFriendlyDuration,

    /// # Network error retry policy
    ///
    /// The retry policy for network related errors
    pub network_error_retry_policy: RetryPolicy,

    /// # Initialization timeout
    ///
    /// The timeout until the node gives up joining a cluster and initializing itself.
    pub initialization_timeout: NonZeroFriendlyDuration,

    /// # Disable telemetry
    ///
    /// Restate uses Scarf to collect anonymous usage data to help us understand how the software is being used.
    /// You can set this flag to true to disable this collection. It can also be set with the environment variable DO_NOT_TRACK=1.
    pub disable_telemetry: bool,

    /// # Disable the config table
    ///
    /// Disables the `config` SQL table, which exposes the node's running
    /// configuration via SQL queries.
    ///
    /// Since v1.7.1
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub disable_config_sql_table: bool,

    /// Options of gossip-based failure detector
    #[serde(flatten)]
    pub gossip: GossipOptions,

    /// # HLC maximum drift
    ///
    /// Restate uses an internal hybrid-logical-clock (HLC) to track causality between
    /// different nodes in the cluster. This requires that the wall clock of all nodes
    /// of the cluster to be synchronized (i.e. with NTP/PTP). This configuration option
    /// allows you to configure the maximum allowed drift between nodes before the node
    /// starts rejecting requests. The default value is `5000ms` which is sufficiently
    /// large to cover the majority of cases.
    ///
    /// However, cluster operators may prefer to reduce this value (e.g. to `1000ms`) if
    /// they trust the clock synchronization between nodes to be reliable.
    ///
    /// Setting this value to `0` disables the drift check entirely. This is not recommended
    /// unless you are trying to recover a cluster from previous synchronization-related issues.
    #[serde(default)]
    hlc_max_drift: FriendlyDuration,

    #[serde(flatten)]
    pub experimental: Experimental,

    /// # Explicitly disable the `controlled-idempotent-sharding`
    ///
    /// TODO: Removed in Restate v1.8. This is a stopgap solution to
    /// fix e2e forward compatibility tests.
    ///
    /// Since v1.7
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub disable_controlled_idempotent_sharding: bool,
}

/// Declares the [`Experimental`] feature-flag struct from a list of feature names.
///
/// Each entry is a bare identifier (optionally preceded by doc comments / attributes) inside
/// `experimental! { ... }`. For a feature `foo` the macro generates:
/// - a `experimental_enable_foo: bool` field on [`Experimental`] — this is the on-disk /
///   JSON-schema name, so the configuration schema always exposes flags as
///   `experimental_enable_<feature>`;
/// - `Experimental::is_foo_enabled()` and `Experimental::set_foo(enable)` accessors;
/// - an entry in [`Experimental::features`] keyed on the bare name `"foo"` (without the
///   `experimental_enable_` prefix), which is what is surfaced through the admin `/version` API.
///
/// Adding a new experimental flag is therefore a one-line change at the invocation site below:
/// no other code needs to be touched for the flag to show up in `/version`.
macro_rules! experimental {
    (@gen_struct [] -> [$($body:tt)*]) => {
        #[derive(Debug, Clone, Default, Serialize, Deserialize)]
        #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
        #[cfg_attr(feature = "schemars", schemars(default))]
        #[serde(rename_all = "kebab-case")]
        pub struct Experimental {
            $($body)*
        }
    };
    (@gen_struct [$(#[$($attrss:meta)*])* $feat:ident $(, $($tail:tt)*)?] -> [$($body:tt)*]) => {
        paste!{
            experimental!(@gen_struct [$($($tail)*)?] -> [
                $($body)*

                $(#[$($attrss)*])*
                #[cfg_attr(feature = "schemars", schemars(skip))]
                #[serde(skip_serializing_if = "std::ops::Not::not", default)]
                [<experimental_enable_ $feat>]: bool,
            ]);
        }
    };
    (@gen_features [] -> [$($field:ident)*]) => {
        impl Experimental {
            pub fn features(&self) -> std::collections::HashMap<std::borrow::Cow<'static, str>, bool> {
                let mut map = std::collections::HashMap::default();
                $(
                    paste!{
                        map.insert(std::borrow::Cow::Borrowed(stringify!($field)), self.[<experimental_enable_ $field>]);
                    }
                )*
                map
            }
        }
    };
    (@gen_features [$(#[$($attrss:meta)*])* $feat:ident $(, $($tail:tt)*)?] -> [$($acc:ident)*]) => {
        experimental!(@gen_features [$($($tail)*)?] -> [$($acc)* $feat]);
    };
    (@gen_getters [] -> [$($field:ident)*]) => {
        impl Experimental {
            $(
                paste!{
                    pub fn [<is_ $field _enabled>](&self) -> bool {
                        self.[<experimental_enable_ $field>]
                    }

                    pub fn [<set_ $field>](&mut self, enable: bool) {
                        self.[<experimental_enable_ $field>] = enable;
                    }
                }
            )*
        }
    };
    (@gen_getters [$(#[$($attrss:meta)*])* $feat:ident $(, $($tail:tt)*)?] -> [$($acc:ident)*]) => {
        experimental!(@gen_getters [$($($tail)*)?] -> [$($acc)* $feat]);
    };


    {$($tokens:tt)*} => {
        experimental!(@gen_struct [$($tokens)*] -> []);
        experimental!(@gen_features [$($tokens)*] -> []);
        experimental!(@gen_getters [$($tokens)*] -> []);
    };
}

// List of experimental features. Add a new identifier below to introduce a flag; the
// `experimental!` macro will generate the `experimental_enable_<name>` config field, the
// `is_<name>_enabled()` / `set_<name>()` accessors, and the entry exposed (under the bare
// name, without the `experimental_enable_` prefix) by the admin `/version` API.
experimental! {
    /// Current in heavy development, do not enable this feature unless you are a contributor
    vqueues,

    /// When enabled, invocations that exhaust their memory budget will yield back to
    /// the scheduler instead of consuming retry attempts. Requires all nodes in the
    /// cluster to be running v1.7.0 or later because it introduces a new WAL variant.
    ///
    /// Since v1.7.0
    invoker_yield,

    /// # Enables service protocol v7
    ///
    /// Introduced in Restate v1.7
    ///
    /// Set to `true` to enable the experimental service protocol v7
    ///
    /// Once enabled, you **cannot** rollback back to previous versions
    /// where v7 is not supported < v1.7
    protocol_v7,

    /// # Enables unique random seeds
    ///
    /// When enabled, invocations get a unique random seed assigned.
    ///
    /// Since v1.7.0
    unique_random_seeds,

    /// # Migrate the unscoped promise table into its scoped variant
    ///
    /// When enabled, partition stores migrate every entry of the legacy unscoped
    /// promise table into its scoped variant (with `scope = None`)
    /// on open, and route all subsequent promise reads and writes through
    /// the scoped tables.
    ///
    /// Once enabled, you **cannot** roll back to a Restate-server version that
    /// did not yet recognize the resulting on-disk schema version.
    ///
    /// Since v1.7.9
    scoped_promise_table_migration,

    /// # Migrate the unscoped state table into its scoped variant
    ///
    /// When enabled, partition stores migrate every entry of the legacy unscoped
    /// state table into its scoped variant (with `scope = None`)
    /// on open, and route all subsequent promise reads and writes through
    /// the scoped tables.
    ///
    /// Once enabled, you **cannot** roll back to a Restate-server version that
    /// did not yet recognize the resulting on-disk schema version.
    ///
    /// Since v1.7.9
    scoped_state_table_migration,

    /// # Allow scope on Virtual Object targets
    ///
    /// Scoped Virtual Objects are not officially supported in v1.7. Requires
    /// `vqueues` to be enabled as well.
    ///
    /// Since v1.7.0
    scoped_virtual_objects,

    /// # Enables Kafka header support for scoped invocations
    ///
    /// When enabled, Kafka subscriptions read `x-restate-scope` and
    /// `x-restate-limit-key` record headers to drive vqueue scope and
    /// hierarchical limit-key routing. Requires `vqueues` to also be enabled.
    ///
    /// Since v1.7.0
    kafka_scope,

    /// # Skip completed invocations during the vqueues migration
    ///
    /// When enabled, the vqueues migration does not migrate completed
    /// invocations into their vqueue's `Finished` stage. Completed invocations
    /// keep their existing status and are still cleaned up by their
    /// `CleanInvocationStatus` timer, but they will not appear in vqueue
    /// introspection. This can significantly speed up the migration on stores
    /// with a large completion-retention backlog. Requires `vqueues` to also be
    /// enabled.
    ///
    /// By default, all invocations (including completed ones) are migrated.
    ///
    /// Since v1.7.5
    vqueues_migration_skip_completed,

    /// Apply completion and journal retention when terminating a preflight invocation.
    ///
    /// Since v1.7.8
    preflight_invocation_termination_retention,

    /// # Asynchronous VQueue refills
    ///
    /// Moves VQueue storage refills to Tokio's blocking thread pool when the required data is not
    /// already cached by RocksDB.
    ///
    /// Since v1.7.9
    vqueues_async_refill,

    /// # Write invocation response result using reference
    ///
    /// Instead of embedding the response result (which can be huge)
    /// in the invocation status, we instead keep a reference to the
    /// output journal entry.
    ///
    /// Since v1.8.0
    write_result_reference,
}

serde_with::with_prefix!(pub prefix_tokio_console "tokio_console_");

#[cfg(feature = "schemars")]
pub(crate) mod schema {
    use super::*;

    pub fn enumset_role_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        // EnumSet<Role> serializes as an array of Role values
        generator.subschema_for::<Vec<Role>>()
    }
}

impl CommonOptions {
    pub fn fabric_listener_options(&self) -> &ListenerOptions<FabricPort> {
        &self.fabric_listener_options
    }

    /// The fabric TLS options, unless TLS is disabled (section absent or
    /// `mode = "off"`). Use this instead of accessing `tls` directly so that
    /// `off` behaves exactly like an absent section.
    pub fn fabric_tls(&self) -> Option<&FabricTlsOptions> {
        self.tls.as_ref().filter(|t| t.mode.is_enabled())
    }

    pub fn fabric_tls_mut(&mut self) -> &mut Option<FabricTlsOptions> {
        &mut self.tls
    }

    pub fn fabric_tls_mode(&self) -> TlsMode {
        self.tls.as_ref().map(|t| t.mode).unwrap_or_default()
    }

    pub fn tokio_listener_options(&self) -> &ListenerOptions<TokioConsolePort> {
        &self.tokio_console_listener_options
    }

    pub fn shutdown_grace_period(&self) -> Duration {
        self.shutdown_timeout.into()
    }
    // todo: It's imperative that the node doesn't change its name after start. Move this to a
    // Once lock to ensure it doesn't change over time, even if the physical hostname changes.
    pub fn node_name(&self) -> &str {
        self.node_name.as_ref().unwrap_or(&HOSTNAME)
    }

    /// The node location as defined in the configuration file, or the default configuration if
    /// unset.
    pub fn location(&self) -> &NodeLocation {
        static DEFAULT_LOCATION: NodeLocation = NodeLocation::new();
        self.location.as_ref().unwrap_or(&DEFAULT_LOCATION)
    }

    pub fn bind_address(&self) -> BindAddress<FabricPort> {
        self.fabric_listener_options.bind_address()
    }

    pub fn advertised_address(&self, address_book: &AddressBook) -> AdvertisedAddress<FabricPort> {
        self.fabric_listener_options()
            .advertised_address(address_book)
    }

    #[cfg(feature = "unsafe-mutable-config")]
    pub fn set_node_name(&mut self, node_name: impl Into<String>) {
        self.node_name = Some(node_name.into())
    }

    // same as node_name
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    #[cfg(feature = "unsafe-mutable-config")]
    pub fn set_cluster_name(&mut self, cluster_name: impl Into<String>) {
        self.cluster_name = cluster_name.into()
    }

    #[cfg(feature = "unsafe-mutable-config")]
    pub fn set_base_dir(&mut self, path: impl Into<PathBuf>) {
        self.base_dir = Some(path.into());
    }

    pub fn base_dir(&self) -> PathBuf {
        self.base_dir.clone().unwrap_or_else(|| {
            std::env::current_dir()
                .unwrap()
                .join(DEFAULT_STORAGE_DIRECTORY)
        })
    }

    #[cfg(feature = "test-util")]
    pub fn base_dir_opt(&self) -> Option<&PathBuf> {
        self.base_dir.as_ref()
    }

    #[cfg(target_os = "linux")]
    pub fn process_total_memory_size(&self) -> Option<NonZeroUsize> {
        self.process_total_memory_size.or_else(|| {
            [
                "/sys/fs/cgroup/memory.max", // cgroup v2, takes precedence
                "/sys/fs/cgroup/memory/memory.limit_in_bytes", // cgroup v1
            ]
            .iter()
            .find_map(|path| std::fs::read_to_string(path).ok())
            .and_then(|contents| contents.trim().parse().ok())
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub fn process_total_memory_size(&self) -> Option<NonZeroUsize> {
        self.process_total_memory_size
    }

    pub fn rocksdb_total_memory_size(&self) -> NonZeroByteCount {
        self.rocksdb_total_memory_size.max(MIN_ROCKSDB_MEMORY)
    }

    pub fn rocksdb_total_memtables_size(&self) -> NonZeroByteCount {
        let sanitized = self.rocksdb_total_memtables_ratio.clamp(0.1, 1.0) as f64;
        let total_mem = self.rocksdb_total_memory_size().as_usize() as f64;
        let memtables =
            ((total_mem * sanitized) as usize).max(MIN_MEMTABLE_TOTAL_BUDGET.as_usize());
        NonZeroByteCount::from(NonZeroUsize::new(memtables).unwrap())
    }

    pub fn storage_high_priority_bg_threads(&self) -> NonZeroUsize {
        self.storage_high_priority_bg_threads
            .unwrap_or_else(|| (*CPU_COUNT).try_into().unwrap())
    }

    pub fn default_thread_pool_size(&self) -> usize {
        self.default_thread_pool_size
            .unwrap_or_else(|| CPU_COUNT.get()) as usize
    }

    pub fn default_thread_stack_size(&self) -> Option<usize> {
        self.default_thread_stack_size.map(|s| s.as_usize())
    }

    pub fn storage_low_priority_bg_threads(&self) -> NonZeroUsize {
        self.storage_low_priority_bg_threads
            .unwrap_or_else(|| (*CPU_COUNT).try_into().unwrap())
    }

    pub fn rocksdb_high_priority_bg_threads(&self) -> NonZeroU32 {
        // Give 1/4 of the CPUs to flushes unless the user specifies a value.
        self.rocksdb_high_priority_threads
            .unwrap_or_else(|| CPU_COUNT.div_ceil(NonZeroU32::new(4).unwrap()))
    }

    pub fn rocksdb_low_priority_bg_threads(&self) -> NonZeroU32 {
        self.rocksdb_low_priority_threads.unwrap_or_else(|| {
            // how many cpu threads are assigned for high-priority background tasks?
            let remaining = CPU_COUNT
                .get()
                .saturating_sub(self.rocksdb_high_priority_bg_threads().get())
                .max(1);
            // Safe because of max(1) above.
            NonZeroU32::new(remaining).unwrap()
        })
    }

    /// set derived values if they are not configured to reduce verbose configurations
    pub fn set_derived_values(
        &mut self,
        network_options: &NetworkingOptions,
    ) -> Result<(), InvalidConfigurationError> {
        self.tokio_console_listener_options
            .merge(&self.fabric_listener_options);

        self.metadata_client.merge(network_options);

        Ok(())
    }

    pub fn hlc_max_drift(&self) -> Option<NonZeroUsize> {
        if self.hlc_max_drift.is_zero() {
            None
        } else {
            Some(
                NonZeroUsize::new(self.hlc_max_drift.as_millis() as usize)
                    .expect("duration milliseconds must fit into usize"),
            )
        }
    }
}

impl Default for CommonOptions {
    fn default() -> Self {
        Self {
            roles: EnumSet::all(),
            node_name: None,
            location: None,
            force_node_id: None,
            cluster_name: "localcluster".to_owned(),
            // auto provision the cluster by default. This is very likely to change in the future to be
            // false by default. For now, this is true to make the converged deployment backward
            // compatible and easy for users.
            auto_provision: true,
            base_dir: None,
            metadata_client: MetadataClientOptions::default(),
            fabric_listener_options: Default::default(),
            tls: None,
            default_num_partitions: 24,
            default_replication: ReplicationProperty::new_unchecked(1),
            disable_prometheus: false,
            #[allow(deprecated)]
            service_client: Default::default(),
            shutdown_timeout: NonZeroFriendlyDuration::from_secs_unchecked(60),
            tracing: TracingOptions::default(),
            log_filter: "warn,restate=info".to_string(),
            log_format: Default::default(),
            log_disable_ansi_codes: false,
            tokio_console_bind_address: None,
            tokio_console_listener_options: Default::default(),
            default_thread_pool_size: None,
            default_thread_stack_size: None,
            storage_high_priority_bg_threads: None,
            storage_low_priority_bg_threads: None,
            process_total_memory_size: None,
            rocksdb_max_write_rate_per_second: NonZeroByteCount::try_from(7 * 1024 * 1024 * 1024)
                .unwrap(),
            rocksdb_total_memory_size: NonZeroByteCount::try_from(2 * 1024 * 1024 * 1024).unwrap(), // 2GiB
            rocksdb_total_memtables_ratio: 0.85, // (85% of rocksdb-total-memory-size)
            rocksdb_low_priority_threads: None,
            rocksdb_high_priority_threads: None,
            rocksdb_perf_level: PerfStatsLevel::EnableCount,
            rocksdb: Default::default(),
            metadata_update_interval: NonZeroFriendlyDuration::from_secs_unchecked(10),
            metadata_fetch_from_peer_timeout: NonZeroFriendlyDuration::from_secs_unchecked(3),
            network_error_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(10),
                2.0,
                Some(15),
                Some(Duration::from_secs(5)),
            ),
            initialization_timeout: NonZeroFriendlyDuration::from_secs_unchecked(5 * 60),
            disable_telemetry: false,
            disable_config_sql_table: false,
            gossip: GossipOptions::default(),
            hlc_max_drift: FriendlyDuration::from_millis(5000),
            experimental: Experimental::default(),
            disable_controlled_idempotent_sharding: false,
        }
    }
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

/// # Metadata client options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(rename = "MetadataClientOptions", default)
)]
#[builder(default)]
#[serde(rename_all = "kebab-case", default)]
pub struct MetadataClientOptions {
    /// # Metadata client type
    ///
    /// Which metadata client type to use for the cluster.
    #[serde(flatten)]
    pub kind: MetadataClientKind,

    /// # Connect timeout
    ///
    /// TCP connection timeout for connecting to the metadata store.
    pub connect_timeout: NonZeroFriendlyDuration,

    /// # Metadata Store Keep Alive Interval
    ///
    /// Interval at which keep-alive probes are sent on the connection to the
    /// metadata store, to keep it alive and to detect a store that has become
    /// unreachable.
    pub keep_alive_interval: NonZeroFriendlyDuration,

    /// # Metadata Store Keep Alive Timeout
    ///
    /// How long to wait for a keep-alive probe to be acknowledged by the
    /// metadata store before treating the connection as dead and closing it.
    pub keep_alive_timeout: NonZeroFriendlyDuration,

    /// # Backoff policy used by the metadata client
    ///
    /// Backoff policy used by the metadata client when it encounters concurrent modifications.
    pub backoff_policy: RetryPolicy,

    /// # Metadata Network Message Size
    ///
    /// Maximum size of network messages that metadata client can receive from a metadata server.
    ///
    /// If unset, defaults to `networking.message-size-limit`. If set, it will be clamped at
    /// the value of `networking.message-size-limit` since larger messages cannot be transmitted
    /// over the cluster internal network.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_size_limit: Option<NonZeroByteCount>,
}

impl MetadataClientOptions {
    pub(crate) fn merge(&mut self, network_options: &NetworkingOptions) {
        self.message_size_limit = Some(
            self.message_size_limit
                .map(|limit| limit.min(network_options.message_size_limit))
                .unwrap_or(network_options.message_size_limit),
        );
    }

    pub fn message_size_limit(&self) -> NonZeroUsize {
        self.message_size_limit
            .map(|v| v.as_non_zero_usize())
            .unwrap_or(DEFAULT_MESSAGE_SIZE_LIMIT)
    }
}

impl Default for MetadataClientOptions {
    fn default() -> Self {
        Self {
            kind: MetadataClientKind::Replicated { addresses: vec![] },
            connect_timeout: NonZeroFriendlyDuration::from_secs_unchecked(3),
            keep_alive_interval: NonZeroFriendlyDuration::from_secs_unchecked(5),
            keep_alive_timeout: NonZeroFriendlyDuration::from_secs_unchecked(5),
            // default total time is ~5.3s
            backoff_policy: RetryPolicy::exponential(
                Duration::from_millis(100),
                1.4,
                Some(10),
                Some(Duration::from_millis(1000)),
            ),
            message_size_limit: None,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_more::Display, PartialEq)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case",
    try_from = "MetadataClientKindShadow"
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(
        title = "Metadata client type",
        description = "The metadata client type to store metadata",
        !try_from,
    )
)]
pub enum MetadataClientKind {
    /// Store metadata on the replicated metadata store that runs on nodes with the metadata-server role.
    #[display("replicated")]
    Replicated {
        /// # Restate metadata server address list
        #[cfg_attr(feature = "schemars", schemars(with = "Vec<String>"))]
        addresses: Vec<AdvertisedAddress<FabricPort>>,
    },
    /// Store metadata on an external etcd cluster.
    ///
    /// The addresses are formatted as `host:port`
    #[display("etcd")]
    Etcd {
        /// # Etcd cluster node address list
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        addresses: Vec<String>,
    },
    /// Store metadata on an external object store.
    #[display("object-store")]
    ObjectStore {
        /// # Object store path for metadata storage
        ///
        /// This location will be used to persist cluster metadata. Takes the form of a URL
        /// with `s3://` as the protocol and bucket name as the authority, plus an optional
        /// prefix specified as the path component.
        ///
        /// Example: `s3://bucket/prefix`
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        path: String,

        #[serde(flatten)]
        object_store: ObjectStoreOptions,

        /// # Error retry policy
        ///
        /// Retry policy for the object store requests the metadata client
        /// makes, covering both reads of the current metadata version and the
        /// conditional writes used to update it.
        ///
        /// Retries here absorb the transient errors and throttling responses
        /// object stores return under load, so a short or non-retrying policy
        /// can surface those as metadata operation failures.
        #[serde(default = "MetadataClientKind::default_object_store_retry_policy")]
        object_store_retry_policy: RetryPolicy,
    },

    #[display("dynamo-db")]
    // Don't include the DynamoDB variant in our publicly released configuration schema because it
    // is a paid product only feature atm.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    DynamoDb {
        /// # DynamoDB table name
        ///
        /// Name or ARN of the DynamoDB table that stores cluster metadata.
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        table: String,

        /// # Key Prefix
        ///
        /// Optional prefix added to each metadata key so multiple Restate clusters can share the same table.
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        key_prefix: Option<String>,

        #[serde(flatten)]
        dynamo_db: DynamoDbOptions,
    },
}

impl MetadataClientKind {
    fn default_object_store_retry_policy() -> RetryPolicy {
        RetryPolicy::exponential(
            Duration::from_millis(100),
            2.,
            Some(10),
            Some(Duration::from_secs(10)),
        )
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
// TODO(azmy): Remove this Shadow struct once we no longer support the `address` configuration param.
enum MetadataClientKindShadow {
    #[serde(alias = "embedded")]
    Replicated {
        address: Option<AdvertisedAddress<FabricPort>>,
        #[serde(default)]
        addresses: Vec<AdvertisedAddress<FabricPort>>,
    },
    Etcd {
        addresses: Vec<String>,
    },
    ObjectStore {
        path: String,
        #[serde(flatten)]
        object_store: ObjectStoreOptions,
        #[serde(default = "MetadataClientKind::default_object_store_retry_policy")]
        object_store_retry_policy: RetryPolicy,
    },
    DynamoDb {
        table: String,
        key_prefix: Option<String>,
        #[serde(flatten)]
        dynamo_db: DynamoDbOptions,
    },
    // Fallback to support not having to specify the type field
    #[serde(untagged)]
    Fallback {
        address: Option<AdvertisedAddress<FabricPort>>,
        #[serde(default)]
        addresses: Vec<AdvertisedAddress<FabricPort>>,
    },
}

impl TryFrom<MetadataClientKindShadow> for MetadataClientKind {
    type Error = &'static str;
    fn try_from(value: MetadataClientKindShadow) -> Result<Self, Self::Error> {
        let result = match value {
            MetadataClientKindShadow::ObjectStore {
                path,
                object_store,
                object_store_retry_policy,
            } => Self::ObjectStore {
                path,
                object_store,
                object_store_retry_policy,
            },
            MetadataClientKindShadow::Etcd { addresses } => Self::Etcd { addresses },
            MetadataClientKindShadow::DynamoDb {
                table,
                key_prefix,
                dynamo_db,
            } => Self::DynamoDb {
                table,
                key_prefix,
                dynamo_db,
            },
            MetadataClientKindShadow::Replicated { address, addresses }
            | MetadataClientKindShadow::Fallback { address, addresses } => Self::Replicated {
                addresses: match address {
                    Some(_) if !addresses.is_empty() => {
                        return Err(
                            "Conflicting configuration, embedded metadata-client cannot have both `address` and `addresses`",
                        );
                    }
                    Some(address) => vec![address],
                    None => addresses,
                },
            },
        };

        Ok(result)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(title = "Tracing", description = "Options for tracing")
)]
pub struct TracingOptions {
    /// # Tracing Endpoint
    ///
    /// This is a shortcut to set both [`Self::tracing_runtime_endpoint`], and [`Self::tracing_services_endpoint`].
    ///
    /// Specify the tracing endpoint to send runtime traces to.
    /// Traces will be exported using [OTLP gRPC](https://opentelemetry.io/docs/specs/otlp/#otlpgrpc)
    /// through [opentelemetry_otlp](https://docs.rs/opentelemetry-otlp/0.12.0/opentelemetry_otlp/).
    ///
    /// To configure the sampling, please refer to the [opentelemetry autoconfigure docs](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#sampler).
    pub tracing_endpoint: Option<String>,

    /// # Runtime Tracing Endpoint
    ///
    /// Overrides [`Self::tracing_endpoint`] for runtime traces
    ///
    /// Specify the tracing endpoint to send runtime traces to.
    /// Traces will be exported using [OTLP gRPC](https://opentelemetry.io/docs/specs/otlp/#otlpgrpc)
    /// through [opentelemetry_otlp](https://docs.rs/opentelemetry-otlp/0.12.0/opentelemetry_otlp/).
    ///
    /// To configure the sampling, please refer to the [opentelemetry autoconfigure docs](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#sampler).
    pub tracing_runtime_endpoint: Option<String>,

    /// # Services Tracing Endpoint
    ///
    /// Overrides [`Self::tracing_endpoint`] for services traces
    ///
    /// Specify the tracing endpoint to send services traces to.
    /// Traces will be exported using [OTLP gRPC](https://opentelemetry.io/docs/specs/otlp/#otlpgrpc)
    /// through [opentelemetry_otlp](https://docs.rs/opentelemetry-otlp/0.12.0/opentelemetry_otlp/).
    ///
    /// To configure the sampling, please refer to the [opentelemetry autoconfigure docs](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#sampler).
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
    pub tracing_json_path: Option<String>,

    /// # Tracing Filter
    ///
    /// Distributed tracing exporter filter.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    pub tracing_filter: String,

    /// # Additional tracing headers
    ///
    /// Specify additional headers you want the system to send to the tracing endpoint (e.g.
    /// authentication headers).
    #[serde(skip_serializing_if = "SerdeableHeaderHashMap::is_empty")]
    #[serde(default)]
    pub tracing_headers: SerdeableHeaderHashMap,
}

impl Default for TracingOptions {
    fn default() -> Self {
        Self {
            tracing_endpoint: None,
            tracing_runtime_endpoint: None,
            tracing_services_endpoint: None,
            tracing_json_path: None,
            tracing_filter: "info".to_owned(),
            tracing_headers: SerdeableHeaderHashMap::default(),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(
        title = "Ingestion Options",
        description = "Options for ingestion client"
    )
)]
pub struct IngestionOptions {
    /// # Inflight Memory Budget
    ///
    /// Maximum total size of in-flight ingestion requests in bytes.
    /// Tune this to your workload so there are enough unpersisted
    /// requests for efficient batching without exhausting memory.
    ///
    /// Defaults to 1 MiB.
    pub inflight_memory_budget: NonZeroByteCount,

    /// # Connection retry policy
    ///
    /// Retry policy for the ingestion client. It must allow unlimited
    /// retries; if configured with a cap, the client falls back to
    /// retrying every 2 seconds.
    pub connection_retry_policy: RetryPolicy,

    /// # Request Batch Size
    ///
    /// Maximum size of a single ingestion request batch.
    /// Tune to keep enough requests per batch for
    /// throughput; overly large batches can increase tail latency.
    ///
    /// Defaults to 50 KiB.
    pub request_batch_size: NonZeroByteCount,
}

impl Default for IngestionOptions {
    fn default() -> Self {
        Self {
            inflight_memory_budget: NonZeroByteCount::new(
                NonZeroUsize::new(1024 * 1024).expect("non zero"),
            ), //1 MiB
            connection_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(250),
                2.0,
                None,
                Some(Duration::from_secs(3)),
            ),
            request_batch_size: NonZeroByteCount::new(
                NonZeroUsize::new(50 * 1024).expect("non zero"),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use googletest::prelude::eq;
    use googletest::{assert_that, elements_are, pat};

    use crate::config::MetadataClientKind;
    use crate::config_loader::ConfigLoaderBuilder;
    use crate::net::address::AdvertisedAddress;

    use super::*;

    fn minimal_tls_config() -> FabricTlsOptions {
        toml::from_str(
            r#"
            cert-file = "/certs/node.crt"
            key-file = "/certs/node.key"
            ca-files = ["/certs/ca.crt"]
        "#,
        )
        .unwrap()
    }

    #[test]
    fn tls_config_defaults() {
        assert!(CommonOptions::default().tls.is_none());

        let opts = minimal_tls_config();
        assert_eq!(opts.mode, TlsMode::Off);
        assert_eq!(opts.cert_file, PathBuf::from("/certs/node.crt"));
        assert_eq!(opts.key_file, PathBuf::from("/certs/node.key"));
        assert_eq!(opts.ca_files, vec![PathBuf::from("/certs/ca.crt")]);
        assert!(!opts.require_client_auth);
        assert_eq!(*opts.refresh_interval, Duration::from_secs(3600));
        assert!(opts.allowed_subject_names.is_empty());

        let common = CommonOptions {
            tls: Some(opts),
            ..CommonOptions::default()
        };
        let serialized = toml::to_string(&common).unwrap();
        assert!(serialized.contains("[tls]"));
        assert!(!serialized.contains("[networking.tls]"));
        let deserialized: CommonOptions = toml::from_str(&serialized).unwrap();
        assert!(deserialized.tls.is_some());
    }

    #[test]
    fn tls_mode_rollout_semantics() {
        for (mode, enabled, advertises, plaintext) in [
            (TlsMode::Off, false, false, true),
            (TlsMode::Allow, true, false, true),
            (TlsMode::Prefer, true, true, true),
            (TlsMode::Require, true, true, false),
        ] {
            assert_eq!(mode.is_enabled(), enabled, "{mode:?}");
            assert_eq!(mode.advertises_tls(), advertises, "{mode:?}");
            assert_eq!(mode.accepts_plaintext(), plaintext, "{mode:?}");
        }
    }

    #[test]
    fn tls_config_validation() {
        let mut opts = minimal_tls_config();
        assert!(opts.validate().is_ok());

        opts.require_client_auth = true;
        let err = opts.validate().unwrap_err();
        assert!(
            err.to_string().contains("allowed-subject-names is empty"),
            "unexpected validation error: {err}"
        );

        opts.allowed_subject_names = vec!["*".to_owned()];
        assert!(opts.validate().is_ok());

        opts.allowed_subject_names = vec!["spiffe://domain/restate-*".to_owned()];
        assert!(opts.validate().is_ok());

        opts.require_client_auth = false;
        opts.allowed_subject_names.clear();
        assert!(opts.validate().is_ok());
    }

    #[test]
    #[ignore]
    fn metadata_client_kind_backwards_compatibility() -> googletest::Result<()> {
        let address_only = r#"
        address = "http://127.0.0.1:15123/"
        "#;

        let metadata_client_kind: MetadataClientKind = toml::from_str(address_only)?;

        assert_that!(
            metadata_client_kind,
            pat!(MetadataClientKind::Replicated {
                addresses: elements_are![eq(AdvertisedAddress::from_str(
                    "http://127.0.0.1:15123/"
                )
                .unwrap())]
            })
        );

        let addresses_only = r#"
        addresses = ["http://127.0.0.1:15123/", "http://127.0.0.1:15124/"]
        "#;

        let metadata_client_kind: MetadataClientKind = toml::from_str(addresses_only)?;

        assert_that!(
            metadata_client_kind,
            pat!(MetadataClientKind::Replicated {
                addresses: elements_are![
                    eq(AdvertisedAddress::from_str("http://127.0.0.1:15123/").unwrap()),
                    eq(AdvertisedAddress::from_str("http://127.0.0.1:15124/").unwrap())
                ]
            })
        );

        let addresses_only = r#"
        type = "etcd"
        addresses = ["http://127.0.0.1:15123/", "http://127.0.0.1:15124/"]
        "#;

        let metadata_client_kind: MetadataClientKind = toml::from_str(addresses_only)?;

        assert_that!(
            metadata_client_kind,
            pat!(MetadataClientKind::Etcd {
                addresses: elements_are![
                    eq("http://127.0.0.1:15123/"),
                    eq("http://127.0.0.1:15124/")
                ]
            })
        );

        Ok(())
    }

    #[test]
    #[ignore]
    fn metadata_client_compatibility() -> googletest::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let config_path_address = temp_dir.path().join("config1.toml");
        let config_file_address = r#"
        [metadata-client]
        address = "http://127.0.0.1:15123/"
        "#;

        std::fs::write(config_path_address.clone(), config_file_address)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_address))
            .build()?;
        let configuration = config_loader.load_once()?;

        assert_that!(
            configuration.common.metadata_client.kind,
            pat!(MetadataClientKind::Replicated {
                addresses: elements_are![eq(AdvertisedAddress::from_str(
                    "http://127.0.0.1:15123/"
                )
                .unwrap())]
            })
        );

        let config_path_addresses = temp_dir.path().join("config2.toml");
        let config_file_addresses = r#"
        [metadata-client]
        addresses = ["http://127.0.0.1:15123/", "http://127.0.0.1:15124/"]
        "#;

        std::fs::write(config_path_addresses.clone(), config_file_addresses)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_addresses))
            .build()?;
        let configuration = config_loader.load_once()?;

        assert_that!(
            configuration.common.metadata_client.kind,
            pat!(MetadataClientKind::Replicated {
                addresses: elements_are![
                    eq(AdvertisedAddress::from_str("http://127.0.0.1:15123/").unwrap()),
                    eq(AdvertisedAddress::from_str("http://127.0.0.1:15124/").unwrap())
                ]
            })
        );

        let config_path_etcd = temp_dir.path().join("config2.toml");
        let config_file_etcd = r#"
        [metadata-client]
        type = "etcd"
        addresses = ["http://127.0.0.1:15123/", "http://127.0.0.1:15124/"]
        "#;

        std::fs::write(config_path_etcd.clone(), config_file_etcd)?;

        let config_loader = ConfigLoaderBuilder::default()
            .path(Some(config_path_etcd))
            .build()?;
        let configuration = config_loader.load_once()?;

        assert_that!(
            configuration.common.metadata_client.kind,
            pat!(MetadataClientKind::Etcd {
                addresses: elements_are![
                    eq("http://127.0.0.1:15123/"),
                    eq("http://127.0.0.1:15124/")
                ]
            })
        );

        Ok(())
    }
}
