// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Opens Restate partition snapshots from a snapshot repository (S3 or compatible) and runs SQL
//! queries against them — without a running cluster. It auto-discovers the partitions in the
//! repository, downloads each partition's latest snapshot (caching the imported store locally so
//! re-runs skip the download), wires up the partition store and the DataFusion query layer, and
//! drops you into an interactive SQL terminal.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use bytestring::ByteString;
use cling::prelude::*;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::info;
use url::Url;

use restate_cli_util::{c_println, c_warn};
use restate_core::task_center::TaskCenterFutureExt;
use restate_core::{
    MetadataBuilder, MetadataManager, MetadataWriter, TaskCenterBuilder, spawn_metadata_manager,
};
use restate_metadata_store::{
    MetadataStore, MetadataStoreClient, ProvisionError, ReadError, WriteError,
};
use restate_object_store_util::create_object_store_client;
use restate_partition_store::PartitionStoreManager;
use restate_partition_store::snapshots::SnapshotRepository;
use restate_rocksdb::RocksDbManager;
use restate_storage_query_datafusion::context::{PartitionTables, QueryContext, SelectPartitions};
use restate_storage_query_datafusion::remote_query_scanner_manager::RemoteScannerManager;
use restate_types::Version;
use restate_types::clock::ClockUpkeep;
use restate_types::config::{Configuration, ObjectStoreOptions, set_current_config};
use restate_types::errors::GenericError;
use restate_types::identifiers::PartitionId;
use restate_types::memory::NonZeroByteCount;
use restate_types::metadata::{Precondition, VersionedValue};
use restate_types::net::metadata::MetadataContainer;
use restate_types::nodes_config::{ClusterFingerprint, NodesConfiguration};
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;

mod repl;
mod server;

/// Run SQL queries against Restate partition snapshots from a repository.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_snapshot")]
pub struct SnapshotArgs {
    /// Snapshot repository base URL, e.g. `s3://my-bucket/snapshots`.
    repository: String,

    /// AWS region of the object store (also inferable from the environment).
    #[arg(long)]
    aws_region: Option<String>,

    /// Object store API endpoint URL override (required for Minio and other S3-compatible stores).
    #[arg(long)]
    aws_endpoint_url: Option<String>,

    /// AWS configuration profile to use.
    #[arg(long)]
    aws_profile: Option<String>,

    /// S3 access key id (or Minio username).
    #[arg(long)]
    aws_access_key_id: Option<String>,

    /// S3 secret access key (or Minio password).
    #[arg(long)]
    aws_secret_access_key: Option<String>,

    /// S3 session token (for short-term STS credentials).
    #[arg(long)]
    aws_session_token: Option<String>,

    /// Allow plain HTTP to the object store endpoint (required for non-HTTPS endpoints).
    #[arg(long)]
    aws_allow_http: bool,

    /// Local directory where downloaded snapshots are imported and cached.
    #[arg(long, default_value = "./snapshot-debugger-data")]
    cache_dir: PathBuf,

    /// Inspect only this partition id instead of all partitions in the repository.
    #[arg(long)]
    partition_id: Option<u16>,

    /// Cluster name to validate snapshots against (auto-detected from the repository if omitted).
    #[arg(long)]
    cluster_name: Option<String>,

    /// Run a single SQL query, print the result, and exit (instead of starting the REPL).
    #[arg(long)]
    query: Option<String>,

    /// Serve the admin-compatible SQL query API (POST /query) on this address instead of starting
    /// the REPL. Defaults to 127.0.0.1:9078 when given without a value.
    #[arg(
        long,
        value_name = "ADDR",
        num_args = 0..=1,
        default_missing_value = "127.0.0.1:9078",
        conflicts_with = "query"
    )]
    listen: Option<SocketAddr>,

    #[clap(long, default_value = "2147483648")]
    rocksdb_memory_budget: NonZeroUsize,
}

impl SnapshotArgs {
    fn object_store_options(&self) -> ObjectStoreOptions {
        ObjectStoreOptions {
            aws_profile: self.aws_profile.clone(),
            aws_region: self.aws_region.clone(),
            aws_access_key_id: self.aws_access_key_id.clone(),
            aws_secret_access_key: self.aws_secret_access_key.clone(),
            aws_session_token: self.aws_session_token.clone(),
            aws_endpoint_url: self.aws_endpoint_url.clone(),
            aws_allow_http: self.aws_allow_http.then_some(true),
        }
    }
}

/// A fixed set of locally-opened partitions handed to the DataFusion query layer.
#[derive(Clone, Debug)]
struct LocalPartitions(Arc<Vec<(PartitionId, Partition)>>);

#[async_trait]
impl SelectPartitions for LocalPartitions {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok((*self.0).clone())
    }
}

/// A metadata store with no backing storage. The debugger publishes the nodes configuration
/// directly into the in-process metadata manager and never needs durable metadata, so reads are
/// always empty and writes are accepted as no-ops.
#[derive(Debug, Default)]
struct OfflineMetadataStore;

#[async_trait]
impl MetadataStore for OfflineMetadataStore {
    async fn get(&self, _key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        Ok(None)
    }

    async fn get_version(&self, _key: ByteString) -> Result<Option<Version>, ReadError> {
        Ok(None)
    }

    async fn put(
        &self,
        _key: ByteString,
        _value: VersionedValue,
        _precondition: Precondition,
    ) -> Result<(), WriteError> {
        Ok(())
    }

    async fn delete(
        &self,
        _key: ByteString,
        _precondition: Precondition,
    ) -> Result<(), WriteError> {
        Ok(())
    }

    async fn provision(
        &self,
        _nodes_configuration: &NodesConfiguration,
    ) -> Result<bool, ProvisionError> {
        Ok(true)
    }
}

pub async fn run_snapshot(args: &SnapshotArgs) -> anyhow::Result<()> {
    let repository_url =
        Url::parse(&args.repository).context("failed to parse the snapshot repository URL")?;
    let object_store_options = args.object_store_options();

    // Seed configuration: the partition store and the snapshot repository both read their settings
    // from the global configuration, so it must be set before either is created.
    let mut config = Configuration::default();
    config.common.set_base_dir(&args.cache_dir);
    config.worker.snapshots.destination = Some(args.repository.clone());
    config.worker.snapshots.object_store = object_store_options.clone();
    config
        .worker
        .storage
        .set_rocksdb_memory_budget(NonZeroByteCount::new(args.rocksdb_memory_budget));

    set_current_config(config.clone());

    // Reuse the ambient multi-threaded Tokio runtime that restate-doctor's `#[tokio::main]` set up,
    // rather than building a dedicated one. Tracing and CLI context are already initialized by the
    // top-level `init`, so they are not set up again here.
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .options(config.common.clone())
        .build()
        .expect("task center builds")
        .into_handle();

    // The metadata manager owns the global metadata (incl. the nodes configuration whose cluster
    // name snapshot downloads are validated against). An in-memory store is enough offline.
    let metadata_builder = MetadataBuilder::default();
    let metadata = metadata_builder.to_metadata();
    let metadata_manager = MetadataManager::new(
        metadata_builder,
        MetadataStoreClient::new(OfflineMetadataStore, None),
    );
    let metadata_writer = metadata_manager.writer();
    tc.try_set_global_metadata(metadata);

    let task_center = tc.clone();
    async move {
        let _upkeep = ClockUpkeep::start().expect("clock upkeep starts");
        spawn_metadata_manager(metadata_manager)?;

        let result = run(
            args,
            &repository_url,
            &object_store_options,
            metadata_writer,
        )
        .await;

        task_center.shutdown_node("completed", 0).await;
        result
    }
    .in_tc(&tc)
    .await
}

async fn run(
    args: &SnapshotArgs,
    repository_url: &Url,
    object_store_options: &ObjectStoreOptions,
    metadata_writer: MetadataWriter,
) -> anyhow::Result<()> {
    let config = Configuration::pinned();

    // A standalone client used only for discovering partitions and the cluster identity, before
    // the partition store machinery is up.
    let object_store = create_object_store_client(
        repository_url.clone(),
        object_store_options,
        &config.worker.snapshots.object_store_retry_policy,
    )
    .await
    .context("failed to connect to the snapshot repository")?;
    let prefix = ObjectPath::from(repository_url.path());

    let partition_ids = match args.partition_id {
        Some(id) => vec![PartitionId::new_unchecked(id)],
        None => discover_partitions(object_store.as_ref(), &prefix).await?,
    };
    anyhow::ensure!(
        !partition_ids.is_empty(),
        "no partitions found in the snapshot repository at {repository_url}"
    );
    c_println!(
        "Found {} partition(s): {}",
        partition_ids.len(),
        partition_ids
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Publish a nodes configuration whose cluster identity matches the repository, otherwise the
    // snapshot download validation (cluster name / fingerprint) rejects every snapshot.
    let (cluster_name, cluster_fingerprint) = match &args.cluster_name {
        Some(name) => (name.clone(), None),
        None => detect_cluster_identity(object_store.as_ref(), &prefix, partition_ids[0]).await?,
    };
    info!(%cluster_name, "Using cluster identity for snapshot validation");
    let nodes_config = NodesConfiguration::new(
        Version::MIN,
        cluster_name,
        cluster_fingerprint.unwrap_or_else(ClusterFingerprint::generate),
    );
    metadata_writer
        .update(MetadataContainer::NodesConfiguration(Arc::new(
            nodes_config,
        )))
        .await?;

    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true)
        .await
        .context("failed to create the partition store manager")?;
    let snapshot_repository = SnapshotRepository::new_from_config(
        &config.worker.snapshots,
        config.worker.storage.snapshots_staging_dir(),
    )
    .await?
    .context("snapshot repository is not configured")?;

    let mut partitions = Vec::with_capacity(partition_ids.len());
    for partition_id in partition_ids {
        let partition = open_partition(&manager, &snapshot_repository, partition_id).await?;
        partitions.push((partition_id, partition));
    }

    let query_context = QueryContext::create(
        &config.admin.query_engine,
        PartitionTables::new(
            LocalPartitions(Arc::new(partitions)),
            manager,
            RemoteScannerManager::local_only(restate_core::Metadata::current()),
        ),
    )
    .await
    .context("failed to build the query context")?;

    match (&args.query, args.listen) {
        (Some(query), _) => repl::run_query(&query_context, query).await,
        (None, Some(addr)) => server::run_server(query_context, addr).await,
        (None, None) => repl::run_repl(&query_context, &args.cache_dir).await,
    }
}

/// Opens a partition store, reusing a previously imported local store when present and otherwise
/// downloading and importing the partition's latest snapshot from the repository.
async fn open_partition(
    manager: &Arc<PartitionStoreManager>,
    snapshot_repository: &SnapshotRepository,
    partition_id: PartitionId,
) -> anyhow::Result<Partition> {
    let probe = Partition::new(partition_id, KeyRange::FULL);
    let data_path = Configuration::pinned()
        .worker
        .storage
        .data_dir(probe.db_name(true).as_ref());

    if data_path.exists() {
        // Reuse the cached import. The key range argument is irrelevant here: no import happens
        // when a local store already exists, and the real range is read back from the store.
        let store = manager.open(&probe, None).await?;
        let range = store.partition_key_range();
        c_println!("Partition {partition_id}: reusing cached local store");
        return Ok(Partition::new(partition_id, range));
    }

    c_println!("Partition {partition_id}: downloading latest snapshot…");
    let snapshot = snapshot_repository
        .get_latest(partition_id)
        .await
        .with_context(|| format!("failed to download snapshot for partition {partition_id}"))?
        .with_context(|| format!("no snapshot found for partition {partition_id}"))?;
    let partition = Partition::new(partition_id, snapshot.key_range);
    let store = manager.open_from_snapshot(&partition, snapshot).await?;
    drop(store);
    Ok(partition)
}

/// Lists the partition ids present in the repository by enumerating the `<prefix>/<partition_id>/`
/// directories.
async fn discover_partitions(
    object_store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> anyhow::Result<Vec<PartitionId>> {
    let listing = object_store
        .list_with_delimiter(Some(prefix))
        .await
        .context("failed to list the snapshot repository")?;

    let mut partitions: Vec<PartitionId> = listing
        .common_prefixes
        .iter()
        .filter_map(|path| path.parts().next_back())
        .filter_map(|segment| segment.as_ref().parse::<u16>().ok())
        .map(PartitionId::new_unchecked)
        .collect();
    partitions.sort();
    partitions.dedup();
    Ok(partitions)
}

/// Reads `latest.json` for a partition to learn the cluster name and fingerprint the snapshots were
/// written with.
async fn detect_cluster_identity(
    object_store: &dyn ObjectStore,
    prefix: &ObjectPath,
    partition_id: PartitionId,
) -> anyhow::Result<(String, Option<ClusterFingerprint>)> {
    let latest_path = prefix
        .clone()
        .join(partition_id.to_string())
        .join("latest.json");
    let bytes = object_store
        .get(&latest_path)
        .await
        .with_context(|| format!("failed to read '{latest_path}' from the repository"))?
        .bytes()
        .await?;

    let latest: serde_json::Value = serde_json::from_slice(&bytes)?;
    let cluster_name = latest
        .get("cluster_name")
        .and_then(|v| v.as_str())
        .context("snapshot metadata is missing 'cluster_name'")?
        .to_owned();
    let cluster_fingerprint = latest
        .get("cluster_fingerprint")
        .and_then(|v| v.as_u64())
        .and_then(|v| ClusterFingerprint::try_from(v).ok());

    if cluster_fingerprint.is_none() {
        c_warn!("Snapshot has no cluster fingerprint; skipping fingerprint validation");
    }
    Ok((cluster_name, cluster_fingerprint))
}
