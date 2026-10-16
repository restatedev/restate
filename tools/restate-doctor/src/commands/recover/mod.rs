// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tool-only, best-effort recovery. Sources are read-only; the destination is a new directory and
//! a new metadata keyspace. No workers, ingress, or log servers run during preparation.

use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::num::NonZeroU16;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, ensure};
use cling::prelude::*;
use enumset::EnumSet;
use serde::{Deserialize, Serialize};
use url::Url;

use restate_cli_util::{c_success, c_warn};
use restate_core::task_center::TaskCenterFutureExt;
use restate_core::{MetadataBuilder, MetadataManager, TaskCenterBuilder, spawn_metadata_manager};
use restate_limiter::RuleBook;
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::deduplication_table::{
    DedupSequenceNumber, ProducerId, ReadDeduplicationTable,
};
use restate_storage_api::fsm_table::{CachedEpochMetadata, ReadFsmTable, WriteFsmTable};
use restate_types::clock::ClockUpkeep;
use restate_types::config::{Configuration, MetadataClientKind, set_current_config};
use restate_types::config_loader::ConfigLoaderBuilder;
use restate_types::identifiers::{LeaderEpoch, PartitionId};
use restate_types::logs::metadata::ProviderKind;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::metadata_store::keys::{
    NODES_CONFIG_KEY, PARTITION_TABLE_KEY, RULE_BOOK_KEY, SCHEMA_INFORMATION_KEY,
};
use restate_types::nodes_config::{ClusterFeature, ClusterFingerprint, NodesConfiguration, Role};
use restate_types::partition_table::{
    Partition, PartitionReplication, PartitionTable, PartitionTableBuilder,
};
use restate_types::replication::ReplicationProperty;
use restate_types::schema::Schema;
use restate_types::{SemanticRestateVersion, Version, Versioned};

mod bootstrap;
mod repository;

use repository::{Artifact, Repository};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_recover")]
pub struct RecoverArgs {
    /// Existing snapshot repository URL (S3, S3-compatible, or file://).
    repository: Url,
    /// New output directory. Existing directories are never overwritten.
    #[arg(long)]
    output: PathBuf,
    /// Target server configuration, including the NEW external metadata destination if desired.
    /// Environment overrides are deliberately not loaded.
    #[arg(long)]
    config_file: Option<PathBuf>,
    /// Source metadata connection configuration. Read-only; never used as the target configuration.
    #[arg(long)]
    source_metadata_config: Option<PathBuf>,
    /// Original partition table as JSON (e.g. exported with restatectl metadata get).
    #[arg(long, conflicts_with = "num_partitions")]
    partition_table: Option<PathBuf>,
    /// Original number of equally sized partitions. Checked against every selected snapshot.
    #[arg(long)]
    num_partitions: Option<NonZeroU16>,
    /// Original cluster-wide idempotency sharding setting; REQUIRED without source nodes metadata.
    #[arg(long, action = clap::ArgAction::Set)]
    controlled_idempotent_sharding: Option<bool>,
    /// Source object-store options as JSON. Otherwise use standard AWS credentials/environment.
    #[arg(long)]
    object_store_config: Option<PathBuf>,
    /// Select an exact snapshot instead of discovery's newest one; repeat for multiple partitions.
    #[arg(long, value_name = "PARTITION_ID=RELATIVE_METADATA_PATH")]
    snapshot: Vec<String>,
    /// Explicitly discard a partition's snapshots and recover it empty; requires known topology.
    #[arg(long, value_delimiter = ',')]
    skip_partition: Vec<u16>,
    /// Permit absent snapshots to become empty partitions. Requires known topology.
    #[arg(long)]
    allow_missing_partitions: bool,
    /// Import the deployment registry from source metadata (not required for data-only recovery).
    #[arg(
        long,
        conflicts_with = "schema_from_partition",
        requires = "source_metadata_config"
    )]
    restore_schema: bool,
    /// Salvage a deployment registry from this selected snapshot's cached schema.
    #[arg(long)]
    schema_from_partition: Option<u16>,
    /// Name for a newly identified cluster (default: <source-name>-recovered).
    #[arg(long, conflicts_with = "preserve_cluster_id")]
    cluster_name: Option<String>,
    /// Preserve the source cluster name and fingerprint after loss of the original infrastructure.
    #[arg(long, requires = "original_cluster_stopped")]
    preserve_cluster_id: bool,
    /// Acknowledge that old nodes are stopped/fenced and cannot connect to the recovered cluster.
    #[arg(long)]
    original_cluster_stopped: bool,
    /// Print the JSON recovery plan without creating a directory or writing target metadata.
    #[arg(long)]
    dry_run: bool,
    /// Acknowledge data loss, inconsistent cross-partition cuts, stalled work, and replayed effects.
    #[arg(long)]
    accept_data_loss: bool,
}

#[derive(Default)]
struct SourceMetadata {
    nodes: Option<NodesConfiguration>,
    table: Option<PartitionTable>,
    schema: Option<Schema>,
    rule_book: Option<RuleBook>,
}

#[derive(Serialize)]
struct RecoveryPlan {
    source_cluster_name: String,
    source_cluster_fingerprint: Option<ClusterFingerprint>,
    target_cluster_name: String,
    target_cluster_fingerprint: ClusterFingerprint,
    preserve_cluster_id: bool,
    partition_table: PartitionTable,
    artifacts: BTreeMap<PartitionId, Artifact>,
    empty_partitions: Vec<PartitionId>,
    cluster_features: EnumSet<ClusterFeature>,
    schema_source: Option<String>,
    /// Filled after import: immutable metadata only gives a LOWER BOUND for this position.
    applied_lsns: BTreeMap<PartitionId, Lsn>,
    complete: bool,
}

struct Imported {
    partition: Partition,
    store: PartitionStore,
    applied_lsn: Lsn,
    cached_epoch: Option<CachedEpochMetadata>,
    self_proposal_epoch: LeaderEpoch,
}

pub async fn run_recover(args: &RecoverArgs) -> anyhow::Result<()> {
    Box::pin(run_recover_inner(args)).await
}

async fn run_recover_inner(args: &RecoverArgs) -> anyhow::Result<()> {
    ensure!(
        args.dry_run || args.accept_data_loss,
        "recovery requires --accept-data-loss (or inspect with --dry-run)"
    );
    let output = std::path::absolute(&args.output)?;
    ensure!(
        !output.try_exists()?,
        "output directory already exists; choose a new directory"
    );
    let mut config = load_config(args.config_file.as_deref())?;
    config.common.set_base_dir(output.join("data"));
    // A config copied from the failed cluster can name a separate WAL mount. Merely changing
    // base-dir does not relocate it; ordinary startup must not replay or purge source WALs.
    let mut log_server = serde_json::to_value(&config.log_server)?;
    log_server["rocksdb-wal-dir"] = serde_json::Value::Null;
    config.log_server = serde_json::from_value(log_server)?;
    config.common.set_node_name("recovery");
    config.common.auto_provision = false;
    config.common.force_node_id = Some(restate_types::PlainNodeId::new(1));
    config.common.roles = EnumSet::all();
    if !matches!(
        config.common.metadata_client.kind,
        MetadataClientKind::Replicated { .. }
    ) {
        config.common.roles.remove(Role::MetadataServer);
    }
    if let MetadataClientKind::Replicated { addresses } = &config.common.metadata_client.kind {
        ensure!(
            addresses.is_empty(),
            "built-in target metadata must be local, with no configured remote addresses"
        );
    }
    ensure!(
        matches!(
            config.bifrost.default_provider,
            ProviderKind::Local | ProviderKind::Replicated
        ),
        "recovery requires a durable local or replicated log provider"
    );
    // Never point the recovered server's automatic snapshots at the source repository.
    config.worker.snapshots.destination = None;
    config.common.default_replication = ReplicationProperty::new_unchecked(1);
    set_current_config(config.clone());

    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .options(config.common.clone())
        .build()?
        .into_handle();
    let result = async {
        let _clock = ClockUpkeep::start()?;
        let options = args.object_store_config.as_deref().map(read_json).transpose()?.unwrap_or_default();
        let repository = Repository::new(args.repository.clone(), &options).await?;
        let source_result = read_source_metadata(args.source_metadata_config.as_deref(), args.restore_schema).await;
        set_current_config(config.clone());
        let source = source_result?;
        let mut artifacts = BTreeMap::new();
        let mut selected = HashSet::new();
        for selection in &args.snapshot {
            let (id, artifact) = repository.select(selection).await?;
            ensure!(selected.insert(id), "partition {id} selected more than once");
            artifacts.insert(id, artifact);
        }
        ensure!(args.skip_partition.iter().all(|id| !selected.contains(&PartitionId::new_unchecked(*id))), "a partition cannot be both selected and skipped");
        selected.extend(args.skip_partition.iter().map(|id| PartitionId::new_unchecked(*id)));
        artifacts.extend(repository.discover(&selected).await?);
        for id in &args.skip_partition { artifacts.remove(&PartitionId::new_unchecked(*id)); }
        let mut plan = plan(args, &source, artifacts)?;
        if args.dry_run {
            println!("{}", serde_json::to_string_pretty(&plan)?);
            return Ok(());
        }
        c_warn!("Best-effort recovery: snapshots are NOT a consistent cluster checkpoint. Old nodes must remain isolated. Source snapshots will not be modified.");
        let mut dir = std::fs::DirBuilder::new();
        dir.mode(0o700).create(&output).context("failed reserving fresh recovery directory")?;
        write_json(&output.join("recovery.json"), &plan)?;
        config.common.set_cluster_name(plan.target_cluster_name.clone());
        config.common.default_num_partitions = plan.partition_table.num_partitions();
        set_current_config(config.clone());
        // Large salvage jobs can legitimately take hours. Object-store clients retain their
        // request retry limits, but no wall-clock cap should discard a completed download prefix.
        let result = prepare(args, &repository, &source, &mut plan, &config).await;
        if result.is_ok() {
            plan.complete = true;
            // Write the report before the startup config, so interrupted/failed recovery never
            // hands the operator a launchable configuration.
            write_json(&output.join("recovery.json"), &plan)?;
            write_private(&output.join("restate.toml"), config.dump().map_err(anyhow::Error::from_boxed)?.as_bytes())?;
            c_success!("Recovery prepared. Report: {}. Start restate-server --config-file {} after reviewing the report and isolating deployment endpoints.", output.join("recovery.json").display(), output.join("restate.toml").display());
        } else {
            c_warn!("Recovery did not complete. Partial data is retained at {}; no startup config was written. A partially initialized external metadata destination must not be reused.", output.display());
        }
        result
    }.in_tc(&tc).await;
    tc.shutdown_node("recovery finished", 0).await;
    if let Some(manager) = RocksDbManager::maybe_get() {
        manager.shutdown().await;
    }
    result
}

fn load_config(path: Option<&Path>) -> anyhow::Result<Configuration> {
    Ok(ConfigLoaderBuilder::default()
        .path(path.map(Path::to_path_buf))
        .load_env(false)
        .build()?
        .load_once()?)
}

async fn read_source_metadata(
    path: Option<&Path>,
    restore_schema: bool,
) -> anyhow::Result<SourceMetadata> {
    let Some(path) = path else {
        return Ok(SourceMetadata::default());
    };
    let config = load_config(Some(path))?;
    set_current_config(config.clone());
    let client = restate_metadata_providers::create_client(config.common.metadata_client).await?;
    let source = SourceMetadata {
        nodes: client.get(NODES_CONFIG_KEY.clone()).await?,
        table: client.get(PARTITION_TABLE_KEY.clone()).await?,
        schema: if restore_schema {
            client.get(SCHEMA_INFORMATION_KEY.clone()).await?
        } else {
            None
        },
        rule_book: client.get(RULE_BOOK_KEY.clone()).await?,
    };
    ensure!(
        source.nodes.is_some(),
        "source metadata has no nodes configuration; omit --source-metadata-config to recover without it"
    );
    Ok(source)
}

fn plan(
    args: &RecoverArgs,
    source: &SourceMetadata,
    artifacts: BTreeMap<PartitionId, Artifact>,
) -> anyhow::Result<RecoveryPlan> {
    let first = artifacts
        .values()
        .next()
        .context("no usable partition snapshots found")?;
    let source_name = first.metadata.cluster_name.clone();
    let source_fingerprint = source
        .nodes
        .as_ref()
        .and_then(NodesConfiguration::cluster_fingerprint)
        .or_else(|| {
            artifacts
                .values()
                .find_map(|a| a.metadata.cluster_fingerprint)
        });
    if let Some(nodes) = &source.nodes {
        ensure!(
            nodes.cluster_name() == source_name,
            "source metadata and snapshots have different cluster names"
        );
    }
    for artifact in artifacts.values() {
        artifact
            .metadata
            .validate(&source_name, source_fingerprint)?;
    }
    let controlled = if let Some(nodes) = &source.nodes {
        let value = nodes
            .features()
            .contains(ClusterFeature::ControlledIdempotentSharding);
        ensure!(
            args.controlled_idempotent_sharding
                .is_none_or(|given| given == value),
            "idempotency sharding disagrees with source metadata"
        );
        value
    } else {
        args.controlled_idempotent_sharding.context("without source metadata, specify --controlled-idempotent-sharding true or false to match the original cluster")?
    };
    let explicit_table = args
        .partition_table
        .as_deref()
        .map(read_json::<PartitionTable>)
        .transpose()?;
    let known_topology =
        explicit_table.is_some() || args.num_partitions.is_some() || source.table.is_some();
    ensure!(
        known_topology || (!args.allow_missing_partitions && args.skip_partition.is_empty()),
        "empty partitions require an original partition table or --num-partitions"
    );
    let original_table = if let Some(table) = explicit_table {
        table
    } else if let Some(count) = args.num_partitions {
        let mut builder = PartitionTableBuilder::default();
        builder.with_equally_sized_partitions(count.get())?;
        builder.build()
    } else if let Some(table) = &source.table {
        table.clone()
    } else {
        let mut builder = PartitionTableBuilder::default();
        for artifact in artifacts.values() {
            builder.add_partition(snapshot_partition(&artifact.metadata)?)?;
        }
        builder.build()
    };
    ensure!(
        args.skip_partition
            .iter()
            .all(|id| original_table.contains(&PartitionId::new_unchecked(*id))),
        "a skipped partition is absent from the expected topology"
    );
    let mut builder = PartitionTableBuilder::default();
    let mut empty = Vec::new();
    for (_, partition) in original_table.iter() {
        if let Some(artifact) = artifacts.get(&partition.partition_id) {
            ensure!(
                partition.key_range == artifact.metadata.key_range,
                "snapshot key range disagrees with topology for partition {}",
                partition.partition_id
            );
            ensure!(
                partition.log_id() == artifact.metadata.log_id,
                "snapshot log ID disagrees with topology for partition {}",
                partition.partition_id
            );
        } else {
            ensure!(
                args.allow_missing_partitions
                    || args
                        .skip_partition
                        .contains(&u16::from(partition.partition_id)),
                "partition {} has no snapshot; use --allow-missing-partitions to explicitly recover it empty",
                partition.partition_id
            );
            empty.push(partition.partition_id);
        }
        // Keep identities/ranges/log IDs but use fresh local CF names and current physical layout.
        let recovered: Partition = serde_json::from_value(serde_json::json!({
            "partition_id": partition.partition_id, "key_range": partition.key_range,
            "log_id": partition.log_id(), "cf_name": null,
        }))?;
        builder.add_partition(recovered)?;
    }
    ensure!(
        artifacts.keys().all(|id| original_table.get(id).is_some()),
        "snapshot partition is absent from expected topology (possibly an under-counted --num-partitions)"
    );
    builder.set_partition_replication(PartitionReplication::from(
        ReplicationProperty::new_unchecked(1),
    ));
    let table = builder.build();
    validate_full_keyspace(&table)?;
    ensure!(
        !args.restore_schema || source.schema.is_some(),
        "source metadata contains no deployment schema"
    );
    let schema_source = if args.restore_schema {
        Some("source-metadata".to_owned())
    } else {
        args.schema_from_partition
            .map(|id| format!("partition-{id}"))
    };
    if let Some(id) = args.schema_from_partition {
        ensure!(
            artifacts.contains_key(&PartitionId::new_unchecked(id)),
            "schema source partition has no selected snapshot"
        );
    }
    let target_fingerprint = if args.preserve_cluster_id {
        source_fingerprint.context("source snapshots have no cluster fingerprint; preserving identity requires surviving source metadata")?
    } else {
        ClusterFingerprint::generate()
    };
    Ok(RecoveryPlan {
        source_cluster_name: source_name.clone(),
        source_cluster_fingerprint: source_fingerprint,
        target_cluster_name: if args.preserve_cluster_id {
            source_name
        } else {
            args.cluster_name
                .clone()
                .unwrap_or_else(|| format!("{source_name}-recovered"))
        },
        target_cluster_fingerprint: target_fingerprint,
        preserve_cluster_id: args.preserve_cluster_id,
        partition_table: table,
        artifacts,
        empty_partitions: empty,
        cluster_features: source.nodes.as_ref().map_or_else(
            || {
                if controlled {
                    EnumSet::only(ClusterFeature::ControlledIdempotentSharding)
                } else {
                    EnumSet::empty()
                }
            },
            NodesConfiguration::features,
        ),
        schema_source,
        applied_lsns: BTreeMap::new(),
        complete: false,
    })
}

fn snapshot_partition(
    metadata: &restate_partition_store::snapshots::PartitionSnapshotMetadata,
) -> anyhow::Result<Partition> {
    Ok(serde_json::from_value(serde_json::json!({
        "partition_id": metadata.partition_id, "key_range": metadata.key_range,
        "log_id": metadata.log_id, "cf_name": null,
    }))?)
}

fn validate_full_keyspace(table: &PartitionTable) -> anyhow::Result<()> {
    let mut ranges: Vec<_> = table.iter().map(|(_, p)| p.key_range).collect();
    ranges.sort_unstable_by_key(|range| range.start());
    let mut start = Some(0u64);
    for range in ranges {
        ensure!(
            start == Some(range.start()),
            "partition topology has a gap or overlap; provide the original table or correct partition count"
        );
        start = range.end().checked_add(1);
    }
    ensure!(
        start.is_none(),
        "partition topology does not cover the full keyspace; provide the original table or partition count"
    );
    Ok(())
}

async fn prepare(
    args: &RecoverArgs,
    repository: &Repository,
    source: &SourceMetadata,
    plan: &mut RecoveryPlan,
    config: &Configuration,
) -> anyhow::Result<()> {
    // Set up only a metadata manager; the embedded metadata server is started after all imports
    // have been validated. No target partition table is published while data is being prepared.
    RocksDbManager::init();
    let metadata_builder = MetadataBuilder::default();
    let metadata = metadata_builder.to_metadata();
    restate_core::TaskCenter::try_set_global_metadata(metadata);
    let (client, service) = bootstrap::metadata_client(config).await?;
    if service.is_none() {
        bootstrap::ensure_empty(&client, plan).await?;
    }
    let metadata_manager = MetadataManager::new(metadata_builder, client.clone());
    let writer = metadata_manager.writer();
    spawn_metadata_manager(metadata_manager)?;
    let manager = PartitionStoreManager::create(config.worker.use_multi_db_layout).await?;
    let staging = config.worker.storage.snapshots_staging_dir();
    std::fs::create_dir_all(&staging)?;
    let mut imported = Vec::new();
    let mut schema = if args.restore_schema {
        source.schema.clone().unwrap()
    } else {
        Schema::default()
    };
    for (_, partition) in plan.partition_table.iter() {
        let mut store = if let Some(artifact) = plan.artifacts.get(&partition.partition_id) {
            let snapshot = repository.download(artifact, &staging).await?;
            manager.open_from_snapshot(partition, snapshot).await?
        } else {
            manager.open(partition, None).await?
        };
        ensure!(
            store.get_min_restate_version().await? <= *SemanticRestateVersion::current(),
            "partition {} requires a newer Restate version",
            partition.partition_id
        );
        let applied = store.get_applied_lsn().await?;
        ensure!(
            store.get_seal_marker().await?.is_none(),
            "partition {} is sealed as unsafe; select another snapshot or explicitly skip it",
            partition.partition_id
        );
        if let Some(artifact) = plan.artifacts.get(&partition.partition_id) {
            ensure!(
                applied.is_some_and(|lsn| lsn >= artifact.metadata.min_applied_lsn),
                "partition {} lacks a valid applied log position",
                partition.partition_id
            );
        }
        let applied_lsn = applied.unwrap_or(Lsn::INVALID);
        ensure!(
            applied_lsn != Lsn::MAX,
            "partition {} cannot resume beyond the maximum log position",
            partition.partition_id
        );
        if args.schema_from_partition == Some(u16::from(partition.partition_id)) {
            schema = store
                .get_schema()
                .await?
                .context("selected partition has no cached deployment schema")?;
        }
        let cached_epoch = store.get_partition_config_state().await?;
        // SELF deduplication is the durable leadership fence even in older snapshots that
        // predate cached replica-set metadata. Never reset it or campaign below that epoch.
        let self_proposal_epoch = match store
            .get_dedup_sequence_number(ProducerId::self_producer())
            .await?
        {
            Some(DedupSequenceNumber::Esn(esn)) => esn.leader_epoch,
            None => LeaderEpoch::INVALID,
            Some(DedupSequenceNumber::Sn(_)) => anyhow::bail!(
                "partition {} has invalid SELF deduplication state",
                partition.partition_id
            ),
        };
        imported.push(Imported {
            partition: partition.clone(),
            store,
            applied_lsn,
            cached_epoch,
            self_proposal_epoch,
        });
        plan.applied_lsns
            .insert(partition.partition_id, applied_lsn);
    }
    schema.verify()?;
    if schema.version() == Version::INVALID {
        schema.touch();
    }
    let rule_book = source.rule_book.clone().unwrap_or_default();
    for imported in &mut imported {
        let mut txn = imported.store.transaction();
        // Normalize ALL partition caches to the selected authority. Keeping an imported version
        // floor would make fresh registrations/rule changes silently ignored after recovery.
        txn.put_schema(&schema)?;
        txn.put_rule_book(&rule_book)?;
        txn.commit().await?;
    }
    bootstrap::write_metadata(
        config, plan, &imported, &schema, &rule_book, client, service, writer,
    )
    .await?;
    drop(imported);
    drop(manager);
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> anyhow::Result<T> {
    serde_json::from_reader(File::open(path)?)
        .with_context(|| format!("invalid JSON at {}", path.display()))
}

fn write_json(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    write_private(path, &serde_json::to_vec_pretty(value)?)
}

fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    File::open(path.parent().context("output file has no parent")?)?.sync_all()?;
    Ok(())
}
