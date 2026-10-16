// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, ensure};
use enumset::EnumSet;

use restate_core::network::NetworkServerBuilder;
use restate_core::{MetadataWriter, TaskCenter, TaskKind};
use restate_limiter::RuleBook;
use restate_metadata_server::{BoxedMetadataServer, MetadataServer};
use restate_metadata_store::MetadataStoreClient;
use restate_storage_api::fsm_table::CachedEpochMetadata;
use restate_types::cluster_marker::ClusterMarker;
use restate_types::config::{Configuration, MetadataClientKind, set_current_config};
use restate_types::epoch::EpochMetadata;
use restate_types::health::HealthStatus;
use restate_types::identifiers::LeaderEpoch;
use restate_types::live::Live;
use restate_types::logs::builder::LogsBuilder;
use restate_types::logs::metadata::{
    Chain, LogletParams, Logs, LogsConfiguration, NodeSetSize, ProviderConfiguration, ProviderKind,
    SegmentIndex, new_single_node_loglet_params,
};
use restate_types::logs::{LogletId, SequenceNumber};
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::{
    BIFROST_CONFIG_KEY, NODES_CONFIG_KEY, PARTITION_TABLE_KEY, RULE_BOOK_KEY,
    SCHEMA_INFORMATION_KEY, partition_processor_epoch_key,
};
use restate_types::net::listener::AddressBook;
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::partitions::PartitionConfiguration;
use restate_types::protobuf::common::NodeRpcStatus;
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::replication::NodeSet;
use restate_types::replication::ReplicationProperty;
use restate_types::schema::Schema;
use restate_types::{GenerationalNodeId, RestateVersion, Version};

use super::{Imported, RecoveryPlan};

pub(super) async fn metadata_client(
    config: &Configuration,
) -> anyhow::Result<(MetadataStoreClient, Option<BoxedMetadataServer>)> {
    if !matches!(
        config.common.metadata_client.kind,
        MetadataClientKind::Replicated { .. }
    ) {
        return Ok((
            restate_metadata_providers::create_client(config.common.metadata_client.clone())
                .await?,
            None,
        ));
    }
    // Run ONLY the existing metadata service on an ephemeral loopback endpoint. In particular,
    // never start the normal node/Admin controller while reconstructing logs and partition state.
    let mut runtime_json = serde_json::to_value(config)?;
    runtime_json["bind-address"] = serde_json::json!("127.0.0.1:0");
    runtime_json["advertised-address"] = serde_json::Value::Null;
    runtime_json["advertised-host"] = serde_json::Value::Null;
    runtime_json["listen-mode"] = serde_json::json!("tcp");
    runtime_json["tls"] = serde_json::Value::Null;
    let runtime: Configuration = serde_json::from_value(runtime_json)?;
    set_current_config(runtime.clone());
    let mut bind_config = runtime.clone();
    bind_config.common.roles = EnumSet::only(Role::MetadataServer);
    let mut address_book =
        AddressBook::new(config.common.base_dir().join(config.common.node_name()));
    address_book.bind_from_config(&bind_config).await?;
    let mut builder = NetworkServerBuilder::new(&mut address_book);
    let (service, client) = restate_metadata_server::create_metadata_server_and_client(
        Live::from_value(runtime),
        HealthStatus::default(),
        &mut builder,
        &address_book,
    )
    .await?;
    TaskCenter::current().try_set_address_book(address_book);
    let health = HealthStatus::default();
    let rpc_health = health.clone();
    TaskCenter::spawn(
        TaskKind::MetadataServer,
        "recovery-metadata-rpc",
        async move { builder.run(rpc_health).await },
    )?;
    health.wait_for_value(NodeRpcStatus::Ready).await;
    Ok((client, Some(service)))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_metadata(
    config: &Configuration,
    plan: &RecoveryPlan,
    imported: &[Imported],
    schema: &Schema,
    rule_book: &RuleBook,
    client: MetadataStoreClient,
    service: Option<BoxedMetadataServer>,
    writer: MetadataWriter,
) -> anyhow::Result<()> {
    let logs = target_logs(config, imported)?;
    let epochs = imported
        .iter()
        .map(|imported| target_epoch(imported.cached_epoch.as_ref(), imported.self_proposal_epoch))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut nodes = NodesConfiguration::new(
        Version::MIN,
        plan.target_cluster_name.clone(),
        plan.target_cluster_fingerprint,
    );
    nodes.set_features(plan.cluster_features);
    // Preparation registers generation 1; the unchanged server bumps this to generation 2 on its
    // first start. The fresh replicated loglets below therefore name generation 2 as sequencer.
    let address = if service.is_some() {
        TaskCenter::with_current(|tc| {
            Configuration::pinned()
                .common
                .advertised_address(tc.address_book())
        })
    } else {
        let directory = config.common.base_dir().join(config.common.node_name());
        restate_types::net::address::AdvertisedAddress::with_node_base_dir(&directory)
    };
    // Keep the log-server's default Provisioning state: normal startup must initialize its new
    // empty store before transitioning to ReadWrite, rather than treating it as lost data.
    let node = NodeConfig::builder()
        .name(config.common.node_name().to_owned())
        .current_generation(GenerationalNodeId::INITIAL_NODE_ID)
        .location(config.common.location().clone())
        .address(address)
        .roles(config.common.roles)
        .binary_version(RestateVersion::current())
        .build();
    nodes.upsert_node(node);
    if let Some(service) = service {
        let metadata_writer = writer.clone();
        TaskCenter::spawn(
            TaskKind::MetadataServer,
            "recovery-metadata-store",
            async move { service.run(metadata_writer).await },
        )?;
    }
    ensure!(
        client.provision(&nodes).await?,
        "metadata destination was provisioned concurrently; recovery aborted"
    );
    let nodes: NodesConfiguration = client
        .get(NODES_CONFIG_KEY.clone())
        .await?
        .context("provisioned metadata has no nodes configuration")?;
    writer.update(Arc::new(nodes)).await?;
    client
        .put(
            BIFROST_CONFIG_KEY.clone(),
            &logs,
            Precondition::DoesNotExist,
        )
        .await?;
    for (imported, epoch) in imported.iter().zip(epochs) {
        client
            .put(
                partition_processor_epoch_key(imported.partition.partition_id),
                &epoch,
                Precondition::DoesNotExist,
            )
            .await?;
    }
    client
        .put(
            SCHEMA_INFORMATION_KEY.clone(),
            schema,
            Precondition::DoesNotExist,
        )
        .await?;
    client
        .put(RULE_BOOK_KEY.clone(), rule_book, Precondition::DoesNotExist)
        .await?;
    // Last: the table is the activation boundary. The tool has no worker/controller, but ordinary
    // node startup must never observe topology referring to log chains/epochs not yet prepared.
    client
        .put(
            PARTITION_TABLE_KEY.clone(),
            &plan.partition_table,
            Precondition::DoesNotExist,
        )
        .await?;
    ClusterMarker::validate_or_create(
        &plan.target_cluster_name,
        config.worker.use_multi_db_layout,
    )?;
    ClusterMarker::mark_cluster_as_provisioned(Some(plan.target_cluster_fingerprint))?;
    Ok(())
}

pub(super) async fn ensure_empty(
    client: &MetadataStoreClient,
    plan: &RecoveryPlan,
) -> anyhow::Result<()> {
    // Fail before expensive downloads. A fresh builtin directory is reserved locally; external
    // destinations must contain none of the known cluster keys. Conditional writes remain
    // mandatory: inspection alone is not a reservation against concurrent provisioning.
    for key in [
        NODES_CONFIG_KEY.clone(),
        BIFROST_CONFIG_KEY.clone(),
        PARTITION_TABLE_KEY.clone(),
        SCHEMA_INFORMATION_KEY.clone(),
        RULE_BOOK_KEY.clone(),
    ]
    .into_iter()
    .chain(
        plan.partition_table
            .iter_ids()
            .map(|id| partition_processor_epoch_key(*id)),
    ) {
        ensure!(
            client.get_version(key.clone()).await?.is_none(),
            "target metadata already contains {key}; use a new isolated metadata destination"
        );
    }
    Ok(())
}

fn target_logs(config: &Configuration, imported: &[Imported]) -> anyhow::Result<Logs> {
    let provider = config.bifrost.default_provider;
    let mut builder = LogsBuilder::default();
    builder.set_configuration(LogsConfiguration::from(ProviderConfiguration::from((
        provider,
        ReplicationProperty::new_unchecked(1),
        NodeSetSize::new(1).unwrap(),
    ))));
    let mut seen = HashSet::new();
    for imported in imported {
        let log_id = imported.partition.log_id();
        ensure!(
            seen.insert(log_id),
            "multiple partitions share log {log_id}; this recovery tool requires independent partition logs"
        );
        let params = match provider {
            ProviderKind::Local => new_single_node_loglet_params(provider),
            ProviderKind::Replicated => LogletParams::from(
                ReplicatedLogletParams {
                    loglet_id: LogletId::new(log_id, SegmentIndex::OLDEST),
                    sequencer: GenerationalNodeId::INITIAL_NODE_ID
                        .as_plain()
                        .with_generation(2),
                    replication: ReplicationProperty::new_unchecked(1),
                    nodeset: NodeSet::from_single(GenerationalNodeId::INITIAL_NODE_ID.as_plain()),
                }
                .serialize()?,
            ),
            ProviderKind::InMemory => unreachable!("validated durable provider"),
        };
        builder.add_log(
            log_id,
            Chain::with_base_lsn(imported.applied_lsn.next(), provider, params),
        )?;
    }
    Ok(builder.build())
}

fn target_epoch(
    cached: Option<&CachedEpochMetadata>,
    self_proposal_epoch: LeaderEpoch,
) -> anyhow::Result<EpochMetadata> {
    let current = PartitionConfiguration::new(
        ReplicationProperty::new_unchecked(1),
        NodeSet::from_single(GenerationalNodeId::INITIAL_NODE_ID.as_plain()),
        Default::default(),
    );
    let mut value = serde_json::to_value(EpochMetadata::new(current, None))?;
    if let Some(cached) = cached {
        let version: u32 = cached.version.into();
        version
            .checked_add(2)
            .context("partition epoch metadata version cannot accommodate startup leadership")?;
        let version = version
            .checked_add(1)
            .context("partition epoch metadata version is exhausted")?;
        // Existing public wire representation lets the tool establish version floors without
        // adding a server API or changing runtime behavior. Both metadata and current membership
        // must be newer than the source's cached configuration.
        value["version"] = serde_json::json!(version);
        value["current"]["version"] = serde_json::json!(version);
    }
    let epoch: u64 = cached
        .map_or(self_proposal_epoch, |cached| {
            cached.leader_epoch.max(self_proposal_epoch)
        })
        .into();
    // Startup claims leadership once more. Seeding MAX would only defer exhaustion until boot.
    epoch
        .checked_add(2)
        .context("partition leadership epoch cannot accommodate startup leadership")?;
    value["epoch"] = serde_json::json!(
        epoch
            .checked_add(1)
            .context("partition leadership epoch is exhausted")?
    );
    Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Older snapshots have a SELF dedup fence but no CachedEpochMetadata. A fresh leader must
    // exceed that fence; otherwise its announcements are deduplicated and writes cannot resume.
    #[test]
    fn recovered_epoch_exceeds_legacy_self_fence() -> anyhow::Result<()> {
        let legacy_fence = LeaderEpoch::from(100u64);
        assert!(target_epoch(None, legacy_fence)?.epoch() > legacy_fence);
        assert!(target_epoch(None, LeaderEpoch::from(u64::MAX)).is_err());
        assert!(target_epoch(None, LeaderEpoch::from(u64::MAX - 1)).is_err());
        Ok(())
    }
}
