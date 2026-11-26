// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::{Int64Type, UInt32Type};
use futures::TryStreamExt;

use restate_core::{Metadata, TaskCenter};
use restate_partition_store::stats::Stat;
use restate_partition_store::stats::aggregated::{
    DeploymentStats, ServiceStats, VirtualObjectStats,
};
use restate_storage_query_api::{QueryEngine, QueryEngineVersion, QueryOptions, SessionOptions};
use restate_types::cluster_state::ClusterState;
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_types::partitions::state::{MembershipState, PartitionReplicaSetStates};
use restate_types::{GenerationalNodeId, PlainNodeId};

/// The status gauges the statistics tables read, one per table.
const GAUGES: [&str; 3] = [
    ServiceStats::DEFINITION,
    DeploymentStats::DEFINITION,
    VirtualObjectStats::DEFINITION,
];

/// Whether the statistics tables answer, as far as this node can tell: `false`
/// only on evidence that a statistics read of some partition would fail.
///
/// A partition's statistics read uses the gauges Complete in the replica it goes
/// to (`AggregateKeyHeader` in `crates/partition-store/src/stats/codec.rs`). Only
/// the partition's leader counts as evidence ([`leader`]): a worker opens and
/// reconciles a partition's store before its processor starts and can lead
/// (`crates/worker/src/partition_processor_manager/spawn_processor_task.rs`), so
/// a leader's `cluster.registry` rows are its real state, while another replica
/// can answer before its store is open, for example while the cluster starts.
///
/// The leader is evidence when its node answered, its store is open, which its
/// registry rows show, and one of the three gauges has no `complete` row there
/// (only `backfilling` or `dropped` rows, or none). Every store opened with
/// indexes enabled has a row of its registry, also while its partition hasn't
/// fully migrated to vqueues and so has no gauges yet: such a leader is evidence
/// too. A replica without any row is unknown: the registry scan reads only the stores open on a node
/// (`PartitionStoreManager::migration_report` in
/// `crates/partition-store/src/query_read/report.rs`), so a store that is
/// opening or closing has none. A worker that never enabled indexes has none
/// either, so it reads as unknown too, and its statistics queries fail with
/// `IndexNotComplete`.
///
/// Missing information is no evidence: a partition without a known leader, a
/// leader this node doesn't see alive, and a node whose registry scan failed
/// (its warning names it). Runs one query on `engine`'s V1 implementation, which
/// every admin node has; fails only if that query fails as a whole.
pub async fn stats_tables_ready(
    engine: &QueryEngine,
    replica_sets: &PartitionReplicaSetStates,
) -> anyhow::Result<bool> {
    let session = engine.create_session(SessionOptions {
        engine_version: Some(QueryEngineVersion::V1),
        apply_rate_limit: false,
        schemas: vec!["cluster".into()],
        ..Default::default()
    })?;
    let gauges = GAUGES.map(|gauge| format!("'{gauge}'")).join(", ");
    let result = session
        .execute(
            &format!(
                "SELECT plain_node_id, partition_id, COUNT(DISTINCT CASE \
                   WHEN kind = 'aggregate' AND status = 'complete' AND definition IN ({gauges}) \
                   THEN definition END) \
                 FROM cluster.registry \
                 GROUP BY plain_node_id, partition_id"
            ),
            QueryOptions {},
        )
        .await?;
    let batches: Vec<RecordBatch> = result.stream.try_collect().await?;
    let unanswered = result
        .diagnostics
        .warnings()
        .into_iter()
        .map(|warning| warning.node_id.parse::<PlainNodeId>())
        .collect::<Result<HashSet<_>, _>>()?;

    let mut replicas = Replicas::new();
    for batch in &batches {
        let nodes = batch.column(0).as_string::<i32>();
        let partitions = batch.column(1).as_primitive::<UInt32Type>().values();
        let gauges = batch.column(2).as_primitive::<Int64Type>().values();
        for ((node, partition), gauges) in nodes.iter().zip(partitions).zip(gauges) {
            if let Some(node) = node {
                let complete = *gauges == GAUGES.len() as i64;
                replicas.insert((node.parse::<PlainNodeId>()?, *partition), complete);
            }
        }
    }

    // The registry scan covers the worker nodes of the nodes configuration
    // (`RoleBasedNodeLocator` in `crates/storage-query-datafusion/src/node_fan_out.rs`).
    let (partitions, nodes) = Metadata::with_current(|metadata| {
        (
            metadata.partition_table_snapshot(),
            metadata.nodes_config_snapshot(),
        )
    });
    let answered = |node: PlainNodeId| {
        !unanswered.contains(&node)
            && nodes
                .find_node_by_id(node)
                .is_ok_and(|config| config.has_role(Role::Worker))
    };
    let cluster_state = TaskCenter::with_current(|tc| tc.cluster_state().clone());
    Ok(!missing_gauges(
        partitions.iter_ids().copied(),
        &replica_sets.iter().collect(),
        &cluster_state,
        answered,
        &replicas,
    ))
}

/// The replicas with registry rows, by node and partition: whether all three
/// gauges are Complete there.
type Replicas = HashMap<(PlainNodeId, u32), bool>;

/// Whether the leader of one of `partitions` answered with registry rows but
/// without all three gauges Complete.
fn missing_gauges(
    partitions: impl IntoIterator<Item = PartitionId>,
    memberships: &HashMap<PartitionId, MembershipState>,
    cluster_state: &ClusterState,
    answered: impl Fn(PlainNodeId) -> bool,
    replicas: &Replicas,
) -> bool {
    partitions.into_iter().any(|id| {
        memberships
            .get(&id)
            .and_then(|membership| leader(membership, cluster_state))
            .is_some_and(|node| {
                answered(node) && replicas.get(&(node, u32::from(id))) == Some(&false)
            })
    })
}

/// The partition's leader, if one is known and this node sees it alive. Reads go
/// to the leader when one is known (`PartitionRouting::get_node_by_partition` in
/// `crates/core/src/partitions.rs`); V1 routes every partition read through it
/// (`MetadataAwarePartitionLocator` in
/// `crates/storage-query-datafusion/src/remote_query_scanner_manager.rs`), and so
/// does V2 for the statistics tables (`MetadataAwarePartitionLocator` in
/// `crates/distributed-datafusion/src/source_registry.rs`). Without a leader they
/// go to the first alive replica, whose store may not be open yet, so those
/// replicas give no evidence.
fn leader(membership: &MembershipState, cluster_state: &ClusterState) -> Option<PlainNodeId> {
    let leader = membership.current_leader().current_leader;
    (leader != GenerationalNodeId::INVALID && cluster_state.is_alive(leader.into()))
        .then(|| leader.as_plain())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use restate_clock::MockClock;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnvBuilder};
    use restate_partition_store::{PartitionStore, PartitionStoreManager, QueryIoExecutor};
    use restate_rocksdb::RocksDbManager;
    use restate_types::cluster_state::NodeState;
    use restate_types::config::Configuration;
    use restate_types::identifiers::LeaderEpoch;
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::net::address::AdvertisedAddress;
    use restate_types::net::metadata::MetadataKind;
    use restate_types::nodes_config::{NodeConfig, Role};
    use restate_types::partition_table::PartitionTable;
    use restate_types::partitions::state::{LeadershipState, MemberState, ReplicaSetState};
    use restate_types::{GenerationalNodeId, PlainNodeId, RestateVersion, Version};

    use super::*;
    use crate::context::DataFusionQueryEngine;
    use crate::local_scanners::register_cluster_scanners;
    use crate::mocks::AlwaysLocalPartitionLocator;
    use crate::remote_query_scanner_client::create_remote_scanner_service;
    use crate::remote_query_scanner_manager::RemoteScannerManager;
    use crate::remote_query_scanner_server::RemoteQueryScannerServer;
    use crate::{ClusterTables, DataFusionEnv};

    /// Opens `store` with indexes on or off: on, a migrated partition's gauges
    /// complete at once (it has no entries), and any partition gets its registry
    /// row; off, the gauges are dropped.
    async fn open(store: &mut PartitionStore, indexes: bool) {
        let mut config = Configuration::pinned().as_ref().clone();
        config.common.experimental.set_indexes(indexes);
        store
            .verify_and_run_migrations(Default::default(), &config)
            .await
            .unwrap();
    }

    fn led_by(node: u32) -> LeadershipState {
        LeadershipState {
            current_leader_epoch: LeaderEpoch::INITIAL,
            current_leader: GenerationalNodeId::new(node, 1),
        }
    }

    fn members(nodes: &[u32], version: Version) -> ReplicaSetState {
        ReplicaSetState {
            version,
            members: nodes
                .iter()
                .map(|node| MemberState {
                    node_id: PlainNodeId::new(*node),
                    durable_lsn: Lsn::INVALID,
                })
                .collect(),
        }
    }

    #[restate_core::test]
    async fn false_only_while_an_answering_leader_lacks_a_gauge() {
        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 2);
        let mut core = TestCoreEnvBuilder::with_incoming_only_connector()
            .add_mock_nodes_config()
            .set_partition_table(partition_table.clone());
        let env = DataFusionEnv::new(32 * 1024 * 1024, None, Some(1), &HashMap::new())
            .unwrap()
            .with_mock_clock(MockClock::new())
            .unwrap();
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let replicas = PartitionReplicaSetStates::default();
        let mut stores = Vec::new();
        for (id, partition) in partition_table.iter() {
            stores.push(manager.open(partition, None).await.unwrap());
            replicas.note_observed_membership(*id, led_by(1), &members(&[1], Version::MIN), &None);
        }
        // Every node, this one too, answers `registry` through the scanner server,
        // as in production.
        let worker = RemoteScannerManager::local_only(core.metadata.clone());
        register_cluster_scanners(
            Arc::clone(&manager),
            core.metadata.clone(),
            QueryIoExecutor::new(NonZeroUsize::MIN),
            &worker,
        );
        let server = RemoteQueryScannerServer::new(env.clone(), worker, &mut core.router_builder);
        let coordinator = RemoteScannerManager::new(
            create_remote_scanner_service(core.networking.clone()),
            Arc::new(AlwaysLocalPartitionLocator),
            core.metadata.clone(),
        );
        let core = core.build().await;
        TaskCenter::with_current(|tc| {
            tc.cluster_state()
                .clone()
                .updater()
                .upsert_node_state(GenerationalNodeId::new(1, 1), NodeState::Alive);
        });
        TaskCenter::spawn(
            TaskKind::SystemService,
            "test-registry-scanner",
            server.run(),
        )
        .unwrap();
        env.register_tables(ClusterTables::new(replicas.clone(), None, coordinator))
            .await
            .unwrap();
        let engine = DataFusionQueryEngine::from_env(env, None);

        // Stores without any registry row are unknown, as while they open.
        assert!(
            stats_tables_ready(&engine, &replicas).await.unwrap(),
            "no_registry_rows_is_unknown"
        );
        // An open leader whose partition hasn't migrated to vqueues has only its
        // registry row, and no gauges.
        stores[0].mark_fully_migrated_to_vqueues().await;
        open(&mut stores[0], true).await;
        open(&mut stores[1], true).await;
        assert!(
            !stats_tables_ready(&engine, &replicas).await.unwrap(),
            "unmigrated_leader_with_its_registry_row"
        );
        stores[1].mark_fully_migrated_to_vqueues().await;
        open(&mut stores[1], true).await;
        assert!(
            stats_tables_ready(&engine, &replicas).await.unwrap(),
            "every_leader_complete"
        );
        // Indexes turned off on a leader drop its gauges: its rows say so.
        open(&mut stores[1], false).await;
        assert!(
            !stats_tables_ready(&engine, &replicas).await.unwrap(),
            "leader_dropped_its_gauges"
        );

        // The leader moves to a node that's alive but whose registry scan fails:
        // its warning makes it unknown, which is no evidence.
        let (id, _) = partition_table.iter().last().unwrap();
        replicas.note_observed_membership(
            *id,
            Default::default(),
            &members(&[1, 2], Version::MIN.next()),
            &None,
        );
        replicas.note_observed_leader(
            *id,
            LeadershipState {
                current_leader_epoch: LeaderEpoch::INITIAL.next(),
                current_leader: GenerationalNodeId::new(2, 1),
            },
        );
        TaskCenter::with_current(|tc| {
            tc.cluster_state()
                .clone()
                .updater()
                .upsert_node_state(GenerationalNodeId::new(2, 1), NodeState::Alive);
        });
        let mut nodes = core.metadata.nodes_config_ref().as_ref().clone();
        nodes.upsert_node(
            NodeConfig::builder()
                .name("unreachable".to_owned())
                .current_generation(GenerationalNodeId::new(2, 1))
                .address(AdvertisedAddress::default())
                .roles(Role::Worker.into())
                .binary_version(RestateVersion::current())
                .build(),
        );
        nodes.increment_version();
        let version = nodes.version();
        core.metadata_writer.submit(Arc::new(nodes));
        core.metadata
            .wait_for_version(MetadataKind::NodesConfiguration, version)
            .await
            .unwrap();
        assert!(
            stats_tables_ready(&engine, &replicas).await.unwrap(),
            "unreachable_leader"
        );
    }

    /// Only answered leaders with registry rows count.
    #[test]
    fn evidence_comes_from_answered_leaders_with_rows() {
        let n = PlainNodeId::new;
        let mut updater = ClusterState::default().updater();
        updater.upsert_node_state(GenerationalNodeId::new(1, 1), NodeState::Alive);
        updater.upsert_node_state(GenerationalNodeId::new(2, 1), NodeState::Alive);
        updater.upsert_node_state(GenerationalNodeId::new(3, 1), NodeState::Dead);
        let cluster_state = updater.into_cluster_state();
        // P0 is led by N1 among N1 and N2; P1 has no leader; P2's leader N3 is
        // dead. P1's and P2's replicas lack gauges in every case: no evidence.
        let (p0, p1, p2) = (
            PartitionId::from(0),
            PartitionId::from(1),
            PartitionId::from(2),
        );
        let replicas = PartitionReplicaSetStates::default();
        replicas.note_observed_membership(p0, led_by(1), &members(&[1, 2], Version::MIN), &None);
        replicas.note_observed_membership(
            p1,
            Default::default(),
            &members(&[1, 2], Version::MIN),
            &None,
        );
        replicas.note_observed_membership(p2, led_by(3), &members(&[3], Version::MIN), &None);
        let memberships = replicas.iter().collect();
        let others = [
            ((n(1), p1), false),
            ((n(2), p1), false),
            ((n(3), p2), false),
        ];
        for (case, p0_rows, unanswered, missing) in [
            (
                "non-leader incomplete",
                vec![((n(1), p0), true), ((n(2), p0), false)],
                vec![],
                false,
            ),
            ("leader incomplete", vec![((n(1), p0), false)], vec![], true),
            ("leader answered without rows", vec![], vec![], false),
            (
                "leader not answering",
                vec![((n(1), p0), false)],
                vec![n(1)],
                false,
            ),
        ] {
            let rows: Replicas = p0_rows
                .into_iter()
                .chain(others)
                .map(|((node, partition), complete)| ((node, u32::from(partition)), complete))
                .collect();
            let answered = |node| !unanswered.contains(&node);
            assert_eq!(
                missing_gauges([p0, p1, p2], &memberships, &cluster_state, answered, &rows),
                missing,
                "{case}"
            );
        }
    }
}
