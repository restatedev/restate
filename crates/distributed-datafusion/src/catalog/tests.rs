// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::util::display::array_value_to_string;
use futures::TryStreamExt;

use restate_clock::MockClock;
use restate_core::test_env::TestCoreEnv;
use restate_limiter::RuleBook;
use restate_limiter::rule_book::RuleBookObserver;
use restate_platform::sync::Mutex;
use restate_storage_query_api::tables::index::{
    EntryByCreated, EntryByQueue, EntryByStatusCreated, EntryStatusByStatusCreated,
    EntryStatusByStatusTransitioned,
};
use restate_storage_query_api::{
    LogicalSchema, QueryEngineVersion, QueryOptions, QuerySession, SessionOptions, SessionTable,
};
use restate_storage_query_datafusion as v1;
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::schema::Schema;

use crate::context::{DataFusionQueryEngine, SelectPartitionsFromMetadata};
use crate::local_scanners::register_metadata_scanners;
use crate::mocks::{MockSchemas, rows};
use crate::source_registry::SourceRegistry;
use crate::{ClusterTables, DataFusionEnv, UserTables};

const STATS: [&str; 3] = [
    "sys_service_stats",
    "sys_deployment_stats",
    "sys_virtual_object_stats",
];
/// Every index the planner can read: none of them is a table.
const INDEXES: [&str; 5] = [
    EntryByQueue::NAME,
    EntryByCreated::NAME,
    EntryByStatusCreated::NAME,
    EntryStatusByStatusCreated::NAME,
    EntryStatusByStatusTransitioned::NAME,
];

async fn table_names(session: &dyn QuerySession, schema: &str) -> BTreeSet<String> {
    let batches: Vec<_> = session
        .execute(
            &format!(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
            ),
            QueryOptions {},
        )
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| array_value_to_string(batch.column(0), row).unwrap())
        })
        .collect()
}

/// `nodes` reads the coordinator's nodes configuration and gossip view through
/// the default engine's scanner, so both engines return the same rows, including
/// the states that only some roles have.
#[restate_core::test]
async fn nodes_match_the_default_engine() {
    use restate_core::test_env::{TestCoreEnvBuilder, create_mock_nodes_config};
    use restate_types::nodes_config::{NodeConfig, Role};
    use restate_types::{GenerationalNodeId, RestateVersion};

    let mut nodes = create_mock_nodes_config(1, 1);
    let mut log_server = NodeConfig::builder()
        .name("log-2".to_owned())
        .current_generation(GenerationalNodeId::new(2, 3))
        .address(Default::default())
        .roles(Role::LogServer | Role::MetadataServer)
        .binary_version(RestateVersion::current())
        .build();
    // A node from before v1.6.0, which didn't report its version.
    log_server.binary_version = None;
    nodes.upsert_node(log_server);
    let core = TestCoreEnvBuilder::with_incoming_only_connector()
        .set_nodes_config(nodes)
        .build()
        .await;
    let options = HashMap::from([("datafusion.execution.batch_size".to_owned(), "1".to_owned())]);
    let legacy = v1::DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    legacy
        .register_tables(v1::ClusterTables::new(
            Default::default(),
            None,
            v1::remote_query_scanner_manager::RemoteScannerManager::local_only(
                core.metadata.clone(),
            ),
        ))
        .await
        .unwrap();
    let current = DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    current
        .register_tables(ClusterTables::new(
            SourceRegistry::local_only(core.metadata.clone()),
            Default::default(),
        ))
        .await
        .unwrap();
    let cluster = || SessionOptions {
        schemas: vec!["cluster".into()],
        ..Default::default()
    };
    let old = v1::context::DataFusionQueryEngine::from_env(legacy, None)
        .create_session(cluster())
        .unwrap();
    let new = DataFusionQueryEngine::from_env(current, None)
        .create_session(cluster())
        .unwrap();
    for sql in [
        "SELECT * FROM cluster.nodes ORDER BY plain_node_id",
        "SELECT plain_node_id, worker_state, storage_state, metadata_server_state, binary_version \
         FROM cluster.nodes WHERE has_log_server_role",
        "SELECT name FROM cluster.nodes WHERE has_worker_role LIMIT 1",
    ] {
        assert_eq!(
            rows(new.as_ref(), sql).await,
            rows(old.as_ref(), sql).await,
            "{sql}"
        );
    }
    assert_eq!(
        rows(new.as_ref(), "SELECT COUNT(*) FROM cluster.nodes").await,
        [["2"]]
    );
}

/// `partitions` and `partition_replica_set` read the coordinator's partition table
/// and the memberships it has observed through the default engine's scanners, so
/// both engines return the same rows: a settled partition, one being reconfigured
/// and one never observed.
#[restate_core::test]
async fn partitions_and_replica_sets_match_the_default_engine() {
    use restate_core::TaskCenter;
    use restate_core::test_env::{TestCoreEnvBuilder, create_mock_nodes_config};
    use restate_types::cluster_state::NodeState;
    use restate_types::identifiers::{LeaderEpoch, PartitionId};
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::partition_table::PartitionTable;
    use restate_types::partitions::state::{
        LeadershipState, MemberState, PartitionReplicaSetStates, ReplicaSetState,
    };
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};

    let core = TestCoreEnvBuilder::with_incoming_only_connector()
        .set_nodes_config(create_mock_nodes_config(1, 1))
        .set_partition_table(PartitionTable::with_equally_sized_partitions(
            Version::MIN,
            3,
        ))
        .build()
        .await;
    // Gossip sees N1:1 alive; N2 stays unobserved.
    TaskCenter::current()
        .cluster_state()
        .clone()
        .updater()
        .upsert_node_state(GenerationalNodeId::new(1, 1), NodeState::Alive);
    let members = |nodes: &[u32], version: Version| ReplicaSetState {
        version,
        members: nodes
            .iter()
            .map(|&node| MemberState {
                node_id: PlainNodeId::new(node),
                durable_lsn: Lsn::INVALID,
            })
            .collect(),
    };
    let states = PartitionReplicaSetStates::default();
    // Partition 0: settled on N1 and N2, led by N1; N2 reported a durable LSN.
    states.note_observed_membership(
        PartitionId::from(0),
        LeadershipState {
            current_leader_epoch: LeaderEpoch::from(3),
            current_leader: GenerationalNodeId::new(1, 1),
        },
        &members(&[1, 2], Version::MIN),
        &None,
    );
    states.note_durable_lsn(PartitionId::from(0), PlainNodeId::new(2), Lsn::new(10));
    // Partition 1: moving from N1 to N1 and N2, with no known leader.
    states.note_observed_membership(
        PartitionId::from(1),
        Default::default(),
        &members(&[1], Version::MIN),
        &Some(members(&[1, 2], Version::MIN.next())),
    );
    // Partition 2: never observed.

    let options = HashMap::from([("datafusion.execution.batch_size".to_owned(), "1".to_owned())]);
    let legacy = v1::DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    legacy
        .register_tables(v1::ClusterTables::new(
            states.clone(),
            None,
            v1::remote_query_scanner_manager::RemoteScannerManager::local_only(
                core.metadata.clone(),
            ),
        ))
        .await
        .unwrap();
    let current = DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    current
        .register_tables(ClusterTables::new(
            SourceRegistry::local_only(core.metadata.clone()),
            states,
        ))
        .await
        .unwrap();
    let cluster = || SessionOptions {
        schemas: vec!["cluster".into()],
        ..Default::default()
    };
    let old = v1::context::DataFusionQueryEngine::from_env(legacy, None)
        .create_session(cluster())
        .unwrap();
    let new = DataFusionQueryEngine::from_env(current, None)
        .create_session(cluster())
        .unwrap();
    for sql in [
        "SELECT * FROM cluster.partitions ORDER BY partition_id",
        "SELECT partition_id, next_replica_set FROM cluster.partitions WHERE v_next IS NOT NULL",
        "SELECT partition_id FROM cluster.partitions WHERE leader_plain_node_id IS NULL \
         ORDER BY partition_id LIMIT 1",
        "SELECT * FROM cluster.partition_replica_set \
         ORDER BY partition_id, membership, plain_node_id",
        "SELECT partition_id, plain_node_id, membership_version FROM cluster.partition_replica_set \
         WHERE membership = 'next' ORDER BY plain_node_id",
        "SELECT partition_id, plain_node_id, leader_epoch FROM cluster.partition_replica_set \
         WHERE is_leader",
    ] {
        assert_eq!(
            rows(new.as_ref(), sql).await,
            rows(old.as_ref(), sql).await,
            "{sql}"
        );
    }
    // Every partition has a row; members of both memberships have one for each.
    // N1's three rows carry its live generation; N2's two don't.
    for (sql, count) in [
        ("SELECT COUNT(*) FROM cluster.partitions", "3"),
        ("SELECT COUNT(*) FROM cluster.partition_replica_set", "5"),
        (
            "SELECT COUNT(*) FROM cluster.partition_replica_set WHERE durable_lsn = 10",
            "1",
        ),
        (
            "SELECT COUNT(*) FROM cluster.partition_replica_set \
             WHERE alive_gen_node_id = 'N1:1'",
            "3",
        ),
        (
            "SELECT COUNT(*) FROM cluster.partition_replica_set \
             WHERE plain_node_id = 'N2' AND alive_gen_node_id IS NULL",
            "2",
        ),
    ] {
        assert_eq!(rows(new.as_ref(), sql).await, [[count]], "{sql}");
    }
    // The observed leader, alive in gossip, is the one leader row.
    assert_eq!(
        rows(
            new.as_ref(),
            "SELECT partition_id, plain_node_id, leader_epoch FROM cluster.partition_replica_set \
             WHERE is_leader"
        )
        .await,
        [["0", "N1", "e3"]]
    );
}

/// `logs` reads the coordinator's logs metadata through the default engine's
/// scanner, so both engines return the same rows: the bootstrapped in-memory logs
/// and a replicated log with two segments. `logs_tail_segments` keeps each log's
/// last segment in both.
#[restate_core::test]
async fn logs_and_tail_segments_match_the_default_engine() {
    use std::num::NonZeroU8;

    use restate_types::logs::builder::LogsBuilder;
    use restate_types::logs::metadata::{Chain, LogletParams, ProviderKind, SegmentIndex};
    use restate_types::logs::{LogId, LogletId, Lsn};
    use restate_types::net::metadata::MetadataKind;
    use restate_types::replicated_loglet::ReplicatedLogletParams;
    use restate_types::replication::{NodeSet, ReplicationProperty};
    use restate_types::{GenerationalNodeId, Versioned};

    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    // The scanner looks a segment's loglet up by the ID derived from the log and
    // the segment index, so the fixture uses those IDs.
    let replicated = |segment: u32| {
        LogletParams::from(
            ReplicatedLogletParams {
                loglet_id: LogletId::new(LogId::new(100), SegmentIndex::from(segment)),
                sequencer: GenerationalNodeId::new(1, 1),
                replication: ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
                nodeset: NodeSet::from_iter([1, 2, 3]),
            }
            .serialize()
            .unwrap(),
        )
    };
    let mut builder =
        LogsBuilder::try_from(core.metadata.logs_snapshot().as_ref().clone()).unwrap();
    builder
        .add_log(
            LogId::new(100),
            Chain::new(ProviderKind::Replicated, replicated(0)),
        )
        .unwrap()
        .append_segment(Lsn::new(50), ProviderKind::Replicated, replicated(1))
        .unwrap();
    let logs = builder.build();
    let version = logs.version();
    core.metadata_writer.submit(Arc::new(logs));
    core.metadata
        .wait_for_version(MetadataKind::Logs, version)
        .await
        .unwrap();

    let options = HashMap::from([("datafusion.execution.batch_size".to_owned(), "1".to_owned())]);
    let legacy = v1::DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    legacy
        .register_tables(v1::ClusterTables::new(
            Default::default(),
            None,
            v1::remote_query_scanner_manager::RemoteScannerManager::local_only(
                core.metadata.clone(),
            ),
        ))
        .await
        .unwrap();
    let current = DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    current
        .register_tables(ClusterTables::new(
            SourceRegistry::local_only(core.metadata.clone()),
            Default::default(),
        ))
        .await
        .unwrap();
    let cluster = || SessionOptions {
        schemas: vec!["cluster".into()],
        ..Default::default()
    };
    let old = v1::context::DataFusionQueryEngine::from_env(legacy, None)
        .create_session(cluster())
        .unwrap();
    let new = DataFusionQueryEngine::from_env(current, None)
        .create_session(cluster())
        .unwrap();
    for sql in [
        "SELECT * FROM cluster.logs ORDER BY log_id, segment_index",
        "SELECT segment_index, base_lsn, loglet_id, sequencer, replication, nodeset \
         FROM cluster.logs WHERE log_id = 100 ORDER BY segment_index",
        "SELECT log_id FROM cluster.logs WHERE loglet_id IS NULL ORDER BY log_id LIMIT 1",
        "SELECT * FROM cluster.logs_tail_segments ORDER BY log_id",
    ] {
        assert_eq!(
            rows(new.as_ref(), sql).await,
            rows(old.as_ref(), sql).await,
            "{sql}"
        );
    }
    // The replicated segments show their loglet; the in-memory ones don't.
    assert_eq!(
        rows(
            new.as_ref(),
            "SELECT segment_index, kind, loglet_id, sequencer FROM cluster.logs \
             WHERE log_id = 100 ORDER BY segment_index"
        )
        .await,
        [
            ["0", "replicated", "100_0", "N1:1"],
            ["1", "replicated", "100_1", "N1:1"]
        ]
    );
    assert_eq!(
        rows(
            new.as_ref(),
            "SELECT COUNT(*) FROM cluster.logs \
             WHERE log_id = 100 AND replication = '{node: 2}' AND nodeset IS NOT NULL"
        )
        .await,
        [["2"]]
    );
    assert_eq!(
        rows(
            new.as_ref(),
            "SELECT COUNT(*) FROM cluster.logs WHERE loglet_id IS NULL AND sequencer IS NULL \
             AND replication IS NULL AND nodeset IS NULL"
        )
        .await,
        rows(
            new.as_ref(),
            "SELECT COUNT(*) FROM cluster.logs WHERE log_id <> 100"
        )
        .await
    );
    // The replicated log's tail is its second segment, from LSN 50.
    assert_eq!(
        rows(
            new.as_ref(),
            "SELECT segment_index, base_lsn FROM cluster.logs_tail_segments WHERE log_id = 100"
        )
        .await,
        [["1", "50"]]
    );
}

/// `loglet_workers` asks each log server for its loglets, and each answers with the
/// local scanner the default engine uses too. V2 returns that scanner's rows,
/// including the worker columns only active loglets have, and asks only log
/// servers.
#[restate_core::test]
async fn loglet_workers_serve_the_shared_scanner_rows() {
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use restate_core::test_env::{TestCoreEnvBuilder, create_mock_nodes_config};
    use restate_storage_query_api::Scan;
    use restate_storage_query_api::tables::loglet_workers::LogletWorkers;
    use restate_types::GenerationalNodeId;
    use restate_types::nodes_config::Role;
    use v1::table_util::Builder;

    use crate::tables::loglet_workers::LogletWorkersTable;

    /// Two loglets on this log server: one with a running worker, one without and
    /// with no known sequencer. Rows are written by the default engine's builder,
    /// as the node's real scanner does.
    #[derive(Debug)]
    struct Loglets(GenerationalNodeId);
    impl Scan for Loglets {
        fn scan(
            &self,
            projection: SchemaRef,
            _: &[datafusion::logical_expr::Expr],
            _: usize,
            _: Option<usize>,
        ) -> SendableRecordBatchStream {
            let mut builder = v1::loglet_worker::LogletWorkersBuilder::new(Arc::clone(&projection));
            for (loglet_id, segment, active) in [("100_0", 0, true), ("100_1", 1, false)] {
                let mut row = builder.row();
                row.fmt_plain_node_id(self.0.as_plain());
                row.fmt_gen_node_id(self.0);
                row.loglet_id(loglet_id);
                row.log_id(100);
                row.segment_index(segment);
                row.local_tail(10 + segment);
                row.is_sealed(!active);
                row.trim_point(segment);
                row.known_global_tail(10);
                row.is_active(active);
                if active {
                    row.fmt_sequencer(self.0);
                    row.staging_local_tail(12);
                    row.seal_enqueued(false);
                    row.accepting_writes(true);
                    row.pending_seals(0);
                    row.pending_tail_waiters(2);
                    row.last_request_at(1_700_000_000_000);
                }
            }
            Box::pin(RecordBatchStreamAdapter::new(
                projection,
                futures::stream::iter([builder.finish()]),
            ))
        }
    }

    // This node is a log server; N2 isn't, so it isn't asked.
    let mut nodes = create_mock_nodes_config(1, 1);
    let mut node = nodes
        .find_node_by_id(GenerationalNodeId::new(1, 1))
        .unwrap()
        .clone();
    node.roles |= Role::LogServer;
    nodes.upsert_node(node);
    nodes.upsert_node(
        create_mock_nodes_config(2, 1)
            .find_node_by_id(GenerationalNodeId::new(2, 1))
            .unwrap()
            .clone(),
    );
    let core = TestCoreEnvBuilder::with_incoming_only_connector()
        .set_nodes_config(nodes)
        .build()
        .await;
    let me = core.metadata.my_node_id();

    // The rows the shared scanner produces on this node: what the default engine
    // serves through its remote-scanner service.
    let batches: Vec<_> = Loglets(me)
        .scan(LogletWorkers::arrow_schema(), &[], 128, None)
        .try_collect()
        .await
        .unwrap();
    let mut expected: Vec<Vec<String>> = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column, row).unwrap())
                    .collect()
            })
        })
        .collect();
    expected.sort();

    let sources = SourceRegistry::local_only(core.metadata.clone());
    sources.register_node_scanner::<LogletWorkersTable>(Arc::new(Loglets(me)));
    let env = DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &HashMap::new())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    env.register_tables(ClusterTables::new(sources, Default::default()))
        .await
        .unwrap();
    let session = DataFusionQueryEngine::from_env(env, None)
        .create_session(SessionOptions {
            schemas: vec!["cluster".into()],
            ..Default::default()
        })
        .unwrap();
    let query = |sql: &'static str| {
        let session = Arc::clone(&session);
        async move {
            let result = session.execute(sql, QueryOptions {}).await.unwrap();
            let diagnostics = Arc::clone(&result.diagnostics);
            let batches: Vec<_> = result.stream.try_collect().await.unwrap();
            // Warnings accumulate while the stream runs: read them after it ends.
            let warnings = diagnostics.warnings();
            assert!(warnings.is_empty(), "{sql}: {warnings:?}");
            batches
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows()).map(|row| {
                        batch
                            .columns()
                            .iter()
                            .map(|column| array_value_to_string(column, row).unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        query("SELECT * FROM cluster.loglet_workers ORDER BY loglet_id").await,
        expected
    );
    assert_eq!(
        query(
            "SELECT loglet_id, sequencer, staging_local_tail, pending_tail_waiters \
             FROM cluster.loglet_workers WHERE is_active"
        )
        .await,
        [["100_0", "N1:1", "12", "2"]]
    );
    assert_eq!(
        query(
            "SELECT loglet_id FROM cluster.loglet_workers \
             WHERE sequencer IS NULL AND staging_local_tail IS NULL AND last_request_at IS NULL"
        )
        .await,
        [["100_1"]]
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn migration_tables_serve_the_default_engine_rows() {
    use restate_partition_store::PartitionStoreManager;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_query_api::tables::{migration_jobs::MigrationJobs, registry::Registry};
    use restate_types::identifiers::PartitionId;
    use restate_types::partition_table::Partition;
    use restate_types::sharding::KeyRange;

    use crate::local_scanners::register_cluster_scanners;

    // Both engines decode the same batches, so the shared declarations must
    // match the default engine's schemas exactly.
    assert_eq!(
        v1::migration_records::RegistryBuilder::schema(),
        Registry::arrow_schema()
    );
    assert_eq!(
        v1::migration_records::MigrationJobsBuilder::schema(),
        MigrationJobs::arrow_schema()
    );

    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    // Two replicas open with indexes: three statistics and three entry indexes
    // each, Complete at once over empty partitions, their jobs completed.
    let mut config = Configuration::pinned().as_ref().clone();
    config.common.experimental.set_indexes(true);
    for id in [1, 2] {
        let mut store = manager
            .open(&Partition::new(PartitionId::from(id), KeyRange::FULL), None)
            .await
            .unwrap();
        store.mark_fully_migrated_to_vqueues().await;
        store
            .verify_and_run_migrations(Default::default(), &config)
            .await
            .unwrap();
    }
    // The rows the default engine's scanners produce on this node, unsorted.
    let io = crate::mocks::query_io();
    let (registry, jobs) = v1::migration_records::create_local_scanners(
        Arc::clone(&manager),
        core.metadata.clone(),
        io.clone(),
    );
    let mut legacy = Vec::new();
    for (scanner, schema) in [
        (registry, Registry::arrow_schema()),
        (jobs, MigrationJobs::arrow_schema()),
    ] {
        let batches: Vec<_> = scanner
            .scan(schema, &[], 128, None)
            .try_collect()
            .await
            .unwrap();
        let mut rows: Vec<Vec<String>> = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(|row| {
                    batch
                        .columns()
                        .iter()
                        .map(|column| array_value_to_string(column, row).unwrap())
                        .collect()
                })
            })
            .collect();
        rows.sort();
        legacy.push(rows);
    }

    let sources = SourceRegistry::local_only(core.metadata.clone());
    register_cluster_scanners(Arc::clone(&manager), core.metadata.clone(), io, &sources);
    let env = DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &HashMap::new())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    env.register_tables(ClusterTables::new(sources, Default::default()))
        .await
        .unwrap();
    let session = DataFusionQueryEngine::from_env(env, None)
        .create_session(SessionOptions {
            schemas: vec!["cluster".into()],
            ..Default::default()
        })
        .unwrap();
    // The registry has one more row per replica: the registry's own.
    for (sql, expected, len) in [
        ("SELECT * FROM cluster.registry", &legacy[0], 14),
        ("SELECT * FROM cluster.migration_jobs", &legacy[1], 12),
    ] {
        let mut actual = rows(session.as_ref(), sql).await;
        actual.sort();
        assert_eq!(&actual, expected, "{sql}");
        assert_eq!(actual.len(), len, "{sql}");
    }
    // Index sizes are estimated when the query selects them; completed jobs
    // aren't blocked.
    for sql in [
        "SELECT COUNT(*) FROM cluster.registry
         WHERE status = 'complete' AND approx_size_bytes IS NOT NULL",
        "SELECT COUNT(*) FROM cluster.migration_jobs
         WHERE status = 'completed' AND blocked_by IS NULL",
    ] {
        assert_eq!(rows(session.as_ref(), sql).await, [["12"]], "{sql}");
    }
}

#[restate_core::test]
async fn shared_tables_preserve_v1_column_contracts() {
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let schemas = Live::from_value(Schema::default());
    let legacy = v1::DataFusionEnv::new(10 * 1024 * 1024, None, Some(1), &HashMap::new())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let legacy_sources =
        v1::remote_query_scanner_manager::RemoteScannerManager::local_only(core.metadata.clone());
    legacy
        .register_tables(
            v1::UserTables::new(
                v1::context::SelectPartitionsFromMetadata,
                legacy_sources.clone(),
            )
            .with_metadata(v1::MetadataTables::new(
                schemas.clone(),
                core.metadata_store_client.clone(),
                None,
            )),
        )
        .await
        .unwrap();
    legacy
        .register_tables(v1::ClusterTables::new(
            Default::default(),
            None,
            legacy_sources,
        ))
        .await
        .unwrap();

    let current = DataFusionEnv::new(10 * 1024 * 1024, None, Some(1), &HashMap::new())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let sources = SourceRegistry::local_only(core.metadata.clone());
    current
        .register_tables(
            UserTables::new(SelectPartitionsFromMetadata, sources.clone()).with_metadata(),
        )
        .await
        .unwrap();
    current
        .register_tables(ClusterTables::new(sources, Default::default()))
        .await
        .unwrap();

    // Every current V2 table with a production V1 counterpart. V2's three
    // statistics tables and its inspection-only index have no production V1 table.
    for name in [
        "state",
        "sys_scheduler",
        "sys_invocation_state",
        "sys_user_limits",
        "sys_invocation_status",
        "sys_invocation",
        "sys_journal",
        "sys_journal_events",
        "sys_locks",
        "sys_promise",
        "sys_vqueue_meta",
        "sys_vqueue_entry_status",
        "sys_vqueues",
        "sys_service",
        "sys_deployment",
        "sys_rules",
        "bifrost_read_streams",
        "config",
        "nodes",
        "partitions",
        "partition_replica_set",
        "partition_state",
        "logs",
        "logs_tail_segments",
        "loglet_workers",
    ] {
        // V1 identifies some tables by their declaration name.
        let legacy_name = match name {
            "nodes" => "node",
            "partitions" => "partition",
            "logs" => "log",
            name => name,
        };
        let expected = legacy
            .table_provider(legacy_name)
            .unwrap()
            .schema()
            .fields()
            .to_vec();
        let mut actual = current
            .table_provider(name)
            .unwrap()
            .schema()
            .fields()
            .to_vec();
        // V2's `partition_state` adds `report` after V1's columns.
        if name == "partition_state" {
            assert_eq!(actual.pop().unwrap().name(), "report");
        }
        assert_eq!(
            actual, expected,
            "shared_columns_preserve_names_types_nullability_and_order: {name}"
        );
    }
}

#[restate_core::test]
async fn deployment_rows_match_v1() {
    use restate_types::identifiers::DeploymentId;
    use restate_types::schema::deployment::{Deployment, DeploymentType};
    use restate_types::time::MillisSinceEpoch;

    const ARN: &str = "arn:aws:lambda:eu-central-1:1234567890:function:greeter:1";
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let mut http = Deployment::mock();
    http.id = DeploymentId::from_parts(1, 1);
    http.created_at = MillisSinceEpoch::new(1_700_000_000_000);
    http.supported_protocol_versions = 1..=5;
    let mut lambda = Deployment::mock();
    lambda.id = DeploymentId::from_parts(2, 2);
    lambda.created_at = MillisSinceEpoch::new(1_700_000_001_000);
    lambda.supported_protocol_versions = 5..=6;
    lambda.ty = DeploymentType::Lambda {
        arn: ARN.parse().unwrap(),
        assume_role_arn: None,
        compression: None,
    };
    let (http_id, lambda_id) = (http.id.to_string(), lambda.id.to_string());
    let mut sorted_ids = [http_id.as_str(), lambda_id.as_str()];
    sorted_ids.sort_unstable();
    // Listing order differs from id order, so ORDER BY and a bare LIMIT are distinguishable.
    let schemas = Live::from_value(MockSchemas {
        deployments: vec![
            (lambda, vec![]),
            (
                http,
                vec![("greeter".to_owned(), 1), ("counter".to_owned(), 3)],
            ),
        ],
        ..Default::default()
    });

    let sessions = metadata_sessions(&core, schemas, [None, None]).await;

    // The expected first column keeps every case non-vacuous.
    for (sql, first_column) in [
        ("SELECT * FROM sys_deployment ORDER BY id", &sorted_ids[..]),
        (
            "SELECT endpoint, ty, min_service_protocol_version FROM sys_deployment WHERE ty = 'lambda'",
            &[ARN],
        ),
        (
            "SELECT id FROM sys_deployment WHERE created_at > TIMESTAMP '2023-11-14T22:13:20.500Z'",
            &[lambda_id.as_str()],
        ),
        (
            "SELECT id, max_service_protocol_version FROM sys_deployment WHERE max_service_protocol_version > 5",
            &[lambda_id.as_str()],
        ),
        (
            "SELECT id, services FROM sys_deployment WHERE array_has(services, 'counter')",
            &[http_id.as_str()],
        ),
        (
            "SELECT id, services FROM sys_deployment ORDER BY id LIMIT 1 OFFSET 1",
            &[sorted_ids[1]],
        ),
        ("SELECT ty FROM sys_deployment LIMIT 1", &["lambda"]),
        // The first listed deployment fails these predicates: the scan bound must
        // not apply before residual filtering.
        (
            "SELECT id FROM sys_deployment WHERE ty = 'http' LIMIT 1",
            &[http_id.as_str()],
        ),
        (
            "SELECT id FROM sys_deployment WHERE array_has(services, 'counter') LIMIT 1",
            &[http_id.as_str()],
        ),
        (
            "SELECT COUNT(*) FROM (SELECT * FROM sys_deployment LIMIT 0)",
            &["0"],
        ),
        // No services is an empty list, not NULL.
        (
            "SELECT COUNT(*) FROM sys_deployment WHERE services IS NULL",
            &["0"],
        ),
        ("SELECT COUNT(*) FROM sys_deployment", &["2"]),
    ] {
        assert_eq!(
            first_column_of_same_rows(&sessions, sql).await,
            first_column,
            "{sql}"
        );
    }
}

/// Stands in for a co-located worker's rule-book cache.
#[derive(Default)]
struct WorkerRuleBook(Mutex<Arc<RuleBook>>);

impl RuleBookObserver for WorkerRuleBook {
    fn notify_observed(&self, rule_book: RuleBook) {
        *self.0.lock() = Arc::new(rule_book);
    }

    fn get(&self) -> Arc<RuleBook> {
        Arc::clone(&self.0.lock())
    }
}

#[restate_core::test]
async fn rules_rows_match_v1_and_prefer_the_worker_cache() {
    use std::num::NonZeroU32;

    use restate_limiter::{Precondition, RuleChange, RuleUpsert, UserLimits};
    use restate_types::metadata::Precondition as WritePrecondition;
    use restate_types::metadata_store::keys::RULE_BOOK_KEY;

    let book =
        |rules: &[(&str, u32, Option<&str>, bool)]| {
            let mut book = RuleBook::empty();
            book.apply_changes(rules.iter().map(
                |&(pattern, concurrency, description, disabled)| {
                    let upsert = RuleUpsert {
                        limits: UserLimits::new(NonZeroU32::new(concurrency)),
                        description: description.map(str::to_owned),
                        disabled,
                        precondition: Precondition::None,
                    };
                    (pattern.parse().unwrap(), RuleChange::Upsert(upsert))
                },
            ))
            .unwrap();
            book
        };
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let stored = book(&[
        ("scope1/*/tenant1", 10, Some("tenant cap"), false),
        ("scope1/*", 0, None, true),
        ("*", 100, None, false),
    ]);
    core.metadata_store_client
        .put(RULE_BOOK_KEY.clone(), &stored, WritePrecondition::None)
        .await
        .unwrap();
    let patterns = ["*", "scope1/*", "scope1/*/tenant1"];

    // No co-located worker, then a worker that has not observed a book yet: both
    // read the store, and the worker keeps what was read.
    for worker_book in [None, Some(RuleBook::empty())] {
        let workers = [(); 2].map(|()| {
            worker_book
                .clone()
                .map(|book| Arc::new(WorkerRuleBook(Mutex::new(Arc::new(book)))))
        });
        let sessions = metadata_sessions(
            &core,
            Live::from_value(MockSchemas::default()),
            workers
                .clone()
                .map(|worker| worker.map(|worker| worker as Arc<dyn RuleBookObserver>)),
        )
        .await;
        // The expected first column keeps every case non-vacuous.
        for (sql, first_column) in [
            ("SELECT * FROM sys_rules ORDER BY pattern", &patterns[..]),
            (
                "SELECT pattern, concurrency, description FROM sys_rules WHERE concurrency IS NULL",
                &["scope1/*"],
            ),
            (
                "SELECT pattern FROM sys_rules WHERE disabled",
                &["scope1/*"],
            ),
            (
                "SELECT pattern FROM sys_rules WHERE concurrency > 10",
                &["*"],
            ),
            (
                "SELECT pattern FROM sys_rules WHERE description LIKE 'tenant%'",
                &["scope1/*/tenant1"],
            ),
            (
                "SELECT pattern FROM sys_rules WHERE version >= 1 AND last_modified IS NOT NULL ORDER BY pattern",
                &patterns,
            ),
            (
                "SELECT pattern FROM sys_rules ORDER BY pattern LIMIT 1 OFFSET 1",
                &["scope1/*"],
            ),
            (
                "SELECT pattern FROM sys_rules WHERE NOT disabled AND concurrency < 100 LIMIT 1",
                &["scope1/*/tenant1"],
            ),
            (
                "SELECT COUNT(*) FROM (SELECT * FROM sys_rules LIMIT 1)",
                &["1"],
            ),
            ("SELECT COUNT(*) FROM sys_rules", &["3"]),
        ] {
            assert_eq!(
                first_column_of_same_rows(&sessions, sql).await,
                first_column,
                "{sql}"
            );
        }
        for worker in workers.into_iter().flatten() {
            assert_eq!(*worker.get(), stored, "store_reads_fill_the_worker_cache");
        }
    }

    // A worker's observed book is served without reading the store.
    let cached = Arc::new(book(&[("scope2/*", 1, None, false)]));
    let sessions = metadata_sessions(
        &core,
        Live::from_value(MockSchemas::default()),
        [(); 2].map(|()| {
            Some(Arc::new(WorkerRuleBook(Mutex::new(Arc::clone(&cached))))
                as Arc<dyn RuleBookObserver>)
        }),
    )
    .await;
    assert_eq!(
        first_column_of_same_rows(&sessions, "SELECT pattern FROM sys_rules").await,
        ["scope2/*"]
    );
}

/// V1 and V2 sessions over the same metadata, each wired as in production. Each
/// engine gets its own co-located worker rule-book cache.
async fn metadata_sessions<T>(
    core: &TestCoreEnv<T>,
    schemas: Live<MockSchemas>,
    [legacy_worker, worker]: [Option<Arc<dyn RuleBookObserver>>; 2],
) -> [Arc<dyn QuerySession>; 2] {
    let options = HashMap::from([("datafusion.execution.batch_size".to_owned(), "1".to_owned())]);
    let legacy = v1::DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    legacy
        .register_tables(
            v1::UserTables::new(
                v1::context::SelectPartitionsFromMetadata,
                v1::remote_query_scanner_manager::RemoteScannerManager::local_only(
                    core.metadata.clone(),
                ),
            )
            .with_metadata(v1::MetadataTables::new(
                schemas.clone(),
                core.metadata_store_client.clone(),
                legacy_worker,
            )),
        )
        .await
        .unwrap();
    let current = DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    // Reads route to this node's registered metadata scanners.
    let sources = SourceRegistry::local_only(core.metadata.clone());
    register_metadata_scanners(
        schemas,
        core.metadata_store_client.clone(),
        worker,
        &sources,
    );
    current
        .register_tables(UserTables::new(SelectPartitionsFromMetadata, sources).with_metadata())
        .await
        .unwrap();
    [
        v1::context::DataFusionQueryEngine::from_env(legacy, None)
            .create_session(SessionOptions::default())
            .unwrap(),
        DataFusionQueryEngine::from_env(current, None)
            .create_session(SessionOptions::default())
            .unwrap(),
    ]
}

/// Asserts V1 and V2 return the same rows; returns their first column.
async fn first_column_of_same_rows(
    sessions: &[Arc<dyn QuerySession>; 2],
    sql: &str,
) -> Vec<String> {
    let expected = rows(sessions[0].as_ref(), sql).await;
    assert_eq!(rows(sessions[1].as_ref(), sql).await, expected, "{sql}");
    expected
        .into_iter()
        .map(|mut row| row.swap_remove(0))
        .collect()
}

#[tokio::test]
async fn stats_tables_follow_indexing_and_index_tables_never_show() {
    let previous_config = Configuration::pinned().clone();
    let mut config = Configuration::default();
    Configuration::set(config.clone());

    let query_env = v1::DataFusionEnv::new(10 * 1024 * 1024, None, Some(1), &HashMap::new())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    query_env
        .register_tables(v1::UserTables::new(
            v1::context::SelectPartitionsFromMetadata,
            v1::remote_query_scanner_manager::RemoteScannerManager::local_only(
                restate_core::MetadataBuilder::default().to_metadata(),
            ),
        ))
        .await
        .unwrap();
    let v1 = Arc::new(v1::context::DataFusionQueryEngine::from_env(
        query_env, None,
    ));
    let query_env = DataFusionEnv::new(10 * 1024 * 1024, None, Some(1), &HashMap::new())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    query_env
        .register_tables(UserTables::new(
            SelectPartitionsFromMetadata,
            SourceRegistry::local_only(restate_core::MetadataBuilder::default().to_metadata()),
        ))
        .await
        .unwrap();
    let v2 = Arc::new(DataFusionQueryEngine::from_env(query_env, None));
    // Both engines show the statistics tables where indexing is enabled, also with
    // the planner's index reads turned off: they read the gauges through their own
    // reader. Neither shows index tables: only the planner reads indexes. V1 is
    // linked as a normal library with its original catalog.
    let engines = [("v1", v1, false), ("v2", v2, true)];
    let mut enabled_sessions = Vec::new();
    for (indexes, disable_index_reads) in
        [(false, false), (true, false), (true, true), (false, false)]
    {
        config.common.experimental.set_indexes(indexes);
        config.admin.query_engine.disable_index_reads = disable_index_reads;
        Configuration::set(config.clone());
        for (engine_name, engine, is_v2) in &engines {
            let session = engine.create_session(SessionOptions::default()).unwrap();
            let names = table_names(session.as_ref(), "public").await;
            if *is_v2 {
                let expected: BTreeSet<_> = [
                    "state",
                    "sys_scheduler",
                    "sys_invocation_state",
                    "sys_user_limits",
                    "sys_invocation_status",
                    "sys_invocation",
                    "sys_journal",
                    "sys_journal_events",
                    "sys_locks",
                    "sys_promise",
                    "sys_vqueue_meta",
                    "sys_vqueue_entry_status",
                    "sys_vqueues",
                ]
                .into_iter()
                .chain(STATS.into_iter().filter(|_| indexes))
                .map(str::to_owned)
                .collect();
                assert_eq!(names, expected, "stats_follow_indexing");
            } else {
                assert!(names.contains("state") && names.contains("sys_invocation"));
            }
            let selected: Vec<_> = STATS
                .into_iter()
                .map(|name| (name, indexes))
                .chain(INDEXES.into_iter().map(|name| (name, false)))
                .collect();
            for (name, visible) in &selected {
                assert_eq!(names.contains(*name), *visible, "{engine_name}: {name}");
                if !visible {
                    assert!(
                        session
                            .execute(&format!("SELECT * FROM {name}"), QueryOptions {})
                            .await
                            .is_err()
                    );
                }
            }

            // Explicit selections, including renamed identities, cannot reach a hidden table.
            let aliases = engine
                .create_session(SessionOptions {
                    tables: Some(
                        selected
                            .iter()
                            .enumerate()
                            .map(|(i, (name, _))| SessionTable::new(*name, format!("selected_{i}")))
                            .collect(),
                    ),
                    ..Default::default()
                })
                .unwrap();
            let expected: BTreeSet<_> = selected
                .iter()
                .enumerate()
                .filter(|(_, (_, visible))| *visible)
                .map(|(i, _)| format!("selected_{i}"))
                .collect();
            assert_eq!(
                table_names(aliases.as_ref(), "public").await,
                expected,
                "{engine_name}"
            );
            if indexes {
                enabled_sessions.push(session);
            }
        }
    }
    // Turning indexing off affects only new sessions, not catalogs already
    // constructed.
    for session in enabled_sessions {
        let names = table_names(session.as_ref(), "public").await;
        for table in STATS {
            assert!(names.contains(table));
        }
        for table in INDEXES {
            assert!(!names.contains(table));
        }
    }
    Configuration::set(previous_config);
}

/// Both engines serve `config` from the scanner every node registers, which
/// redacts values that may be secret, and both hide it from sessions created
/// while `disable-config-sql-table` is set.
#[restate_core::test]
async fn config_reload_applies_to_both_engines() {
    use restate_storage_query_api::QueryEngineTable;
    use restate_storage_query_datafusion as v1;
    use restate_types::config::Configuration;

    use v1::config::ConfigTable;

    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let original = Configuration::pinned().clone();
    let mut config = original.clone();
    config.common.disable_config_sql_table = true;
    config.worker.snapshots.object_store.aws_secret_access_key = Some("hunter2".into());
    Configuration::set(config.clone());

    let options = HashMap::from([("datafusion.execution.target_partitions".into(), "1".into())]);
    let legacy_env = v1::DataFusionEnv::new(64 * 1024 * 1024, None, None, &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let env = DataFusionEnv::new(64 * 1024 * 1024, None, None, &options)
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let legacy_scanners =
        v1::remote_query_scanner_manager::RemoteScannerManager::local_only(core.metadata.clone());
    let scanners = SourceRegistry::local_only(core.metadata.clone());
    let scanner = v1::config::create_scanner(core.metadata.clone(), Configuration::live());
    legacy_scanners.register_node_scanner::<ConfigTable>(Arc::clone(&scanner));
    scanners.register_node_scanner::<crate::tables::config::ConfigTable>(Arc::clone(&scanner));
    legacy_env
        .register_tables(v1::ClusterTables::new(
            Default::default(),
            None,
            legacy_scanners,
        ))
        .await
        .unwrap();
    env.register_tables(ClusterTables::new(scanners, Default::default()))
        .await
        .unwrap();
    assert!(
        legacy_env
            .table_provider(&ConfigTable::identity())
            .is_some()
    );
    // Both engines identify the table as `config`, which is what Admin `/query` excludes.
    assert_eq!(
        <crate::tables::config::ConfigTable as crate::QuerySource>::identity(),
        ConfigTable::identity()
    );
    assert!(env.table_provider(&ConfigTable::identity()).is_some());
    // The scanner still serves rows while SQL exposure is disabled. ClusterTables uses
    // RPC even for node-local scans, so exercise it directly in this local-only fixture.
    let batches: Vec<_> = scanner
        .scan(
            legacy_env
                .table_provider(&ConfigTable::identity())
                .unwrap()
                .schema(),
            &[],
            1024,
            None,
        )
        .try_collect()
        .await
        .unwrap();
    assert!(batches.iter().any(|batch| batch.num_rows() > 0));
    let engine = v1::context::DataFusionQueryEngine::from_env(legacy_env, None)
        .with_engine(DataFusionQueryEngine::from_env(env, None));

    let mut sessions = Vec::new();
    for disabled in [true, false, true] {
        config.common.disable_config_sql_table = disabled;
        Configuration::set(config.clone());
        for version in [QueryEngineVersion::V1, QueryEngineVersion::V2] {
            for (tables, name) in [
                (None, "restate.cluster.config"),
                (
                    Some(vec![SessionTable::for_table::<ConfigTable>(
                        "restate.public.settings",
                    )]),
                    "settings",
                ),
            ] {
                let session = engine
                    .create_session(SessionOptions {
                        engine_version: Some(version),
                        schemas: vec!["cluster".into()],
                        tables,
                        // V1 identifies `partitions` as `partition`; V2 by its name.
                        excluded_tables: vec!["partition".into(), "partitions".into()],
                        ..Default::default()
                    })
                    .unwrap();
                let result = session
                    .execute(&format!("SELECT * FROM {name} LIMIT 0"), QueryOptions {})
                    .await;
                let hidden = disabled;
                if hidden {
                    assert!(result.is_err(), "{version}: {name}");
                } else {
                    result
                        .unwrap()
                        .stream
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();
                }
                // V2 reaches this node's scanner directly; V1 needs its remote-scanner
                // service even locally, which this fixture doesn't run.
                if !hidden && version == QueryEngineVersion::V2 {
                    assert_eq!(
                        rows(
                            session.as_ref(),
                            &format!(
                                "SELECT plain_node_id, value FROM {name} \
                                 WHERE key = 'worker.snapshots.aws-secret-access-key'"
                            ),
                        )
                        .await,
                        [["N1", "<REDACTED>"]],
                        "{name}"
                    );
                }
                sessions.push((session, hidden));
            }
        }
        // Reloads affect new sessions only, including explicit aliases. Existing exclusions
        // are preserved alongside the config setting.
        for (session, disabled_at_creation) in &sessions {
            for (names, expected) in [
                (
                    "'config', 'settings'",
                    if *disabled_at_creation { "0" } else { "1" },
                ),
                ("'partitions'", "0"),
            ] {
                let batches: Vec<_> = session.execute(
                        &format!("SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ({names})"),
                        QueryOptions {},
                    ).await.unwrap().stream.try_collect().await.unwrap();
                assert_eq!(
                    array_value_to_string(batches[0].column(0), 0).unwrap(),
                    expected
                );
            }
        }
    }
    Configuration::set(original);
}
