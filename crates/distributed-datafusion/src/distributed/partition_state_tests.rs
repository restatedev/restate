// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};
use std::task::Poll;
use std::time::Duration;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::exec_err;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt, stream};
use tokio::sync::watch;

use restate_core::TaskCenter;
use restate_core::network::transport_connector::test_util::MockConnector;
use restate_core::test_env::{TestCoreEnv, TestCoreEnvBuilder, create_mock_nodes_config};
use restate_storage_query_api::{QueryOptions, QuerySession, SessionOptions};
use restate_types::cluster::cluster_state::PartitionProcessorStatus;
use restate_types::cluster_state::NodeState;
use restate_types::identifiers::PartitionId;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::nodes_config::Role;
use restate_types::partition_table::PartitionTable;
use restate_types::partitions::state::{
    LeadershipState, MemberState, PartitionReplicaSetStates, ReplicaSetState,
};
use restate_types::{GenerationalNodeId, PlainNodeId, Version};
use restate_worker_api::{ProcessorsManagerCommand, ProcessorsManagerHandle};

use crate::context::DataFusionQueryEngine;
use crate::source_registry::SourceRegistry;
use crate::table_providers::Scan;
use crate::tables::partition_state::{PartitionStateReports, PartitionStateTable, reports_schema};

use super::DistributedQueryServer;
use super::tests::{TestInvokers, UnusedRuleBook, environment};

/// A partition processor manager answering every state request with `state`, or
/// never answering without one.
fn manager(state: Option<&[u16]>) -> ProcessorsManagerHandle {
    let state: Option<BTreeMap<_, _>> = state.map(|partitions| {
        partitions
            .iter()
            .map(|p| (PartitionId::from(*p), PartitionProcessorStatus::default()))
            .collect()
    });
    let (commands, mut receiver) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        // Without a state, requests stay queued and unanswered.
        let Some(state) = state else {
            return std::future::pending().await;
        };
        while let Some(command) = receiver.recv().await {
            let ProcessorsManagerCommand::GetState(reply) = command else {
                panic!("expected a state request");
            };
            let _ = reply.send(state.clone());
        }
    });
    ProcessorsManagerHandle::new(commands, Arc::new(UnusedRuleBook), TestInvokers::empty())
}

/// Fails before sending any row.
#[derive(Debug)]
struct Fails(&'static str);

impl Scan for Fails {
    fn scan(
        &self,
        projection: SchemaRef,
        _: &[datafusion::logical_expr::Expr],
        _: usize,
        _: Option<usize>,
    ) -> SendableRecordBatchStream {
        let message = self.0;
        Box::pin(RecordBatchStreamAdapter::new(
            projection,
            stream::once(async move { exec_err!("{message}") }),
        ))
    }
}

/// Sends the first row of its inner report, then fails.
#[derive(Debug)]
struct FailsAfterFirstRow(Arc<dyn Scan>);

impl Scan for FailsAfterFirstRow {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[datafusion::logical_expr::Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let first = self
            .0
            .scan(Arc::clone(&projection), filters, batch_size, limit)
            .take(1)
            .map_ok(|batch| batch.slice(0, 1));
        Box::pin(RecordBatchStreamAdapter::new(
            projection,
            first.chain(stream::once(async { exec_err!("connection lost") })),
        ))
    }
}

/// Never answers; `live` counts its scans that haven't been dropped.
#[derive(Debug)]
struct NeverAnswers {
    live: Arc<watch::Sender<usize>>,
}

struct Live(Arc<watch::Sender<usize>>);

impl Drop for Live {
    fn drop(&mut self) {
        self.0.send_modify(|live| *live -= 1);
    }
}

impl Scan for NeverAnswers {
    fn scan(
        &self,
        projection: SchemaRef,
        _: &[datafusion::logical_expr::Expr],
        _: usize,
        _: Option<usize>,
    ) -> SendableRecordBatchStream {
        self.live.send_modify(|live| *live += 1);
        let live = Live(Arc::clone(&self.live));
        Box::pin(RecordBatchStreamAdapter::new(
            projection,
            stream::poll_fn(move |_| {
                let _ = &live;
                Poll::Pending
            }),
        ))
    }
}

async fn wait_live(live: &watch::Sender<usize>, expected: usize) {
    tokio::time::timeout(
        Duration::from_secs(60),
        live.subscribe().wait_for(|live| *live == expected),
    )
    .await
    .expect("the never-answering worker sees its scans start and stop")
    .unwrap();
}

fn members(nodes: &[u32]) -> ReplicaSetState {
    ReplicaSetState {
        version: Version::MIN,
        members: nodes
            .iter()
            .map(|node| MemberState {
                node_id: PlainNodeId::new(*node),
                durable_lsn: Lsn::INVALID,
            })
            .collect(),
    }
}

/// Sorted rows with columns joined by `|` (NULL as empty), and sorted
/// `node: message` warnings, read from the query's public diagnostics.
async fn rows_and_warnings(session: &dyn QuerySession, sql: &str) -> (Vec<String>, Vec<String>) {
    let result = session.execute(sql, QueryOptions {}).await.unwrap();
    let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
    let mut rows: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column, row).unwrap())
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect();
    if !sql.contains("ORDER BY") {
        rows.sort();
    }
    let mut warnings: Vec<_> = result
        .diagnostics
        .warnings()
        .iter()
        .map(|warning| format!("{}: {}", warning.node_id, warning.message))
        .collect();
    warnings.sort();
    (rows, warnings)
}

/// Asserts each warning starts with its node and contains its expected text.
fn assert_warnings(actual: &[String], expected: &[(&str, &str)]) {
    assert_eq!(actual.len(), expected.len(), "{actual:#?}");
    for (actual, (node, text)) in actual.iter().zip(expected) {
        assert!(
            actual.starts_with(&format!("{node}: ")) && actual.contains(text),
            "{actual} should be {node}: …{text}…"
        );
    }
}

/// A coordinator without a cluster controller, N1, asks the workers for their
/// reports and reconciles them with its partition table and observed memberships:
///
/// - P0: N1 reports; N2 (failing over) has a complete, empty report; N5 never
///   answers. N2 is in both the current and next membership, and appears once.
/// - P1/P2: N3 fails before any row; N4 sends P1 and fails before P2.
/// - P3: N6 is dead in gossip at its configured generation, so it isn't asked;
///   gossip sees N7 dead only at an older generation, so N7 is asked and answers;
///   N8 isn't a worker. N1 also reports P3 without being a member.
/// - P4 has no known replicas.
/// - P5: N9 restarted with a new generation, which rejects the old one's task.
///
/// Gossip never heard of N3; it is asked too.
#[restate_core::test(start_paused = true)]
async fn partition_state_reconciles_worker_reports() {
    let timeout = Duration::from_secs(3);
    let workers = Arc::new(OnceLock::<HashMap<GenerationalNodeId, SourceRegistry>>::new());
    let (connector, _) = MockConnector::new({
        let workers = Arc::clone(&workers);
        move |node, router: &mut restate_core::network::MessageRouterBuilder| {
            let server = DistributedQueryServer::new(
                environment(1, 128),
                workers.get().unwrap()[&node].clone(),
                router,
            );
            crate::mocks::spawn_query_server("partition-state-worker", server);
        }
    });
    // The configured generations; N7's is newer than the one gossip saw die.
    let configured = |id| GenerationalNodeId::new(id, if id == 7 { 2 } else { 1 });
    let mut nodes = create_mock_nodes_config(1, 1);
    for id in 2..=9 {
        let mut node = create_mock_nodes_config(id, configured(id).generation())
            .find_node_by_id(configured(id))
            .unwrap()
            .clone();
        if id == 8 {
            node.roles = Role::Admin.into();
        }
        nodes.upsert_node(node);
    }
    let mut coordinator = TestCoreEnvBuilder::with_transport_connector(connector)
        .set_nodes_config(nodes)
        .set_partition_table(PartitionTable::with_equally_sized_partitions(
            Version::MIN,
            6,
        ));
    let states = PartitionReplicaSetStates::default();
    for (partition, current, next) in [
        (0, members(&[1, 2]), Some(members(&[2, 5]))),
        (1, members(&[3, 4]), None),
        (2, members(&[4]), None),
        (3, members(&[6, 7, 8]), None),
        (5, members(&[9]), None),
    ] {
        states.note_observed_membership(
            PartitionId::from(partition),
            LeadershipState::default(),
            &current,
            &next,
        );
    }

    let coordinator_sources = SourceRegistry::local_only(coordinator.metadata.clone());
    let server = DistributedQueryServer::new(
        environment(1, 128),
        coordinator_sources.clone(),
        &mut coordinator.router_builder,
    );
    crate::mocks::spawn_query_server("partition-state-coordinator", server);
    let coordinator = coordinator.build().await;
    coordinator_sources.register_node_scanner::<PartitionStateReports>(
        PartitionStateTable::create_reports_scanner(
            manager(Some(&[0, 3])),
            coordinator.metadata.clone(),
        ),
    );

    // The worker's own side: rows for its node, and a bound on its manager of its
    // own, so a stuck manager fails the scan even if no cancellation arrives.
    let scan = |processors| {
        PartitionStateTable::create_reports_scanner(processors, coordinator.metadata.clone())
            .scan(reports_schema(), &[], 128, None)
            .try_collect::<Vec<_>>()
    };
    let batches = scan(manager(Some(&[0, 3]))).await.unwrap();
    assert_eq!(
        (batches.len(), batches[0].num_rows()),
        (1, 2),
        "{batches:?}"
    );
    let row = |column: &str, row| {
        array_value_to_string(batches[0].column_by_name(column).unwrap(), row).unwrap()
    };
    assert_eq!(
        [
            row("partition_id", 1),
            row("plain_node_id", 1),
            row("gen_node_id", 1)
        ],
        ["3", "N1", "N1:1"]
    );
    let started = tokio::time::Instant::now();
    let stuck = scan(manager(None)).await.unwrap_err().to_string();
    assert!(stuck.contains("didn't answer within 3s"), "{stuck}");
    assert_eq!(started.elapsed(), timeout);

    let never_answers = Arc::new(watch::Sender::new(0));
    let mut remote = HashMap::new();
    for id in 2..=9 {
        let generation = if id == 9 {
            2
        } else {
            configured(id).generation()
        };
        let worker = TestCoreEnv::create_with_single_node(id, generation).await;
        let sources = SourceRegistry::local_only(worker.metadata.clone());
        let reports = |partitions: &[u16]| {
            PartitionStateTable::create_reports_scanner(
                manager(Some(partitions)),
                worker.metadata.clone(),
            )
        };
        sources.register_node_scanner::<PartitionStateReports>(match id {
            2 => reports(&[]),
            3 => Arc::new(Fails("manager crashed")),
            4 => Arc::new(FailsAfterFirstRow(reports(&[1, 2]))),
            5 => Arc::new(NeverAnswers {
                live: Arc::clone(&never_answers),
            }),
            6 => Arc::new(Fails("a skipped worker was asked")),
            7 => reports(&[3]),
            _ => reports(&[5]),
        });
        // N9's new generation answers the dial of its old one.
        remote.insert(configured(id), sources);
    }
    workers.set(remote).unwrap();

    let mut gossip = TaskCenter::current().cluster_state().clone().updater();
    let env = environment(1, 128).with_distributed_execution(coordinator.networking.clone());
    env.register_table::<PartitionStateTable>(
        "public",
        "partition_state",
        PartitionStateTable::create_provider(
            coordinator.metadata.clone(),
            coordinator_sources.clone(),
            TaskCenter::current().cluster_state().clone(),
            states.clone(),
        ),
    )
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let strings = |values: &[&str]| values.iter().map(|v| (*v).to_owned()).collect::<Vec<_>>();
    let explain = |sql: &str| {
        let session = Arc::clone(&session);
        let sql = format!("EXPLAIN {sql}");
        async move { rows_and_warnings(session.as_ref(), &sql).await.0.join("\n") }
    };

    for (node, state) in [
        (GenerationalNodeId::new(1, 1), NodeState::Alive),
        (GenerationalNodeId::new(2, 1), NodeState::FailingOver),
        (GenerationalNodeId::new(4, 1), NodeState::Alive),
        (GenerationalNodeId::new(5, 1), NodeState::Alive),
        (GenerationalNodeId::new(6, 1), NodeState::Dead),
        (GenerationalNodeId::new(7, 1), NodeState::Dead),
        (GenerationalNodeId::new(9, 1), NodeState::Alive),
    ] {
        gossip.upsert_node_state(node, state);
    }
    let sql = "SELECT partition_id, plain_node_id, report, gen_node_id FROM partition_state";
    let started = tokio::time::Instant::now();
    let (rows, warnings) = rows_and_warnings(session.as_ref(), sql).await;
    // Collection waits for the slowest worker up to the bound, not longer.
    assert_eq!(started.elapsed(), timeout);
    assert_eq!(
        rows,
        strings(&[
            "0|N1|reported|N1:1",
            "0|N2|not_reported|",
            "0|N5|node_unavailable|",
            "1|N3|node_unavailable|",
            "1|N4|reported|N4:1",
            "2|N4|node_unavailable|",
            "3|N1|reported|N1:1",
            "3|N6|node_unavailable|",
            "3|N7|reported|N7:2",
            "3|N8|node_unavailable|",
            "4||no_known_replicas|",
            "5|N9|node_unavailable|",
        ])
    );
    assert_warnings(
        &warnings,
        &[
            ("N3", "manager crashed"),
            ("N4", "connection lost"),
            ("N5", "no complete report within 3s"),
            ("N6", "skipped: gossip reports the node dead"),
            ("N8", "not asked: not a worker in the nodes configuration"),
            ("N9", "wrong owner"),
        ],
    );
    let plan = explain(sql).await;
    assert!(
        plan.contains(
            "asked=[N1:1(local), N2:1, N3:1, N4:1, N5:1, N7:2, N9:1], \
             not_asked=[N6 (gossip dead), N8 (not a worker)], timeout=3s"
        ),
        "{plan}"
    );

    // Rows without a report have NULL processor columns, typed for selection, and
    // filters and limits apply to the reconciled rows.
    for (sql, expected) in [
        (
            "SELECT COUNT(*) FROM partition_state WHERE effective_mode IS NULL",
            &["8"][..],
        ),
        (
            "SELECT COUNT(*) FROM partition_state WHERE plain_node_id IS NULL",
            &["1"][..],
        ),
        (
            "SELECT partition_id, plain_node_id FROM partition_state \
             WHERE report = 'node_unavailable' ORDER BY partition_id DESC, plain_node_id LIMIT 2",
            &["5|N9", "3|N6"][..],
        ),
    ] {
        assert_eq!(
            rows_and_warnings(session.as_ref(), sql).await.0,
            strings(expected),
            "{sql}"
        );
    }

    // Cancelling during collection: no row has been emitted, and dropping the query
    // stream stops the collection, even though the diagnostics keep the plan.
    wait_live(&never_answers, 0).await;
    let started = tokio::time::Instant::now();
    let result = session
        .execute("SELECT * FROM partition_state", QueryOptions {})
        .await
        .unwrap();
    let diagnostics = result.diagnostics;
    let mut stream = result.stream;
    assert!(
        tokio::time::timeout(timeout / 3, stream.next())
            .await
            .is_err()
    );
    wait_live(&never_answers, 1).await;
    drop(stream);
    wait_live(&never_answers, 0).await;
    assert!(started.elapsed() < timeout, "stopped only by the bound");
    assert!(diagnostics.warnings().is_empty());

    // Every worker dead in gossip at its configured generation: nobody is asked,
    // and every expected replica is still a row.
    for id in [1, 2, 3, 4, 5, 6, 7, 9] {
        gossip.upsert_node_state(configured(id), NodeState::Dead);
    }
    let sql = "SELECT report, COUNT(*) FROM partition_state GROUP BY report";
    let (rows, warnings) = rows_and_warnings(session.as_ref(), sql).await;
    assert_eq!(
        rows,
        strings(&["no_known_replicas|1", "node_unavailable|10"])
    );
    let mut expected: Vec<_> = [1, 2, 3, 4, 5, 6, 7, 9]
        .map(|id| (format!("N{id}"), "skipped: gossip reports the node dead"))
        .into_iter()
        .collect();
    expected.push(("N8".to_owned(), "not asked: not a worker"));
    expected.sort();
    let expected: Vec<_> = expected.iter().map(|(n, t)| (n.as_str(), *t)).collect();
    assert_warnings(&warnings, &expected);
    let plan = explain(sql).await;
    assert!(
        plan.contains("PartitionStateExec: partitions=6, asked=[], not_asked=[N1 (gossip dead)"),
        "{plan}"
    );
}
