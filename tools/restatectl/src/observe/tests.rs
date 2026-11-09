// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};

use chrono::Utc;
use itertools::Itertools;

use restate_core::protobuf::node_ctl_svc::IdentResponse;
use restate_types::logs::LogId;
use restate_types::logs::LogletId;
use restate_types::logs::builder::LogsBuilder;
use restate_types::logs::metadata::{Chain, LogletParams, ProviderKind, SegmentIndex};
use restate_types::nodes_config::{
    ClusterFingerprint, LogServerConfig, NodeConfig, NodesConfiguration, QualifiedAddress, Role,
    StorageState, WorkerConfig, WorkerState,
};
use restate_types::partition_table::{PartitionReplication, PartitionTable};
use restate_types::protobuf::cluster::node_state::State;
use restate_types::protobuf::cluster::{
    AliveNode, ClusterState, DeadNode, NodeState, PartitionProcessorStatus, ReplayStatus, RunMode,
};
use restate_types::protobuf::common;
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::replication::{NodeSet, ReplicationProperty};
use restate_types::{GenerationalNodeId, PlainNodeId, RestateVersion, Version};

use super::sql::{PartitionRow, ReadStreamRow, SqlData, Tables};
use super::*;

/// The tables a cluster returns with `sql`'s rows: those read once, and those sampled.
fn tables(sql: SqlData) -> (Tables, Tables) {
    fn rows<T: serde::Serialize>(
        rows: impl IntoIterator<Item = T>,
    ) -> Result<Vec<serde_json::Value>, String> {
        Ok(rows
            .into_iter()
            .map(|row| serde_json::to_value(row).expect("rows serialize"))
            .collect())
    }
    let settled = Tables::from([("partitions".to_owned(), rows(sql.memberships.into_values()))]);
    let sampled = Tables::from([
        (
            "loglet_workers".to_owned(),
            rows(sql.loglet_workers.into_values().flatten()),
        ),
        (
            "bifrost_read_streams".to_owned(),
            rows(sql.read_streams.into_values().flatten()),
        ),
    ]);
    (settled, sampled)
}

fn node(id: u32) -> NodeConfig {
    NodeConfig::builder()
        .name(format!("node-{id}"))
        .current_generation(GenerationalNodeId::new(id, 1))
        .address(format!("http://node-{id}:5122/").parse().unwrap())
        .roles(Role::Worker | Role::LogServer)
        .log_server_config(LogServerConfig {
            storage_state: StorageState::ReadWrite,
        })
        .worker_config(WorkerConfig {
            worker_state: WorkerState::Active,
        })
        .binary_version(RestateVersion::current())
        .build()
}

fn processor(mode: RunMode, replay: ReplayStatus, leader: u32) -> PartitionProcessorStatus {
    PartitionProcessorStatus {
        planned_mode: mode.into(),
        effective_mode: mode.into(),
        replay_status: replay.into(),
        last_observed_leader_epoch: Some(common::LeaderEpoch { value: 3 }),
        last_observed_leader_node: Some(common::NodeId {
            id: leader,
            generation: Some(1),
        }),
        last_applied_log_lsn: Some(common::Lsn { value: 100 }),
        target_tail_lsn: Some(common::Lsn { value: 101 }),
        updated_at: Some(std::time::SystemTime::now().into()),
        ..Default::default()
    }
}

fn alive(id: u32, partitions: Vec<(u32, PartitionProcessorStatus)>) -> NodeState {
    NodeState {
        state: Some(State::Alive(AliveNode {
            generational_node_id: Some(common::GenerationalNodeId { id, generation: 1 }),
            partitions: partitions.into_iter().collect(),
            uptime_s: 3_600,
            ..Default::default()
        })),
    }
}

fn replicated_chain(log: u32, sequencer: u32, nodeset: Vec<u32>) -> Chain {
    let params = ReplicatedLogletParams {
        loglet_id: LogletId::new(LogId::from(log), SegmentIndex::from(0)),
        sequencer: GenerationalNodeId::new(sequencer, 1),
        replication: ReplicationProperty::new_unchecked(2),
        nodeset: NodeSet::from(nodeset),
    };
    Chain::new(
        ProviderKind::Replicated,
        LogletParams::from(params.serialize().unwrap()),
    )
}

fn reader(node: u32, log: u32, state: &str, read_pointer: u64) -> ReadStreamRow {
    ReadStreamRow {
        plain_node_id: format!("N{node}"),
        log_id: log,
        read_pointer: Some(read_pointer),
        end_lsn: Some(u64::MAX),
        state: Some(state.to_owned()),
        current_segment: Some(0),
        is_tailing: Some(true),
    }
}

fn membership(partition_id: u32, current: &str, next: Option<&str>) -> (u32, PartitionRow) {
    (
        partition_id,
        PartitionRow {
            partition_id,
            v_current: Some(1),
            current_replica_set: Some(current.to_owned()),
            v_next: next.map(|_| 2),
            next_replica_set: next.map(str::to_owned),
        },
    )
}

/// N3 is dead. Partition 0 is healthy, partition 1 is led by N1 but its log can't take appends
/// (only N1 of its nodeset is writable), and partition 2 has no leader while its replica set is
/// moved from [N2,N3] to [N1,N2].
pub(crate) fn cluster_that_lost_a_node() -> ClusterView {
    let mut nodes_config = NodesConfiguration::new(
        Version::MIN,
        "test".to_owned(),
        ClusterFingerprint::generate(),
    );
    for id in 1..=3 {
        nodes_config.upsert_node(node(id));
    }

    let mut partition_table =
        PartitionTable::with_equally_sized_partitions(Version::MIN, 3).into_builder();
    partition_table.set_partition_replication(PartitionReplication::Limit(
        ReplicationProperty::new_unchecked(2),
    ));
    let partition_table = partition_table.build();

    let mut logs = LogsBuilder::default();
    logs.add_log(LogId::from(0u32), replicated_chain(0, 1, vec![1, 2]))
        .unwrap();
    logs.add_log(LogId::from(1u32), replicated_chain(1, 1, vec![1, 3]))
        .unwrap();
    logs.add_log(LogId::from(2u32), replicated_chain(2, 3, vec![2, 3]))
        .unwrap();

    let cluster_state = ClusterState {
        nodes: BTreeMap::from([
            (
                1,
                alive(
                    1,
                    vec![
                        (0, processor(RunMode::Leader, ReplayStatus::Active, 1)),
                        (1, processor(RunMode::Leader, ReplayStatus::Active, 1)),
                    ],
                ),
            ),
            (
                2,
                alive(
                    2,
                    vec![
                        (0, processor(RunMode::Follower, ReplayStatus::Active, 1)),
                        (2, processor(RunMode::Follower, ReplayStatus::CatchingUp, 3)),
                    ],
                ),
            ),
            (
                3,
                NodeState {
                    state: Some(State::Dead(DeadNode {
                        last_seen_alive: Some(std::time::SystemTime::now().into()),
                    })),
                },
            ),
        ]),
        ..Default::default()
    };

    let ident = |id: u32| {
        (
            PlainNodeId::new(id),
            IdentResponse {
                restate_version: "1.8.2".to_owned(),
                nodes_config_version: 1,
                ..Default::default()
            },
        )
    };

    let (tables, sampled) = tables(SqlData {
        memberships: HashMap::from([
            membership(0, "N1,N2", None),
            membership(1, "N1,N3", None),
            membership(2, "N2,N3", Some("N1,N2")),
        ]),
        loglet_workers: HashMap::new(),
        read_streams: HashMap::from([
            (
                0,
                vec![reader(1, 0, "Reading", 99), reader(2, 0, "Reading", 40)],
            ),
            (
                2,
                vec![
                    reader(2, 2, "AwaitingOrSealChain", 60),
                    reader(3, 2, "Reading", 1),
                ],
            ),
        ]),
    });
    ClusterView::build(Snapshot {
        nodes_config,
        partition_table: Some(partition_table),
        logs: Some(logs.build()),
        idents: HashMap::from([ident(1), ident(2)]),
        metadata_servers: BTreeMap::new(),
        tables,
        nodes_probed: true,
        dialed: HashMap::new(),
        warnings: Vec::new(),
        sample: Sample {
            captured_at: Utc::now(),
            cluster_state: Some(cluster_state),
            tables: sampled,
            warnings: Vec::new(),
        },
    })
}

#[test]
fn health_of_a_cluster_that_lost_a_node() {
    let view = cluster_that_lost_a_node();
    let health: Vec<_> = view.partitions.iter().map(|p| p.health).collect();
    assert_eq!(
        health,
        [
            PartitionHealth::Healthy,
            PartitionHealth::Unavailable,
            PartitionHealth::Unavailable,
        ]
    );
    assert_eq!(view.partitions[1].missing, [PlainNodeId::new(3)]);
    assert!(view.partitions[1].is_led());
    assert!(!view.partitions[2].is_led());
    assert_eq!(view.nodes[0].partitions.leaders, 2);
    assert_eq!(view.nodes[0].logs.sequencer, 2);
    assert_eq!(view.logs[2].sequencer_state, Some(SequencerState::Dead));
    assert_eq!(view.logs[1].writable_nodes, 1);

    let findings = analyze(&view);
    let found = |code: &str, subject: Subject| {
        findings
            .iter()
            .find(|f| f.code == code && f.subject == subject)
            .map(|f| f.severity)
    };
    assert_eq!(
        found("partition-log-down", Subject::Partition(1)),
        Some(Severity::Critical)
    );
    assert_eq!(
        found("partition-unavailable", Subject::Partition(2)),
        Some(Severity::Critical)
    );
    assert_eq!(
        found("reconfiguring", Subject::Partition(2)),
        Some(Severity::Info)
    );
    assert_eq!(
        found("replica-missing", Subject::Partition(1)),
        Some(Severity::Warning)
    );
    assert_eq!(
        found("catching-up", Subject::Partition(2)),
        Some(Severity::Warning)
    );
    assert_eq!(
        found("sequencer-dead", Subject::Log(2)),
        Some(Severity::Critical)
    );
    assert_eq!(
        found("nodeset-unwritable", Subject::Log(1)),
        Some(Severity::Critical)
    );
    // a dead node is a warning on its own; what it costs shows in the findings above
    assert_eq!(
        found("node-dead", Subject::Node(PlainNodeId::new(3))),
        Some(Severity::Warning)
    );
    assert!(!findings.iter().any(|f| f.subject == Subject::Partition(0)));
    // read streams: a reader waiting for a seal, and one on a node without the partition
    assert_eq!(
        found("reader-awaiting-seal", Subject::Log(2)),
        Some(Severity::Warning)
    );
    assert_eq!(
        found("reader-without-processor", Subject::Log(2)),
        Some(Severity::Info)
    );
    let leader = view.partitions[0].leader_replica().unwrap();
    assert_eq!(
        leader.reader.as_ref().and_then(|r| r.read_pointer),
        Some(99)
    );
    assert_eq!(view.nodes[1].logs.readers, 2);

    // N3 is dead: not eligible, and the even split spreads over N1 and N2
    let partitions: Vec<_> = view.partitions.iter().collect();
    let placement = PartitionPlacement::of(&view, &partitions);
    let n3 = placement
        .nodes
        .iter()
        .find(|s| s.node == PlainNodeId::new(3))
        .unwrap();
    assert!(!n3.eligible);
    assert_eq!((n3.leaders, n3.replicas), (0, 2));
    assert_eq!(placement.nodes[0].leaders, 2);
    assert_eq!(placement.even_leaders, 1.0);
    let logs: Vec<_> = view.logs.iter().collect();
    let placement = LogPlacement::of(&view, &logs);
    assert_eq!(placement.nodes[2].sequencers, 1);
    assert!(!placement.nodes[2].sequencer_eligible);
    // most severe first
    assert!(findings.is_sorted_by(|a, b| a.severity >= b.severity));
}

/// A 20-node cluster with 256 partitions (replication 3) and as many logs (replication 2,
/// nodesets of 4), going through what real ones do: N7 is dead, N12 restarted and catches up,
/// N20 drains, N18 runs an older version and leadership leans towards N3. The partition N7 led
/// first has no leader yet, while its log's sequencer is still N7 and its tail segment is
/// sealed; four replica sets move off N7; logs have up to four segments.
pub(crate) fn large_cluster() -> ClusterView {
    ClusterView::build(large_cluster_snapshot())
}

/// What a cluster returns for `large_cluster`.
pub(crate) fn large_cluster_snapshot() -> Snapshot {
    use restate_metadata_server_grpc::grpc::{
        MetadataServerConfiguration, RaftSummary, SnapshotSummary, StatusResponse,
    };
    use restate_types::logs::Lsn;
    use restate_types::logs::metadata::{
        LogsConfiguration, NodeSetSize, ProviderConfiguration, ReplicatedLogletConfig,
    };
    use restate_types::nodes_config::{MetadataServerConfig, MetadataServerState};

    use super::sql::LogletWorkerRow;

    const NODES: u32 = 20;
    const PARTITIONS: u32 = 256;
    const DEAD: u32 = 7;
    const RESTARTED: u32 = 12;
    const DRAINING: u32 = 20;
    const OLD_VERSION: u32 = 18;
    let generation = |node: u32| if node == RESTARTED { 4 } else { 2 };
    let gen_id = |node: u32| GenerationalNodeId::new(node, generation(node));

    let mut nodes_config = NodesConfiguration::new(
        Version::from(42),
        "prod-eu-1".to_owned(),
        ClusterFingerprint::generate(),
    );
    for id in 1..=NODES {
        let mut roles = Role::Worker | Role::LogServer;
        if id <= 5 {
            roles |= Role::MetadataServer;
        }
        if id <= 3 {
            roles |= Role::Admin | Role::HttpIngress;
        }
        nodes_config.upsert_node(
            NodeConfig::builder()
                .name(format!("prod-eu-1-{id:02}"))
                .current_generation(gen_id(id))
                .address(format!("http://10.0.1.{id}:5122/").parse().unwrap())
                .addresses(vec![QualifiedAddress {
                    address: format!("http://n{id}.eu-west-1.internal:5122/")
                        .parse()
                        .unwrap(),
                    location: Some("eu-west-1".parse().unwrap()),
                }])
                .ctrl_address(Some(format!("https://10.0.1.{id}:5123/").parse().unwrap()))
                .ctrl_addresses(vec![
                    QualifiedAddress {
                        address: format!("https://10.0.1.{id}:5123/").parse().unwrap(),
                        location: Some(
                            format!("eu-west-1.az{}", (id - 1) % 3 + 1).parse().unwrap(),
                        ),
                    },
                    QualifiedAddress {
                        address: format!(
                            "https://prod-eu-1-{id:02}.restate.eu-west-1.compute.internal:5123/"
                        )
                        .parse()
                        .unwrap(),
                        location: Some("eu-west-1".parse().unwrap()),
                    },
                ])
                .location(format!("eu-west-1.az{}", (id - 1) % 3 + 1).parse().unwrap())
                .roles(roles)
                .log_server_config(LogServerConfig {
                    storage_state: StorageState::ReadWrite,
                })
                .worker_config(WorkerConfig {
                    worker_state: if id == DRAINING {
                        WorkerState::Draining
                    } else {
                        WorkerState::Active
                    },
                })
                .metadata_server_config(MetadataServerConfig {
                    metadata_server_state: if id <= 5 {
                        MetadataServerState::Member
                    } else {
                        MetadataServerState::Standby
                    },
                })
                .binary_version(RestateVersion::current())
                .build(),
        );
    }

    let mut partition_table =
        PartitionTable::with_equally_sized_partitions(Version::from(3), PARTITIONS as u16)
            .into_builder();
    partition_table.set_partition_replication(PartitionReplication::Limit(
        ReplicationProperty::new_unchecked(3),
    ));
    let partition_table = partition_table.build();

    // replica sets spread over the 19 active workers; leadership leans towards N3
    let members = |p: u32| [p % 19 + 1, (p + 6) % 19 + 1, (p + 12) % 19 + 1];
    let first_leader = |p: u32| {
        let m = members(p);
        if m.contains(&3) {
            3
        } else {
            m[(p as usize / 19) % 3]
        }
    };
    let unled = (0..PARTITIONS).find(|p| first_leader(*p) == DEAD).unwrap();
    // a node that catches up doesn't lead
    let leader = |p: u32| {
        let m = members(p);
        match first_leader(p) {
            DEAD if p == unled => None,
            DEAD | RESTARTED => m.iter().copied().find(|n| *n != DEAD && *n != RESTARTED),
            l => Some(l),
        }
    };
    let moving: Vec<u32> = (0..PARTITIONS)
        .filter(|p| *p != unled && members(*p).contains(&DEAD))
        .take(4)
        .collect();
    let tail = |p: u32| 4_000_000 + u64::from(p) * 37_000 + u64::from(p * 13 % 5_000);

    let mut processors: BTreeMap<u32, Vec<(u32, PartitionProcessorStatus)>> = BTreeMap::new();
    let mut memberships = HashMap::new();
    let mut read_streams: HashMap<u32, Vec<ReadStreamRow>> = HashMap::new();
    let mut loglet_workers: HashMap<u32, Vec<LogletWorkerRow>> = HashMap::new();
    let mut logs = LogsBuilder::default();
    logs.set_configuration(LogsConfiguration {
        default_provider: ProviderConfiguration::Replicated(ReplicatedLogletConfig {
            replication_property: ReplicationProperty::new_unchecked(2),
            target_nodeset_size: NodeSetSize::ZERO,
        }),
    });

    for p in 0..PARTITIONS {
        let m = members(p);
        let leader = leader(p);
        let tail = tail(p);
        let epoch = 10 + u64::from(p % 7) + u64::from(first_leader(p) == DEAD);
        let leader_applied = tail - 1 - u64::from(p % 40);

        memberships.insert(
            p,
            PartitionRow {
                partition_id: p,
                v_current: Some(5),
                current_replica_set: Some(m.iter().map(|n| format!("N{n}")).join(",")),
                v_next: moving.contains(&p).then_some(6),
                next_replica_set: moving.contains(&p).then(|| {
                    let spare = (8..=11).find(|n| !m.contains(n)).unwrap();
                    m.iter()
                        .map(|n| if *n == DEAD { spare } else { *n })
                        .map(|n| format!("N{n}"))
                        .join(",")
                }),
            },
        );

        // the log: up to four segments, the tail one sequenced by the leader
        let segments = 1 + p % 4;
        let tail_base = tail - 100_000 - u64::from(p % 7) * 10_000;
        let bases: Vec<u64> = (0..segments)
            .rev()
            .map(|back| tail_base - u64::from(back) * 400_000)
            .collect();
        let nodeset: Vec<u32> = (0..4).map(|i| (p + i * 5) % 20 + 1).collect();
        let tail_sequencer = match leader {
            None => DEAD,
            Some(l) if p % 11 == 0 => m.iter().copied().find(|n| *n != l && *n != DEAD).unwrap(),
            Some(l) => l,
        };
        let params = |index: u32, sequencer: u32| {
            LogletParams::from(
                ReplicatedLogletParams {
                    loglet_id: LogletId::new(LogId::from(p), SegmentIndex::from(index)),
                    sequencer: gen_id(sequencer),
                    replication: ReplicationProperty::new_unchecked(2),
                    nodeset: NodeSet::from(nodeset.clone()),
                }
                .serialize()
                .unwrap(),
            )
        };
        // the builder numbers a log's segments from 0
        let first_index = 0;
        let mut chain = logs
            .add_log(
                LogId::from(p),
                Chain::with_base_lsn(
                    Lsn::from(bases[0]),
                    ProviderKind::Replicated,
                    params(
                        first_index,
                        if segments == 1 {
                            tail_sequencer
                        } else {
                            m[(p as usize) % 3]
                        },
                    ),
                ),
            )
            .unwrap();
        for (i, base) in bases.iter().enumerate().skip(1) {
            let sequencer = if i + 1 == bases.len() {
                tail_sequencer
            } else {
                m[(p as usize + i) % 3]
            };
            chain
                .append_segment(
                    Lsn::from(*base),
                    ProviderKind::Replicated,
                    params(first_index + i as u32, sequencer),
                )
                .unwrap();
        }
        let tail_index = first_index + segments - 1;
        let trim = leader_applied - 140_000;
        for node in nodeset.iter().filter(|n| **n != DEAD) {
            let workers = loglet_workers.entry(p).or_default();
            workers.push(LogletWorkerRow {
                plain_node_id: format!("N{node}"),
                log_id: p,
                segment_index: tail_index,
                known_global_tail: Some((tail - tail_base + 1) as u32),
                trim_point: (trim >= tail_base).then(|| (trim - tail_base + 1) as u32),
                is_sealed: Some(leader.is_none()),
                ..Default::default()
            });
            if trim < tail_base && segments > 1 {
                workers.push(LogletWorkerRow {
                    plain_node_id: format!("N{node}"),
                    log_id: p,
                    segment_index: tail_index - 1,
                    trim_point: Some((trim - bases[bases.len() - 2] + 1) as u32),
                    is_sealed: Some(true),
                    ..Default::default()
                });
            }
        }

        // the processors and their readers
        for (i, node) in m.iter().copied().enumerate() {
            if node == DEAD {
                continue;
            }
            let leads = leader == Some(node);
            let candidate = leader.is_none() && i == 1;
            let catching_up = node == RESTARTED;
            let applied = if catching_up {
                leader_applied - 140_000 - u64::from(p % 50) * 1_000
            } else if leads {
                leader_applied
            } else {
                leader_applied - u64::from(p % 25)
            };
            let status = PartitionProcessorStatus {
                planned_mode: if leads || candidate {
                    RunMode::Leader
                } else {
                    RunMode::Follower
                }
                .into(),
                effective_mode: if leads {
                    RunMode::Leader
                } else {
                    RunMode::Follower
                }
                .into(),
                replay_status: if catching_up {
                    ReplayStatus::CatchingUp
                } else {
                    ReplayStatus::Active
                }
                .into(),
                last_observed_leader_epoch: Some(common::LeaderEpoch { value: epoch }),
                last_observed_leader_node: leader.map(|l| common::NodeId {
                    id: l,
                    generation: Some(generation(l)),
                }),
                last_applied_log_lsn: Some(common::Lsn { value: applied }),
                durable_lsn: Some(common::Lsn {
                    value: applied - 3_000,
                }),
                last_archived_log_lsn: leads.then_some(common::Lsn {
                    value: leader_applied - 120_000,
                }),
                target_tail_lsn: Some(common::Lsn { value: tail }),
                last_applied_schema_version: Some(common::Version { value: 17 }),
                enabled_features: vec!["journal_v2".to_owned(), "vqueues".to_owned()],
                updated_at: Some(std::time::SystemTime::now().into()),
                ..Default::default()
            };
            processors.entry(node).or_default().push((p, status));
            let read_pointer = applied + 1;
            read_streams.entry(p).or_default().push(ReadStreamRow {
                plain_node_id: format!("N{node}"),
                log_id: p,
                read_pointer: Some(read_pointer),
                end_lsn: Some(u64::MAX),
                state: Some(if leader.is_none() && !candidate {
                    "AwaitingOrSealChain".to_owned()
                } else {
                    "Reading".to_owned()
                }),
                current_segment: bases
                    .iter()
                    .rposition(|b| *b <= read_pointer)
                    .map(|at| first_index + at as u32),
                is_tailing: Some(true),
            });
        }
    }

    let cluster_state = ClusterState {
        nodes: (1..=NODES)
            .map(|id| {
                let state = if id == DEAD {
                    State::Dead(DeadNode {
                        last_seen_alive: Some(
                            (std::time::SystemTime::now() - std::time::Duration::from_secs(134))
                                .into(),
                        ),
                    })
                } else {
                    State::Alive(AliveNode {
                        generational_node_id: Some(common::GenerationalNodeId {
                            id,
                            generation: generation(id),
                        }),
                        partitions: processors
                            .remove(&id)
                            .unwrap_or_default()
                            .into_iter()
                            .collect(),
                        uptime_s: if id == RESTARTED {
                            41
                        } else {
                            260_000 + u64::from(id) * 1_000
                        },
                        ..Default::default()
                    })
                };
                (id, NodeState { state: Some(state) })
            })
            .collect(),
        ..Default::default()
    };

    // admins and ingresses on N1-N3; control isn't advertised in a node's report
    let servers = |id: u32, advertised: bool| {
        use restate_core::protobuf::node_ctl_svc::ident_response::{AddressKind, NetAddress};
        let address = |name: &str, address: String, kind: AddressKind| NetAddress {
            name: name.to_owned(),
            address,
            kind: kind.into(),
        };
        let mut servers = Vec::new();
        if id <= 3 {
            servers.push(if advertised {
                address(
                    "http-ingress-server",
                    format!("http://n{id}.eu-west-1.internal:8080/"),
                    AddressKind::Http,
                )
            } else {
                address(
                    "http-ingress-server",
                    "0.0.0.0:8080".to_owned(),
                    AddressKind::Tcp,
                )
            });
            servers.push(if advertised {
                address(
                    "admin-api-server",
                    format!("http://n{id}.eu-west-1.internal:9070/"),
                    AddressKind::Http,
                )
            } else {
                address(
                    "admin-api-server",
                    "0.0.0.0:9070".to_owned(),
                    AddressKind::Tcp,
                )
            });
        }
        if advertised {
            servers.push(address(
                "message-fabric-server",
                format!("http://10.0.1.{id}:5122/"),
                AddressKind::Http,
            ));
        } else {
            servers.push(address(
                "message-fabric-server",
                "0.0.0.0:5122".to_owned(),
                AddressKind::Tcp,
            ));
            servers.push(address(
                "message-fabric-server",
                format!("/var/lib/restate/restate-data/prod-eu-1-{id:02}/fabric.sock"),
                AddressKind::Unix,
            ));
            servers.push(address(
                "control-server",
                "0.0.0.0:5123".to_owned(),
                AddressKind::Tcp,
            ));
        }
        servers
    };
    let idents = (1..=NODES)
        .filter(|id| *id != DEAD)
        .map(|id| {
            (
                PlainNodeId::new(id),
                IdentResponse {
                    advertised_addresses: servers(id, true),
                    bound_addresses: servers(id, false),
                    restate_version: if id == OLD_VERSION { "1.8.1" } else { "1.8.2" }.to_owned(),
                    nodes_config_version: 42,
                    logs_version: 1_207,
                    partition_table_version: 3,
                    schema_version: 17,
                    ..Default::default()
                },
            )
        })
        .collect();
    let metadata_servers = (1..=5)
        .filter(|id| *id != DEAD)
        .map(|id| {
            (
                PlainNodeId::new(id),
                Ok(StatusResponse {
                    status: restate_types::protobuf::common::MetadataServerStatus::Member.into(),
                    configuration: Some(MetadataServerConfiguration {
                        version: Some(common::Version { value: 3 }),
                        members: (1..=5).map(|m| (m, 1)).collect(),
                    }),
                    leader: Some(2),
                    snapshot: Some(SnapshotSummary {
                        index: 118_000,
                        size: 2_400_000,
                    }),
                    raft: Some(RaftSummary {
                        term: 9,
                        applied: 120_431,
                        committed: 120_431,
                        first_index: 118_001,
                        last_index: 120_431,
                    }),
                }),
            )
        })
        .collect();

    let (tables, sampled) = tables(SqlData {
        memberships,
        loglet_workers,
        read_streams,
    });
    Snapshot {
        nodes_config,
        partition_table: Some(partition_table),
        logs: Some(logs.build()),
        idents,
        metadata_servers,
        tables,
        nodes_probed: true,
        // restatectl runs in eu-west-1 outside the zones: the region-wide entries
        dialed: (1..=NODES)
            .map(|id| {
                (
                    PlainNodeId::new(id),
                    format!("https://prod-eu-1-{id:02}.restate.eu-west-1.compute.internal:5123/"),
                )
            })
            .collect(),
        warnings: Vec::new(),
        sample: Sample {
            captured_at: Utc::now(),
            cluster_state: Some(cluster_state),
            tables: sampled,
            warnings: Vec::new(),
        },
    }
}
