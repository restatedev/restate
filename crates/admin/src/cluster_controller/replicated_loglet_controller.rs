// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use enumset::EnumSet;
use itertools::Itertools;
use rand::seq::IteratorRandom;
use rand::RngCore;
use tracing::{debug, info, warn};

use restate_bifrost::providers::replicated_loglet::replication::{FMajorityResult, NodeSetChecker};
use restate_types::cluster_controller::{LogletLifecycleState, SchedulingPlan, TargetLogletState};
use restate_types::logs::metadata::{LogletParams, SegmentIndex};
use restate_types::logs::LogId;
use restate_types::nodes_config::{
    LogServerConfig, NodeConfig, NodesConfiguration, Role, StorageState,
};
use restate_types::replicated_loglet::{
    NodeSet, ReplicatedLogletId, ReplicatedLogletParams, ReplicationProperty,
};
use restate_types::{GenerationalNodeId, PlainNodeId};

/// The loglet controller is responsible for safely configuring loglet segments based on overall
/// policy and the available log servers, and for transitioning segments to new node sets as cluster
/// members come and go. It repeatedly decides on a loglet scheduling plan which it writes to the
/// metadata store, and continually adjusts based on observing the cluster.
///
/// The principal inputs are the number of logs required (which map to partitions); the available
/// log severs in an appropriate state (healthy and self-report as read-writable); and configured
/// constraints such as minimum replication and fault tolerance levels.
///
/// The loglet controller does not directly interact with loglets. Instead, it writes the loglet
/// scheduling plan into the metadata store and leaves it to log servers to act on this information.
pub struct ReplicatedLogletController {
    config: LogletControllerConfig,
}

/// Configuration spec schema for the replicated loglet controller. This defines the
/// user-configurable aspects of replicated logs. This should get folded into Restate config.
#[derive(Debug, Clone)]
pub struct LogletControllerConfig {
    /// The desired upper bound of log copies.
    ///
    /// todo: we should replace the ad-hoc selector with a new `SelectorStrategy` type - perhaps a
    ///  new policy of `Subset` with a configurable goal, or similar.
    log_replication_target: usize,

    /// The hard requirement for durable log replication.
    replication_requirement: ReplicationProperty,
}

/// Possible effects that the controller inner decider might request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogletEffect {
    /// Bootstrap?
    Initialize(LogId, ReplicatedLogletId),

    /// Seal the specified segment and extend the loglet chain with a new segment.
    SealAndExtendChain(LogId, SegmentIndex),
}

/// Input to the inner decider representing the state of the world we've observed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ObservedClusterState {
    /// The known cluster configuration.
    nodes_config: NodesConfiguration,

    /// Logs and the current tail segment index.
    logs: BTreeMap<LogId, SegmentIndex>,

    /// Nodes considered healthy and usable for their respective roles.
    alive_nodes: BTreeMap<PlainNodeId, GenerationalNodeId>,
    /// Any node that was previously known but isn't seen as healthy.
    dead_nodes: BTreeSet<PlainNodeId>,
}

impl ReplicatedLogletController {
    pub fn new(config: LogletControllerConfig) -> Self {
        Self { config }
    }

    /// Derive a new scheduling plan for a single log based on the previous plan and observed cluster state.
    // todo: decouple from SchedulingPlan
    fn decide_next_state(
        &self,
        scheduling_plan: &SchedulingPlan, // latest schedule from metadata store
        cluster_state: &ObservedClusterState, // observed cluster state pertinent to replicated loglets
    ) -> (Option<SchedulingPlan>, Vec<LogletEffect>) {
        // debug_assert!(
        //     cluster_state
        //         .alive_nodes
        //         .keys()
        //         .copied()
        //         .collect::<BTreeSet::<_>>()
        //         .symmetric_difference(&cluster_state.dead_nodes)
        //         .collect_vec()
        //         .is_empty(),
        //     "Alive and dead nodes must not overlap"
        // );

        let healthy_workers = cluster_state
            .nodes_config
            .iter()
            .filter(|(_, node)| node.has_role(Role::Worker))
            .filter_map(|(id, _)| cluster_state.alive_nodes.get(&id))
            .copied()
            .collect::<BTreeSet<_>>();

        let healthy_log_servers = cluster_state
            .nodes_config
            .iter()
            .filter(|(_, &ref node)| node.has_role(Role::LogServer))
            .filter_map(|(id, _)| cluster_state.alive_nodes.get(&id))
            .copied()
            .collect::<BTreeSet<_>>();

        let have_workers = !healthy_workers.is_empty();

        // This is perhaps a configurable preference: should we consider making any decisions until we have
        // the preferred number of log servers available, or just the minimum hard requirement? The default
        // _should_ be to start with the optimum to allow for failure, but with an option to bootstrap with
        // less than the ideal number if the operator really wants to.
        let have_log_servers =
            healthy_log_servers.len() >= self.config.replication_requirement.num_copies() as usize;

        debug!(
            ?have_workers,
            ?have_log_servers,
            log_servers = healthy_log_servers.len(),
            log_replication_target = ?self.config.log_replication_target,
            replication = self.config.replication_requirement.num_copies(),
            "Deciding on next loglet configuration"
        );

        if have_workers && have_log_servers {
            // First, create loglet segments for any logs that don't exist in the plan
            let mut updated_plan = scheduling_plan.clone().into_builder();
            let mut effects = vec![];

            for (log_id, tail_segment_idx) in cluster_state.logs.iter() {
                match scheduling_plan.loglet_config(log_id) {
                    // NEW! SHINY! No prior log configuration, we'll be bootstrapping this log!
                    None => {
                        if healthy_log_servers.len() < self.config.log_replication_target {
                            warn!(
                                log_id = ?log_id,
                                log_replication_target = ?self.config.log_replication_target,
                                available_log_servers = healthy_log_servers.len(),
                                "Refusing to bootstrap loglet with less than the preferred number of log servers"
                            );
                            continue;
                        }

                        let nodes = healthy_log_servers
                            .iter()
                            .copied()
                            .map(GenerationalNodeId::as_plain)
                            .choose_multiple(
                                &mut rand::thread_rng(),
                                self.config.log_replication_target,
                            );
                        let log_servers_node_set = NodeSet::from_iter(nodes);
                        assert_eq!(
                            log_servers_node_set.len(),
                            self.config.log_replication_target
                        );

                        let sequencer = healthy_workers
                            .iter()
                            .choose(&mut rand::thread_rng())
                            .copied()
                            .expect("we have at least one healthy worker");

                        // can we make this a proper Restate ID?
                        let loglet_id = rand::thread_rng().next_u64(); // todo: use external generator/seed?
                        let loglet_id = ReplicatedLogletId::new(loglet_id);
                        let params = ReplicatedLogletParams {
                            loglet_id,
                            nodeset: log_servers_node_set.clone(),
                            replication: self.config.replication_requirement.clone(),
                            sequencer,
                            write_set: None,
                        };

                        updated_plan.insert_loglet(TargetLogletState {
                            log_id: *log_id,
                            loglet_id,
                            segment_index: *tail_segment_idx,
                            loglet_state: LogletLifecycleState::Available, // we're (soon going to be) in business, baby!
                            sequencer,
                            log_servers: log_servers_node_set,
                            replication: self.config.replication_requirement.clone(),
                            params: LogletParams::from(params.serialize().expect("can serialize")),
                        });

                        // right now this is purely informational - alt we can make this be the bootstrap signal?
                        effects.push(LogletEffect::Initialize(*log_id, loglet_id));
                    }

                    // We've got prior state - check if the loglet requires any remediating actions
                    Some(loglet_state) => {
                        // 1. Is the log node set healthy? Can we do better than the existing state?
                        // Nodes passed into the healthy_log_servers list must meet the storage criteria
                        // for writable log server, so we intersect with those and check if the result is ok.
                        let mut healthy_quorum = NodeSetChecker::new(
                            &loglet_state.log_servers,
                            &cluster_state.nodes_config,
                            &self.config.replication_requirement,
                        );
                        healthy_quorum.set_attribute_on_each(
                            healthy_log_servers
                                .iter()
                                .copied()
                                .map(GenerationalNodeId::as_plain)
                                .collect_vec()
                                .as_slice(),
                            || true,
                        );

                        let f_majority = healthy_quorum.check_fmajority(|attr| *attr);

                        // Do the healthy log servers still form write quorum? We need that for any
                        // reconfiguration to take place. If we can, we will reconfigure the loglet segment
                        // for higher availability. If necessary, a new sequencer will also be chosen.
                        // todo(open question): should we attempt reconfiguration from BestEffort, too?
                        if f_majority == FMajorityResult::SuccessWithRisk {
                            info!(log_id = ?loglet_state.log_id, "Not all log servers are healthy, will attempt to add replacements");

                            // Servers that are shared between the previous and next segments
                            let mut next_segment_node_set = healthy_quorum
                                .filter(|attr| *attr)
                                .map(|(id, _)| *id)
                                .collect::<HashSet<_>>();

                            let healthy_log_servers_set = healthy_log_servers
                                .iter()
                                .copied()
                                .map(GenerationalNodeId::as_plain)
                                .collect::<HashSet<_>>();

                            debug!(
                                ?next_segment_node_set,
                                "Carrying over healthy log servers from previous segment"
                            );

                            // We can draw additional servers from this pool to pad up the node set for the next segment
                            let candidate_log_servers = healthy_log_servers_set
                                .difference(&next_segment_node_set)
                                .copied()
                                .collect::<Vec<_>>();

                            let extra_servers =
                                candidate_log_servers.iter().cloned().choose_multiple(
                                    &mut rand::thread_rng(),
                                    loglet_state.log_servers.len() - healthy_quorum.len(),
                                );
                            debug!(
                                ?extra_servers,
                                "Selected additional log servers for next segment"
                            );
                            next_segment_node_set.extend(extra_servers.clone());

                            let mut replacement_sequencer: Option<GenerationalNodeId> = None;
                            let mut state = loglet_state.clone();
                            state.loglet_state = LogletLifecycleState::Sealing;

                            // Favor the old sequencer if available to minimize reconfiguration (e.g. PP leadership)
                            if !healthy_workers.contains(&loglet_state.sequencer) {
                                let new_sequencer = healthy_workers
                                    .iter()
                                    .choose(&mut rand::thread_rng())
                                    .copied()
                                    .expect("we have at least  healthy worker");
                                info!(
                                    ?new_sequencer,
                                    "Updating sequencer for the next loglet segment"
                                );
                                state.sequencer = new_sequencer;
                                replacement_sequencer.replace(new_sequencer);
                            }

                            // Sanity check: if we haven't been able to pick a new sequencer, and aren't extending the
                            // loglet to additional log servers, then just leave the loglet as is.
                            if replacement_sequencer.is_none() && extra_servers.is_empty() {
                                info!(
                                    ?log_id,
                                    "No changes possible for loglet segment at the moment"
                                );
                                continue;
                            }

                            let log_servers_nodeset =
                                NodeSet::from_iter(next_segment_node_set.iter().cloned());
                            state.log_servers = log_servers_nodeset.clone();
                            let params = ReplicatedLogletParams {
                                loglet_id: loglet_state.loglet_id,
                                nodeset: log_servers_nodeset,
                                replication: self.config.replication_requirement.clone(),
                                sequencer: state.sequencer,
                                write_set: None,
                            };
                            state.params =
                                LogletParams::from(params.serialize().expect("can serialize"));

                            effects.push(LogletEffect::SealAndExtendChain(
                                *log_id,
                                loglet_state.segment_index,
                            ));
                            updated_plan.insert_loglet(state);
                        } else if f_majority == FMajorityResult::Success
                            && !healthy_workers.contains(&loglet_state.sequencer)
                        {
                            // Is the current sequencer still healthy and valid? We might be ok on loglet config and
                            // just need a sequencer replacement Note that even a sequencer node restart would require a
                            // reconfiguration, as its generation will change.

                            let replacement_sequencer = healthy_workers
                                .iter()
                                .choose(&mut rand::thread_rng())
                                .cloned()
                                .expect("we have at least one healthy worker");

                            info!(
                                ?replacement_sequencer,
                                "Sequencer node is dead, will attempt to replace"
                            );

                            let mut state = loglet_state.clone();
                            state.sequencer = replacement_sequencer;

                            effects.push(LogletEffect::SealAndExtendChain(
                                *log_id,
                                loglet_state.segment_index,
                            ));
                            updated_plan.insert_loglet(state);
                        }
                    }
                }
            }

            (updated_plan.build_if_modified(), effects)
        } else {
            match (have_workers, have_log_servers) {
                (false, false) => warn!("No healthy workers or log servers available!"),
                (false, true) => warn!("No healthy worker node available to act as sequencer"),
                (true, false) => warn!("No healthy log servers available"),
                _ => unreachable!("have_workers and have_log_servers can't both be false"),
            }
            (None, vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use axum::response::IntoResponse;
    use super::*;

    use enumset::enum_set;
    use googletest::prelude::*;
    use restate_bifrost::providers::replicated_loglet::test_util::generate_logserver_node;
    use restate_types::cluster_controller::{
        ReplicationStrategy, SchedulingPlan, SchedulingPlanBuilder, TargetLogletState,
    };
    use restate_types::logs::metadata::{LogletParams, SegmentIndex};
    use restate_types::logs::LogId;
    use restate_types::nodes_config::Role;
    use restate_types::partition_table::PartitionTable;
    use restate_types::replicated_loglet::{NodeSet, ReplicatedLogletId, ReplicationProperty};
    use restate_types::{PlainNodeId, Version};

    use crate::cluster_controller::replicated_loglet_controller::{
        LogletControllerConfig, ObservedClusterState, ReplicatedLogletController,
    };

    #[test]
    fn test_schedule_empty_cluster() -> Result<()> {
        let nodes_config = NodesConfiguration::new(Version::MIN, "empty-cluster".to_owned());

        let controller = ReplicatedLogletController::new(LogletControllerConfig {
            replication_requirement: ReplicationProperty::new(2.try_into()?),
            log_replication_target: 3,
        });

        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 1);
        let initial_scheduling_plan =
            SchedulingPlan::from(&partition_table, ReplicationStrategy::OnAllNodes);

        let state = ObservedClusterState {
            nodes_config,
            logs: (0u16..)
                .map(|idx| (LogId::from(idx), SegmentIndex::from(0)))
                .take(partition_table.num_partitions() as usize)
                .collect(),
            alive_nodes: BTreeMap::new(),
            dead_nodes: BTreeSet::new(),
        };

        let (plan, effects) = controller.decide_next_state(&initial_scheduling_plan, &state);

        assert_eq!(plan, None);
        assert!(effects.is_empty());

        Ok(())
    }

    #[test]
    fn test_schedule_no_log_servers() -> Result<()> {
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "no-log-servers".to_owned());
        nodes_config.upsert_node(generate_worker_node(1));
        nodes_config.upsert_node(generate_worker_node(2));

        let controller = ReplicatedLogletController::new(LogletControllerConfig {
            replication_requirement: ReplicationProperty::new(2.try_into()?),
            log_replication_target: 3,
        });

        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 1);
        let initial_scheduling_plan =
            SchedulingPlan::from(&partition_table, ReplicationStrategy::OnAllNodes);

        let n1 = PlainNodeId::new(1);
        let state = ObservedClusterState {
            nodes_config,
            logs: (0u16..)
                .map(|idx| (LogId::from(idx), SegmentIndex::from(0)))
                .take(partition_table.num_partitions() as usize)
                .collect(),
            alive_nodes: BTreeMap::from([(n1, n1.with_generation(1))]),
            dead_nodes: BTreeSet::new(),
        };

        let (plan, effects) = controller.decide_next_state(&initial_scheduling_plan, &state);

        assert_eq!(plan, None);
        assert!(effects.is_empty());

        Ok(())
    }

    #[test]
    fn test_schedule_insufficient_log_servers() -> Result<()> {
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "no-log-servers".to_owned());
        nodes_config.upsert_node(generate_worker_node(1));
        nodes_config.upsert_node(generate_worker_node(2));

        let controller = ReplicatedLogletController::new(LogletControllerConfig {
            replication_requirement: ReplicationProperty::new(2.try_into()?),
            log_replication_target: 3,
        });

        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 1);
        let initial_scheduling_plan =
            SchedulingPlan::from(&partition_table, ReplicationStrategy::OnAllNodes);

        let n1 = PlainNodeId::new(1);
        let state = ObservedClusterState {
            nodes_config,
            logs: (0u16..)
                .map(|idx| (LogId::from(idx), SegmentIndex::from(0)))
                .take(partition_table.num_partitions() as usize)
                .collect(),
            alive_nodes: BTreeMap::from([(n1, n1.with_generation(1))]),
            dead_nodes: BTreeSet::new(),
        };

        let (plan, effects) = controller.decide_next_state(&initial_scheduling_plan, &state);

        assert_eq!(plan, None);
        assert!(effects.is_empty());

        Ok(())
    }

    #[test]
    fn test_schedule_dont_bootstrap_loglet_with_less_than_preferred_spread() -> Result<()> {
        let mut nodes_config =
            NodesConfiguration::new(Version::MIN, "insufficient-capacity".to_owned());
        nodes_config.upsert_node(generate_worker_node(0));
        nodes_config.upsert_node(generate_logserver_node(1, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_logserver_node(2, StorageState::ReadWrite));

        let controller = ReplicatedLogletController::new(LogletControllerConfig {
            replication_requirement: ReplicationProperty::new(2.try_into()?),
            log_replication_target: 3,
        });

        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 1);
        let initial_scheduling_plan =
            SchedulingPlan::from(&partition_table, ReplicationStrategy::OnAllNodes);

        // Disjoint worker and log server nodes

        let w1 = PlainNodeId::new(0);

        let n1 = PlainNodeId::new(1);
        let n2 = PlainNodeId::new(2);

        let state = ObservedClusterState {
            nodes_config,
            logs: (0u16..)
                .map(|idx| (LogId::from(idx), SegmentIndex::from(0)))
                .take(partition_table.num_partitions() as usize)
                .collect(),
            alive_nodes: BTreeMap::from([(n1, n1.with_generation(1)), (n2, n2.with_generation(1))]),
            dead_nodes: BTreeSet::new(),
        };

        let (proposed_plan, effects) =
            controller.decide_next_state(&initial_scheduling_plan, &state);

        assert!(proposed_plan.is_none());
        assert_that!(effects, empty());

        Ok(())
    }

    #[test]
    fn test_schedule_bootstrap_loglet() -> Result<()> {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();

        // Disjoint worker and log server nodes, enough to bootstrap a loglet at preferred replication
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "minimum-viable".to_owned());
        nodes_config.upsert_node(generate_worker_node(0));
        nodes_config.upsert_node(generate_logserver_node(1, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_logserver_node(2, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_logserver_node(3, StorageState::ReadWrite));

        let controller = ReplicatedLogletController::new(LogletControllerConfig {
            replication_requirement: ReplicationProperty::new(2.try_into()?),
            log_replication_target: 3,
        });

        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 1);
        let initial_scheduling_plan =
            SchedulingPlan::from(&partition_table, ReplicationStrategy::OnAllNodes);

        let w1 = PlainNodeId::new(0);
        let n1 = PlainNodeId::new(1);
        let n2 = PlainNodeId::new(2);
        let n3 = PlainNodeId::new(3);

        let state = ObservedClusterState {
            nodes_config,
            logs: (0u16..)
                .map(|idx| (LogId::from(idx), SegmentIndex::from(0)))
                .take(partition_table.num_partitions() as usize)
                .collect(),
            alive_nodes: BTreeMap::from([
                (w1, w1.with_generation(1)),
                (n1, n1.with_generation(1)),
                (n2, n2.with_generation(1)),
                (n3, n3.with_generation(1)),
            ]),
            dead_nodes: BTreeSet::new(),
        };

        let (proposed_plan, effects) =
            controller.decide_next_state(&initial_scheduling_plan, &state);

        assert!(proposed_plan.is_some());
        let proposed_plan = proposed_plan.unwrap();
        assert_that!(
            proposed_plan,
            matches_pattern!(SchedulingPlan {
                version: eq(Version::from(2)),
            })
        );
        assert_that!(proposed_plan.logs, len(eq(1)));
        let (log_id, target_state) = proposed_plan.logs.iter().next().unwrap();

        assert_that!(*log_id, eq(LogId::from(0u32)));
        assert_that!(
            target_state.clone(),
            pat!(TargetLogletState {
                log_id: eq(LogId::from(0u32)),
                replication: eq(ReplicationProperty::new(2.try_into()?)),
                segment_index: eq(SegmentIndex::from(0)),
                sequencer: eq(w1.with_generation(1)),
                log_servers: eq(NodeSet::from_iter(vec![n1, n2, n3])), // can not be any other
            })
        );
        // todo: check loglet config, serialized params

        assert_that!(
            effects,
            elements_are![pat!(LogletEffect::Initialize(anything()))]
        );

        Ok(())
    }

    #[test]
    fn test_schedule_loglet_steady_state_noop() -> Result<()> {
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "healthy".to_owned());
        nodes_config.upsert_node(generate_mixed_use_node(1, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_mixed_use_node(2, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_mixed_use_node(3, StorageState::ReadWrite));

        let controller = ReplicatedLogletController::new(LogletControllerConfig {
            replication_requirement: ReplicationProperty::new(2.try_into()?),
            log_replication_target: 3,
        });

        // Co-located everything
        let n1 = PlainNodeId::new(1);
        let n2 = PlainNodeId::new(2);
        let n3 = PlainNodeId::new(3);

        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 1);
        let nodes = vec![n1, n2, n3];
        let existing_plan = SchedulingPlan::from(&partition_table, ReplicationStrategy::OnAllNodes)
            .into_builder()
            .insert_loglet(TargetLogletState {
                log_id: 0u32.into(),
                loglet_id: ReplicatedLogletId::new(42),
                loglet_state: LogletLifecycleState::Available,
                replication: ReplicationProperty::new(2.try_into()?),
                params: LogletParams::from("config".to_owned()),
                segment_index: SegmentIndex::default(),
                log_servers: NodeSet::from_iter(nodes),
                sequencer: n1.with_generation(1),
            })
            .build();

        // Nothing interesting happens

        let healthy = BTreeMap::from([
            (n1, n1.with_generation(1)),
            (n2, n2.with_generation(1)),
            (n3, n3.with_generation(1)),
        ]);

        let mixed_roles = enum_set!(Role::LogServer | Role::Worker);
        let state = ObservedClusterState {
            nodes_config,
            logs: (0u16..)
                .map(|idx| (LogId::from(idx), SegmentIndex::from(0)))
                .take(partition_table.num_partitions() as usize)
                .collect(),
            alive_nodes: healthy,
            dead_nodes: BTreeSet::new(),
        };

        let (proposed_plan, effects) = controller.decide_next_state(&existing_plan, &state);
        assert_that!(effects, empty());

        Ok(())
    }

    #[test]
    fn test_schedule_loglet_loses_sequencer_and_log_capacity_meets_replication_factor() -> Result<()>
    {
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "mixed-use".to_owned());
        nodes_config.upsert_node(generate_mixed_use_node(1, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_mixed_use_node(2, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_mixed_use_node(3, StorageState::ReadWrite));

        let controller = ReplicatedLogletController::new(LogletControllerConfig {
            replication_requirement: ReplicationProperty::new(2.try_into()?),
            log_replication_target: 3,
        });

        // Co-located everything
        let n1 = PlainNodeId::new(1);
        let n2 = PlainNodeId::new(2);
        let n3 = PlainNodeId::new(3);

        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 1);
        let nodes = vec![n1, n2, n3];
        let existing_plan = SchedulingPlan::from(&partition_table, ReplicationStrategy::OnAllNodes)
            .into_builder()
            .insert_loglet(TargetLogletState {
                log_id: 0u32.into(),
                loglet_id: ReplicatedLogletId::new(42),
                loglet_state: LogletLifecycleState::Available,
                replication: ReplicationProperty::new(2.try_into()?),
                params: LogletParams::from("config".to_owned()),
                segment_index: SegmentIndex::default(),
                log_servers: NodeSet::from_iter(nodes),
                sequencer: n1.with_generation(1),
            })
            .build();

        // n1, which was previously the sequencer for log 1, goes AWOL. The bare minimum of healthy log servers to
        // continue operations exist, but we need to reconfigure the loglet to get a new sequencer.

        let healthy = BTreeMap::from([(n2, n2.with_generation(1)), (n3, n3.with_generation(1))]);

        let mixed_roles = enum_set!(Role::LogServer | Role::Worker);
        let state = ObservedClusterState {
            nodes_config,
            logs: (0u16..)
                .map(|idx| (LogId::from(idx), SegmentIndex::from(0)))
                .take(partition_table.num_partitions() as usize)
                .collect(),
            alive_nodes: healthy,
            dead_nodes: BTreeSet::from([n1]),
        };

        let (proposed_plan, effects) = controller.decide_next_state(&existing_plan, &state);

        assert!(proposed_plan.is_some());
        let proposed_plan = proposed_plan.unwrap();
        assert_that!(
            proposed_plan,
            matches_pattern!(SchedulingPlan {
                version: eq(Version::from(3)),
            })
        );
        assert_that!(proposed_plan.logs, len(eq(1)));
        let (log_id, target_state) = proposed_plan.logs.iter().next().unwrap();

        assert_that!(*log_id, eq(LogId::from(0u32)));
        assert_that!(
            target_state.clone(),
            pat!(TargetLogletState {
                log_id: eq(LogId::from(0u32)),
                replication: eq(ReplicationProperty::new(2.try_into()?)),
                segment_index: eq(SegmentIndex::from(0)),
                sequencer: any!(eq(n2.with_generation(1)), eq(n3.with_generation(1))), // must be one of the two
                log_servers: eq(NodeSet::from_iter(vec![n1, n2, n3])), // unchanged since we can't do better than this with what's available
            })
        );
        // todo: check loglet config, serialized params

        assert_that!(
            effects,
            elements_are![eq(LogletEffect::SealAndExtendChain(0u32.into(), 0.into()))]
        );

        Ok(())
    }

    #[test]
    fn test_schedule_loglet_loses_log_server_capacity_and_gets_reconfigured() -> Result<()> {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();

        let mut nodes_config = NodesConfiguration::new(Version::MIN, "mixed-use".to_owned());
        nodes_config.upsert_node(generate_mixed_use_node(1, StorageState::Disabled));
        nodes_config.upsert_node(generate_mixed_use_node(2, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_mixed_use_node(3, StorageState::ReadWrite));
        nodes_config.upsert_node(generate_mixed_use_node(4, StorageState::ReadWrite));

        let controller = ReplicatedLogletController::new(LogletControllerConfig {
            replication_requirement: ReplicationProperty::new(2.try_into()?),
            log_replication_target: 3,
        });

        // Co-located everything
        let n1 = PlainNodeId::new(1);
        let n2 = PlainNodeId::new(2);
        let n3 = PlainNodeId::new(3);

        let partition_table = PartitionTable::with_equally_sized_partitions(Version::MIN, 1);
        let nodes = vec![n1, n2, n3];
        let existing_plan = SchedulingPlan::from(&partition_table, ReplicationStrategy::OnAllNodes)
            .into_builder()
            .insert_loglet(TargetLogletState {
                log_id: 0u32.into(),
                loglet_id: ReplicatedLogletId::new(42),
                loglet_state: LogletLifecycleState::Available,
                replication: ReplicationProperty::new(2.try_into()?),
                params: LogletParams::from(
                    "don't care - will be overwritten in the next iteration".to_owned(),
                ),
                segment_index: SegmentIndex::default(),
                log_servers: NodeSet::from_iter(nodes),
                sequencer: n1.with_generation(1),
            })
            .build();

        // n1 (previous sequencer for loglet 1) dies, and n4 gets added to replace it

        let n4 = PlainNodeId::new(4);
        let healthy = BTreeMap::from([
            (n2, n2.with_generation(2)),
            (n3, n3.with_generation(3)),
            (n4, n4.with_generation(1)),
        ]);

        let mixed_roles = enum_set!(Role::LogServer | Role::Worker);
        let state = ObservedClusterState {
            nodes_config,
            logs: (0u16..)
                .map(|idx| (LogId::from(idx), SegmentIndex::from(0)))
                .take(partition_table.num_partitions() as usize)
                .collect(),
            alive_nodes: healthy.clone(),
            dead_nodes: BTreeSet::from([n1]),
        };

        let (proposed_plan, effects) = controller.decide_next_state(&existing_plan, &state);

        assert!(proposed_plan.is_some());
        let proposed_plan = proposed_plan.unwrap();
        assert_that!(
            proposed_plan,
            matches_pattern!(SchedulingPlan {
                version: eq(Version::from(3)),
            })
        );
        assert_that!(proposed_plan.logs, len(eq(1)));
        let (log_id, target_state) = proposed_plan.logs.iter().next().unwrap();

        assert_that!(*log_id, eq(LogId::from(0u32)));
        assert_that!(
            effects,
            elements_are![eq(LogletEffect::SealAndExtendChain(
                0u32.into(),
                SegmentIndex::from(0)
            ))]
        );
        assert_that!(
            target_state.clone(),
            pat!(TargetLogletState {
                log_id: eq(LogId::from(0u32)),
                loglet_state: eq(LogletLifecycleState::Available),
                replication: eq(ReplicationProperty::new(2.try_into()?)),
                segment_index: eq(SegmentIndex::from(0)),
                sequencer: predicate(|seq| healthy.values().any(|gen_node_id| gen_node_id == seq)),
                log_servers: eq(NodeSet::from_iter(vec![n1, n2, n3])), // best we can do under the conditions, sufficiently intersects with old set
            })
        );
        // todo: check loglet config, serialized params

        Ok(())
    }

    pub fn generate_worker_node(id: impl Into<PlainNodeId>) -> NodeConfig {
        let id: PlainNodeId = id.into();
        NodeConfig::new(
            format!("worker-{}", id),
            GenerationalNodeId::new(id.into(), 1),
            format!("http://w{}", id).parse().unwrap(),
            Role::Worker.into(),
            LogServerConfig {
                storage_state: StorageState::Disabled,
            },
        )
    }

    pub fn generate_mixed_use_node(
        id: impl Into<PlainNodeId>,
        storage_state: StorageState,
    ) -> NodeConfig {
        let id: PlainNodeId = id.into();
        NodeConfig::new(
            format!("node-{}", id),
            GenerationalNodeId::new(id.into(), 1),
            format!("http://n{}", id).parse().unwrap(),
            enum_set!(Role::LogServer | Role::Worker),
            LogServerConfig { storage_state },
        )
    }
}
