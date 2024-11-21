// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::seq::IteratorRandom;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use tracing::debug;
use xxhash_rust::xxh3::Xxh3Builder;

use restate_core::metadata_store::{
    retry_on_network_error, MetadataStoreClient, Precondition, ReadError, ReadWriteError,
    WriteError,
};
use restate_core::network::{NetworkSender, Networking, Outgoing, TransportConnect};
use restate_core::{metadata, ShutdownError, SyncError, TaskCenter, TaskKind};
use restate_types::cluster_controller::{
    ReplicationStrategy, SchedulingPlan, SchedulingPlanBuilder, TargetPartitionState,
};
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::logs::metadata::Logs;
use restate_types::logs::LogId;
use restate_types::metadata_store::keys::SCHEDULING_PLAN_KEY;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::{NodeId, PlainNodeId, Versioned};

use crate::cluster_controller::logs_controller;
use crate::cluster_controller::observed_cluster_state::ObservedClusterState;

type HashSet<T> = std::collections::HashSet<T, Xxh3Builder>;

#[derive(Debug, thiserror::Error)]
#[error("failed reading scheduling plan from metadata store: {0}")]
pub struct BuildError(#[from] ReadWriteError);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed writing to metadata store: {0}")]
    MetadataStoreWrite(#[from] WriteError),
    #[error("failed reading from metadata store: {0}")]
    MetadataStoreRead(#[from] ReadError),
    #[error("failed syncing metadata: {0}")]
    Metadata(#[from] SyncError),
    #[error("system is shutting down")]
    Shutdown(#[from] ShutdownError),
}

enum UpdateOutcome<T> {
    Written(T),
    NewerVersionFound(T),
}

impl<T> UpdateOutcome<T> {
    fn into_inner(self) -> T {
        match self {
            UpdateOutcome::Written(value) => value,
            UpdateOutcome::NewerVersionFound(value) => value,
        }
    }
}

/// Placement hints for the [`Scheduler`]. The hints can specify which nodes should be chosen for
/// the partition processor placement and on which node the leader should run.
pub trait PartitionProcessorPlacementHints {
    fn preferred_nodes(&self, partition_id: &PartitionId) -> impl Iterator<Item = &PlainNodeId>;

    fn preferred_leader(&self, partition_id: &PartitionId) -> Option<PlainNodeId>;
}

impl<T: PartitionProcessorPlacementHints> PartitionProcessorPlacementHints for &T {
    fn preferred_nodes(&self, partition_id: &PartitionId) -> impl Iterator<Item = &PlainNodeId> {
        (*self).preferred_nodes(partition_id)
    }

    fn preferred_leader(&self, partition_id: &PartitionId) -> Option<PlainNodeId> {
        (*self).preferred_leader(partition_id)
    }
}

pub struct Scheduler<T> {
    scheduling_plan: SchedulingPlan,
    last_updated_scheduling_plan: Instant,

    task_center: TaskCenter,
    metadata_store_client: MetadataStoreClient,
    networking: Networking<T>,
}

/// The scheduler is responsible for assigning partition processors to nodes and to electing
/// leaders. It achieves it by deciding on a scheduling plan which is persisted to the metadata
/// store and then driving the observed cluster state to the target state (represented by the
/// scheduling plan).
impl<T: TransportConnect> Scheduler<T> {
    pub async fn init(
        configuration: &Configuration,
        task_center: TaskCenter,
        metadata_store_client: MetadataStoreClient,
        networking: Networking<T>,
    ) -> Result<Self, BuildError> {
        let scheduling_plan = retry_on_network_error(
            configuration.common.network_error_retry_policy.clone(),
            || {
                metadata_store_client
                    .get_or_insert(SCHEDULING_PLAN_KEY.clone(), SchedulingPlan::default)
            },
        )
        .await?;

        Ok(Self {
            scheduling_plan,
            last_updated_scheduling_plan: Instant::now(),
            task_center,
            metadata_store_client,
            networking,
        })
    }

    pub async fn on_observed_cluster_state(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        nodes_config: &NodesConfiguration,
        placement_hints: impl PartitionProcessorPlacementHints,
    ) -> Result<(), Error> {
        // todo: Only update scheduling plan on observed cluster changes?
        let alive_workers = observed_cluster_state
            .alive_nodes
            .keys()
            .cloned()
            .filter(|node_id| nodes_config.has_worker_role(node_id))
            .collect();

        self.update_scheduling_plan(&alive_workers, nodes_config, placement_hints)
            .await?;
        self.instruct_nodes(observed_cluster_state)?;

        Ok(())
    }

    pub async fn _on_tick(&mut self) {
        // nothing to do since we don't make time based scheduling decisions yet
    }

    pub async fn on_logs_update(
        &mut self,
        logs: &Logs,
        partition_table: &PartitionTable,
    ) -> Result<(), Error> {
        let mut builder = self.scheduling_plan.clone().into_builder();

        loop {
            // add partitions to the scheduling plan for which we have provisioned the logs
            for (log_id, _) in logs.iter() {
                let partition_id = (*log_id).into();

                // add the partition to the scheduling plan if we aren't already scheduling it
                if !builder.contains_partition(&partition_id) {
                    // check whether the provisioned log is actually needed
                    if let Some(partition) = partition_table.get_partition(&partition_id) {
                        builder.insert_partition(
                            partition_id,
                            TargetPartitionState::new(
                                partition.key_range.clone(),
                                ReplicationStrategy::OnAllNodes,
                            ),
                        )
                    }
                }
            }

            if let Some(scheduling_plan) = builder.build_if_modified() {
                let scheduling_plan = self.try_update_scheduling_plan(scheduling_plan).await?;
                match scheduling_plan {
                    UpdateOutcome::Written(scheduling_plan) => {
                        self.assign_scheduling_plan(scheduling_plan);
                        break;
                    }
                    UpdateOutcome::NewerVersionFound(scheduling_plan) => {
                        self.assign_scheduling_plan(scheduling_plan);
                        builder = self.scheduling_plan.clone().into_builder();
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn update_scheduling_plan(
        &mut self,
        alive_workers: &HashSet<PlainNodeId>,
        nodes_config: &NodesConfiguration,
        placement_hints: impl PartitionProcessorPlacementHints,
    ) -> Result<(), Error> {
        // todo temporary band-aid to ensure convergence of multiple schedulers. Remove once we
        //  accept equivalent configurations and remove persisting of the SchedulingPlan
        if self.last_updated_scheduling_plan.elapsed() > Duration::from_secs(10) {
            let new_scheduling_plan = self.fetch_scheduling_plan().await?;

            if new_scheduling_plan.version() > self.scheduling_plan.version() {
                debug!(
                    "Found a newer scheduling plan in the metadata store. Updating to version {}.",
                    new_scheduling_plan.version()
                );
            }

            self.assign_scheduling_plan(new_scheduling_plan);
        }

        let mut builder = self.scheduling_plan.clone().into_builder();

        self.ensure_replication(&mut builder, alive_workers, nodes_config, &placement_hints);
        self.ensure_leadership(&mut builder, placement_hints);

        if let Some(scheduling_plan) = builder.build_if_modified() {
            let scheduling_plan = self
                .try_update_scheduling_plan(scheduling_plan)
                .await?
                .into_inner();

            debug!("Updated scheduling plan: {scheduling_plan:?}");
            self.assign_scheduling_plan(scheduling_plan);
        }

        Ok(())
    }

    fn assign_scheduling_plan(&mut self, scheduling_plan: SchedulingPlan) {
        self.scheduling_plan = scheduling_plan;
        self.last_updated_scheduling_plan = Instant::now();
    }

    async fn try_update_scheduling_plan(
        &self,
        scheduling_plan: SchedulingPlan,
    ) -> Result<UpdateOutcome<SchedulingPlan>, Error> {
        match self
            .metadata_store_client
            .put(
                SCHEDULING_PLAN_KEY.clone(),
                &scheduling_plan,
                Precondition::MatchesVersion(self.scheduling_plan.version()),
            )
            .await
        {
            Ok(_) => Ok(UpdateOutcome::Written(scheduling_plan)),
            Err(err) => match err {
                WriteError::FailedPrecondition(_) => {
                    // There was a concurrent modification of the scheduling plan. Fetch the latest version.
                    let scheduling_plan = self.fetch_scheduling_plan().await?;
                    Ok(UpdateOutcome::NewerVersionFound(scheduling_plan))
                }
                err => Err(err.into()),
            },
        }
    }

    async fn fetch_scheduling_plan(&self) -> Result<SchedulingPlan, ReadError> {
        self.metadata_store_client
            .get(SCHEDULING_PLAN_KEY.clone())
            .await
            .map(|scheduling_plan| scheduling_plan.expect("must be present"))
    }

    fn ensure_replication(
        &self,
        scheduling_plan_builder: &mut SchedulingPlanBuilder,
        alive_workers: &HashSet<PlainNodeId>,
        nodes_config: &NodesConfiguration,
        placement_hints: impl PartitionProcessorPlacementHints,
    ) {
        let partition_ids: Vec<_> = scheduling_plan_builder.partition_ids().cloned().collect();

        let mut rng = rand::thread_rng();

        for partition_id in &partition_ids {
            scheduling_plan_builder.modify_partition(partition_id, |target_state| {
                let mut modified = false;

                match target_state.replication_strategy {
                    ReplicationStrategy::OnAllNodes => {
                        if target_state.node_set != *alive_workers {
                            target_state.node_set.clone_from(alive_workers);
                            modified = true;
                        }
                    }
                    ReplicationStrategy::Factor(replication_factor) => {
                        // only retain alive nodes => remove dead ones
                        target_state.node_set.retain(|node| {
                            let result = alive_workers.contains(node);
                            modified |= !result;
                            result
                        });

                        let replication_factor = usize::try_from(replication_factor.get())
                            .expect("u32 should fit into usize");

                        if target_state.node_set.len() == replication_factor {
                            return modified;
                        }

                        let preferred_worker_nodes = placement_hints
                            .preferred_nodes(partition_id)
                            .filter(|node_id| nodes_config.has_worker_role(node_id));
                        let preferred_leader = placement_hints
                            .preferred_leader(partition_id)
                            .and_then(|node_id| {
                                if alive_workers.contains(&node_id) {
                                    Some(node_id)
                                } else {
                                    None
                                }
                            });

                        // if we are under replicated and have other alive nodes available
                        if target_state.node_set.len() < replication_factor
                            && target_state.node_set.len() < alive_workers.len()
                        {
                            if let Some(preferred_leader) = preferred_leader {
                                modified |= !target_state.node_set.contains(&preferred_leader);
                                target_state.node_set.insert(preferred_leader);
                            }

                            // todo: Implement cleverer strategies
                            // randomly choose from the preferred workers nodes first
                            let new_nodes = preferred_worker_nodes
                                .filter(|node_id| !target_state.node_set.contains(node_id))
                                .choose_multiple(
                                    &mut rng,
                                    replication_factor - target_state.node_set.len(),
                                );

                            modified |= !new_nodes.is_empty();
                            target_state.node_set.extend(new_nodes);

                            if target_state.node_set.len() < replication_factor {
                                // randomly choose from the remaining worker nodes
                                let new_nodes = alive_workers
                                    .iter()
                                    .filter(|node| !target_state.node_set.contains(*node))
                                    .cloned()
                                    .choose_multiple(
                                        &mut rng,
                                        replication_factor - target_state.node_set.len(),
                                    );

                                modified |= !new_nodes.is_empty();
                                target_state.node_set.extend(new_nodes);
                            }
                        } else if target_state.node_set.len() > replication_factor {
                            let preferred_worker_nodes: HashSet<PlainNodeId> =
                                preferred_worker_nodes.cloned().collect();

                            // first remove the not preferred nodes
                            for node_id in target_state
                                .node_set
                                .iter()
                                .filter(|node_id| {
                                    !preferred_worker_nodes.contains(node_id)
                                        && Some(**node_id) != preferred_leader
                                })
                                .cloned()
                                .choose_multiple(
                                    &mut rng,
                                    replication_factor - target_state.node_set.len(),
                                )
                            {
                                target_state.node_set.remove(&node_id);
                                modified = true;
                            }

                            if target_state.node_set.len() > replication_factor {
                                for node_id in target_state
                                    .node_set
                                    .iter()
                                    .filter(|node_id| Some(**node_id) != preferred_leader)
                                    .cloned()
                                    .choose_multiple(
                                        &mut rng,
                                        replication_factor - target_state.node_set.len(),
                                    )
                                {
                                    target_state.node_set.remove(&node_id);
                                    modified = true;
                                }
                            }
                        }
                    }
                }

                // check if the leader is still part of the node set; if not, then clear leader field
                if let Some(leader) = target_state.leader.as_ref() {
                    if !target_state.node_set.contains(leader) {
                        target_state.leader = None;
                        modified = true;
                    }
                }

                modified
            })
        }
    }

    fn ensure_leadership(
        &self,
        scheduling_plan_builder: &mut SchedulingPlanBuilder,
        placement_hints: impl PartitionProcessorPlacementHints,
    ) {
        let partition_ids: Vec<_> = scheduling_plan_builder.partition_ids().cloned().collect();
        for partition_id in partition_ids {
            scheduling_plan_builder.modify_partition(&partition_id, |target_state| {
                let preferred_leader = placement_hints.preferred_leader(&partition_id);
                if target_state.leader.is_none() {
                    target_state.leader =
                        self.select_leader_from(&target_state.node_set, preferred_leader);
                    // check whether we modified the leader
                    return target_state.leader.is_some();
                } else if preferred_leader.is_some_and(|preferred_leader| {
                    Some(preferred_leader) != target_state.leader
                        && target_state.node_set.contains(&preferred_leader)
                }) {
                    target_state.leader = preferred_leader;
                    return true;
                }

                false
            })
        }
    }

    fn select_leader_from(
        &self,
        leader_candidates: &HashSet<PlainNodeId>,
        preferred_leader: Option<PlainNodeId>,
    ) -> Option<PlainNodeId> {
        // todo: Implement leader balancing between nodes
        preferred_leader
            .filter(|leader| leader_candidates.contains(leader))
            .or_else(|| {
                let mut rng = rand::thread_rng();
                leader_candidates.iter().choose(&mut rng).cloned()
            })
    }

    fn instruct_nodes(&self, observed_cluster_state: &ObservedClusterState) -> Result<(), Error> {
        let mut partitions: BTreeSet<_> = self.scheduling_plan.partition_ids().cloned().collect();
        partitions.extend(observed_cluster_state.partitions.keys().cloned());

        let mut commands = BTreeMap::default();

        for partition_id in &partitions {
            self.generate_instructions_for_partition(
                partition_id,
                observed_cluster_state,
                &mut commands,
            );
        }

        for (node_id, commands) in commands.into_iter() {
            // only send control processors message if there are commands to send
            if !commands.is_empty() {
                let control_processors = ControlProcessors {
                    // todo: Maybe remove unneeded partition table version
                    min_partition_table_version: metadata().partition_table_version(),
                    min_logs_table_version: metadata().logs_version(),
                    commands,
                };

                self.task_center.spawn_child(
                    TaskKind::Disposable,
                    "send-control-processors-to-node",
                    None,
                    {
                        let networking = self.networking.clone();
                        async move {
                            networking
                                .send(Outgoing::new(node_id, control_processors))
                                .await?;
                            Ok(())
                        }
                    },
                )?;
            }
        }

        Ok(())
    }

    fn generate_instructions_for_partition(
        &self,
        partition_id: &PartitionId,
        observed_cluster_state: &ObservedClusterState,
        commands: &mut BTreeMap<PlainNodeId, Vec<ControlProcessor>>,
    ) {
        let target_state = self.scheduling_plan.get(partition_id);
        // todo: Avoid cloning of node_set if this becomes measurable
        let mut observed_state = observed_cluster_state
            .partitions
            .get(partition_id)
            .map(|state| state.partition_processors.clone())
            .unwrap_or_default();

        if let Some(target_state) = target_state {
            for (node_id, run_mode) in target_state.iter() {
                if !observed_state
                    .remove(&node_id)
                    .is_some_and(|observed_run_mode| observed_run_mode == run_mode)
                {
                    commands.entry(node_id).or_default().push(ControlProcessor {
                        partition_id: *partition_id,
                        command: ProcessorCommand::from(run_mode),
                    });
                }
            }
        }

        // all remaining entries in observed_state are not part of target, thus, stop them!
        for node_id in observed_state.keys() {
            commands
                .entry(*node_id)
                .or_default()
                .push(ControlProcessor {
                    partition_id: *partition_id,
                    command: ProcessorCommand::Stop,
                });
        }
    }
}

/// Placement hints for the [`logs_controller::LogsController`] based on the current
/// [`SchedulingPlan`].
pub struct SchedulingPlanNodeSetSelectorHints<'a> {
    scheduling_plan: &'a SchedulingPlan,
}

impl<'a, T> From<&'a Scheduler<T>> for SchedulingPlanNodeSetSelectorHints<'a> {
    fn from(value: &'a Scheduler<T>) -> Self {
        Self {
            scheduling_plan: &value.scheduling_plan,
        }
    }
}

impl<'a> logs_controller::NodeSetSelectorHints for SchedulingPlanNodeSetSelectorHints<'a> {
    fn preferred_sequencer(&self, log_id: &LogId) -> Option<NodeId> {
        let partition_id = PartitionId::from(*log_id);

        self.scheduling_plan
            .get(&partition_id)
            .and_then(|target_state| target_state.leader.map(Into::into))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use googletest::assert_that;
    use googletest::matcher::{Matcher, MatcherResult};
    use http::Uri;
    use rand::prelude::ThreadRng;
    use rand::Rng;
    use std::collections::BTreeMap;
    use std::iter;
    use std::num::NonZero;
    use std::time::Duration;
    use test_log::test;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
    use crate::cluster_controller::scheduler::{
        HashSet, PartitionProcessorPlacementHints, Scheduler,
    };
    use restate_core::network::{ForwardingHandler, Incoming, MessageCollectorMockConnector};
    use restate_core::{metadata, TaskCenterBuilder, TestCoreEnv, TestCoreEnvBuilder};
    use restate_types::cluster::cluster_state::{
        AliveNode, ClusterState, DeadNode, NodeState, PartitionProcessorStatus, RunMode,
    };
    use restate_types::cluster_controller::{ReplicationStrategy, SchedulingPlan};
    use restate_types::config::Configuration;
    use restate_types::identifiers::PartitionId;
    use restate_types::metadata_store::keys::SCHEDULING_PLAN_KEY;
    use restate_types::net::codec::WireDecode;
    use restate_types::net::partition_processor_manager::{ControlProcessors, ProcessorCommand};
    use restate_types::net::{AdvertisedAddress, TargetName};
    use restate_types::nodes_config::{LogServerConfig, NodeConfig, NodesConfiguration, Role};
    use restate_types::partition_table::PartitionTable;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};

    struct NoPlacementHints;

    impl PartitionProcessorPlacementHints for NoPlacementHints {
        fn preferred_nodes(
            &self,
            _partition_id: &PartitionId,
        ) -> impl Iterator<Item = &PlainNodeId> {
            iter::empty()
        }

        fn preferred_leader(&self, _partition_id: &PartitionId) -> Option<PlainNodeId> {
            None
        }
    }

    #[test(tokio::test)]
    async fn empty_leadership_changes_dont_modify_plan() -> googletest::Result<()> {
        let test_env = TestCoreEnv::create_with_single_node(0, 0).await;
        let tc = test_env.tc.clone();
        let metadata_store_client = test_env.metadata_store_client.clone();
        let networking = test_env.networking.clone();

        test_env
            .tc
            .run_in_scope("test", None, async {
                let initial_scheduling_plan = metadata_store_client
                    .get::<SchedulingPlan>(SCHEDULING_PLAN_KEY.clone())
                    .await
                    .expect("scheduling plan");
                let mut scheduler = Scheduler::init(
                    Configuration::pinned().as_ref(),
                    tc,
                    metadata_store_client.clone(),
                    networking,
                )
                .await?;
                let observed_cluster_state = ObservedClusterState::default();

                scheduler
                    .on_observed_cluster_state(
                        &observed_cluster_state,
                        &metadata().nodes_config_ref(),
                        NoPlacementHints,
                    )
                    .await?;

                let scheduling_plan = metadata_store_client
                    .get::<SchedulingPlan>(SCHEDULING_PLAN_KEY.clone())
                    .await
                    .expect("scheduling plan");

                assert_eq!(initial_scheduling_plan, scheduling_plan);

                Ok(())
            })
            .await
    }

    #[test(tokio::test(start_paused = true))]
    async fn schedule_partitions_with_replication_factor() -> googletest::Result<()> {
        schedule_partitions(ReplicationStrategy::Factor(
            NonZero::new(3).expect("non-zero"),
        ))
        .await?;
        Ok(())
    }

    #[test(tokio::test(start_paused = true))]
    async fn schedule_partitions_with_all_nodes_replication() -> googletest::Result<()> {
        schedule_partitions(ReplicationStrategy::OnAllNodes).await?;
        Ok(())
    }

    async fn schedule_partitions(
        replication_strategy: ReplicationStrategy,
    ) -> googletest::Result<()> {
        let num_partitions = 64;
        let num_nodes = 5;
        let num_scheduling_rounds = 10;

        let node_ids: Vec<_> = (1..=num_nodes)
            .map(|idx| GenerationalNodeId::new(idx, idx))
            .collect();
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());

        for node_id in &node_ids {
            let node_config = NodeConfig::new(
                format!("{node_id}"),
                *node_id,
                AdvertisedAddress::Http(Uri::default()),
                Role::Worker.into(),
                LogServerConfig::default(),
            );
            nodes_config.upsert_node(node_config);
        }

        let tc = TaskCenterBuilder::default_for_tests()
            .build()
            .expect("task_center builds");

        // network messages going to other nodes are written to `tx`
        let (tx, control_recv) = mpsc::channel(100);
        let connector = MessageCollectorMockConnector::new(tc.clone(), 10, tx.clone());

        let mut builder = TestCoreEnvBuilder::with_transport_connector(tc, connector);
        builder.router_builder.add_raw_handler(
            TargetName::ControlProcessors,
            // network messages going to my node is also written to `tx`
            Box::new(ForwardingHandler::new(GenerationalNodeId::new(1, 1), tx)),
        );

        let mut control_recv = ReceiverStream::new(control_recv)
            .filter_map(|(node_id, message)| async move {
                if message.body().target() == TargetName::ControlProcessors {
                    let message = message
                        .try_map(|mut m| {
                            ControlProcessors::decode(
                                &mut m.payload,
                                restate_types::net::CURRENT_PROTOCOL_VERSION,
                            )
                        })
                        .unwrap();
                    Some((node_id, message))
                } else {
                    None
                }
            })
            .boxed();

        let partition_table =
            PartitionTable::with_equally_sized_partitions(Version::MIN, num_partitions);
        let initial_scheduling_plan = SchedulingPlan::from(&partition_table, replication_strategy);
        let metadata_store_client = builder.metadata_store_client.clone();

        let networking = builder.networking.clone();

        let env = builder
            .set_nodes_config(nodes_config.clone())
            .set_partition_table(partition_table.clone())
            .set_scheduling_plan(initial_scheduling_plan)
            .build()
            .await;
        let tc = env.tc.clone();
        env.tc
            .run_in_scope("test", None, async move {
                let mut scheduler = Scheduler::init(
                    Configuration::pinned().as_ref(),
                    tc,
                    metadata_store_client.clone(),
                    networking,
                )
                .await?;
                let mut observed_cluster_state = ObservedClusterState::default();

                for _ in 0..num_scheduling_rounds {
                    let cluster_state = random_cluster_state(&node_ids, num_partitions);

                    observed_cluster_state.update(&cluster_state);
                    scheduler
                        .on_observed_cluster_state(
                            &observed_cluster_state,
                            &metadata().nodes_config_ref(),
                            NoPlacementHints,
                        )
                        .await?;
                    // collect all control messages from the network to build up the effective scheduling plan
                    let control_messages = control_recv
                        .as_mut()
                        .take_until(tokio::time::sleep(Duration::from_secs(10)))
                        .collect::<Vec<_>>()
                        .await;

                    let observed_cluster_state =
                        derive_observed_cluster_state(&cluster_state, control_messages);
                    let target_scheduling_plan = metadata_store_client
                        .get::<SchedulingPlan>(SCHEDULING_PLAN_KEY.clone())
                        .await?
                        .expect("the scheduler should have created a scheduling plan");

                    // assert that the effective scheduling plan aligns with the target scheduling plan
                    assert_that!(
                        observed_cluster_state,
                        matches_scheduling_plan(&target_scheduling_plan)
                    );

                    let alive_nodes: HashSet<_> = cluster_state
                        .alive_nodes()
                        .map(|node| node.generational_node_id.as_plain())
                        .collect();

                    for (_, target_state) in target_scheduling_plan.iter() {
                        // assert that every partition has a leader which is part of the alive nodes set
                        assert!(target_state
                            .leader
                            .is_some_and(|leader| alive_nodes.contains(&leader)));

                        // assert that the replication strategy was respected
                        match replication_strategy {
                            ReplicationStrategy::OnAllNodes => {
                                assert_eq!(target_state.node_set, alive_nodes)
                            }
                            ReplicationStrategy::Factor(replication_factor) => assert_eq!(
                                target_state.node_set.len(),
                                alive_nodes.len().min(
                                    usize::try_from(replication_factor.get())
                                        .expect("u32 fits into usize")
                                )
                            ),
                        }
                    }
                }
                googletest::Result::Ok(())
            })
            .await?;

        Ok(())
    }

    fn matches_scheduling_plan(scheduling_plan: &SchedulingPlan) -> SchedulingPlanMatcher<'_> {
        SchedulingPlanMatcher { scheduling_plan }
    }

    struct SchedulingPlanMatcher<'a> {
        scheduling_plan: &'a SchedulingPlan,
    }

    impl<'a> Matcher for SchedulingPlanMatcher<'a> {
        type ActualT = ObservedClusterState;

        fn matches(&self, actual: &Self::ActualT) -> MatcherResult {
            if actual.partitions.len() != self.scheduling_plan.partitions().len() {
                return MatcherResult::NoMatch;
            }

            for (partition_id, target_state) in self.scheduling_plan.iter() {
                if let Some(observed_state) = actual.partitions.get(partition_id) {
                    if observed_state.partition_processors.len() != target_state.node_set.len() {
                        return MatcherResult::NoMatch;
                    }

                    for (node_id, run_mode) in target_state.iter() {
                        if observed_state.partition_processors.get(&node_id) != Some(&run_mode) {
                            return MatcherResult::NoMatch;
                        }
                    }
                } else {
                    return MatcherResult::NoMatch;
                }
            }

            MatcherResult::Match
        }

        fn describe(&self, matcher_result: MatcherResult) -> String {
            match matcher_result {
                MatcherResult::Match => {
                    format!(
                        "should reflect the scheduling plan {:?}",
                        self.scheduling_plan
                    )
                }
                MatcherResult::NoMatch => {
                    format!(
                        "does not reflect the scheduling plan {:?}",
                        self.scheduling_plan
                    )
                }
            }
        }
    }

    fn derive_observed_cluster_state(
        cluster_state: &ClusterState,
        control_messages: Vec<(GenerationalNodeId, Incoming<ControlProcessors>)>,
    ) -> ObservedClusterState {
        let mut observed_cluster_state = ObservedClusterState::default();
        observed_cluster_state.update(cluster_state);

        // apply commands
        for (target_node, control_processors) in control_messages {
            let plain_node_id = target_node.as_plain();
            for control_processor in control_processors.into_body().commands {
                match control_processor.command {
                    ProcessorCommand::Stop => {
                        observed_cluster_state.remove_node_from_partition(
                            &control_processor.partition_id,
                            &plain_node_id,
                        );
                    }
                    ProcessorCommand::Follower => {
                        observed_cluster_state.add_node_to_partition(
                            control_processor.partition_id,
                            plain_node_id,
                            RunMode::Follower,
                        );
                    }
                    ProcessorCommand::Leader => {
                        observed_cluster_state.add_node_to_partition(
                            control_processor.partition_id,
                            plain_node_id,
                            RunMode::Leader,
                        );
                    }
                }
            }
        }

        observed_cluster_state
    }

    fn random_cluster_state(
        node_ids: &Vec<GenerationalNodeId>,
        num_partitions: u16,
    ) -> ClusterState {
        let nodes = random_nodes_state(node_ids, num_partitions);

        ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            logs_metadata_version: Version::MIN,
            nodes,
        }
    }

    fn random_nodes_state(
        node_ids: &Vec<GenerationalNodeId>,
        num_partitions: u16,
    ) -> BTreeMap<PlainNodeId, NodeState> {
        let mut result = BTreeMap::default();
        let mut rng = rand::thread_rng();
        let mut has_alive_node = false;

        for node_id in node_ids {
            let node_state = if rng.gen_bool(0.66) {
                let alive_node = random_alive_node(&mut rng, *node_id, num_partitions);
                has_alive_node = true;
                NodeState::Alive(alive_node)
            } else {
                NodeState::Dead(DeadNode {
                    last_seen_alive: Some(MillisSinceEpoch::now()),
                })
            };

            result.insert(node_id.as_plain(), node_state);
        }

        // make sure we have at least one alive node
        if !has_alive_node {
            let idx = rng.gen_range(0..node_ids.len());
            let node_id = node_ids[idx];
            *result.get_mut(&node_id.as_plain()).expect("must exist") =
                NodeState::Alive(random_alive_node(&mut rng, node_id, num_partitions));
        }

        result
    }

    fn random_alive_node(
        rng: &mut ThreadRng,
        node_id: GenerationalNodeId,
        num_partitions: u16,
    ) -> AliveNode {
        let partitions = random_partition_status(rng, num_partitions);
        AliveNode {
            generational_node_id: node_id,
            last_heartbeat_at: MillisSinceEpoch::now(),
            partitions,
        }
    }

    fn random_partition_status(
        rng: &mut ThreadRng,
        num_partitions: u16,
    ) -> BTreeMap<PartitionId, PartitionProcessorStatus> {
        let mut result = BTreeMap::default();

        for idx in 0..num_partitions {
            if rng.gen_bool(0.5) {
                let mut status = PartitionProcessorStatus::new();

                if rng.gen_bool(0.5) {
                    // make the partition the leader
                    status.planned_mode = RunMode::Leader;
                    status.effective_mode = RunMode::Leader;
                }

                result.insert(PartitionId::from(idx), status);
            }
        }

        result
    }
}
