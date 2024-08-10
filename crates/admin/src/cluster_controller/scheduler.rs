// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rand::seq::IteratorRandom;
use tracing::{debug, trace};

use restate_core::metadata_store::{MetadataStoreClient, Precondition, ReadError, WriteError};
use restate_core::network::NetworkSender;
use restate_core::{ShutdownError, SyncError, TaskCenter, TaskKind};
use restate_types::cluster::cluster_state::{ClusterState, NodeState, RunMode};
use restate_types::cluster_controller::{
    ReplicationStrategy, SchedulingPlan, SchedulingPlanBuilder,
};
use restate_types::identifiers::PartitionId;
use restate_types::metadata_store::keys::SCHEDULING_PLAN_KEY;
use restate_types::net::cluster_controller::Action;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::{GenerationalNodeId, PlainNodeId, Version, Versioned};

#[derive(Debug, thiserror::Error)]
#[error("failed reading scheduling plan from metadata store: {0}")]
pub struct BuildError(#[from] ReadError);

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

pub struct Scheduler<N> {
    scheduling_plan: SchedulingPlan,
    observed_cluster_state: ObservedClusterState,

    task_center: TaskCenter,
    metadata_store_client: MetadataStoreClient,
    networking: N,
}

/// The scheduler is responsible for assigning partition processors to nodes and to electing
/// leaders. It achieves it by deciding on a scheduling plan which is persisted to the metadata
/// store and then driving the observed cluster state to the target state (represented by the
/// scheduling plan).
impl<N> Scheduler<N>
where
    N: NetworkSender + 'static,
{
    pub async fn init(
        task_center: TaskCenter,
        metadata_store_client: MetadataStoreClient,
        networking: N,
    ) -> Result<Self, BuildError> {
        let scheduling_plan = metadata_store_client
            .get(SCHEDULING_PLAN_KEY.clone())
            .await?
            .expect("Scheduling plan should be initialized by bootstrap node");

        Ok(Self {
            scheduling_plan,
            observed_cluster_state: ObservedClusterState::default(),
            task_center,
            metadata_store_client,
            networking,
        })
    }

    pub async fn on_attach_node(
        &mut self,
        node: GenerationalNodeId,
    ) -> Result<Vec<Action>, ShutdownError> {
        trace!(node = %node, "Node is attaching to cluster");
        // the convergence loop will make sure that the node receives its instructions
        Ok(Vec::new())
    }

    pub async fn on_cluster_state_update(
        &mut self,
        cluster_state: Arc<ClusterState>,
    ) -> Result<(), Error> {
        self.update_observed_cluster_state(cluster_state);
        // todo: Only update scheduling plan on observed cluster changes?
        self.update_scheduling_plan().await?;
        self.instruct_nodes()?;

        Ok(())
    }

    pub async fn _on_tick(&mut self) {
        // nothing to do since we don't make time based scheduling decisions yet
    }

    fn update_observed_cluster_state(&mut self, cluster_state: Arc<ClusterState>) {
        self.observed_cluster_state.update(&cluster_state);
    }

    async fn update_scheduling_plan(&mut self) -> Result<(), Error> {
        let mut builder = self.scheduling_plan.clone().into_builder();

        self.ensure_replication(&mut builder);
        self.ensure_leadership(&mut builder);

        if let Some(scheduling_plan) = builder.build_if_modified() {
            if let Err(err) = self
                .metadata_store_client
                .put(
                    SCHEDULING_PLAN_KEY.clone(),
                    &scheduling_plan,
                    Precondition::MatchesVersion(self.scheduling_plan.version()),
                )
                .await
            {
                return match err {
                    WriteError::FailedPrecondition(_) => {
                        // There was a concurrent modification of the scheduling plan. Fetch the latest version.
                        self.fetch_scheduling_plan().await?;
                        Ok(())
                    }
                    err => Err(err.into()),
                };
            }

            debug!("Updated scheduling plan: {scheduling_plan:?}");
            self.scheduling_plan = scheduling_plan;
        }

        Ok(())
    }

    async fn fetch_scheduling_plan(&mut self) -> Result<(), ReadError> {
        if let Some(scheduling_plan) = self
            .metadata_store_client
            .get(SCHEDULING_PLAN_KEY.clone())
            .await?
        {
            debug!("Fetched scheduling plan from metadata store: {scheduling_plan:?}");
            self.scheduling_plan = scheduling_plan;
        }

        Ok(())
    }

    fn ensure_replication(&self, scheduling_plan_builder: &mut SchedulingPlanBuilder) {
        let partition_ids: Vec<_> = scheduling_plan_builder.partition_ids().cloned().collect();

        let alive_nodes: BTreeSet<_> = self
            .observed_cluster_state
            .alive_nodes
            .keys()
            .cloned()
            .collect();
        let mut rng = rand::thread_rng();

        for partition_id in &partition_ids {
            scheduling_plan_builder.modify_partition(partition_id, |target_state| {
                let mut modified = false;

                match target_state.replication_strategy {
                    ReplicationStrategy::OnAllNodes => {
                        if target_state.node_set != alive_nodes {
                            target_state.node_set.clone_from(&alive_nodes);
                            modified = true;
                        }
                    }
                    ReplicationStrategy::Factor(replication_factor) => {
                        // only retain alive nodes => remove dead ones
                        target_state.node_set.retain(|node| {
                            let result = alive_nodes.contains(node);
                            modified |= !result;
                            result
                        });

                        let replication_factor = usize::try_from(replication_factor.get())
                            .expect("u32 should fit into usize");

                        // if we are under replicated and have other alive nodes available
                        if target_state.node_set.len() < replication_factor
                            && target_state.node_set.len() < alive_nodes.len()
                        {
                            // randomly choose from the available set of nodes
                            // todo: Implement cleverer strategies
                            let new_nodes = alive_nodes
                                .iter()
                                .filter(|node| !target_state.node_set.contains(*node))
                                .cloned()
                                .choose_multiple(
                                    &mut rng,
                                    replication_factor - target_state.node_set.len(),
                                );

                            modified |= !new_nodes.is_empty();
                            target_state.node_set.extend(new_nodes);
                        } else if target_state.node_set.len() > replication_factor {
                            for node_id in target_state.node_set.iter().cloned().choose_multiple(
                                &mut rng,
                                replication_factor - target_state.node_set.len(),
                            ) {
                                target_state.node_set.remove(&node_id);
                                modified = true;
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

    fn ensure_leadership(&self, scheduling_plan_builder: &mut SchedulingPlanBuilder) {
        let partition_ids: Vec<_> = scheduling_plan_builder.partition_ids().cloned().collect();
        for partition_id in partition_ids {
            scheduling_plan_builder.modify_partition(&partition_id, |target_state| {
                if target_state.leader.is_none() {
                    target_state.leader = self.select_leader_from(&target_state.node_set);
                    return true;
                }

                false
            })
        }
    }

    fn select_leader_from(&self, leader_candidates: &BTreeSet<PlainNodeId>) -> Option<PlainNodeId> {
        // todo: Implement leader balancing between nodes
        let mut rng = rand::thread_rng();
        leader_candidates.iter().choose(&mut rng).cloned()
    }

    fn instruct_nodes(&self) -> Result<(), Error> {
        let mut partitions: BTreeSet<_> = self.scheduling_plan.partition_ids().cloned().collect();
        partitions.extend(self.observed_cluster_state.partitions.keys().cloned());

        let mut commands = BTreeMap::default();

        for partition_id in &partitions {
            self.generate_instructions_for_partition(partition_id, &mut commands);
        }

        for (node_id, commands) in commands.into_iter() {
            let control_processors = ControlProcessors {
                // todo: Maybe remove unneeded partition table version
                min_partition_table_version: Version::MIN,
                commands,
            };

            self.task_center.spawn_child(
                TaskKind::Disposable,
                "send-control-processors-to-node",
                None,
                {
                    let networking = self.networking.clone();
                    async move {
                        networking.send(node_id.into(), &control_processors).await?;
                        Ok(())
                    }
                },
            )?;
        }

        Ok(())
    }

    fn generate_instructions_for_partition(
        &self,
        partition_id: &PartitionId,
        commands: &mut BTreeMap<PlainNodeId, Vec<ControlProcessor>>,
    ) {
        let target_state = self.scheduling_plan.get(partition_id);
        // todo: Avoid cloning of node_set if this becomes measurable
        let mut observed_state = self
            .observed_cluster_state
            .partitions
            .get(partition_id)
            .map(|state| state.node_set.clone())
            .unwrap_or_default();

        if let Some(target_state) = target_state {
            for (node_id, run_mode) in target_state.iter() {
                observed_state.remove(&node_id);

                commands.entry(node_id).or_default().push(ControlProcessor {
                    partition_id: *partition_id,
                    command: ProcessorCommand::from(run_mode),
                });
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

/// Represents the scheduler's observed state of the cluster. The scheduler will use this
/// information and the target scheduling plan to instruct nodes to start/stop partition processors.
#[derive(Debug, Default, Clone)]
struct ObservedClusterState {
    partitions: BTreeMap<PartitionId, ObservedPartitionState>,
    alive_nodes: BTreeMap<PlainNodeId, GenerationalNodeId>,
    dead_nodes: BTreeSet<PlainNodeId>,
    nodes_to_partitions: BTreeMap<PlainNodeId, BTreeSet<PartitionId>>,
}

impl ObservedClusterState {
    fn update(&mut self, cluster_state: &ClusterState) {
        self.update_nodes(cluster_state);
        self.update_partitions(cluster_state);
    }

    fn update_nodes(&mut self, cluster_state: &ClusterState) {
        for (node_id, node_state) in &cluster_state.nodes {
            match node_state {
                NodeState::Alive(alive_node) => {
                    self.dead_nodes.remove(node_id);
                    self.alive_nodes
                        .insert(*node_id, alive_node.generational_node_id);
                }
                NodeState::Dead(_) => {
                    self.alive_nodes.remove(node_id);
                    self.dead_nodes.insert(*node_id);
                }
            }
        }
    }

    fn update_partitions(&mut self, cluster_state: &ClusterState) {
        // remove dead nodes
        for dead_node in cluster_state.dead_nodes() {
            if let Some(partitions) = self.nodes_to_partitions.remove(dead_node) {
                for partition_id in partitions {
                    if let Some(partition) = self.partitions.get_mut(&partition_id) {
                        partition.remove_node(dead_node);
                    }
                }
            }
        }

        // update node_sets and leaders of partitions
        for alive_node in cluster_state.alive_nodes() {
            let mut current_partitions = BTreeSet::default();

            let node_id = alive_node.generational_node_id.as_plain();

            for (partition_id, status) in &alive_node.partitions {
                let partition = self.partitions.entry(*partition_id).or_default();
                partition.upsert_node(node_id, status.effective_mode);

                current_partitions.insert(*partition_id);
            }

            if let Some(previous_partitions) = self.nodes_to_partitions.get(&node_id) {
                // remove partitions that are no longer running on the given node
                for partition_id in previous_partitions.difference(&current_partitions) {
                    if let Some(partition) = self.partitions.get_mut(partition_id) {
                        partition.remove_node(&node_id);
                    }
                }
            }

            // update nodes to partition index
            self.nodes_to_partitions.insert(node_id, current_partitions);
        }

        // remove empty partitions
        self.partitions
            .retain(|_, partition| !partition.node_set.is_empty());
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ObservedPartitionState {
    node_set: BTreeMap<PlainNodeId, RunMode>,
}

impl ObservedPartitionState {
    fn remove_node(&mut self, node_id: &PlainNodeId) {
        self.node_set.remove(node_id);
    }

    fn upsert_node(&mut self, node_id: PlainNodeId, run_mode: RunMode) {
        self.node_set.insert(node_id, run_mode);
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster_controller::scheduler::{
        ObservedClusterState, ObservedPartitionState, Scheduler,
    };
    use futures::StreamExt;
    use googletest::matcher::{Matcher, MatcherResult};
    use googletest::matchers::{empty, eq};
    use googletest::{assert_that, elements_are, unordered_elements_are};
    use http::Uri;
    use rand::prelude::ThreadRng;
    use rand::Rng;
    use restate_core::TestCoreEnvBuilder;
    use restate_types::cluster::cluster_state::{
        AliveNode, ClusterState, DeadNode, NodeState, PartitionProcessorStatus, RunMode,
    };
    use restate_types::cluster_controller::{ReplicationStrategy, SchedulingPlan};
    use restate_types::identifiers::PartitionId;
    use restate_types::metadata_store::keys::SCHEDULING_PLAN_KEY;
    use restate_types::net::partition_processor_manager::{ControlProcessors, ProcessorCommand};
    use restate_types::net::AdvertisedAddress;
    use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
    use restate_types::partition_table::PartitionTable;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZero;
    use std::sync::Arc;
    use std::time::Duration;
    use test_log::test;

    impl ObservedClusterState {
        fn remove_node_from_partition(
            &mut self,
            partition_id: &PartitionId,
            node_id: &PlainNodeId,
        ) {
            if let Some(partition) = self.partitions.get_mut(partition_id) {
                partition.remove_node(node_id)
            }
            if let Some(partitions) = self.nodes_to_partitions.get_mut(node_id) {
                partitions.remove(partition_id);
            }
        }

        fn add_node_to_partition(
            &mut self,
            partition_id: PartitionId,
            node_id: PlainNodeId,
            run_mode: RunMode,
        ) {
            self.partitions
                .entry(partition_id)
                .or_default()
                .upsert_node(node_id, run_mode);
            self.nodes_to_partitions
                .entry(node_id)
                .or_default()
                .insert(partition_id);
        }
    }

    impl ObservedPartitionState {
        fn new(node_set: impl IntoIterator<Item = (PlainNodeId, RunMode)>) -> Self {
            let node_set: BTreeMap<_, _> = node_set.into_iter().collect();

            Self { node_set }
        }
    }

    #[test]
    fn observed_partition_state_updates_leader() {
        let node_1 = PlainNodeId::from(1);
        let node_2 = PlainNodeId::from(2);
        let node_3 = PlainNodeId::from(3);

        let mut state = ObservedPartitionState::default();
        assert_that!(state.node_set, empty());

        state.upsert_node(node_1, RunMode::Leader);
        state.upsert_node(node_2, RunMode::Leader);
        state.upsert_node(node_3, RunMode::Follower);

        assert_that!(
            state.node_set,
            unordered_elements_are![
                (eq(node_1), eq(RunMode::Leader)),
                (eq(node_2), eq(RunMode::Leader)),
                (eq(node_3), eq(RunMode::Follower))
            ]
        );

        state.remove_node(&node_2);

        assert_that!(
            state.node_set,
            unordered_elements_are![
                (eq(node_1), eq(RunMode::Leader)),
                (eq(node_3), eq(RunMode::Follower))
            ]
        );
    }

    #[test]
    fn updating_observed_cluster_state() {
        let mut observed_cluster_state = ObservedClusterState::default();
        let partition_1 = PartitionId::from(0);
        let partition_2 = PartitionId::from(1);
        let partition_3 = PartitionId::from(2);
        let node_1 = GenerationalNodeId::new(1, 0);
        let partitions_1 = [
            (partition_1, leader_partition()),
            (partition_2, leader_partition()),
        ]
        .into_iter()
        .collect();
        let node_2 = GenerationalNodeId::new(2, 0);
        let partitions_2 = [
            (partition_1, follower_partition()),
            (partition_2, follower_partition()),
        ]
        .into_iter()
        .collect();

        let cluster_state = ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            nodes: [
                (node_1.as_plain(), alive_node(node_1, partitions_1)),
                (node_2.as_plain(), alive_node(node_2, partitions_2)),
            ]
            .into_iter()
            .collect(),
        };

        observed_cluster_state.update(&cluster_state);

        assert_that!(
            observed_cluster_state
                .alive_nodes
                .keys()
                .collect::<Vec<_>>(),
            unordered_elements_are![eq(&node_1.as_plain()), eq(&node_2.as_plain())]
        );
        assert_that!(observed_cluster_state.dead_nodes, empty());
        assert_that!(
            observed_cluster_state.nodes_to_partitions,
            unordered_elements_are![
                (
                    eq(node_1.as_plain()),
                    unordered_elements_are![eq(partition_1), eq(partition_2)]
                ),
                (
                    eq(node_2.as_plain()),
                    unordered_elements_are![eq(partition_1), eq(partition_2)]
                )
            ]
        );
        assert_that!(
            observed_cluster_state.partitions,
            unordered_elements_are![
                (
                    eq(partition_1),
                    eq(ObservedPartitionState::new([
                        (node_1.as_plain(), RunMode::Leader),
                        (node_2.as_plain(), RunMode::Follower)
                    ]))
                ),
                (
                    eq(partition_2),
                    eq(ObservedPartitionState::new([
                        (node_1.as_plain(), RunMode::Leader),
                        (node_2.as_plain(), RunMode::Follower)
                    ]))
                )
            ]
        );

        let partitions_1_new = [
            // forget partition_1
            (partition_2, leader_partition()),
            (partition_3, follower_partition()), // insert a new partition
        ]
        .into_iter()
        .collect();
        let cluster_state = ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            nodes: [
                (node_1.as_plain(), alive_node(node_1, partitions_1_new)),
                // report node_2 as dead
                (node_2.as_plain(), dead_node()),
            ]
            .into_iter()
            .collect(),
        };

        observed_cluster_state.update(&cluster_state);

        assert_that!(
            observed_cluster_state
                .alive_nodes
                .keys()
                .collect::<Vec<_>>(),
            elements_are![eq(&node_1.as_plain())]
        );
        assert_that!(
            observed_cluster_state.dead_nodes,
            elements_are![eq(node_2.as_plain())]
        );
        assert_that!(
            observed_cluster_state.nodes_to_partitions,
            unordered_elements_are![(
                eq(node_1.as_plain()),
                unordered_elements_are![eq(partition_2), eq(partition_3)]
            )]
        );
        assert_that!(
            observed_cluster_state.partitions,
            unordered_elements_are![
                (
                    eq(partition_2),
                    eq(ObservedPartitionState::new([(
                        node_1.as_plain(),
                        RunMode::Leader
                    )]))
                ),
                (
                    eq(partition_3),
                    eq(ObservedPartitionState::new([(
                        node_1.as_plain(),
                        RunMode::Follower
                    )]))
                ),
            ]
        );
    }

    fn leader_partition() -> PartitionProcessorStatus {
        PartitionProcessorStatus {
            planned_mode: RunMode::Leader,
            effective_mode: RunMode::Leader,
            ..PartitionProcessorStatus::default()
        }
    }

    fn follower_partition() -> PartitionProcessorStatus {
        PartitionProcessorStatus::default()
    }

    fn alive_node(
        generational_node_id: GenerationalNodeId,
        partitions: BTreeMap<PartitionId, PartitionProcessorStatus>,
    ) -> NodeState {
        NodeState::Alive(AliveNode {
            generational_node_id,
            last_heartbeat_at: MillisSinceEpoch::now(),
            partitions,
        })
    }

    fn dead_node() -> NodeState {
        NodeState::Dead(DeadNode {
            last_seen_alive: None,
        })
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
            );
            nodes_config.upsert_node(node_config);
        }

        let mut builder = TestCoreEnvBuilder::new_with_mock_network();
        let mut control_processors = builder
            .router_builder
            .subscribe_to_stream::<ControlProcessors>(32);

        let partition_table =
            PartitionTable::with_equally_sized_partitions(Version::MIN, num_partitions);
        let initial_scheduling_plan = SchedulingPlan::from(&partition_table, replication_strategy);
        let metadata_store_client = builder.metadata_store_client.clone();

        let network_sender = builder.network_sender.clone();

        let env = builder
            .with_nodes_config(nodes_config)
            .with_partition_table(partition_table.clone())
            .with_scheduling_plan(initial_scheduling_plan)
            .build()
            .await;
        let tc = env.tc.clone();
        env.tc
            .run_in_scope("test", None, async move {
                let mut scheduler =
                    Scheduler::init(tc, metadata_store_client.clone(), network_sender).await?;

                for _ in 0..num_scheduling_rounds {
                    let cluster_state = random_cluster_state(&node_ids, num_partitions);

                    let cluster_state = Arc::new(cluster_state);
                    scheduler
                        .on_cluster_state_update(Arc::clone(&cluster_state))
                        .await?;
                    // collect all control messages from the network to build up the effective scheduling plan
                    let control_messages = control_processors
                        .as_mut()
                        .take_until(tokio::time::sleep(Duration::from_secs(10)))
                        .map(|message| message.split())
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

                    let alive_nodes: BTreeSet<_> = cluster_state
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
                    if observed_state.node_set.len() != target_state.node_set.len() {
                        return MatcherResult::NoMatch;
                    }

                    for (node_id, run_mode) in target_state.iter() {
                        if observed_state.node_set.get(&node_id) != Some(&run_mode) {
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
        control_messages: Vec<(GenerationalNodeId, ControlProcessors)>,
    ) -> ObservedClusterState {
        let mut observed_cluster_state = ObservedClusterState::default();
        observed_cluster_state.update(cluster_state);

        // apply commands
        for (node_id, control_processors) in control_messages {
            for control_processor in control_processors.commands {
                match control_processor.command {
                    ProcessorCommand::Stop => {
                        observed_cluster_state.remove_node_from_partition(
                            &control_processor.partition_id,
                            &node_id.as_plain(),
                        );
                    }
                    ProcessorCommand::Follower => {
                        observed_cluster_state.add_node_to_partition(
                            control_processor.partition_id,
                            node_id.as_plain(),
                            RunMode::Follower,
                        );
                    }
                    ProcessorCommand::Leader => {
                        observed_cluster_state.add_node_to_partition(
                            control_processor.partition_id,
                            node_id.as_plain(),
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
        num_partitions: u64,
    ) -> ClusterState {
        let nodes = random_nodes_state(node_ids, num_partitions);

        ClusterState {
            last_refreshed: None,
            nodes_config_version: Version::MIN,
            partition_table_version: Version::MIN,
            nodes,
        }
    }

    fn random_nodes_state(
        node_ids: &Vec<GenerationalNodeId>,
        num_partitions: u64,
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
        num_partitions: u64,
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
        num_partitions: u64,
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
