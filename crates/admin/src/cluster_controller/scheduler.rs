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
use restate_types::live::Pinned;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::debug;
use xxhash_rust::xxh3::Xxh3Builder;

use restate_core::metadata_store::{Precondition, ReadError, ReadWriteError, WriteError};
use restate_core::network::{NetworkSender, Networking, Outgoing, TransportConnect};
use restate_core::{
    cancellation_watcher, Metadata, MetadataKind, MetadataWriter, ShutdownError, SyncError,
    TargetVersion, TaskCenter, TaskHandle, TaskKind,
};
use restate_types::identifiers::PartitionId;
use restate_types::logs::LogId;
use restate_types::metadata_store::keys::PARTITION_TABLE_KEY;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::{
    Partition, PartitionPlacement, PartitionTable, ReplicationStrategy,
};
use restate_types::{NodeId, PlainNodeId, Version};

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
    #[error("failed read/write on metadata store: {0}")]
    MetadataStoreReadWrite(#[from] ReadWriteError),
    #[error("failed syncing metadata: {0}")]
    Metadata(#[from] SyncError),
    #[error("system is shutting down")]
    Shutdown(#[from] ShutdownError),
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
    metadata_writer: MetadataWriter,
    networking: Networking<T>,
    inflight_sync_task: Option<TaskHandle<()>>,
}

/// The scheduler is responsible for assigning partition processors to nodes and to electing
/// leaders. It achieves it by deciding on a partition placement which is persisted in the partition table
/// and then driving the observed cluster state to the target state (represented by the
/// partition table).
impl<T: TransportConnect> Scheduler<T> {
    pub fn new(metadata_writer: MetadataWriter, networking: Networking<T>) -> Self {
        Self {
            metadata_writer,
            networking,
            inflight_sync_task: None,
        }
    }

    pub async fn on_observed_cluster_state(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        nodes_config: &NodesConfiguration,
        placement_hints: impl PartitionProcessorPlacementHints,
    ) -> Result<(), Error> {
        let alive_workers = observed_cluster_state
            .alive_nodes
            .keys()
            .cloned()
            .filter(|node_id| nodes_config.has_worker_role(node_id))
            .collect();

        self.update_partition_placement(&alive_workers, nodes_config, placement_hints)
            .await?;

        self.instruct_nodes(observed_cluster_state)?;

        Ok(())
    }

    pub async fn _on_tick(&mut self) {
        // nothing to do since we don't make time based scheduling decisions yet
    }

    async fn update_partition_placement(
        &mut self,
        alive_workers: &HashSet<PlainNodeId>,
        nodes_config: &NodesConfiguration,
        placement_hints: impl PartitionProcessorPlacementHints,
    ) -> Result<(), Error> {
        let logs = Metadata::with_current(|m| m.logs_ref());
        let partition_table = Metadata::with_current(|m| m.partition_table_ref());

        if logs.num_logs() != partition_table.num_partitions() as usize {
            // either the partition table or the logs are not fully initialized
            // hence there is nothing we can do atm.
            // we need to wait until both partitions and logs are created
            return Ok(());
        }

        let version = partition_table.version();

        // todo(azmy): avoid cloning the partition table every time by keeping
        // the latest built always available as a field
        let mut builder = partition_table.clone().into_builder();
        let replication_strategy = builder.replication_strategy();

        builder.for_each(|partition_id, placement| {
            let mut target_state = TargetPartitionPlacementState::new(placement);
            self.ensure_replication(
                partition_id,
                &mut target_state,
                alive_workers,
                replication_strategy,
                nodes_config,
                &placement_hints,
            );

            self.ensure_leadership(partition_id, &mut target_state, &placement_hints);
        });

        if let Some(partition_table) = builder.build_if_modified() {
            debug!("Updated partition table placement: {partition_table:?}");
            self.try_update_partition_table(version, partition_table)
                .await?;

            return Ok(());
        }

        Ok(())
    }

    async fn try_update_partition_table(
        &mut self,
        version: Version,
        partition_table: PartitionTable,
    ) -> Result<(), Error> {
        match self
            .metadata_writer
            .metadata_store_client()
            .put(
                PARTITION_TABLE_KEY.clone(),
                &partition_table,
                Precondition::MatchesVersion(version),
            )
            .await
        {
            Ok(_) => {}
            Err(WriteError::FailedPrecondition(msg)) => {
                debug!("Partition table update failed due to: {msg}");
                // There is no need to wait for the partition table
                // to synchronize. The update_partition_placement will
                // get called again anyway once the partition table is updated.
                self.sync_partition_table()?;
            }
            Err(err) => return Err(err.into()),
        }

        self.metadata_writer
            .update(Arc::new(partition_table))
            .await?;
        Ok(())
    }

    /// Synchronize partition table asynchronously
    fn sync_partition_table(&mut self) -> Result<(), Error> {
        if self
            .inflight_sync_task
            .as_ref()
            .is_some_and(|t| !t.is_finished())
        {
            return Ok(());
        }

        let task = TaskCenter::spawn_unmanaged(
            TaskKind::Disposable,
            "scheduler-sync-partition-table",
            async {
                let cancelled = cancellation_watcher();
                let metadata = Metadata::current();
                tokio::select! {
                    result = metadata.sync(MetadataKind::PartitionTable, TargetVersion::Latest) => {
                        if let Err(err) = result {
                            debug!("Failed to sync partition table metadata: {err}");
                        }
                    }
                    _ = cancelled => {}
                };
            },
        )?;

        self.inflight_sync_task = Some(task);
        Ok(())
    }

    fn ensure_replication<H: PartitionProcessorPlacementHints>(
        &self,
        partition_id: &PartitionId,
        target_state: &mut TargetPartitionPlacementState,
        alive_workers: &HashSet<PlainNodeId>,
        replication_strategy: ReplicationStrategy,
        nodes_config: &NodesConfiguration,
        placement_hints: &H,
    ) {
        let mut rng = rand::thread_rng();
        target_state
            .node_set
            .retain(|node_id| alive_workers.contains(node_id));

        match replication_strategy {
            ReplicationStrategy::OnAllNodes => {
                // The extend will only add the new nodes that
                // don't exist in the node set.
                // the retain done above will make sure alive nodes in the set
                // will keep there initial order.
                target_state.node_set.extend(alive_workers.iter().cloned());
            }
            ReplicationStrategy::Factor(replication_factor) => {
                let replication_factor =
                    usize::try_from(replication_factor.get()).expect("u32 should fit into usize");

                if target_state.node_set.len() == replication_factor {
                    return;
                }

                let preferred_worker_nodes = placement_hints
                    .preferred_nodes(partition_id)
                    .filter(|node_id| nodes_config.has_worker_role(node_id));
                let preferred_leader =
                    placement_hints
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
                        target_state.node_set.insert(preferred_leader);
                    }

                    // todo: Implement cleverer strategies
                    // randomly choose from the preferred workers nodes first
                    let new_nodes = preferred_worker_nodes
                        .filter(|node_id| !target_state.node_set.contains(node_id))
                        .cloned()
                        .choose_multiple(
                            &mut rng,
                            replication_factor - target_state.node_set.len(),
                        );

                    target_state.node_set.extend(new_nodes);

                    if target_state.node_set.len() < replication_factor {
                        // randomly choose from the remaining worker nodes
                        let new_nodes = alive_workers
                            .iter()
                            .filter(|node| !target_state.node_set.contains(node))
                            .cloned()
                            .choose_multiple(
                                &mut rng,
                                replication_factor - target_state.node_set.len(),
                            );

                        target_state.node_set.extend(new_nodes);
                    }
                } else if target_state.node_set.len() > replication_factor {
                    let preferred_worker_nodes: HashSet<PlainNodeId> =
                        preferred_worker_nodes.cloned().collect();

                    // first remove the not preferred nodes
                    for node_id in target_state
                        .node_set
                        .nodes()
                        .filter(|node_id| {
                            !preferred_worker_nodes.contains(node_id)
                                && Some(**node_id) != preferred_leader
                        })
                        .cloned()
                        .choose_multiple(&mut rng, target_state.node_set.len() - replication_factor)
                    {
                        target_state.node_set.remove(&node_id);
                    }

                    if target_state.node_set.len() > replication_factor {
                        for node_id in target_state
                            .node_set
                            .nodes()
                            .filter(|node_id| Some(**node_id) != preferred_leader)
                            .cloned()
                            .choose_multiple(
                                &mut rng,
                                target_state.node_set.len() - replication_factor,
                            )
                        {
                            target_state.node_set.remove(&node_id);
                        }
                    }
                }
            }
        }

        // check if the leader is still part of the node set; if not, then clear leader field
        if let Some(leader) = target_state.leader.as_ref() {
            if !target_state.node_set.contains(leader) {
                target_state.leader = None;
            }
        }
    }

    fn ensure_leadership<H: PartitionProcessorPlacementHints>(
        &self,
        partition_id: &PartitionId,
        target_state: &mut TargetPartitionPlacementState,
        placement_hints: &H,
    ) {
        let preferred_leader = placement_hints.preferred_leader(partition_id);

        if target_state.leader.is_none() {
            target_state.leader = self.select_leader_from(target_state, preferred_leader);
        } else if preferred_leader
            .is_some_and(|preferred_leader| target_state.node_set.contains(&preferred_leader))
        {
            target_state.leader = preferred_leader;
        }
    }

    fn select_leader_from(
        &self,
        leader_candidates: &TargetPartitionPlacementState,
        preferred_leader: Option<PlainNodeId>,
    ) -> Option<PlainNodeId> {
        // todo: Implement leader balancing between nodes
        preferred_leader
            .filter(|leader| leader_candidates.contains(leader))
            .or_else(|| {
                let mut rng = rand::thread_rng();
                leader_candidates.node_set.nodes().choose(&mut rng).cloned()
            })
    }

    fn instruct_nodes(&self, observed_cluster_state: &ObservedClusterState) -> Result<(), Error> {
        let partition_table = Metadata::with_current(|m| m.partition_table_ref());

        let mut commands = BTreeMap::default();

        for (partition_id, partition) in partition_table.partitions() {
            self.generate_instructions_for_partition(
                partition_id,
                partition,
                observed_cluster_state,
                &mut commands,
            );
        }

        let (cur_partition_table_version, cur_logs_version) =
            Metadata::with_current(|m| (m.partition_table_version(), m.logs_version()));
        for (node_id, commands) in commands.into_iter() {
            // only send control processors message if there are commands to send
            if !commands.is_empty() {
                let control_processors = ControlProcessors {
                    // todo: Maybe remove unneeded partition table version
                    min_partition_table_version: cur_partition_table_version,
                    min_logs_table_version: cur_logs_version,
                    commands,
                };

                TaskCenter::spawn_child(
                    TaskKind::Disposable,
                    "send-control-processors-to-node",
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
        partition: &Partition,
        observed_cluster_state: &ObservedClusterState,
        commands: &mut BTreeMap<PlainNodeId, Vec<ControlProcessor>>,
    ) {
        // todo: Avoid cloning of node_set if this becomes measurable
        let mut observed_state = observed_cluster_state
            .partitions
            .get(partition_id)
            .map(|state| state.partition_processors.clone())
            .unwrap_or_default();

        for (node_id, run_mode) in partition.placement.iter() {
            if !observed_state
                .remove(node_id)
                .is_some_and(|observed_run_mode| observed_run_mode == run_mode)
            {
                commands
                    .entry(*node_id)
                    .or_default()
                    .push(ControlProcessor {
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

/// Placement hints for the [`logs_controller::LogsController`] based on the current
/// [`SchedulingPlan`].
pub struct PartitionTableNodeSetSelectorHints {
    partition_table: Pinned<PartitionTable>,
}

impl From<Pinned<PartitionTable>> for PartitionTableNodeSetSelectorHints {
    fn from(value: Pinned<PartitionTable>) -> Self {
        Self {
            partition_table: value,
        }
    }
}

impl logs_controller::NodeSetSelectorHints for PartitionTableNodeSetSelectorHints {
    fn preferred_sequencer(&self, log_id: &LogId) -> Option<NodeId> {
        let partition_id = PartitionId::from(*log_id);

        self.partition_table
            .get_partition(&partition_id)
            .and_then(|partition| partition.placement.leader().cloned().map(NodeId::from))
    }
}

/// The target state of a partition.
#[derive(Debug)]
struct TargetPartitionPlacementState<'a> {
    /// Node which is the designated leader
    pub leader: Option<PlainNodeId>,
    /// Set of nodes that should run a partition processor for this partition
    pub node_set: &'a mut PartitionPlacement,
}

impl<'a> TargetPartitionPlacementState<'a> {
    fn new(placement: &'a mut PartitionPlacement) -> Self {
        Self {
            leader: placement.leader().cloned(),
            node_set: placement,
        }
    }
}

impl<'a> TargetPartitionPlacementState<'a> {
    #[cfg(test)]
    pub fn contains_all(&self, set: &HashSet<PlainNodeId>) -> bool {
        set.iter().all(|id| self.node_set.contains(id))
    }

    pub fn contains(&self, value: &PlainNodeId) -> bool {
        self.node_set.contains(value)
    }
}

impl<'a> Drop for TargetPartitionPlacementState<'a> {
    fn drop(&mut self) {
        if let Some(node_id) = self.leader.take() {
            self.node_set.set_leader(node_id);
        }
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

    use crate::cluster_controller::logs_controller::tests::MockNodes;
    use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
    use crate::cluster_controller::scheduler::{
        HashSet, PartitionProcessorPlacementHints, Scheduler, TargetPartitionPlacementState,
    };
    use restate_core::network::{ForwardingHandler, Incoming, MessageCollectorMockConnector};
    use restate_core::{Metadata, TestCoreEnv, TestCoreEnvBuilder};
    use restate_types::cluster::cluster_state::{
        AliveNode, ClusterState, DeadNode, NodeState, PartitionProcessorStatus, RunMode,
    };
    use restate_types::identifiers::PartitionId;
    use restate_types::locality::NodeLocation;
    use restate_types::metadata_store::keys::PARTITION_TABLE_KEY;
    use restate_types::net::codec::WireDecode;
    use restate_types::net::partition_processor_manager::{ControlProcessors, ProcessorCommand};
    use restate_types::net::{AdvertisedAddress, TargetName};
    use restate_types::nodes_config::{
        LogServerConfig, NodeConfig, NodesConfiguration, Role, StorageState,
    };
    use restate_types::partition_table::{
        PartitionPlacement, PartitionTable, PartitionTableBuilder, ReplicationStrategy,
    };
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

    #[test(restate_core::test)]
    async fn empty_leadership_changes_donot_modify_partition_table() -> googletest::Result<()> {
        let test_env = TestCoreEnv::create_with_single_node(0, 0).await;
        let metadata_store_client = test_env.metadata_store_client.clone();
        let metadata_writer = test_env.metadata_writer.clone();
        let networking = test_env.networking.clone();

        let initial_partition_table = test_env.metadata.partition_table_ref();

        let mut scheduler = Scheduler::new(metadata_writer, networking);
        let observed_cluster_state = ObservedClusterState::default();

        scheduler
            .on_observed_cluster_state(
                &observed_cluster_state,
                &Metadata::with_current(|m| m.nodes_config_ref()),
                NoPlacementHints,
            )
            .await?;

        let partition_table = metadata_store_client
            .get::<PartitionTable>(PARTITION_TABLE_KEY.clone())
            .await
            .expect("partition table")
            .unwrap();

        assert_eq!(*initial_partition_table, partition_table);

        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn schedule_partitions_with_replication_factor() -> googletest::Result<()> {
        schedule_partitions(ReplicationStrategy::Factor(
            NonZero::new(3).expect("non-zero"),
        ))
        .await?;
        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
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
                NodeLocation::default(),
                AdvertisedAddress::Http(Uri::default()),
                Role::Worker.into(),
                LogServerConfig::default(),
            );
            nodes_config.upsert_node(node_config);
        }

        // network messages going to other nodes are written to `tx`
        let (tx, control_recv) = mpsc::channel(100);
        let connector = MessageCollectorMockConnector::new(10, tx.clone());

        let mut builder = TestCoreEnvBuilder::with_transport_connector(connector);
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

        let mut partition_table_builder =
            PartitionTable::with_equally_sized_partitions(Version::MIN, num_partitions)
                .into_builder();
        partition_table_builder.set_replication_strategy(replication_strategy);
        let partition_table = partition_table_builder.build();

        let metadata_store_client = builder.metadata_store_client.clone();
        let metadata_writer = builder.metadata_writer.clone();

        let networking = builder.networking.clone();

        let _env = builder
            .set_nodes_config(nodes_config.clone())
            .set_partition_table(partition_table.clone())
            .build()
            .await;
        let mut scheduler = Scheduler::new(metadata_writer, networking);
        let mut observed_cluster_state = ObservedClusterState::default();

        for _ in 0..num_scheduling_rounds {
            let cluster_state = random_cluster_state(&node_ids, num_partitions);

            observed_cluster_state.update(&cluster_state);
            scheduler
                .on_observed_cluster_state(
                    &observed_cluster_state,
                    &Metadata::with_current(|m| m.nodes_config_ref()),
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
            let mut target_partition_table = metadata_store_client
                .get::<PartitionTable>(PARTITION_TABLE_KEY.clone())
                .await?
                .expect("the scheduler should have created a partition table");

            // assert that the effective scheduling plan aligns with the target scheduling plan
            assert_that!(
                observed_cluster_state,
                matches_partition_table(&target_partition_table)
            );

            let alive_nodes: HashSet<_> = cluster_state
                .alive_nodes()
                .map(|node| node.generational_node_id.as_plain())
                .collect();

            for (_, partition) in target_partition_table.partitions_mut() {
                let target_state = TargetPartitionPlacementState::new(&mut partition.placement);
                // assert that the replication strategy was respected
                match replication_strategy {
                    ReplicationStrategy::OnAllNodes => {
                        // assert that every partition has a leader which is part of the alive nodes set
                        assert!(target_state.contains_all(&alive_nodes));

                        assert!(target_state
                            .leader
                            .is_some_and(|leader| alive_nodes.contains(&leader)));
                    }
                    ReplicationStrategy::Factor(replication_factor) => {
                        // assert that every partition has a leader which is part of the alive nodes set
                        assert!(target_state
                            .leader
                            .is_some_and(|leader| alive_nodes.contains(&leader)));

                        assert_eq!(
                            target_state.node_set.len(),
                            alive_nodes.len().min(
                                usize::try_from(replication_factor.get())
                                    .expect("u32 fits into usize")
                            )
                        );
                    }
                }
            }
        }

        Ok(())
    }

    #[test(restate_core::test)]
    async fn handle_too_few_placed_partition_processors() -> googletest::Result<()> {
        let num_partition_processors = NonZero::new(2).expect("non-zero");

        let mut partition_table_builder =
            PartitionTable::with_equally_sized_partitions(Version::MIN, 2).into_builder();

        partition_table_builder.for_each(|_, target_state| {
            target_state.extend([PlainNodeId::from(0)]);
        });

        let partition_table = run_ensure_replication_test(
            partition_table_builder,
            ReplicationStrategy::Factor(num_partition_processors),
        )
        .await?;
        let partition = partition_table
            .get_partition(&PartitionId::from(0))
            .expect("must be present");

        assert_eq!(
            partition.placement.len(),
            num_partition_processors.get() as usize
        );

        Ok(())
    }

    #[test(restate_core::test)]
    async fn handle_too_many_placed_partition_processors() -> googletest::Result<()> {
        let num_partition_processors = NonZero::new(2).expect("non-zero");

        let mut partition_table_builder =
            PartitionTable::with_equally_sized_partitions(Version::MIN, 2).into_builder();

        partition_table_builder.for_each(|_, placement| {
            placement.extend([
                PlainNodeId::from(0),
                PlainNodeId::from(1),
                PlainNodeId::from(2),
            ]);
        });

        let partition_table = run_ensure_replication_test(
            partition_table_builder,
            ReplicationStrategy::Factor(num_partition_processors),
        )
        .await?;
        let partition = partition_table
            .get_partition(&PartitionId::from(0))
            .expect("must be present");

        assert_eq!(
            partition.placement.len(),
            num_partition_processors.get() as usize
        );

        Ok(())
    }

    async fn run_ensure_replication_test(
        mut partition_table_builder: PartitionTableBuilder,
        replication_strategy: ReplicationStrategy,
    ) -> googletest::Result<PartitionTable> {
        let env = TestCoreEnv::create_with_single_node(0, 0).await;

        let scheduler = Scheduler::new(env.metadata_writer.clone(), env.networking.clone());
        let alive_workers = vec![
            PlainNodeId::from(0),
            PlainNodeId::from(1),
            PlainNodeId::from(2),
        ]
        .into_iter()
        .collect();
        let nodes_config = MockNodes::builder()
            .with_nodes([0, 1, 2], Role::Worker.into(), StorageState::ReadWrite)
            .build()
            .nodes_config;

        partition_table_builder.for_each(|partition_id, placement| {
            let mut target_state = TargetPartitionPlacementState::new(placement);

            scheduler.ensure_replication(
                partition_id,
                &mut target_state,
                &alive_workers,
                replication_strategy,
                &nodes_config,
                &NoPlacementHints,
            );
        });

        Ok(partition_table_builder.build())
    }

    fn matches_partition_table(partition_table: &PartitionTable) -> PartitionTableMatcher<'_> {
        PartitionTableMatcher { partition_table }
    }

    struct PartitionTableMatcher<'a> {
        partition_table: &'a PartitionTable,
    }

    impl<'a> Matcher for PartitionTableMatcher<'a> {
        type ActualT = ObservedClusterState;

        fn matches(&self, actual: &Self::ActualT) -> MatcherResult {
            if actual.partitions.len() != self.partition_table.num_partitions() as usize {
                return MatcherResult::NoMatch;
            }

            for (partition_id, partition) in self.partition_table.partitions() {
                if let Some(observed_state) = actual.partitions.get(partition_id) {
                    if observed_state.partition_processors.len() != partition.placement.len() {
                        return MatcherResult::NoMatch;
                    }

                    for (node_id, run_mode) in partition.placement.iter() {
                        if observed_state.partition_processors.get(node_id) != Some(&run_mode) {
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
                        "should reflect the partition table {:?}",
                        self.partition_table
                    )
                }
                MatcherResult::NoMatch => {
                    format!(
                        "does not reflect the partition table {:?}",
                        self.partition_table
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
    #[test]
    fn target_placement_state() {
        let mut placement = PartitionPlacement::from_iter([
            PlainNodeId::from(1),
            PlainNodeId::from(2),
            PlainNodeId::from(3),
        ]);

        let mut target_state = TargetPartitionPlacementState::new(&mut placement);

        assert!(matches!(target_state.leader, Some(node_id) if node_id == PlainNodeId::from(1)));

        target_state.leader = Some(PlainNodeId::from(3));

        drop(target_state);

        assert_eq!(
            placement,
            PartitionPlacement::from_iter([
                PlainNodeId::from(3),
                PlainNodeId::from(1),
                PlainNodeId::from(2),
            ])
        );
    }
}
