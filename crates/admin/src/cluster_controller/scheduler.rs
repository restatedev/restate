// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use rand::seq::IteratorRandom;
use tracing::{Level, debug, enabled, info, instrument, trace, warn};

use restate_core::network::{NetworkSender as _, Networking, Swimlane, TransportConnect};
use restate_core::{Metadata, MetadataWriter, ShutdownError, SyncError, TaskCenter, TaskKind};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_metadata_store::{ReadError, ReadWriteError, WriteError};
use restate_types::cluster::cluster_state::RunMode;
use restate_types::identifiers::PartitionId;
use restate_types::locality::LocationScope;
use restate_types::logs::LogId;
use restate_types::metadata::Precondition;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::{
    Partition, PartitionPlacement, PartitionReplication, PartitionTable,
};
use restate_types::{NodeId, PlainNodeId, Version};

use crate::cluster_controller::logs_controller;
use crate::cluster_controller::observed_cluster_state::ObservedClusterState;

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
        }
    }

    pub async fn on_observed_cluster_state(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        nodes_config: &NodesConfiguration,
        placement_hints: impl PartitionProcessorPlacementHints,
    ) -> Result<(), Error> {
        trace!(?observed_cluster_state, "On observed cluster state");
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

        if logs.num_logs() != partition_table.len() {
            // either the partition table or the logs are not fully initialized
            // hence there is nothing we can do atm.
            // we need to wait until both partitions and logs are created
            return Ok(());
        }
        let version = partition_table.version();

        // todo(azmy): avoid cloning the partition table every time by keeping
        //  the latest built always available as a field
        let mut builder = partition_table.clone().into_builder();
        let partition_replication = builder.partition_replication().clone();

        builder.for_each(|partition_id, placement| {
            let mut target_state = TargetPartitionPlacementState::new(placement);
            self.ensure_replication(
                partition_id,
                &mut target_state,
                alive_workers,
                &partition_replication,
                nodes_config,
                &placement_hints,
            );

            self.ensure_leadership(partition_id, &mut target_state, &placement_hints);
        });

        if let Some(partition_table) = builder.build_if_modified() {
            if enabled!(Level::TRACE) {
                debug!(
                    ?partition_table,
                    "Will attempt to write partition table {} to metadata store",
                    partition_table.version()
                );
            } else {
                debug!(
                    "Will attempt to write partition table {} to metadata store",
                    partition_table.version()
                );
            }

            self.try_update_partition_table(version, partition_table)
                .await?;

            return Ok(());
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn try_update_partition_table(
        &mut self,
        previous_version: Version,
        partition_table: PartitionTable,
    ) -> Result<(), Error> {
        let new_version = partition_table.version();
        match self
            .metadata_writer
            .global_metadata()
            .put(
                partition_table.into(),
                Precondition::MatchesVersion(previous_version),
            )
            .log_slow_after(
                Duration::from_secs(1),
                Level::DEBUG,
                format!("Updating partition table to version {new_version}"),
            )
            .with_overdue(Duration::from_secs(3), tracing::Level::INFO)
            .await
        {
            Ok(_) => {
                debug!("Partition table {new_version} has been written to metadata store",);
            }
            Err(WriteError::FailedPrecondition(err)) => {
                info!(
                    err,
                    "Write partition table to metadata store was rejected due to version conflict, \
                        this is benign unless it's happening repeatedly. In such case, we might be in \
                        a tight race with another admin node"
                );
                // There is no need to wait for the partition table to synchronize.
                // The update_partition_placement will get called again anyway once
                // the partition table is updated.
            }
            Err(err) => return Err(err.into()),
        }

        Ok(())
    }

    fn ensure_replication<H: PartitionProcessorPlacementHints>(
        &self,
        partition_id: &PartitionId,
        target_state: &mut TargetPartitionPlacementState,
        alive_workers: &HashSet<PlainNodeId>,
        partition_replication: &PartitionReplication,
        nodes_config: &NodesConfiguration,
        placement_hints: &H,
    ) {
        let mut rng = rand::rng();
        target_state
            .node_set
            .retain(|node_id| alive_workers.contains(node_id));

        match partition_replication {
            PartitionReplication::Everywhere => {
                // The extend will only add the new nodes that
                // don't exist in the node set.
                // the retain done above will make sure alive nodes in the set
                // will keep there initial order.
                target_state.node_set.extend(alive_workers.iter().cloned());
            }
            PartitionReplication::Limit(replication_factor) => {
                if replication_factor.greatest_defined_scope() > LocationScope::Node {
                    warn!("Location aware partition replication is not supported yet");
                }

                let replication_factor = usize::from(replication_factor.num_copies());

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
                        .filter(|node_id| !target_state.node_set.contains(**node_id))
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
                            .filter(|node| !target_state.node_set.contains(**node))
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
                        .iter()
                        .copied()
                        .filter(|node_id| {
                            !preferred_worker_nodes.contains(node_id)
                                && Some(*node_id) != preferred_leader
                        })
                        .choose_multiple(&mut rng, target_state.node_set.len() - replication_factor)
                    {
                        target_state.node_set.remove(node_id);
                    }

                    if target_state.node_set.len() > replication_factor {
                        for node_id in target_state
                            .node_set
                            .iter()
                            .filter(|node_id| Some(**node_id) != preferred_leader)
                            .copied()
                            .choose_multiple(
                                &mut rng,
                                target_state.node_set.len() - replication_factor,
                            )
                        {
                            target_state.node_set.remove(node_id);
                        }
                    }
                }
            }
        }

        // check if the leader is still part of the node set; if not, then clear leader field
        if let Some(leader) = target_state.leader.as_ref() {
            if !target_state.node_set.contains(*leader) {
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
            .is_some_and(|preferred_leader| target_state.node_set.contains(preferred_leader))
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
            .filter(|leader| leader_candidates.contains(*leader))
            .or_else(|| {
                let mut rng = rand::rng();
                leader_candidates.node_set.iter().choose(&mut rng).cloned()
            })
    }

    fn instruct_nodes(&self, observed_cluster_state: &ObservedClusterState) -> Result<(), Error> {
        let partition_table = Metadata::with_current(|m| m.partition_table_ref());

        let mut commands = BTreeMap::default();

        for (partition_id, partition) in partition_table.iter() {
            self.generate_instructions_for_partition(
                partition_id,
                partition,
                observed_cluster_state,
                &mut commands,
            );
        }

        if !commands.is_empty() {
            trace!(
                "Instruct nodes with partition processor commands: {:?} ",
                commands
            );
        } else {
            trace!(
                "No need to instruct nodes as they are running the correct partition processors"
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
                        // doesn't retry, we don't want to keep bombarding a node that's
                        // potentially dead.
                        async move {
                            let Ok(connection) = networking
                                .get_connection(node_id, Swimlane::default())
                                .await
                            else {
                                // ignore connection errors, no need to mark the task as failed
                                // as it pollutes the log.
                                return Ok(());
                            };

                            let Some(permit) = connection.reserve().await else {
                                // ditto
                                return Ok(());
                            };
                            permit.send_unary(control_processors, None);

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

        for (position, node_id) in partition.placement.iter().with_position() {
            let run_mode = if matches!(
                position,
                itertools::Position::First | itertools::Position::Only
            ) {
                RunMode::Leader
            } else {
                RunMode::Follower
            };
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
    partition_table: Arc<PartitionTable>,
}

impl From<Arc<PartitionTable>> for PartitionTableNodeSetSelectorHints {
    fn from(value: Arc<PartitionTable>) -> Self {
        Self {
            partition_table: value,
        }
    }
}

impl logs_controller::NodeSetSelectorHints for PartitionTableNodeSetSelectorHints {
    fn preferred_sequencer(&self, log_id: &LogId) -> Option<NodeId> {
        let partition_id = PartitionId::from(*log_id);

        self.partition_table
            .get(&partition_id)
            .and_then(|partition| partition.placement.leader().map(NodeId::from))
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
            leader: placement.leader(),
            node_set: placement,
        }
    }
}

impl TargetPartitionPlacementState<'_> {
    pub fn contains(&self, value: PlainNodeId) -> bool {
        self.node_set.contains(value)
    }
}

impl Drop for TargetPartitionPlacementState<'_> {
    fn drop(&mut self) {
        if let Some(node_id) = self.leader.take() {
            self.node_set.set_leader(node_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::iter;
    use std::num::NonZero;
    use std::time::Duration;

    use futures::StreamExt;
    use googletest::prelude::*;
    use itertools::Itertools;
    use rand::Rng;
    use rand::prelude::ThreadRng;
    use test_log::test;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use restate_core::network::{
        BackPressureMode, Incoming, MessageRouterBuilder, MockConnector, Unary,
    };
    use restate_core::{Metadata, TestCoreEnv, TestCoreEnvBuilder};
    use restate_types::cluster::cluster_state::{
        AliveNode, ClusterState, DeadNode, NodeState, PartitionProcessorStatus, RunMode,
    };
    use restate_types::identifiers::PartitionId;
    use restate_types::metadata_store::keys::PARTITION_TABLE_KEY;
    use restate_types::net::partition_processor_manager::{
        ControlProcessors, PartitionManagerService, ProcessorCommand,
    };
    use restate_types::net::{AdvertisedAddress, UnaryMessage};
    use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role, StorageState};
    use restate_types::partition_table::{
        PartitionPlacement, PartitionReplication, PartitionTable, PartitionTableBuilder,
    };
    use restate_types::replication::NodeSet;
    use restate_types::replication::ReplicationProperty;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};

    use crate::cluster_controller::logs_controller::tests::MockNodes;
    use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
    use crate::cluster_controller::scheduler::{
        PartitionProcessorPlacementHints, Scheduler, TargetPartitionPlacementState,
    };

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
        schedule_partitions(PartitionReplication::Limit(ReplicationProperty::new(
            NonZero::new(3).expect("non-zero"),
        )))
        .await?;
        Ok(())
    }

    #[test(restate_core::test(start_paused = true))]
    async fn schedule_partitions_with_all_nodes_replication() -> googletest::Result<()> {
        schedule_partitions(PartitionReplication::Everywhere).await?;
        Ok(())
    }

    async fn schedule_partitions(
        partition_replication: PartitionReplication,
    ) -> googletest::Result<()> {
        let num_partitions = 64;
        let num_nodes = 5;
        let num_scheduling_rounds = 10;

        let node_ids: Vec<_> = (1..=num_nodes)
            .map(|idx| GenerationalNodeId::new(idx, idx))
            .collect();
        let mut nodes_config = NodesConfiguration::new(Version::MIN, "test-cluster".to_owned());

        for node_id in &node_ids {
            let node_config = NodeConfig::builder()
                .name(format!("{node_id}"))
                .current_generation(*node_id)
                .address(AdvertisedAddress::Http(
                    format!("http://localhost-{}:5122", node_id.id())
                        .parse()
                        .unwrap(),
                ))
                .roles(Role::Worker.into())
                .build();
            nodes_config.upsert_node(node_config);
        }

        // network messages going to other nodes are written to `tx`
        let (tx, control_recv) = mpsc::unbounded_channel();

        let router_factory = move |peer_node_id: GenerationalNodeId,
                                   router: &mut MessageRouterBuilder| {
            let service_receiver =
                router.register_service::<PartitionManagerService>(128, BackPressureMode::PushBack);

            let tx = tx.clone();
            tokio::spawn(async move {
                let mut stream = service_receiver.start();
                while let Some(msg) = stream.next().await {
                    match msg {
                        restate_core::network::ServiceMessage::Unary(msg)
                            if msg.msg_type() == ControlProcessors::TYPE =>
                        {
                            let _ = tx.send((peer_node_id, msg.into_typed::<ControlProcessors>()));
                        }
                        msg => {
                            msg.fail(restate_core::network::Verdict::MessageUnrecognized);
                        }
                    }
                }
            });
        };

        let (connector, _new_connections) = MockConnector::new(router_factory.clone());
        let mut builder = TestCoreEnvBuilder::with_transport_connector(connector);
        // also pass my own control messages to the same service handler
        router_factory(GenerationalNodeId::new(1, 1), &mut builder.router_builder);

        let mut partition_table_builder =
            PartitionTable::with_equally_sized_partitions(Version::MIN, num_partitions)
                .into_builder();
        partition_table_builder.set_partition_replication(partition_replication.clone());

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
        let mut control_recv = std::pin::pin!(UnboundedReceiverStream::new(control_recv));

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
            matches_partition_table(&target_partition_table, &observed_cluster_state)?;

            let alive_nodes: NodeSet = cluster_state
                .alive_nodes()
                .map(|node| node.generational_node_id.as_plain())
                .collect();

            for (_, partition) in target_partition_table.iter_mut() {
                let target_state = TargetPartitionPlacementState::new(&mut partition.placement);
                // assert that the replication strategy was respected
                match &partition_replication {
                    PartitionReplication::Everywhere => {
                        // assert that every partition has a leader which is part of the alive nodes set
                        assert!(target_state.node_set.is_subset(&alive_nodes));

                        assert!(
                            target_state
                                .leader
                                .is_some_and(|leader| alive_nodes.contains(leader))
                        );
                    }
                    PartitionReplication::Limit(replication_property) => {
                        // assert that every partition has a leader which is part of the alive nodes set
                        assert!(
                            target_state
                                .leader
                                .is_some_and(|leader| alive_nodes.contains(leader))
                        );

                        assert_eq!(
                            target_state.node_set.len(),
                            alive_nodes
                                .len()
                                .min(usize::from(replication_property.num_copies()))
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
            PartitionReplication::Limit(ReplicationProperty::new(num_partition_processors)),
        )
        .await?;
        let partition = partition_table
            .get(&PartitionId::from(0))
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
            PartitionReplication::Limit(ReplicationProperty::new(num_partition_processors)),
        )
        .await?;
        let partition = partition_table
            .get(&PartitionId::from(0))
            .expect("must be present");

        assert_eq!(
            partition.placement.len(),
            num_partition_processors.get() as usize
        );

        Ok(())
    }

    async fn run_ensure_replication_test(
        mut partition_table_builder: PartitionTableBuilder,
        partition_replication: PartitionReplication,
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
                &partition_replication,
                &nodes_config,
                &NoPlacementHints,
            );
        });

        Ok(partition_table_builder.build())
    }

    fn matches_partition_table(
        partition_table: &PartitionTable,
        observed_state: &ObservedClusterState,
    ) -> googletest::Result<()> {
        assert_that!(observed_state.partitions.len(), eq(partition_table.len()));

        for (partition_id, partition) in partition_table.iter() {
            let Some(observed_state) = observed_state.partitions.get(partition_id) else {
                panic!("partition {partition_id} not found in observed state");
            };
            assert_that!(
                observed_state.partition_processors.len(),
                eq(partition.placement.len())
            );

            for (position, node_id) in partition.placement.iter().with_position() {
                let run_mode = if matches!(
                    position,
                    itertools::Position::First | itertools::Position::Only
                ) {
                    RunMode::Leader
                } else {
                    RunMode::Follower
                };
                assert_that!(
                    observed_state.partition_processors.get(node_id),
                    eq(Some(&run_mode))
                );
            }
        }
        Ok(())
    }

    fn derive_observed_cluster_state(
        cluster_state: &ClusterState,
        control_messages: Vec<(GenerationalNodeId, Incoming<Unary<ControlProcessors>>)>,
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
        let mut rng = rand::rng();
        let mut has_alive_node = false;

        for node_id in node_ids {
            let node_state = if rng.random_bool(0.66) {
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
            let idx = rng.random_range(0..node_ids.len());
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
            uptime: Duration::default(),
        }
    }

    fn random_partition_status(
        rng: &mut ThreadRng,
        num_partitions: u16,
    ) -> BTreeMap<PartitionId, PartitionProcessorStatus> {
        let mut result = BTreeMap::default();

        for idx in 0..num_partitions {
            if rng.random_bool(0.5) {
                let mut status = PartitionProcessorStatus::new();

                if rng.random_bool(0.5) {
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
