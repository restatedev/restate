// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ahash::HashMap;
use assert2::let_assert;
use std::collections::BTreeMap;
use std::collections::hash_map::Entry;
use std::iter;
use std::time::Duration;
use tracing::{Level, debug, enabled, info, instrument, trace};

use crate::cluster_controller::logs_controller;
use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
use restate_core::metadata_store::{ReadError, ReadModifyWriteError, ReadWriteError, WriteError};
use restate_core::network::{NetworkSender as _, Networking, Swimlane, TransportConnect};
use restate_core::{Metadata, MetadataWriter, ShutdownError, SyncError, TaskCenter, TaskKind};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_types::cluster::cluster_state::RunMode;
use restate_types::epoch::{EpochMetadata, PartitionProcessorConfiguration};
use restate_types::identifiers::PartitionId;
use restate_types::logs::LogId;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::nodes_config::{NodesConfiguration, Role};
use restate_types::partition_table::{PartitionPlacement, PartitionReplication, PartitionTable};
use restate_types::replication::{
    NodeSet, NodeSetSelector, NodeSetSelectorOptions, ReplicationProperty,
};
use restate_types::{NodeId, PlainNodeId, Version, Versioned};

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

struct PartitionConfiguration {
    leader: Option<PlainNodeId>,
    current: PartitionProcessorConfiguration,
    next: Option<PartitionProcessorConfiguration>,
}

impl PartitionConfiguration {
    fn new(
        current: PartitionProcessorConfiguration,
        next: Option<PartitionProcessorConfiguration>,
    ) -> Self {
        Self {
            leader: None,
            current,
            next,
        }
    }

    fn merge_current(&mut self, current: PartitionProcessorConfiguration) {
        if self.current.version() < current.version() {
            self.current = current;
        }
    }

    fn merge_next(&mut self, next: PartitionProcessorConfiguration) {
        if self
            .next
            .as_ref()
            .is_none_or(|config| config.version() < next.version())
        {
            self.next = Some(next);
        }
    }

    fn replicas(&self) -> impl Iterator<Item = &PlainNodeId> {
        self.current.replica_set.iter().chain(
            self.next
                .as_ref()
                .map(|config| itertools::Either::Left(config.replica_set.iter()))
                .unwrap_or(itertools::Either::Right(iter::empty())),
        )
    }

    fn generate_instructions(
        &self,
        partition_id: &PartitionId,
        observed_cluster_state: &ObservedClusterState,
        commands: &mut BTreeMap<PlainNodeId, Vec<ControlProcessor>>,
    ) {
        // todo: Avoid cloning of node_set if this becomes measurable
        let mut observed_state = observed_cluster_state
            .partitions
            .get(partition_id)
            .map(|state| state.partition_processors.clone())
            .unwrap_or_default();

        if let Some(leader) = &self.leader {
            if !observed_state
                .remove(leader)
                .is_some_and(|observed_run_mode| observed_run_mode == RunMode::Leader)
            {
                commands.entry(*leader).or_default().push(ControlProcessor {
                    partition_id: *partition_id,
                    command: ProcessorCommand::from(RunMode::Leader),
                });
            }
        }

        for node_id in self.replicas() {
            let run_mode = if self.leader == Some(*node_id) {
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

struct ConcurrentPartitionProcessorConfigurationUpdate {
    current: PartitionProcessorConfiguration,
    next: Option<PartitionProcessorConfiguration>,
}

pub struct Scheduler<T> {
    metadata_writer: MetadataWriter,
    networking: Networking<T>,
    partitions: HashMap<PartitionId, PartitionConfiguration>,
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
            partitions: HashMap::default(),
        }
    }

    pub fn update_partition_configuration(
        &mut self,
        partition_id: PartitionId,
        current: PartitionProcessorConfiguration,
        next: Option<PartitionProcessorConfiguration>,
    ) {
        match self.partitions.entry(partition_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().merge_current(current);
                if let Some(next) = next {
                    entry.get_mut().merge_next(next);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(PartitionConfiguration::new(current, next));
            }
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

    async fn update_partition_placement(
        &mut self,
        alive_workers: &NodeSet,
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

        for partition_id in partition_table.partition_ids() {
            // todo a bulk get of all EpochMetadata if self.partitions.is_empty()
            let entry = self.partitions.entry(*partition_id);

            // make sure that we have a valid partition processor configuration
            if !matches!(&entry, Entry::Occupied(entry) if entry.get().current.is_valid()) {
                let preferred_nodes: NodeSet = placement_hints
                    .preferred_nodes(partition_id)
                    .cloned()
                    .collect();
                let_assert!(
                    PartitionReplication::Limit(partition_replication) =
                        partition_table.partition_replication(),
                    "Limit should be the only used partition replication type"
                );

                // no or no valid current configuration, pick a valid configuration
                if let Some(current) = Self::choose_partition_processor_configuration(
                    nodes_config,
                    partition_replication.clone(),
                    alive_workers,
                    placement_hints.preferred_leader(partition_id),
                    &preferred_nodes,
                ) {
                    match self
                        .metadata_writer
                        .raw_metadata_store_client()
                        .read_modify_write(
                            partition_processor_epoch_key(*partition_id),
                            |epoch_metadata: Option<EpochMetadata>| {
                                if let Some(epoch_metadata) = epoch_metadata {
                                    // check if current has been modified in the meantime
                                    if epoch_metadata.current().version() < current.version() {
                                        Ok(epoch_metadata
                                            .update_current_configuration(current.clone()))
                                    } else {
                                        let (_, current, next) = epoch_metadata.into_inner();
                                        Err(ConcurrentPartitionProcessorConfigurationUpdate {
                                            current,
                                            next,
                                        })
                                    }
                                } else {
                                    Ok(EpochMetadata::new(current.clone(), None))
                                }
                            },
                        )
                        .await
                    {
                        Ok(_) => {
                            entry.insert_entry(PartitionConfiguration::new(current, None));
                        }
                        Err(ReadModifyWriteError::FailedOperation(concurrent_update)) => {
                            entry.insert_entry(PartitionConfiguration::new(
                                concurrent_update.current,
                                concurrent_update.next,
                            ));
                        }
                        Err(ReadModifyWriteError::ReadWrite(err)) => {
                            return Err(err.into());
                        }
                    }
                } else {
                    // no valid configuration, skip
                    continue;
                }
            }

            // todo handle automatic reconfiguration

            // select the leader based on the observed cluster state
            self.select_leader(
                partition_id,
                alive_workers,
                placement_hints.preferred_leader(partition_id),
            );
        }

        let mut builder = partition_table.clone().into_builder();
        builder.for_each(|partition_id, placement| {
            self.update_placement(partition_id, placement);
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

    fn choose_partition_processor_configuration(
        nodes_config: &NodesConfiguration,
        partition_replication: ReplicationProperty,
        alive_workers: &NodeSet,
        preferred_leader: Option<PlainNodeId>,
        preferred_nodes: &NodeSet,
    ) -> Option<PartitionProcessorConfiguration> {
        let options = if let Some(preferred_leader) = preferred_leader {
            NodeSetSelectorOptions::default().with_top_priority_node(preferred_leader)
        } else {
            NodeSetSelectorOptions::default()
        }
        .with_preferred_nodes(preferred_nodes);

        // todo make nodeset selector select minimal sets ({zone: 2, node: 5} returning a nodeset of 5 nodes spread across 2 zones)
        NodeSetSelector::select(
            nodes_config,
            &partition_replication,
            |_node_id, node_config| node_config.has_role(Role::Worker),
            |node_id, _node_config| alive_workers.contains(node_id),
            options,
        )
        .map(|nodeset| {
            PartitionProcessorConfiguration::new(partition_replication, nodeset, HashMap::default())
        })
        .inspect_err(|err| debug!("Failed to select nodeset for partition processor: {}", err))
        .ok()
    }

    fn select_leader(
        &mut self,
        partition_id: &PartitionId,
        alive_workers: &NodeSet,
        preferred_leader: Option<PlainNodeId>,
    ) {
        if let Some(partition) = self.partitions.get_mut(partition_id) {
            // try to select the preferred leader first if it's still alive
            if let Some(preferred_leader) = preferred_leader {
                if alive_workers.contains(preferred_leader)
                    && partition.current.replica_set.contains(preferred_leader)
                {
                    partition.leader = Some(preferred_leader);
                    return;
                }
            }

            if let Some(alive_replica) = partition
                .current
                .replica_set
                .intersect(alive_workers)
                .next()
            {
                partition.leader = Some(alive_replica);
                return;
            }

            if let Some(alive_next_replica) = partition
                .next
                .as_ref()
                .and_then(|config| config.replica_set.intersect(alive_workers).next())
            {
                partition.leader = Some(alive_next_replica);
            }
        }
    }

    fn update_placement(&self, partition_id: &PartitionId, placement: &mut PartitionPlacement) {
        if let Some(partition) = self.partitions.get(partition_id) {
            // a bit wasteful to create new nodesets over and over again if nothing changes; but
            // it's hopefully not for too long
            *placement = partition.replicas().cloned().collect();

            if let Some(leader) = partition.leader {
                placement.set_leader(leader);
            }
        }
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

    fn instruct_nodes(&self, observed_cluster_state: &ObservedClusterState) -> Result<(), Error> {
        let mut commands = BTreeMap::default();

        for (partition_id, partition) in &self.partitions {
            partition.generate_instructions(partition_id, observed_cluster_state, &mut commands);
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
}

/// Placement hints for the [`logs_controller::LogsController`] based on the current
/// state of the scheduler.
pub struct SchedulerNodeSetSelectorHints<'a, T> {
    scheduler: &'a Scheduler<T>,
}

impl<'a, T> SchedulerNodeSetSelectorHints<'a, T> {
    pub fn new(scheduler: &'a Scheduler<T>) -> Self {
        Self { scheduler }
    }
}

impl<T> logs_controller::NodeSetSelectorHints for SchedulerNodeSetSelectorHints<'_, T> {
    fn preferred_sequencer(&self, log_id: &LogId) -> Option<NodeId> {
        let partition_id = PartitionId::from(*log_id);

        self.scheduler
            .partitions
            .get(&partition_id)
            .and_then(|partition| partition.leader.map(NodeId::from))
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
    use restate_types::locality::NodeLocation;
    use restate_types::metadata_store::keys::PARTITION_TABLE_KEY;
    use restate_types::net::partition_processor_manager::{
        ControlProcessors, PartitionManagerService, ProcessorCommand,
    };
    use restate_types::net::{AdvertisedAddress, UnaryMessage};
    use restate_types::nodes_config::{
        LogServerConfig, MetadataServerConfig, NodeConfig, NodesConfiguration, Role,
    };
    use restate_types::partition_table::{PartitionReplication, PartitionTable};
    use restate_types::replication::NodeSet;
    use restate_types::replication::ReplicationProperty;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};

    use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
    use crate::cluster_controller::scheduler::{PartitionProcessorPlacementHints, Scheduler};

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
            let node_config = NodeConfig::new(
                format!("{node_id}"),
                *node_id,
                NodeLocation::default(),
                AdvertisedAddress::Http(
                    format!("http://localhost-{}:5122", node_id.id())
                        .parse()
                        .unwrap(),
                ),
                Role::Worker.into(),
                LogServerConfig::default(),
                MetadataServerConfig::default(),
            );
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

            for (_, partition) in target_partition_table.partitions_mut() {
                // assert that the replication strategy was respected
                match &partition_replication {
                    PartitionReplication::Everywhere => {
                        // assert that every partition has a leader which is part of the alive nodes set
                        assert!(partition.placement.is_subset(&alive_nodes));

                        assert!(
                            partition
                                .placement
                                .leader()
                                .is_some_and(|leader| alive_nodes.contains(leader))
                        );
                    }
                    PartitionReplication::Limit(replication_property) => {
                        // assert that every partition has a leader which is part of the alive nodes set
                        assert!(
                            partition
                                .placement
                                .leader()
                                .is_some_and(|leader| alive_nodes.contains(leader))
                        );

                        assert_eq!(
                            partition.placement.len(),
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

    fn matches_partition_table(
        partition_table: &PartitionTable,
        observed_state: &ObservedClusterState,
    ) -> googletest::Result<()> {
        assert_that!(
            observed_state.partitions.len(),
            eq(partition_table.num_partitions() as usize)
        );

        for (partition_id, partition) in partition_table.partitions() {
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
}
