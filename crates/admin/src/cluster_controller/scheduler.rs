// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::hash_map::Entry;
use std::iter;
use std::time::Duration;

use ahash::HashMap;
use assert2::let_assert;
use tracing::{Level, debug, enabled, info, instrument, trace};

use crate::cluster_controller::logs_controller;
use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
use restate_core::network::{NetworkSender as _, Networking, Swimlane, TransportConnect};
use restate_core::{Metadata, MetadataWriter, ShutdownError, SyncError, TaskCenter, TaskKind};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_metadata_store::{
    MetadataStoreClient, ReadError, ReadModifyWriteError, ReadWriteError, WriteError,
};
use restate_types::cluster::cluster_state::RunMode;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::logs::LogId;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::{PartitionPlacement, PartitionReplication, PartitionTable};
use restate_types::partitions::{PartitionConfiguration, worker_candidate_filter};
use restate_types::replication::balanced_spread_selector::{
    BalancedSpreadSelector, SelectorOptions,
};
use restate_types::replication::{NodeSet, ReplicationProperty};
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

#[derive(Debug, Clone)]
struct PartitionState {
    leader: Option<PlainNodeId>,
    current: PartitionConfiguration,
    next: Option<PartitionConfiguration>,
}

impl PartitionState {
    fn new(current: PartitionConfiguration, next: Option<PartitionConfiguration>) -> Self {
        Self {
            leader: None,
            current,
            next,
        }
    }

    fn update_configuration(
        &mut self,
        current: PartitionConfiguration,
        next: Option<PartitionConfiguration>,
    ) {
        if self.current.version() < current.version() {
            self.current = current;
        }

        if let Some(next) = next {
            if self
                .next
                .as_ref()
                .is_none_or(|my_next| my_next.version() < next.version())
            {
                self.next = Some(next);
            }
        }

        if self
            .next
            .as_ref()
            .is_some_and(|next| next.version() <= self.current.version())
        {
            self.next = None;
        }
    }

    fn replicas(&self) -> impl Iterator<Item = &PlainNodeId> {
        self.current.replica_set().iter().chain(
            self.next
                .as_ref()
                .map(|config| itertools::Either::Left(config.replica_set().iter()))
                .unwrap_or(itertools::Either::Right(iter::empty())),
        )
    }

    fn contains_replica_current(&self, node_id: PlainNodeId) -> bool {
        self.current.replica_set().contains(node_id)
    }

    fn generate_instructions(
        &self,
        partition_id: &PartitionId,
        observed_cluster_state: &ObservedClusterState,
        commands: &mut BTreeMap<PlainNodeId, Vec<ControlProcessor>>,
    ) {
        // todo: Avoid cloning of node_set if this becomes measurable in flame graphs
        let mut observed_state = observed_cluster_state
            .partition_state(partition_id)
            .map(|state| state.partition_processors.clone())
            .unwrap_or_default();

        if let Some(leader) = &self.leader {
            if !observed_state
                .remove(leader)
                .is_some_and(|state| state.run_mode == RunMode::Leader)
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
                .is_some_and(|state| state.run_mode == run_mode)
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

struct PartitionConfigurationUpdate {
    current: PartitionConfiguration,
    next: Option<PartitionConfiguration>,
}

pub struct Scheduler<T> {
    metadata_writer: MetadataWriter,
    networking: Networking<T>,
    partitions: HashMap<PartitionId, PartitionState>,
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
        current: PartitionConfiguration,
        next: Option<PartitionConfiguration>,
    ) {
        match self.partitions.entry(partition_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().update_configuration(current, next);
            }
            Entry::Vacant(entry) => {
                entry.insert(PartitionState::new(current, next));
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

        self.ensure_valid_partition_configuration(
            observed_cluster_state,
            nodes_config,
            placement_hints,
        )
        .await?;

        // todo move draining workers to disabled if they no longer run any partition processors;
        //  since the worker state is stored in the NodesConfiguration and the replica sets are
        //  stored in the EpochMetadata we cannot guarantee linearizability. Hence, when setting a
        //  worker to draining it might still be added to replica sets by cluster controllers until
        //  they learn about the updated nodes configuration. To reduce the risk of this, we should
        //  wait a little bit to give the nodes configuration time to be spread across the cluster.

        self.instruct_nodes(observed_cluster_state)?;

        Ok(())
    }

    async fn ensure_valid_partition_configuration(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        nodes_config: &NodesConfiguration,
        placement_hints: impl PartitionProcessorPlacementHints,
    ) -> Result<(), Error> {
        let logs = Metadata::with_current(|m| m.logs_ref());
        let partition_table = Metadata::with_current(|m| m.partition_table_ref());

        if logs.num_logs() != partition_table.len() {
            // either the partition table or the logs are not fully initialized, hence there is
            // nothing we can do atm. we need to wait until both partitions and logs are created
            return Ok(());
        }
        let version = partition_table.version();

        // todo a bulk get of all EpochMetadata if self.partitions.is_empty()

        for partition_id in partition_table.iter_ids() {
            let entry = self.partitions.entry(*partition_id);

            // make sure that we have a valid partition processor configuration
            let mut occupied_entry = match entry {
                Entry::Occupied(mut entry) if entry.get().current.is_valid() => {
                    if Self::requires_reconfiguration(entry.get(), nodes_config) {
                        trace!("Partition {} requires reconfiguration", partition_id);

                        let_assert!(
                            PartitionReplication::Limit(partition_replication) =
                                partition_table.replication(),
                            "Limit should be the only used partition replication type"
                        );

                        // select all valid worker candidates as the preferred nodes for the next
                        // configuration
                        let preferred_nodes = entry
                            .get()
                            .replicas()
                            .filter(|replica| {
                                // only keep alive nodes in the preferred nodes set to allow moving
                                // slowly to a more evenly spread replica set if nodes are currently
                                // dead.
                                observed_cluster_state.alive_generation(**replica).is_some()
                            })
                            .copied()
                            .collect();

                        if let Some(next) = Self::choose_partition_configuration(
                            *partition_id,
                            nodes_config,
                            partition_replication.clone(),
                            preferred_nodes,
                        ) {
                            *entry.get_mut() = Self::reconfigure_partition_configuration(
                                self.metadata_writer.raw_metadata_store_client(),
                                *partition_id,
                                entry
                                    .get()
                                    .next
                                    .as_ref()
                                    .map(|next| next.version())
                                    .unwrap_or_else(|| entry.get().current.version()),
                                next,
                            )
                            .await?;
                        }
                    }

                    entry
                }
                entry => {
                    let preferred_nodes: NodeSet = placement_hints
                        .preferred_nodes(partition_id)
                        .cloned()
                        .chain(placement_hints.preferred_leader(partition_id))
                        .collect();
                    let_assert!(
                        PartitionReplication::Limit(partition_replication) =
                            partition_table.replication(),
                        "Limit should be the only used partition replication type"
                    );

                    // no or no valid current configuration, pick a valid configuration
                    if let Some(current) = Self::choose_partition_configuration(
                        *partition_id,
                        nodes_config,
                        partition_replication.clone(),
                        preferred_nodes,
                    ) {
                        entry.insert_entry(
                            Self::store_initial_partition_configuration(
                                self.metadata_writer.raw_metadata_store_client(),
                                *partition_id,
                                current,
                            )
                            .await?,
                        )
                    } else {
                        // no valid configuration, skip
                        continue;
                    }
                }
            };

            // check whether we can transition from the current configuration to the next
            // configuration, which is possible as soon as a single partition processor from the
            // next configuration has become active
            if let Some(next) = &occupied_entry.get().next {
                if next.replica_set().iter().any(|node_id| {
                    observed_cluster_state.is_partition_processor_active(partition_id, node_id)
                }) {
                    let partition_configuration_update = Self::complete_reconfiguration(
                        self.metadata_writer.raw_metadata_store_client(),
                        *partition_id,
                        occupied_entry.get().current.version(),
                        next.version(),
                    )
                    .await?;
                    occupied_entry.get_mut().update_configuration(
                        partition_configuration_update.current,
                        partition_configuration_update.next,
                    );
                }
            }

            // select the leader based on the observed cluster state
            self.select_leader(
                partition_id,
                observed_cluster_state,
                placement_hints.preferred_leader(partition_id),
            );
        }

        // update the PartitionTable placement which is still needed for routing messages from the
        // ingress and datafusion
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

    async fn store_initial_partition_configuration(
        metadata_store_client: &MetadataStoreClient,
        partition_id: PartitionId,
        current: PartitionConfiguration,
    ) -> Result<PartitionState, Error> {
        match metadata_store_client
            .read_modify_write(
                partition_processor_epoch_key(partition_id),
                |epoch_metadata: Option<EpochMetadata>| {
                    if let Some(epoch_metadata) = epoch_metadata {
                        // check if current has been modified in the meantime
                        if epoch_metadata.current().version() < current.version() {
                            Ok(epoch_metadata.update_current_configuration(current.clone()))
                        } else {
                            let (_, _, current, next) = epoch_metadata.into_inner();
                            Err(PartitionConfigurationUpdate { current, next })
                        }
                    } else {
                        Ok(EpochMetadata::new(current.clone(), None))
                    }
                },
            )
            .await
        {
            Ok(_) => {
                debug!("Initialized partition {} with {:?}", partition_id, current);
                Ok(PartitionState::new(current, None))
            }
            Err(ReadModifyWriteError::FailedOperation(concurrent_update)) => Ok(
                PartitionState::new(concurrent_update.current, concurrent_update.next),
            ),
            Err(ReadModifyWriteError::ReadWrite(err)) => Err(err.into()),
        }
    }

    async fn reconfigure_partition_configuration(
        metadata_store_client: &MetadataStoreClient,
        partition_id: PartitionId,
        expected_next_version: Version,
        next: PartitionConfiguration,
    ) -> Result<PartitionState, Error> {
        match metadata_store_client
            .read_modify_write(
                partition_processor_epoch_key(partition_id),
                |epoch_metadata: Option<EpochMetadata>| {
                    if let Some(epoch_metadata) = epoch_metadata {
                        // Check if next has been modified in the meantime. If next is not present,
                        // then check whether current contains a larger version than the expected next
                        // version because we might have completed a reconfiguration in the meantime.
                        if epoch_metadata
                            .next()
                            .map(|next| next.version())
                            .unwrap_or(epoch_metadata.current().version())
                            <= expected_next_version
                        {
                            Ok(epoch_metadata.reconfigure(next.clone()))
                        } else {
                            let (_, _, current, next) = epoch_metadata.into_inner();
                            Err(PartitionConfigurationUpdate { current, next })
                        }
                    } else {
                        // missing epoch metadata so we set next to be current right away
                        Ok(EpochMetadata::new(next.clone(), None))
                    }
                },
            )
            .await
        {
            Ok(epoch_metadata) => {
                debug!("Reconfigured partition {} to {:?}", partition_id, next);
                let (_, _, current, next) = epoch_metadata.into_inner();
                Ok(PartitionState::new(current, next))
            }
            Err(ReadModifyWriteError::FailedOperation(concurrent_update)) => Ok(
                PartitionState::new(concurrent_update.current, concurrent_update.next),
            ),
            Err(ReadModifyWriteError::ReadWrite(err)) => Err(err.into()),
        }
    }

    async fn complete_reconfiguration(
        metadata_store_client: &MetadataStoreClient,
        partition_id: PartitionId,
        current_version: Version,
        next_version: Version,
    ) -> Result<PartitionConfigurationUpdate, Error> {
        match metadata_store_client.read_modify_write(partition_processor_epoch_key(partition_id), |epoch_metadata: Option<EpochMetadata>| {
            match epoch_metadata {
                None => panic!("Did not find epoch metadata which should be present. This indicates a corruption of the metadata store."),
                Some(epoch_metadata) => {
                    let Some(next_version) = epoch_metadata.next().map(|config| config.version()) else {
                        // if there is no next configuration, then a concurrent modification has happened
                        let (_, _, current, next) = epoch_metadata.into_inner();
                        return Err(PartitionConfigurationUpdate {
                            current,
                            next,
                        });
                    };

                    match next_version.cmp(&next_version) {
                        Ordering::Less => unreachable!("we should not know about a newer next configuration than the metadata store"),
                        Ordering::Equal => Ok(epoch_metadata.complete_reconfiguration()),
                        Ordering::Greater => {
                            let (_, _, current, next) = epoch_metadata.into_inner();
                            Err(PartitionConfigurationUpdate {
                                current,
                                next,
                            })
                        }
                    }
                }
            }
        }).await {
            Ok(epoch_metadata) => {
                info!("Successfully transitioned from partition processor configuration {} to {}", current_version, next_version);
                let (_, _, current, next) = epoch_metadata.into_inner();
                Ok(PartitionConfigurationUpdate {
                    current,
                    next,
                })
            }
            Err(ReadModifyWriteError::FailedOperation(concurrent_update)) => {
                Ok(concurrent_update)
            }
            Err(ReadModifyWriteError::ReadWrite(err)) => {
                Err(err.into())
            }
        }
    }

    /// Checks whether the given partition requires reconfiguration. A partition requires
    /// reconfiguration in the following cases:
    ///
    /// * next contains a replica that is no longer a valid worker candidate.
    /// * current contains a replica that is no longer a valid worker candidate, and there is no
    ///   ongoing reconfiguration happening already
    ///
    /// In this case, the method returns true, otherwise false.
    fn requires_reconfiguration(
        partition_state: &PartitionState,
        nodes_config: &NodesConfiguration,
    ) -> bool {
        let current_requires_reconfiguration =
            partition_state.current.replica_set().iter().any(|replica| {
                !nodes_config
                    .find_node_by_id(*replica)
                    .map(|node_config| worker_candidate_filter(*replica, node_config))
                    .unwrap_or_default()
            });

        let next_requires_reconfiguration = partition_state.next.as_ref().map(|next| {
            next.replica_set().iter().any(|replica| {
                !nodes_config
                    .find_node_by_id(*replica)
                    .map(|node_config| worker_candidate_filter(*replica, node_config))
                    .unwrap_or_default()
            })
        });

        let ongoing_reconfiguration = next_requires_reconfiguration.is_some();
        let next_requires_reconfiguration = next_requires_reconfiguration.unwrap_or(false);

        (current_requires_reconfiguration && !ongoing_reconfiguration)
            || next_requires_reconfiguration
    }

    fn choose_partition_configuration(
        partition_id: PartitionId,
        nodes_config: &NodesConfiguration,
        partition_replication: ReplicationProperty,
        preferred_nodes: NodeSet,
    ) -> Option<PartitionConfiguration> {
        let options =
            SelectorOptions::new(u64::from(partition_id)).with_preferred_nodes(preferred_nodes);

        BalancedSpreadSelector::select(
            nodes_config,
            &partition_replication,
            worker_candidate_filter,
            &options,
        )
        .map(|replica_set| {
            PartitionConfiguration::new(partition_replication, replica_set, HashMap::default())
        })
        .inspect_err(|err| {
            debug!(
                "Failed to select replica set for partition {partition_id}: {}",
                err
            )
        })
        .ok()
    }

    fn select_leader(
        &mut self,
        partition_id: &PartitionId,
        observed_cluster_state: &ObservedClusterState,
        preferred_leader: Option<PlainNodeId>,
    ) {
        let Some(partition) = self.partitions.get_mut(partition_id) else {
            return;
        };

        // try to select the preferred leader first if it's still alive and part of current
        if let Some(preferred_leader) = preferred_leader {
            if observed_cluster_state
                .alive_generation(preferred_leader)
                .is_some()
                && partition.contains_replica_current(preferred_leader)
            {
                partition.leader = Some(preferred_leader);
                return;
            }
        }

        // pick the alive node currently running as leader if it is a replica in current
        if let Some(leader) = observed_cluster_state
            .partition_state(partition_id)
            .and_then(|partition_state| {
                partition_state
                    .partition_processors
                    .iter()
                    .find(|(node_id, state)| {
                        state.run_mode == RunMode::Leader
                            && partition.contains_replica_current(**node_id)
                    })
                    .map(|(node_id, _)| *node_id)
            })
        {
            assert!(
                observed_cluster_state.alive_generation(leader).is_some(),
                "only alive nodes should run the leader"
            );
            partition.leader = Some(leader);
            return;
        }

        // no leader is currently running in current; pick from the alive nodes in the current configuration
        if let Some(alive_replica) = partition
            .current
            .replica_set()
            .iter()
            .find(|node_id| observed_cluster_state.alive_generation(**node_id).is_some())
        {
            partition.leader = Some(*alive_replica);
            return;
        }

        // couldn't find a leader because no node in current was alive
        partition.leader = None;
    }

    fn update_placement(&self, partition_id: &PartitionId, placement: &mut PartitionPlacement) {
        if let Some(partition) = self.partitions.get(partition_id) {
            if let Some(leader) = partition.leader {
                // a bit wasteful to create new nodesets over and over again if nothing changes; but
                // it's hopefully not for too long
                *placement = partition.replicas().cloned().collect();
                placement.set_leader(leader);
            } else {
                placement.clear();
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
                            let _ = permit.send_unary(control_processors, None);

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
        AliveNode, ClusterState, DeadNode, NodeState, PartitionProcessorStatus, ReplayStatus,
        RunMode,
    };
    use restate_types::identifiers::PartitionId;
    use restate_types::metadata_store::keys::PARTITION_TABLE_KEY;
    use restate_types::net::partition_processor_manager::{
        ControlProcessors, PartitionManagerService, ProcessorCommand,
    };
    use restate_types::net::{AdvertisedAddress, UnaryMessage};
    use restate_types::nodes_config::{
        NodeConfig, NodesConfiguration, Role, WorkerConfig, WorkerState,
    };
    use restate_types::partition_table::{PartitionReplication, PartitionTable};
    use restate_types::replication::NodeSet;
    use restate_types::replication::ReplicationProperty;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, PlainNodeId, Version};

    use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
    use crate::cluster_controller::observed_cluster_state::tests::matches_partition_table;
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

        let mut scheduler = Scheduler::new(metadata_writer, networking);
        let observed_cluster_state = ObservedClusterState::default();

        scheduler
            .on_observed_cluster_state(
                &observed_cluster_state,
                &Metadata::with_current(|m| m.nodes_config_ref()),
                NoPlacementHints,
            )
            .await?;

        let initial_partition_table = metadata_store_client
            .get::<PartitionTable>(PARTITION_TABLE_KEY.clone())
            .await
            .expect("partition table")
            .unwrap();

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

        assert_eq!(initial_partition_table, partition_table);

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
                .worker_config(WorkerConfig {
                    worker_state: WorkerState::Active,
                })
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
            let (cluster_state, cs) = random_cluster_state(&node_ids, num_partitions);

            observed_cluster_state.update_liveness(&cs);
            observed_cluster_state.update_partitions(&cluster_state);
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
                derive_observed_cluster_state(&cluster_state, &cs, control_messages);
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
                // assert that the replication strategy was respected
                match &partition_replication {
                    PartitionReplication::Everywhere => unreachable!("everywhere was deprecated"),
                    PartitionReplication::Limit(replication_property) => {
                        if let Some(leader) = partition.placement.leader() {
                            // if a leader was chosen, then it must be in the set of alive nodes
                            assert!(alive_nodes.contains(leader));

                            // the placement should contain as many nodes as the replication property specifies
                            assert!(
                                partition.placement.len()
                                    >= usize::from(replication_property.num_copies())
                            );
                        } else {
                            assert!(partition.placement.is_empty());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn derive_observed_cluster_state(
        cluster_state: &ClusterState,
        cs: &restate_core::cluster_state::ClusterState,
        control_messages: Vec<(GenerationalNodeId, Incoming<Unary<ControlProcessors>>)>,
    ) -> ObservedClusterState {
        let mut observed_cluster_state = ObservedClusterState::default();
        observed_cluster_state.update_liveness(cs);
        observed_cluster_state.update_partitions(cluster_state);

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
                            ReplayStatus::Active,
                        );
                    }
                    ProcessorCommand::Leader => {
                        observed_cluster_state.add_node_to_partition(
                            control_processor.partition_id,
                            plain_node_id,
                            RunMode::Leader,
                            ReplayStatus::Active,
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
    ) -> (ClusterState, restate_core::cluster_state::ClusterState) {
        let nodes = random_nodes_state(node_ids, num_partitions);

        let cs = restate_core::cluster_state::ClusterState::default();
        let mut updater = cs.clone().updater();

        let mut write_guard = updater.write();

        for (node_id, state) in &nodes {
            match state {
                NodeState::Alive(alive) => {
                    write_guard.upsert_node_state(
                        alive.generational_node_id,
                        restate_core::cluster_state::NodeState::Alive,
                    );
                }
                NodeState::Dead(_) => {
                    write_guard.remove_node(*node_id);
                }
            }
        }

        (
            ClusterState {
                last_refreshed: None,
                nodes_config_version: Version::MIN,
                partition_table_version: Version::MIN,
                logs_metadata_version: Version::MIN,
                nodes,
            },
            cs,
        )
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
                let mut status = PartitionProcessorStatus {
                    replay_status: ReplayStatus::Active,
                    ..PartitionProcessorStatus::default()
                };

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
