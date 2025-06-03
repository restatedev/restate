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
use tracing::{Level, debug, enabled, info, instrument, trace};

use crate::cluster_controller::observed_cluster_state::ObservedClusterState;
use restate_core::network::{NetworkSender as _, Networking, Swimlane, TransportConnect};
use restate_core::{Metadata, MetadataWriter, ShutdownError, SyncError, TaskCenter, TaskKind};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_metadata_store::{
    MetadataStoreClient, ReadError, ReadModifyWriteError, ReadWriteError, WriteError,
};
use restate_types::cluster::cluster_state::{ReplayStatus, RunMode};
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::nodes_config::{NodeConfig, NodesConfiguration};
use restate_types::partition_table::{PartitionPlacement, PartitionReplication, PartitionTable};
use restate_types::partitions::state::{PartitionReplicaSetStates, ReplicaSetState};
use restate_types::partitions::{PartitionConfiguration, worker_candidate_filter};
use restate_types::replication::balanced_spread_selector::{
    BalancedSpreadSelector, SelectorOptions,
};
use restate_types::replication::{NodeSet, ReplicationProperty};
use restate_types::{PlainNodeId, Version, Versioned};

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

#[derive(Debug, Clone)]
struct PartitionState {
    target_leader: Option<PlainNodeId>,
    current: PartitionConfiguration,
    next: Option<PartitionConfiguration>,
}

impl PartitionState {
    fn new(current: PartitionConfiguration, next: Option<PartitionConfiguration>) -> Self {
        Self {
            target_leader: None,
            current,
            next,
        }
    }

    /// Returns true if the partition configuration was updated.
    fn update_configuration(
        &mut self,
        current: PartitionConfiguration,
        next: Option<PartitionConfiguration>,
    ) -> bool {
        let mut updated = false;

        if self.current.version() < current.version() {
            self.current = current;
            updated = true;

            if self
                .target_leader
                .is_some_and(|leader| !self.current.replica_set().contains(leader))
            {
                self.target_leader = None;
            }
        }

        if let Some(next) = next {
            if self
                .next
                .as_ref()
                .is_none_or(|my_next| my_next.version() < next.version())
            {
                self.next = Some(next);
                updated = true;
            }
        }

        if self
            .next
            .as_ref()
            .is_some_and(|next| next.version() <= self.current.version())
        {
            self.next = None;
            updated = true;
        }

        updated
    }

    fn replicas(&self) -> impl Iterator<Item = &PlainNodeId> {
        self.current.replica_set().iter().chain(
            self.next
                .as_ref()
                .map(|config| itertools::Either::Left(config.replica_set().iter()))
                .unwrap_or(itertools::Either::Right(iter::empty())),
        )
    }

    fn generate_instructions(
        &self,
        partition_id: &PartitionId,
        observed_cluster_state: &ObservedClusterState,
        commands: &mut BTreeMap<PlainNodeId, Vec<ControlProcessor>>,
    ) {
        if let Some(leader) = &self.target_leader {
            if !observed_cluster_state
                .partition_state(partition_id)
                .and_then(|state| state.partition_processors.get(leader))
                .is_some_and(|state| state.run_mode == RunMode::Leader)
            {
                commands.entry(*leader).or_default().push(ControlProcessor {
                    partition_id: *partition_id,
                    command: ProcessorCommand::Leader,
                    current_version: self.current.version(),
                });
            }
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
    replica_set_states: PartitionReplicaSetStates,
}

/// The scheduler is responsible for assigning partition processors to nodes and to electing
/// leaders. It achieves it by deciding on a partition placement which is persisted in the partition table
/// and then driving the observed cluster state to the target state (represented by the
/// partition table).
impl<T: TransportConnect> Scheduler<T> {
    pub fn new(
        metadata_writer: MetadataWriter,
        networking: Networking<T>,
        replica_set_states: PartitionReplicaSetStates,
    ) -> Self {
        Self {
            metadata_writer,
            networking,
            partitions: HashMap::default(),
            replica_set_states,
        }
    }

    pub fn update_partition_configuration(
        &mut self,
        partition_id: PartitionId,
        current: PartitionConfiguration,
        next: Option<PartitionConfiguration>,
    ) {
        let (updated, occupied_entry) = match self.partitions.entry(partition_id) {
            Entry::Occupied(mut entry) => {
                (entry.get_mut().update_configuration(current, next), entry)
            }
            Entry::Vacant(entry) => (true, entry.insert_entry(PartitionState::new(current, next))),
        };

        if updated {
            Self::note_observed_membership_update(
                partition_id,
                occupied_entry.get(),
                &self.replica_set_states,
            );
        }
    }

    fn note_observed_membership_update(
        partition_id: PartitionId,
        partition_state: &PartitionState,
        replica_set_states: &PartitionReplicaSetStates,
    ) {
        let current_membership =
            ReplicaSetState::from_partition_configuration(&partition_state.current);
        let next_membership = partition_state
            .next
            .as_ref()
            .map(ReplicaSetState::from_partition_configuration);
        // NOTE: We don't update the leadership state here because we cannot be confident that
        // the leadership epoch has been acquired or not. The leadership state will only be
        // updated when either the actual leader or any of the followers has observed the
        // leader epoch as being the winner of the elections.
        replica_set_states.note_observed_membership(
            partition_id,
            Default::default(),
            &current_membership,
            &next_membership,
        );
    }

    pub async fn on_observed_cluster_state(
        &mut self,
        observed_cluster_state: &ObservedClusterState,
        nodes_config: &NodesConfiguration,
    ) -> Result<(), Error> {
        trace!(?observed_cluster_state, "On observed cluster state");

        self.ensure_valid_partition_configuration(observed_cluster_state, nodes_config)
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
    ) -> Result<(), Error> {
        let partition_table = Metadata::with_current(|m| m.partition_table_ref());

        let version = partition_table.version();

        // todo a bulk get of all EpochMetadata if self.partitions.is_empty()

        for partition_id in partition_table.iter_ids().copied() {
            let entry = self.partitions.entry(partition_id);

            // make sure that we have a valid partition processor configuration
            let mut occupied_entry = match entry {
                Entry::Occupied(mut entry) if entry.get().current.is_valid() => {
                    let partition_replication = Self::partition_replication_to_replication_property(
                        nodes_config,
                        &partition_table,
                    );
                    if Self::requires_reconfiguration(
                        partition_id,
                        entry.get(),
                        &partition_replication,
                        nodes_config,
                    ) {
                        trace!("Partition {} requires reconfiguration", partition_id);

                        if let Some(next) = Self::choose_partition_configuration(
                            partition_id,
                            nodes_config,
                            partition_replication,
                            NodeSet::new(),
                        ) {
                            *entry.get_mut() = Self::reconfigure_partition_configuration(
                                self.metadata_writer.raw_metadata_store_client(),
                                partition_id,
                                entry
                                    .get()
                                    .next
                                    .as_ref()
                                    .map(|next| next.version())
                                    .unwrap_or_else(|| entry.get().current.version()),
                                next,
                            )
                            .await?;
                            Self::note_observed_membership_update(
                                partition_id,
                                entry.get(),
                                &self.replica_set_states,
                            );
                        }
                    }

                    entry
                }
                entry => {
                    let partition_replication = Self::partition_replication_to_replication_property(
                        nodes_config,
                        &partition_table,
                    );

                    // no or no valid current configuration, pick a valid configuration
                    if let Some(current) = Self::choose_partition_configuration(
                        partition_id,
                        nodes_config,
                        partition_replication.clone(),
                        NodeSet::default(),
                    ) {
                        let occupied_entry = entry.insert_entry(
                            Self::store_initial_partition_configuration(
                                self.metadata_writer.raw_metadata_store_client(),
                                partition_id,
                                current,
                            )
                            .await?,
                        );
                        Self::note_observed_membership_update(
                            partition_id,
                            occupied_entry.get(),
                            &self.replica_set_states,
                        );
                        occupied_entry
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
                    observed_cluster_state.is_partition_processor_active(&partition_id, node_id)
                }) {
                    let partition_configuration_update = Self::complete_reconfiguration(
                        self.metadata_writer.raw_metadata_store_client(),
                        partition_id,
                        occupied_entry.get().current.version(),
                        next.version(),
                    )
                    .await?;
                    if occupied_entry.get_mut().update_configuration(
                        partition_configuration_update.current,
                        partition_configuration_update.next,
                    ) {
                        Self::note_observed_membership_update(
                            partition_id,
                            occupied_entry.get(),
                            &self.replica_set_states,
                        );
                    }
                }
            }

            // select the leader based on the observed cluster state
            self.select_leader(&partition_id, observed_cluster_state);
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

    fn partition_replication_to_replication_property(
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
    ) -> ReplicationProperty {
        let partition_replication = match partition_table.replication() {
            PartitionReplication::Everywhere => {
                // only kept for backwards compatibility; this can be removed once
                // we no longer need to support the Everywhere variant
                // for everywhere we pick all current worker candidates but at least 1
                let candidates = nodes_config
                    .iter()
                    .filter(|(node_id, node_config)| worker_candidate_filter(*node_id, node_config))
                    .count()
                    .max(1);
                ReplicationProperty::new_unchecked(candidates.min(usize::from(u8::MAX)) as u8)
            }
            PartitionReplication::Limit(partition_replication) => partition_replication.clone(),
        };
        partition_replication
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
                debug!(%partition_id, "Reconfigured partition to {next:?}");
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
                info!(
                    %partition_id,
                    replica_set = %epoch_metadata.current().replica_set(),
                    "Transitioned from partition configuration {current_version} to {next_version}"
                );
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
    /// * Partition replication has changed.
    /// * Possible improvement/re-balance in replica-set, this includes if a node has been dead for
    ///   some time.
    ///
    /// Note: if we take whether a node is dead or not into account, we can do great job but we
    /// need to rest our dead timers/instants when we switch from follower to leader. This is to
    /// avoid knee-jerk reaction if we are new leaders with outdated view of the world.
    ///
    /// In this case, the method returns true, otherwise false.
    fn requires_reconfiguration(
        partition_id: PartitionId,
        partition_state: &PartitionState,
        default_replication: &ReplicationProperty,
        nodes_config: &NodesConfiguration,
    ) -> bool {
        let next_requires_reconfiguration = partition_state.next.as_ref().map(|next| {
            next.replication() != default_replication ||
                // check if a different replica-set is eminent
                Self::choose_partition_configuration(
                    partition_id,
                    nodes_config,
                    default_replication.clone(),
                    NodeSet::default(),
                )
                .map(|new_config|
                    !new_config.replica_set().is_equivalent(next.replica_set()))
                .unwrap_or(false)
        });

        if next_requires_reconfiguration.is_some_and(|c| c) {
            return true;
        }

        partition_state.current.replication() != default_replication
            || Self::choose_partition_configuration(
                partition_id,
                nodes_config,
                default_replication.clone(),
                NodeSet::default(),
            )
            .map(|new_config| {
                !new_config
                    .replica_set()
                    .is_equivalent(partition_state.current.replica_set())
            })
            .unwrap_or(false)
    }

    fn choose_partition_configuration(
        partition_id: PartitionId,
        nodes_config: &NodesConfiguration,
        partition_replication: ReplicationProperty,
        preferred_nodes: NodeSet,
    ) -> Option<PartitionConfiguration> {
        let options =
            SelectorOptions::new(u64::from(partition_id)).with_preferred_nodes(preferred_nodes);
        let cluster_state = TaskCenter::with_current(|tc| tc.cluster_state().clone());
        let filter = |node_id: PlainNodeId, node_config: &NodeConfig| {
            cluster_state.is_alive(node_id.into()) && worker_candidate_filter(node_id, node_config)
        };

        BalancedSpreadSelector::select(nodes_config, &partition_replication, filter, &options)
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

    /// Selects a leader based on the current target leader, observed cluster state and preferred leader.
    ///
    /// 1. Prefer worker nodes that are caught up
    ///    2.1 Choose the current target leader
    ///    2.2 Choose the preferred leader
    ///    2.3 Pick any of the nodes in the current partition configuration
    /// 2. Pick worker nodes that are alive
    fn select_leader(
        &mut self,
        partition_id: &PartitionId,
        observed_cluster_state: &ObservedClusterState,
    ) {
        let Some(partition) = self.partitions.get_mut(partition_id) else {
            return;
        };

        if let Some(observed_partition_state) = observed_cluster_state.partition_state(partition_id)
        {
            if let Some(leader) =
                Self::select_leader_by_priority(partition, observed_cluster_state, |node_id| {
                    observed_partition_state
                        .replay_status(&node_id)
                        .is_some_and(|status| status == ReplayStatus::Active)
                })
            {
                partition.target_leader = Some(leader);
                return;
            }
        }

        if let Some(leader) =
            Self::select_leader_by_priority(partition, observed_cluster_state, |_node_id| true)
        {
            partition.target_leader = Some(leader);
        }

        // keep the current target leader as we couldn't find any suitable substitute
    }

    fn select_leader_by_priority(
        partition: &PartitionState,
        observed_cluster_state: &ObservedClusterState,
        additional_criterion: impl Fn(PlainNodeId) -> bool,
    ) -> Option<PlainNodeId> {
        // select any of the alive nodes in current
        if let Some(alive_replica) =
            partition
                .current
                .replica_set()
                .iter()
                .copied()
                .find(|node_id| {
                    observed_cluster_state.alive_generation(*node_id).is_some()
                        && additional_criterion(*node_id)
                })
        {
            return Some(alive_replica);
        }

        None
    }

    fn update_placement(&self, partition_id: &PartitionId, placement: &mut PartitionPlacement) {
        if let Some(partition) = self.partitions.get(partition_id) {
            if let Some(leader) = partition.target_leader {
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
