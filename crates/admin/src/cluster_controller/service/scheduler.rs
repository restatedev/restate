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

use ahash::HashMap;
use tracing::{debug, info};

use restate_core::network::{NetworkSender as _, Networking, Swimlane, TransportConnect};
use restate_core::{Metadata, MetadataWriter, ShutdownError, SyncError, TaskCenter, TaskKind};
use restate_metadata_store::{
    MetadataStoreClient, ReadError, ReadModifyWriteError, ReadWriteError, WriteError,
};
use restate_types::cluster::cluster_state::LegacyClusterState;
use restate_types::cluster_state::ClusterState;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, WorkerState};
use restate_types::partition_table::{PartitionReplication, PartitionTable};
use restate_types::partitions::state::{PartitionReplicaSetStates, ReplicaSetState};
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
        // If the provided current configuration is not valid, then this means that the epoch
        // metadata was clobbered by an old version. Reset the partition state so that the scheduler
        // finds a new valid configuration on the next event/tick.
        if !current.is_valid() && self.current.is_valid() {
            self.current = current;
            self.next = None;
            return true;
        }

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

    fn generate_instructions(
        &self,
        partition_id: &PartitionId,
        legacy_cluster_state: &LegacyClusterState,
        commands: &mut BTreeMap<PlainNodeId, Vec<ControlProcessor>>,
    ) {
        if let Some(leader) = &self.target_leader {
            if !legacy_cluster_state.runs_partition_processor_leader(leader, partition_id) {
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
    cluster_state: ClusterState,
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
            cluster_state: TaskCenter::with_current(|h| h.cluster_state().clone()),
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
                let updated = entry.get_mut().update_configuration(current, next);
                debug!(
                    %partition_id,
                    updated,
                    "Updated partition configuration in update_partition_configuration"
                );

                (updated, entry)
            }
            Entry::Vacant(entry) => {
                debug!(%partition_id, "Inserting new partition configuration");
                (true, entry.insert_entry(PartitionState::new(current, next)))
            }
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
        debug!(%partition_id, "Noting observed membership update");

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

    pub async fn on_cluster_state_change(
        &mut self,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
    ) -> Result<(), Error> {
        self.ensure_valid_partition_configuration(
            cluster_state,
            legacy_cluster_state,
            nodes_config,
            partition_table,
        )
        .await?;

        // todo move draining workers to disabled if they no longer run any partition processors;
        //  since the worker state is stored in the NodesConfiguration and the replica sets are
        //  stored in the EpochMetadata we cannot guarantee linearizability. Hence, when setting a
        //  worker to draining it might still be added to replica sets by cluster controllers until
        //  they learn about the updated nodes configuration. To reduce the risk of this, we should
        //  wait a little bit to give the nodes configuration time to be spread across the cluster.

        self.instruct_nodes(legacy_cluster_state)?;

        Ok(())
    }

    async fn ensure_valid_partition_configuration(
        &mut self,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
    ) -> Result<(), Error> {
        // todo a bulk get of all EpochMetadata if self.partitions.is_empty()

        for partition_id in partition_table.iter_ids().copied() {
            let entry = self.partitions.entry(partition_id);

            // make sure that we have a valid partition processor configuration
            let mut occupied_entry = match entry {
                Entry::Occupied(mut entry) if entry.get().current.is_valid() => {
                    let partition_replication = Self::partition_replication_to_replication_property(
                        nodes_config,
                        partition_table,
                    );
                    if Self::requires_reconfiguration(
                        partition_id,
                        entry.get(),
                        &partition_replication,
                        nodes_config,
                        &self.cluster_state,
                    ) {
                        debug!("Partition {} requires reconfiguration", partition_id);

                        if let Some(next) = Self::choose_partition_configuration(
                            partition_id,
                            nodes_config,
                            partition_replication,
                            NodeSet::new(),
                            &self.cluster_state,
                        ) {
                            let partition_configuration_update =
                                Self::reconfigure_partition_configuration(
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
                            let updated = entry.get_mut().update_configuration(
                                partition_configuration_update.current,
                                partition_configuration_update.next,
                            );
                            debug!(
                                %partition_id,
                                updated,
                                "Updated partition configuration in update_partition_configuration"
                            );
                            if updated {
                                Self::note_observed_membership_update(
                                    partition_id,
                                    entry.get(),
                                    &self.replica_set_states,
                                );
                            }
                        }
                    }

                    entry
                }
                entry => {
                    let partition_replication = Self::partition_replication_to_replication_property(
                        nodes_config,
                        partition_table,
                    );

                    // no or no valid current configuration, pick a valid configuration
                    if let Some(current) = Self::choose_partition_configuration(
                        partition_id,
                        nodes_config,
                        partition_replication.clone(),
                        NodeSet::default(),
                        &self.cluster_state,
                    ) {
                        debug!(
                            %partition_id,
                            "Chose initial partition configuration"
                        );

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
                        debug!(
                            %partition_id,
                            "Skipped invalid initial partition configuration"
                        );

                        // no valid configuration, skip
                        continue;
                    }
                }
            };

            let partition_state = occupied_entry.get();

            if Self::should_complete_reconfiguration(
                partition_id,
                nodes_config,
                partition_state,
                legacy_cluster_state,
            ) {
                let partition_configuration_update = Self::complete_reconfiguration(
                    self.metadata_writer.raw_metadata_store_client(),
                    partition_id,
                    occupied_entry.get(),
                )
                .await?;

                let updated = occupied_entry.get_mut().update_configuration(
                    partition_configuration_update.current,
                    partition_configuration_update.next,
                );

                debug!(
                    %partition_id,
                    updated,
                    "Updated partition configuration in update_partition_configuration"
                );

                if updated {
                    Self::note_observed_membership_update(
                        partition_id,
                        occupied_entry.get(),
                        &self.replica_set_states,
                    );
                }
            }

            // select the leader based on the observed cluster state
            self.select_leader(&partition_id, cluster_state, legacy_cluster_state);
        }

        Ok(())
    }

    /// Checks whether a pending reconfiguration should be completed. Conditions for doing this are:
    ///
    /// * All workers in the current configuration are disabled
    /// * Any of the partition processors in the next configuration is active (== caught up)
    ///
    /// Note: We don't complete the reconfiguration if all current nodes are dead for some time,
    /// because we might need any of them to send a partition store snapshot to the next nodes once
    /// we support in-band snapshot exchanges and trimming based on durable lsns.
    fn should_complete_reconfiguration(
        partition_id: PartitionId,
        nodes_config: &NodesConfiguration,
        partition_state: &PartitionState,
        legacy_cluster_state: &LegacyClusterState,
    ) -> bool {
        // we can only complete the reconfiguration if a next configuration has been set
        let Some(next) = partition_state.next.as_ref() else {
            return false;
        };

        let all_current_workers_disabled = partition_state
            .current
            .replica_set()
            .iter()
            .all(|node_id| nodes_config.get_worker_state(node_id) == WorkerState::Disabled);

        // check whether we can transition from the current configuration to the next
        // configuration, which is possible as soon as a single partition processor from the
        // next configuration has become active
        let any_next_pp_active = next.replica_set().iter().any(|node_id| {
            legacy_cluster_state.is_partition_processor_active(&partition_id, node_id)
        });

        all_current_workers_disabled || any_next_pp_active
    }

    fn partition_replication_to_replication_property(
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
    ) -> ReplicationProperty {
        match partition_table.replication() {
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
        }
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
                        // check whether someone else stored an initial current partition configuration
                        if epoch_metadata.current().version() == Version::INVALID {
                            Ok(epoch_metadata.set_initial_current_configuration(current.clone()))
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
            Ok(epoch_metadata) => {
                let (_, _, current, next) = epoch_metadata.into_inner();
                debug!("Initialized partition {} with {:?}", partition_id, current);
                Ok(PartitionState::new(current, next))
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
    ) -> Result<PartitionConfigurationUpdate, Error> {
        debug!(%partition_id, "Reconfiguring partition configuration");
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
                Ok(PartitionConfigurationUpdate { current, next })
            }
            Err(ReadModifyWriteError::FailedOperation(concurrent_update)) => Ok(concurrent_update),
            Err(ReadModifyWriteError::ReadWrite(err)) => Err(err.into()),
        }
    }

    async fn complete_reconfiguration(
        metadata_store_client: &MetadataStoreClient,
        partition_id: PartitionId,
        partition_state: &PartitionState,
    ) -> Result<PartitionConfigurationUpdate, Error> {
        let current_version = partition_state.current.version();
        let expected_next_version = partition_state
            .next
            .as_ref()
            .expect("next should be present")
            .version();

        match metadata_store_client.read_modify_write(partition_processor_epoch_key(partition_id), |epoch_metadata: Option<EpochMetadata>| {
            match epoch_metadata {
                None => panic!("Did not find epoch metadata which should be present. This indicates a corruption of the metadata store."),
                Some(epoch_metadata) => {
                    let Some(actual_next_version) = epoch_metadata.next().map(|config| config.version()) else {
                        // if there is no next configuration, then a concurrent modification has happened
                        let (_, _, current, next) = epoch_metadata.into_inner();
                        return Err(PartitionConfigurationUpdate {
                            current,
                            next,
                        });
                    };

                    match actual_next_version.cmp(&expected_next_version) {
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
                    old_replica_set = %partition_state.current.replica_set(),
                    new_replica_set = %epoch_metadata.current().replica_set(),
                    "Transitioned from partition configuration {current_version} to {expected_next_version}");
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
        cluster_state: &ClusterState,
    ) -> bool {
        // We only need to check current if next == None. If next != None, then there is a
        // reconfiguration ongoing, and we need to check whether this target configuration requires
        // reconfiguration.
        if let Some(next) = partition_state.next.as_ref() {
            next.replication() != default_replication ||
                // check if a different replica-set is eminent
                Self::choose_partition_configuration(
                    partition_id,
                    nodes_config,
                    default_replication.clone(),
                    NodeSet::default(),
                    cluster_state,
                )
                    .map(|new_config|
                        !new_config.replica_set().is_equivalent(next.replica_set()))
                    .unwrap_or(false)
        } else {
            // if we are here then there is no reconfiguration ongoing
            partition_state.current.replication() != default_replication
                || Self::choose_partition_configuration(
                    partition_id,
                    nodes_config,
                    default_replication.clone(),
                    NodeSet::default(),
                    cluster_state,
                )
                .map(|new_config| {
                    !new_config
                        .replica_set()
                        .is_equivalent(partition_state.current.replica_set())
                })
                .unwrap_or(false)
        }
    }

    fn choose_partition_configuration(
        partition_id: PartitionId,
        nodes_config: &NodesConfiguration,
        partition_replication: ReplicationProperty,
        preferred_nodes: NodeSet,
        cluster_state: &ClusterState,
    ) -> Option<PartitionConfiguration> {
        let options =
            SelectorOptions::new(u64::from(partition_id)).with_preferred_nodes(preferred_nodes);
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
    /// 2. Pick worker nodes that are alive
    fn select_leader(
        &mut self,
        partition_id: &PartitionId,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
    ) {
        let Some(partition) = self.partitions.get_mut(partition_id) else {
            debug!(
                %partition_id,
                "Not selecting leader because partition is not in the map"
            );

            return;
        };

        if let Some(leader) = Self::select_leader_by_priority(partition, cluster_state, |node_id| {
            legacy_cluster_state.is_partition_processor_active(partition_id, &node_id)
        }) {
            debug!(
                %partition_id,
                %leader,
                "Selected leader which is an active pp"
            );

            partition.target_leader = Some(leader);
            return;
        }

        if let Some(leader) =
            Self::select_leader_by_priority(partition, cluster_state, |_node_id| true)
        {
            debug!(
                %partition_id,
                %leader,
                "Selected leader which is not an active pp"
            );

            partition.target_leader = Some(leader);
        }

        let cluster_state = cluster_state.all();

        debug!(
            %partition_id,
            ?cluster_state,
            "Failed to select a leader as no nodes are alive"
        );

        // keep the current target leader as we couldn't find any suitable substitute
    }

    fn select_leader_by_priority(
        partition: &PartitionState,
        cluster_state: &ClusterState,
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
                    cluster_state.is_alive(NodeId::from(*node_id)) && additional_criterion(*node_id)
                })
        {
            return Some(alive_replica);
        }

        None
    }

    fn instruct_nodes(&self, legacy_cluster_state: &LegacyClusterState) -> Result<(), Error> {
        let mut commands = BTreeMap::default();

        for (partition_id, partition) in &self.partitions {
            partition.generate_instructions(partition_id, legacy_cluster_state, &mut commands);
        }

        if !commands.is_empty() {
            debug!(
                "Instruct nodes with partition processor commands: {:?} ",
                commands
            );
        } else {
            debug!(
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
                                debug!(
                                    %node_id,
                                    "Failed to instruct node as we couldn't get a connection"
                                );

                                // ignore connection errors, no need to mark the task as failed
                                // as it pollutes the log.
                                return Ok(());
                            };

                            let Some(permit) = connection.reserve().await else {
                                debug!(
                                    %node_id,
                                    "Failed to instruct node as we couldn't reserve the connection"
                                );

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
