// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use std::fmt;

use ahash::HashMap;
use futures::StreamExt;
use tracing::{Level, debug, info, trace};

use restate_core::network::{NetworkSender as _, Networking, Swimlane, TransportConnect};
use restate_core::{Metadata, MetadataWriter, ShutdownError, SyncError, TaskCenter, TaskKind};
use restate_metadata_store::{
    MetadataStoreClient, ReadError, ReadModifyWriteError, ReadWriteError, WriteError,
};
use restate_types::cluster::cluster_state::{
    LegacyClusterState, NodeState as LegacyNodeState, PartitionProcessorStatus, ReplayStatus,
};
use restate_types::cluster_state::{ClusterState, NodeState as GossipNodeState};
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, WorkerState};
use restate_types::partition_table::PartitionTable;
use restate_types::partitions::leadership_policy::{LeaderAffinity, LeadershipPolicy};
use restate_types::partitions::placement_policy::PlacementPolicy;
use restate_types::partitions::state::{
    MembershipUpdateBatch, ObservedPartitionReplicaSetVersion, PartitionReplicaSetStates,
    ReplicaSetState,
};
use restate_types::partitions::{PartitionConfiguration, worker_candidate_filter};
use restate_types::replication::balanced_spread_selector::{
    BalancedSpreadSelector, SelectorOptions,
};
use restate_types::replication::{NodeSet, ReplicationProperty};
use restate_types::{GenerationalNodeId, NodeId, PlainNodeId, Version, Versioned};

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
    /// Policy controlling leader election for this partition.
    leadership_policy: LeadershipPolicy,
    /// Policy controlling automatic placement for this partition.
    placement_policy: PlacementPolicy,
    current: PartitionConfiguration,
    next: Option<PartitionConfiguration>,
}

impl PartitionState {
    fn new(
        current: PartitionConfiguration,
        next: Option<PartitionConfiguration>,
        leadership_policy: LeadershipPolicy,
        placement_policy: PlacementPolicy,
    ) -> Self {
        Self {
            target_leader: None,
            leadership_policy,
            placement_policy,
            current,
            next,
        }
    }

    /// Returns true if the partition configuration was updated. Policy changes do not affect the
    /// return value.
    fn update(
        &mut self,
        current: PartitionConfiguration,
        next: Option<PartitionConfiguration>,
        leadership_policy: LeadershipPolicy,
        placement_policy: PlacementPolicy,
    ) -> bool {
        self.leadership_policy = leadership_policy;
        self.placement_policy = placement_policy;

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

        if let Some(next) = next
            && self
                .next
                .as_ref()
                .is_none_or(|my_next| my_next.version() < next.version())
        {
            self.next = Some(next);
            updated = true;
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
        if let Some(leader) = &self.target_leader
            && !legacy_cluster_state.runs_partition_processor_leader(leader, partition_id)
        {
            commands.entry(*leader).or_default().push(ControlProcessor {
                partition_id: *partition_id,
                command: ProcessorCommand::Leader,
                current_version: self.current.version(),
            });
        }
    }
}

struct PartitionConfigurationUpdate {
    current: PartitionConfiguration,
    next: Option<PartitionConfiguration>,
    leadership_policy: LeadershipPolicy,
    placement_policy: PlacementPolicy,
}

struct CompleteReconfigurationResult {
    configuration: PartitionConfigurationUpdate,
    transition: Option<PartitionConfigurationTransition>,
}

struct PartitionConfigurationTransition {
    current_version: Version,
    current_replica_set: NodeSet,
    next_version: Version,
    next_replica_set: NodeSet,
}

#[derive(Default)]
struct PartitionConfigurationTransitions(BTreeMap<PartitionId, PartitionConfigurationTransition>);

impl PartitionConfigurationTransitions {
    fn insert(&mut self, partition_id: PartitionId, transition: PartitionConfigurationTransition) {
        self.0.insert(partition_id, transition);
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for PartitionConfigurationTransitions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        let mut separator = "";
        for (partition_id, transition) in &self.0 {
            write!(
                f,
                "{separator}P{partition_id}({} {:#} -> {} {:#})",
                transition.current_version,
                transition.current_replica_set,
                transition.next_version,
                transition.next_replica_set,
            )?;
            separator = ", ";
        }
        f.write_str("]")
    }
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
        leadership_policy: LeadershipPolicy,
        placement_policy: PlacementPolicy,
    ) {
        let (updated, occupied_entry) = match self.partitions.entry(partition_id) {
            Entry::Occupied(mut entry) => (
                entry
                    .get_mut()
                    .update(current, next, leadership_policy, placement_policy),
                entry,
            ),
            Entry::Vacant(entry) => (
                true,
                entry.insert_entry(PartitionState::new(
                    current,
                    next,
                    leadership_policy,
                    placement_policy,
                )),
            ),
        };

        if updated {
            let mut batch = self.replica_set_states.membership_update_batch();
            Self::note_observed_membership_update(partition_id, occupied_entry.get(), &mut batch);
        }
    }

    fn note_observed_membership_update(
        partition_id: PartitionId,
        partition_state: &PartitionState,
        batch: &mut MembershipUpdateBatch,
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
        batch.note_observed_membership(
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
        if self.partitions.is_empty() {
            self.load_all_partition_configuration(partition_table)
                .await?;
        }

        // prioritise leadership changes over partition reconfiguration
        // when a pp leader shuts down, the time until we instruct a new leader is partition unavailability.
        // instructing a new leader when we already have the metadata requires no new metadata operations and can be done nearly instantly
        // by comparison, ensure_valid_partition_configuration can take (metadata operation latency * affected partitions)
        // which might be several seconds, and leader instruction would only happen at the end.
        self.ensure_valid_leaders(
            cluster_state,
            legacy_cluster_state,
            nodes_config,
            partition_table,
        );
        self.instruct_nodes(legacy_cluster_state)?;

        self.ensure_valid_partition_configuration(
            cluster_state,
            legacy_cluster_state,
            nodes_config,
            partition_table,
        )
        .await?;
        // we may have chosen new leaders, so we instruct again
        self.instruct_nodes(legacy_cluster_state)?;

        // todo move draining workers to disabled if they no longer run any partition processors;
        //  since the worker state is stored in the NodesConfiguration and the replica sets are
        //  stored in the EpochMetadata we cannot guarantee linearizability. Hence, when setting a
        //  worker to draining it might still be added to replica sets by cluster controllers until
        //  they learn about the updated nodes configuration. To reduce the risk of this, we should
        //  wait a little bit to give the nodes configuration time to be spread across the cluster.

        Ok(())
    }

    async fn load_all_partition_configuration(
        &mut self,
        partition_table: &PartitionTable,
    ) -> Result<(), Error> {
        let mut partition_configs = futures::stream::iter(partition_table.iter_ids().cloned().map(
            async |partition_id| {
                Result::<_, Error>::Ok((
                    partition_id,
                    Self::load_partition_configuration(
                        self.metadata_writer.raw_metadata_store_client(),
                        partition_id,
                    )
                    .await?,
                ))
            },
        ))
        // load partitions concurrently - we choose 24 to match the default partition count
        .buffer_unordered(24);

        let mut partitions = HashMap::default();
        let mut batch = self.replica_set_states.membership_update_batch();
        let mut first_error = None;
        while let Some(val) = partition_configs.next().await {
            match val {
                Ok((partition_id, Some(partition_state))) => {
                    Self::note_observed_membership_update(
                        partition_id,
                        &partition_state,
                        &mut batch,
                    );
                    partitions.insert(partition_id, partition_state);
                }
                Ok((_partition_id, None)) => {}
                Err(err) => {
                    first_error.get_or_insert(err);
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }
        self.partitions = partitions;

        Ok(())
    }

    fn ensure_valid_leaders(
        &mut self,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
    ) {
        for partition_id in partition_table.iter_ids() {
            // select the leader based on the observed cluster state
            self.select_leader(
                partition_id,
                cluster_state,
                legacy_cluster_state,
                nodes_config,
            );
        }
    }

    async fn ensure_valid_partition_configuration(
        &mut self,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
    ) -> Result<(), Error> {
        let mut transitions = PartitionConfigurationTransitions::default();
        let result = self
            .ensure_valid_partition_configuration_inner(
                cluster_state,
                legacy_cluster_state,
                nodes_config,
                partition_table,
                &mut transitions,
            )
            .await;

        if !transitions.is_empty() {
            info!("Partition configuration transitions: {transitions}");
        }

        result
    }

    async fn ensure_valid_partition_configuration_inner(
        &mut self,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
        transitions: &mut PartitionConfigurationTransitions,
    ) -> Result<(), Error> {
        let mut membership_updates = self.replica_set_states.membership_update_batch();

        for partition_id in partition_table.iter_ids().copied() {
            let entry = self.partitions.entry(partition_id);

            // make sure that we have a valid partition processor configuration
            let mut occupied_entry = match entry {
                Entry::Occupied(mut entry) if entry.get().current.is_valid() => {
                    let partition_replication = partition_table.replication_property(nodes_config);
                    if !entry.get().placement_policy.is_frozen()
                        && Self::requires_reconfiguration(
                            partition_id,
                            entry.get(),
                            &partition_replication,
                            nodes_config,
                            &self.cluster_state,
                        )
                    {
                        trace!("Partition {} requires reconfiguration", partition_id);

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
                            if entry.get_mut().update(
                                partition_configuration_update.current,
                                partition_configuration_update.next,
                                partition_configuration_update.leadership_policy,
                                partition_configuration_update.placement_policy,
                            ) {
                                Self::note_observed_membership_update(
                                    partition_id,
                                    entry.get(),
                                    &mut membership_updates,
                                );
                            }
                        }
                    }

                    entry
                }
                entry => {
                    let partition_replication = partition_table.replication_property(nodes_config);

                    // No valid current configuration, pick a valid configuration.
                    if let Some(current) = Self::choose_partition_configuration(
                        partition_id,
                        nodes_config,
                        partition_replication.clone(),
                        NodeSet::default(),
                        &self.cluster_state,
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
                            &mut membership_updates,
                        );
                        occupied_entry
                    } else {
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
                let CompleteReconfigurationResult {
                    configuration: partition_configuration_update,
                    transition,
                } = Self::complete_reconfiguration(
                    self.metadata_writer.raw_metadata_store_client(),
                    partition_id,
                    occupied_entry.get(),
                )
                .await?;

                if let Some(transition) = transition {
                    transitions.insert(partition_id, transition);
                }

                if occupied_entry.get_mut().update(
                    partition_configuration_update.current,
                    partition_configuration_update.next,
                    partition_configuration_update.leadership_policy,
                    partition_configuration_update.placement_policy,
                ) {
                    Self::note_observed_membership_update(
                        partition_id,
                        occupied_entry.get(),
                        &mut membership_updates,
                    );
                }
            }

            // select the leader based on the observed cluster state
            self.select_leader(
                &partition_id,
                cluster_state,
                legacy_cluster_state,
                nodes_config,
            );
        }

        Ok(())
    }

    /// Checks whether a pending reconfiguration should be completed. Conditions for doing this are:
    ///
    /// * The next configuration is empty
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

        next.replica_set().is_empty() || all_current_workers_disabled || any_next_pp_active
    }

    async fn load_partition_configuration(
        metadata_store_client: &MetadataStoreClient,
        partition_id: PartitionId,
    ) -> Result<Option<PartitionState>, Error> {
        match metadata_store_client
            .get::<EpochMetadata>(partition_processor_epoch_key(partition_id))
            .await
        {
            Ok(Some(epoch_metadata)) if epoch_metadata.current().version() != Version::INVALID => {
                let (_, _, current, next, leadership_policy, placement_policy) =
                    epoch_metadata.into_inner();

                Ok(Some(PartitionState::new(
                    current,
                    next,
                    leadership_policy,
                    placement_policy,
                )))
            }
            Ok(_) => Ok(None), // none or invalid partition state
            Err(err) => Err(err.into()),
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
                        // Check whether someone else stored an initial current partition configuration.
                        if epoch_metadata.current().is_valid() {
                            let (_, _, current, next, leadership_policy, placement_policy) =
                                epoch_metadata.into_inner();
                            Err(Box::new(PartitionConfigurationUpdate {
                                current,
                                next,
                                leadership_policy,
                                placement_policy,
                            }))
                        } else {
                            Ok(epoch_metadata.set_initial_current_configuration(current.clone()))
                        }
                    } else {
                        Ok(EpochMetadata::new(current.clone(), None))
                    }
                },
            )
            .await
        {
            Ok(epoch_metadata) => {
                let (_, _, current, next, leadership_policy, placement_policy) =
                    epoch_metadata.into_inner();
                debug!("Initialized partition {} with {:?}", partition_id, current);
                Ok(PartitionState::new(
                    current,
                    next,
                    leadership_policy,
                    placement_policy,
                ))
            }
            Err(ReadModifyWriteError::FailedOperation(concurrent_update)) => {
                Ok(PartitionState::new(
                    concurrent_update.current,
                    concurrent_update.next,
                    concurrent_update.leadership_policy,
                    concurrent_update.placement_policy,
                ))
            }
            Err(ReadModifyWriteError::ReadWrite(err)) => Err(err.into()),
        }
    }

    async fn reconfigure_partition_configuration(
        metadata_store_client: &MetadataStoreClient,
        partition_id: PartitionId,
        expected_next_version: Version,
        next: PartitionConfiguration,
    ) -> Result<PartitionConfigurationUpdate, Error> {
        match metadata_store_client
            .read_modify_write(
                partition_processor_epoch_key(partition_id),
                |epoch_metadata: Option<EpochMetadata>| {
                    if let Some(epoch_metadata) = epoch_metadata {
                        if epoch_metadata.placement_policy().is_frozen() {
                            let (_, _, current, next, leadership_policy, placement_policy) =
                                epoch_metadata.into_inner();
                            return Err(Box::new(PartitionConfigurationUpdate {
                                current,
                                next,
                                leadership_policy,
                                placement_policy,
                            }));
                        }

                        // Check if next has been modified in the meantime. If next is not present,
                        // then check whether current contains a larger version than the expected next
                        // version because we might have completed a reconfiguration in the meantime.
                        if epoch_metadata
                            .next()
                            .map(|next| next.version())
                            .unwrap_or_else(|| epoch_metadata.current().version())
                            <= expected_next_version
                        {
                            Ok(epoch_metadata.reconfigure(next.clone()))
                        } else {
                            let (_, _, current, next, leadership_policy, placement_policy) =
                                epoch_metadata.into_inner();
                            Err(Box::new(PartitionConfigurationUpdate {
                                current,
                                next,
                                leadership_policy,
                                placement_policy,
                            }))
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
                let (_, _, current, next, leadership_policy, placement_policy) =
                    epoch_metadata.into_inner();
                Ok(PartitionConfigurationUpdate {
                    current,
                    next,
                    leadership_policy,
                    placement_policy,
                })
            }
            Err(ReadModifyWriteError::FailedOperation(concurrent_update)) => Ok(*concurrent_update),
            Err(ReadModifyWriteError::ReadWrite(err)) => Err(err.into()),
        }
    }

    async fn complete_reconfiguration(
        metadata_store_client: &MetadataStoreClient,
        partition_id: PartitionId,
        partition_state: &PartitionState,
    ) -> Result<CompleteReconfigurationResult, Error> {
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
                        let (_, _, current, next, leadership_policy, placement_policy) =
                            epoch_metadata.into_inner();
                        return Err(Box::new(PartitionConfigurationUpdate {
                            current,
                            next,
                            leadership_policy,
                            placement_policy,
                        }));
                    };

                    match actual_next_version.cmp(&expected_next_version) {
                        Ordering::Less => unreachable!("we should not know about a newer next configuration than the metadata store"),
                        Ordering::Equal => Ok(epoch_metadata.complete_reconfiguration()),
                        Ordering::Greater => {
                            let (_, _, current, next, leadership_policy, placement_policy) =
                                epoch_metadata.into_inner();
                            Err(Box::new(PartitionConfigurationUpdate {
                                current,
                                next,
                                leadership_policy,
                                placement_policy,
                            }))
                        }
                    }
                }
            }
        }).await {
            Ok(epoch_metadata) => {
                let transition = PartitionConfigurationTransition {
                    current_version,
                    current_replica_set: partition_state.current.replica_set().clone(),
                    next_version: expected_next_version,
                    next_replica_set: epoch_metadata.current().replica_set().clone(),
                };
                let (_, _, current, next, leadership_policy, placement_policy) = epoch_metadata.into_inner();
                Ok(CompleteReconfigurationResult {
                    configuration: PartitionConfigurationUpdate {
                        current,
                        next,
                        leadership_policy,
                        placement_policy,
                    },
                    transition: Some(transition),
                })
            }
            Err(ReadModifyWriteError::FailedOperation(concurrent_update)) => {
                Ok(CompleteReconfigurationResult {
                    configuration: *concurrent_update,
                    transition: None,
                })
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

    /// Selects a leader based on the leadership policy, observed cluster state and replica set.
    ///
    /// Scores each alive replica in a single pass. Higher score wins:
    /// - 3: matches affinity + caught up
    /// - 2: caught up (no affinity match)
    /// - 1: matches affinity + alive (not caught up)
    /// - 0: alive only (baseline)
    ///
    /// If `freeze` is set, the current target leader is kept unchanged.
    fn select_leader(
        &mut self,
        partition_id: &PartitionId,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
    ) {
        let Some(partition) = self.partitions.get_mut(partition_id) else {
            return;
        };

        // Freeze: keep the current target leader, do not elect a new one.
        if partition.leadership_policy.freeze.is_some() {
            return;
        }

        let best = select_leader_candidate(
            partition_id,
            &partition.current,
            partition.leadership_policy.affinity.as_ref(),
            cluster_state,
            legacy_cluster_state,
            nodes_config,
        );

        if let Some(best) = best
            && partition.target_leader != Some(best)
        {
            let winner = leader_candidate(
                partition_id,
                best,
                cluster_state,
                partition.leadership_policy.affinity.as_ref(),
                legacy_cluster_state,
                nodes_config,
            );
            info!(
                partition_id = %partition_id,
                configuration_version = %partition.current.version(),
                previous_target = ?partition.target_leader,
                new_target = %best,
                score = winner.score,
                gossip_eligible = winner.gossip_eligible,
                gossip_state = ?winner.gossip_state,
                gossip_node_id = ?winner.gossip_node_id,
                legacy_get_node_state = ?winner.legacy_node_state,
                replay_status = ?winner.status.map(|status| status.replay_status),
                legacy_node_generation = ?winner.legacy_node_generation,
                "selected partition processor leader"
            );

            if tracing::enabled!(target: "restate_admin::cluster_controller::leader_election", Level::DEBUG)
            {
                let candidates = leader_candidates(
                    partition_id,
                    &partition.current,
                    partition.leadership_policy.affinity.as_ref(),
                    cluster_state,
                    legacy_cluster_state,
                    nodes_config,
                );
                debug!(
                    target: "restate_admin::cluster_controller::leader_election",
                    partition_id = %partition_id,
                    configuration_version = %partition.current.version(),
                    previous_target = ?partition.target_leader,
                    new_target = %best,
                    legacy_cluster_state_age_ms = ?legacy_cluster_state
                        .last_refreshed
                        .map(|last_refreshed| last_refreshed.elapsed().as_millis()),
                    candidates = ?candidates,
                    "partition processor leader election diagnostics"
                );
            }

            partition.target_leader = Some(best);
        }

        // keep the current target leader as we couldn't find any suitable substitute
    }

    fn instruct_nodes(&self, legacy_cluster_state: &LegacyClusterState) -> Result<(), Error> {
        let mut commands = BTreeMap::default();

        for (partition_id, partition) in &self.partitions {
            partition.generate_instructions(partition_id, legacy_cluster_state, &mut commands);
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

    /// Compares the stored epoch metadata for each partitions with the values we observed elsewhere in the system (or through gossip).
    /// Returns the partition ids for which we think the epoch metadata might be stale.
    pub(crate) fn detect_stale_epoch_metadata(&self) -> Vec<PartitionId> {
        fn is_stale(
            partition_state: &PartitionState,
            observed_version: &ObservedPartitionReplicaSetVersion,
        ) -> bool {
            if partition_state.current.version() < observed_version.current_version {
                return true;
            }

            match (partition_state.next.as_ref(), observed_version.next_version) {
                (None, None) => false,
                // The scheduler sticks with its proposed next version even if if it read None from the metadata store.
                // So triggering a refetch wouldn't help. To avoid excessive metadata fetches, let's error on the
                // side of reporting it as not stale.
                (Some(_our_next), None) => false,
                // There's a next version observed, only consider it stale if it's newer than our current version.
                (None, Some(their_next)) => their_next > partition_state.current.version(),
                (Some(our_next), Some(their_next)) => our_next.version() < their_next,
            }
        }

        self.replica_set_states
            .partition_versions()
            .into_iter()
            .filter_map(|observed_version| {
                let partition_id = observed_version.partition_id;
                self.partitions
                    .get(&partition_id)
                    .map(|partition_state| {
                        if is_stale(partition_state, &observed_version) {
                            Some(partition_id)
                        } else {
                            None
                        }
                    })
                    // We haven't seen this partition before, so consider it stale.
                    .unwrap_or(Some(partition_id))
            })
            .collect()
    }
}

/// Diagnostic snapshot of controller-visible candidate state; never used for selection.
struct LeaderCandidate<'a> {
    node_id: PlainNodeId,
    gossip_eligible: bool,
    gossip_node_id: Option<GenerationalNodeId>,
    gossip_state: GossipNodeState,
    legacy_node_generation: Option<GenerationalNodeId>,
    legacy_node_state: LegacyNodeStatus,
    has_affinity: bool,
    status: Option<&'a PartitionProcessorStatus>,
    score: u8,
}

impl fmt::Debug for LeaderCandidate<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut candidate = f.debug_struct("LeaderCandidate");
        candidate
            .field("node_id", &self.node_id)
            .field("gossip_eligible", &self.gossip_eligible)
            .field("gossip_node_id", &self.gossip_node_id)
            .field("gossip_state", &self.gossip_state)
            .field("legacy_get_node_state", &self.legacy_node_state)
            .field("has_affinity", &self.has_affinity)
            .field("has_processor_status", &self.status.is_some());

        if let Some(status) = self.status {
            candidate
                .field("replay_status", &status.replay_status)
                .field("status_age_ms", &status.updated_at.elapsed().as_millis())
                .field("planned_mode", &status.planned_mode)
                .field("effective_mode", &status.effective_mode())
                .field("last_applied_log_lsn", &status.last_applied_log_lsn)
                .field("durable_lsn", &status.durable_lsn)
                .field("last_archived_log_lsn", &status.last_archived_log_lsn)
                .field("target_tail_lsn", &status.target_tail_lsn)
                .field(
                    "last_record_applied_age_ms",
                    &status
                        .last_record_applied_at
                        .map(|last_applied| last_applied.elapsed().as_millis()),
                );
        }

        candidate
            .field("score", &self.score)
            .field("legacy_node_generation", &self.legacy_node_generation)
            .finish()
    }
}

#[derive(Debug)]
enum LegacyNodeStatus {
    Alive,
    Dead,
    Missing,
}

fn leader_candidates<'a>(
    partition_id: &PartitionId,
    configuration: &PartitionConfiguration,
    affinity: Option<&LeaderAffinity>,
    cluster_state: &ClusterState,
    legacy_cluster_state: &'a LegacyClusterState,
    nodes_config: &NodesConfiguration,
) -> Vec<LeaderCandidate<'a>> {
    configuration
        .replica_set()
        .iter()
        .copied()
        .map(|node_id| {
            leader_candidate(
                partition_id,
                node_id,
                cluster_state,
                affinity,
                legacy_cluster_state,
                nodes_config,
            )
        })
        .collect()
}

fn select_leader_candidate(
    partition_id: &PartitionId,
    configuration: &PartitionConfiguration,
    affinity: Option<&LeaderAffinity>,
    cluster_state: &ClusterState,
    legacy_cluster_state: &LegacyClusterState,
    nodes_config: &NodesConfiguration,
) -> Option<PlainNodeId> {
    configuration
        .replica_set()
        .iter()
        .copied()
        .filter(|node_id| cluster_state.is_alive(NodeId::from(*node_id)))
        .max_by_key(|node_id| {
            let has_affinity =
                affinity.is_some_and(|a| matches_affinity(*node_id, a, nodes_config));
            let is_caught_up =
                legacy_cluster_state.is_partition_processor_active(partition_id, node_id);
            leader_score(has_affinity, is_caught_up)
        })
}

fn leader_score(has_affinity: bool, is_caught_up: bool) -> u8 {
    match (has_affinity, is_caught_up) {
        (true, true) => 3,
        (false, true) => 2,
        (true, false) => 1,
        (false, false) => 0,
    }
}

fn leader_candidate<'a>(
    partition_id: &PartitionId,
    node_id: PlainNodeId,
    cluster_state: &ClusterState,
    affinity: Option<&LeaderAffinity>,
    legacy_cluster_state: &'a LegacyClusterState,
    nodes_config: &NodesConfiguration,
) -> LeaderCandidate<'a> {
    let has_affinity = affinity.is_some_and(|a| matches_affinity(node_id, a, nodes_config));
    let node = legacy_cluster_state.nodes.get(&node_id);
    let (legacy_node_generation, legacy_node_state, status) = match node {
        Some(LegacyNodeState::Alive(node)) => (
            Some(node.generational_node_id),
            LegacyNodeStatus::Alive,
            node.partitions.get(partition_id),
        ),
        Some(LegacyNodeState::Dead(_)) => (None, LegacyNodeStatus::Dead, None),
        None => (None, LegacyNodeStatus::Missing, None),
    };
    let (gossip_node_id, gossip_state) = cluster_state
        .get_node_state_and_generation(node_id)
        .map_or((None, GossipNodeState::Dead), |(node_id, state)| {
            (Some(node_id), state)
        });
    let is_caught_up = status.is_some_and(|status| status.replay_status == ReplayStatus::Active);
    let score = leader_score(has_affinity, is_caught_up);

    LeaderCandidate {
        node_id,
        gossip_eligible: cluster_state.is_alive(NodeId::from(node_id)),
        gossip_node_id,
        gossip_state,
        legacy_node_generation,
        legacy_node_state,
        has_affinity,
        status,
        score,
    }
}

/// Returns `true` if the given node matches the leader affinity expression.
fn matches_affinity(
    node_id: PlainNodeId,
    affinity: &LeaderAffinity,
    nodes_config: &NodesConfiguration,
) -> bool {
    match affinity {
        LeaderAffinity::Node(preferred) => node_id == *preferred,
        LeaderAffinity::Location(location) => nodes_config
            .find_node_by_id(node_id)
            .map(|config| {
                config
                    .location
                    .shares_domain_with(location, location.smallest_defined_scope())
            })
            .unwrap_or(false),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use restate_core::network::FailingConnector;
    use restate_types::cluster::cluster_state::{AliveNode, DeadNode, NodeState, ReplayStatus};
    use restate_types::cluster_state::{ClusterState, NodeState as ClusterNodeState};
    use restate_types::metadata::Precondition;
    use restate_types::nodes_config::{Role, WorkerConfig};
    use restate_types::partitions::placement_policy::{PlacementFreeze, PlacementPolicy};
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, RestateVersion};

    use super::*;

    fn configuration(node_id: u32) -> PartitionConfiguration {
        PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(1),
            [PlainNodeId::from(node_id)].into_iter().collect(),
            HashMap::default(),
        )
    }

    fn leadership_test_cluster_state(node_ids: impl IntoIterator<Item = u32>) -> ClusterState {
        let cluster_state = ClusterState::default();
        let mut updater = cluster_state.clone().updater();
        for node_id in node_ids {
            updater.upsert_node_state(GenerationalNodeId::new(node_id, 1), ClusterNodeState::Alive);
        }
        cluster_state
    }

    fn leadership_test_legacy_state(
        partition_id: PartitionId,
        statuses: impl IntoIterator<Item = (u32, Option<ReplayStatus>)>,
    ) -> LegacyClusterState {
        let nodes = statuses
            .into_iter()
            .map(|(node_id, replay_status)| {
                let partitions = replay_status
                    .map(|replay_status| {
                        let status = PartitionProcessorStatus {
                            replay_status,
                            last_applied_log_lsn: Some(101.into()),
                            target_tail_lsn: Some(102.into()),
                            ..PartitionProcessorStatus::default()
                        };
                        (partition_id, status)
                    })
                    .into_iter()
                    .collect();
                (
                    PlainNodeId::from(node_id),
                    NodeState::Alive(AliveNode {
                        last_heartbeat_at: MillisSinceEpoch::now(),
                        generational_node_id: GenerationalNodeId::new(node_id, 1),
                        partitions,
                        uptime: Duration::ZERO,
                    }),
                )
            })
            .collect::<BTreeMap<_, _>>();

        LegacyClusterState {
            last_refreshed: None,
            nodes_config_version: Version::INVALID,
            partition_table_version: Version::INVALID,
            logs_metadata_version: Version::INVALID,
            nodes,
        }
    }

    #[test]
    fn missing_warm_follower_status_can_select_unready_replica() {
        let partition_id = PartitionId::MIN;
        let configuration = PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(2),
            [PlainNodeId::from(2), PlainNodeId::from(1)]
                .into_iter()
                .collect(),
            HashMap::default(),
        );
        let cluster_state = leadership_test_cluster_state([1, 2]);
        let nodes_config = NodesConfiguration::new_for_testing();

        let active_n2 = leadership_test_legacy_state(
            partition_id,
            [
                (2, Some(ReplayStatus::Active)),
                (1, Some(ReplayStatus::Starting)),
            ],
        );
        let best = select_leader_candidate(
            &partition_id,
            &configuration,
            None,
            &cluster_state,
            &active_n2,
            &nodes_config,
        );
        assert_eq!(best, Some(PlainNodeId::from(2)));
        let diagnostic = format!(
            "{:?}",
            leader_candidates(
                &partition_id,
                &configuration,
                None,
                &cluster_state,
                &active_n2,
                &nodes_config,
            )
        );
        assert!(diagnostic.contains(
            "node_id: N2, gossip_eligible: true, gossip_node_id: Some(N2:1), gossip_state: Alive, legacy_get_node_state: Alive, has_affinity: false, has_processor_status: true, replay_status: Active"
        ));
        assert!(diagnostic.contains(
            "node_id: N1, gossip_eligible: true, gossip_node_id: Some(N1:1), gossip_state: Alive, legacy_get_node_state: Alive, has_affinity: false, has_processor_status: true, replay_status: Starting"
        ));

        let mut missing_n2 = leadership_test_legacy_state(
            partition_id,
            [(2, None), (1, Some(ReplayStatus::Starting))],
        );
        // The scheduler uses the gossip failure detector for eligibility and this legacy poll for
        // processor status. A single failed legacy poll can therefore make an otherwise alive warm
        // follower appear dead here while it remains eligible for election.
        missing_n2.nodes.insert(
            PlainNodeId::from(2),
            NodeState::Dead(DeadNode {
                last_seen_alive: Some(MillisSinceEpoch::now()),
            }),
        );
        let initial_best = select_leader_candidate(
            &partition_id,
            &configuration,
            None,
            &cluster_state,
            &missing_n2,
            &nodes_config,
        );
        // Equal scores preserve the existing Iterator::max_by_key tie behavior: the later
        // replica-set entry (N1) wins.
        assert_eq!(initial_best, Some(PlainNodeId::from(1)));
        let diagnostic = format!(
            "{:?}",
            leader_candidates(
                &partition_id,
                &configuration,
                None,
                &cluster_state,
                &missing_n2,
                &nodes_config,
            )
        );
        assert!(
            diagnostic.contains("node_id: N2, gossip_eligible: true, gossip_node_id: Some(N2:1), gossip_state: Alive, legacy_get_node_state: Dead, has_affinity: false, has_processor_status: false")
        );
        assert!(diagnostic.contains(
            "node_id: N1, gossip_eligible: true, gossip_node_id: Some(N1:1), gossip_state: Alive, legacy_get_node_state: Alive, has_affinity: false, has_processor_status: true, replay_status: Starting"
        ));

        let eventual_best = select_leader_candidate(
            &partition_id,
            &configuration,
            None,
            &cluster_state,
            &active_n2,
            &nodes_config,
        );
        assert_eq!(eventual_best, Some(PlainNodeId::from(2)));

        let failing_over_n2 = leadership_test_cluster_state([1, 2]);
        failing_over_n2
            .clone()
            .updater()
            .set_node_state(GenerationalNodeId::new(2, 1), ClusterNodeState::FailingOver);
        let best = select_leader_candidate(
            &partition_id,
            &configuration,
            None,
            &failing_over_n2,
            &active_n2,
            &nodes_config,
        );
        assert_eq!(best, Some(PlainNodeId::from(1)));
        let diagnostic = format!(
            "{:?}",
            leader_candidates(
                &partition_id,
                &configuration,
                None,
                &failing_over_n2,
                &active_n2,
                &nodes_config,
            )
        );
        assert!(diagnostic.contains(
            "node_id: N2, gossip_eligible: false, gossip_node_id: Some(N2:1), gossip_state: FailingOver, legacy_get_node_state: Alive, has_affinity: false, has_processor_status: true, replay_status: Active"
        ));
    }

    #[tokio::test]
    async fn persisted_freeze_blocks_automatic_reconfiguration() {
        let metadata_store_client = MetadataStoreClient::new_in_memory();
        let policy = PlacementPolicy {
            freeze: Some(PlacementFreeze {
                reason: "maintenance".to_owned(),
            }),
        };

        let partition_id = PartitionId::MIN;
        let frozen =
            EpochMetadata::new(configuration(1), None).set_placement_policy(policy.clone());
        metadata_store_client
            .put(
                partition_processor_epoch_key(partition_id),
                &frozen,
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();

        let update = Scheduler::<FailingConnector>::reconfigure_partition_configuration(
            &metadata_store_client,
            partition_id,
            frozen.current().version(),
            configuration(2),
        )
        .await
        .unwrap();
        assert!(update.next.is_none());
        assert_eq!(update.current.replica_set(), configuration(1).replica_set());
        assert_eq!(update.placement_policy, policy);

        let partition_id = PartitionId::new_unchecked(1);
        let frozen = EpochMetadata::new(configuration(1), None)
            .reconfigure(configuration(2))
            .set_placement_policy(policy.clone());
        let expected_next_version = frozen.next().unwrap().version();
        metadata_store_client
            .put(
                partition_processor_epoch_key(partition_id),
                &frozen,
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();

        let update = Scheduler::<FailingConnector>::reconfigure_partition_configuration(
            &metadata_store_client,
            partition_id,
            expected_next_version,
            configuration(3),
        )
        .await
        .unwrap();
        assert_eq!(
            update.next.unwrap().replica_set(),
            configuration(2).replica_set()
        );
        assert_eq!(update.placement_policy, policy);
    }

    #[tokio::test]
    async fn invalid_configuration_is_initialized_even_if_policy_is_frozen() {
        let metadata_store_client = MetadataStoreClient::new_in_memory();
        let policy = PlacementPolicy {
            freeze: Some(PlacementFreeze {
                reason: "maintenance".to_owned(),
            }),
        };

        let partition_id = PartitionId::MIN;
        let frozen = EpochMetadata::new(PartitionConfiguration::default(), None)
            .set_placement_policy(policy.clone());
        metadata_store_client
            .put(
                partition_processor_epoch_key(partition_id),
                &frozen,
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();

        let state = Scheduler::<FailingConnector>::store_initial_partition_configuration(
            &metadata_store_client,
            partition_id,
            configuration(1),
        )
        .await
        .unwrap();
        assert!(state.current.is_valid());
        assert_eq!(state.current.replica_set(), configuration(1).replica_set());
        assert!(state.next.is_none());
        assert_eq!(state.placement_policy, policy);

        let stored = metadata_store_client
            .get::<EpochMetadata>(partition_processor_epoch_key(partition_id))
            .await
            .unwrap()
            .unwrap();
        assert!(stored.current().is_valid());
        assert_eq!(
            stored.current().replica_set(),
            configuration(1).replica_set()
        );
        assert_eq!(stored.placement_policy(), &policy);
    }

    #[tokio::test]
    async fn persisted_freeze_does_not_block_completion() {
        let metadata_store_client = MetadataStoreClient::new_in_memory();
        let policy = PlacementPolicy {
            freeze: Some(PlacementFreeze {
                reason: "maintenance".to_owned(),
            }),
        };
        let partition_id = PartitionId::new_unchecked(1);
        let frozen = EpochMetadata::new(configuration(1), None)
            .reconfigure(configuration(2))
            .set_placement_policy(policy.clone());
        let (_, _, current, next, leadership_policy, placement_policy) =
            frozen.clone().into_inner();
        let state = PartitionState::new(current, next, leadership_policy, placement_policy);
        metadata_store_client
            .put(
                partition_processor_epoch_key(partition_id),
                &frozen,
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();

        let mut nodes_configuration = NodesConfiguration::new_for_testing();
        nodes_configuration.upsert_node(
            NodeConfig::builder()
                .name("node-1".to_owned())
                .current_generation(GenerationalNodeId::new(1, 1))
                .address("unix:/tmp/node-1".parse().unwrap())
                .roles(Role::Worker.into())
                .worker_config(WorkerConfig {
                    worker_state: WorkerState::Disabled,
                })
                .binary_version(RestateVersion::current())
                .build(),
        );
        assert!(
            Scheduler::<FailingConnector>::should_complete_reconfiguration(
                partition_id,
                &nodes_configuration,
                &state,
                &LegacyClusterState::empty(),
            )
        );
        let completed = Scheduler::<FailingConnector>::complete_reconfiguration(
            &metadata_store_client,
            partition_id,
            &state,
        )
        .await
        .unwrap();
        assert_eq!(
            completed.configuration.current.replica_set(),
            configuration(2).replica_set()
        );
        assert!(completed.configuration.next.is_none());
        assert_eq!(completed.configuration.placement_policy, policy);
    }
}
