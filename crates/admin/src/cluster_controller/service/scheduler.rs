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

use ahash::{HashMap, HashSet};
use futures::{StreamExt, TryStreamExt};
use tracing::{debug, info, trace};

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
use restate_types::partition_table::PartitionTable;
use restate_types::partitions::leadership_policy::{LeaderAffinity, LeadershipPolicy};
use restate_types::partitions::state::{
    ObservedPartitionReplicaSetVersion, PartitionReplicaSetStates, ReplicaSetState,
};
use restate_types::partitions::{PartitionConfiguration, worker_candidate_filter};
use restate_types::replication::balanced_spread_selector::{
    BalancedSpreadSelector, SelectorOptions,
};
use restate_types::replication::{NodeSet, ReplicationProperty};
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, PlainNodeId, Version, Versioned};

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
    current: PartitionConfiguration,
    next: Option<PartitionConfiguration>,
}

impl PartitionState {
    fn new(
        current: PartitionConfiguration,
        next: Option<PartitionConfiguration>,
        leadership_policy: LeadershipPolicy,
    ) -> Self {
        Self {
            target_leader: None,
            leadership_policy,
            current,
            next,
        }
    }

    /// Returns true if the partition configuration was updated. Leadership policy changes are not
    /// affecting the return value.
    fn update(
        &mut self,
        current: PartitionConfiguration,
        next: Option<PartitionConfiguration>,
        leadership_policy: LeadershipPolicy,
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

        self.leadership_policy = leadership_policy;

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
}

/// A cached, in-memory derivation of the worker's durably-synthesized `apply_stalled_since`
/// signal (see `restate-worker`'s `apply_progress_tracker` module) -- not itself a source of
/// truth. Rebuilt from scratch from the first cluster-state refresh after any controller leader
/// transition (a fresh `Scheduler` starts with an empty map), so there is nothing to persist.
///
/// Keyed by `(PartitionId, GenerationalNodeId)` rather than a plain node id and never expired on
/// a time TTL: an entry is retained for the current generation until the worker affirmatively
/// reports non-stalled, and is only ever removed on generation replacement or replica-set
/// removal. A single missed/erroring refresh (the node reports `Dead`, or is simply missing from
/// this partition's status) must never be read as "not stalled" -- see
/// `LegacyClusterState::apply_stalled_since`.
#[derive(Debug, Clone, Copy)]
struct QuarantineMemo {
    since: MillisSinceEpoch,
}

pub struct Scheduler<T> {
    metadata_writer: MetadataWriter,
    networking: Networking<T>,
    partitions: HashMap<PartitionId, PartitionState>,
    replica_set_states: PartitionReplicaSetStates,
    cluster_state: ClusterState,
    quarantine_memory: HashMap<(PartitionId, GenerationalNodeId), QuarantineMemo>,
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
            quarantine_memory: HashMap::default(),
        }
    }

    pub fn update_partition_configuration(
        &mut self,
        partition_id: PartitionId,
        current: PartitionConfiguration,
        next: Option<PartitionConfiguration>,
        leadership_policy: LeadershipPolicy,
    ) {
        let (updated, occupied_entry) = match self.partitions.entry(partition_id) {
            Entry::Occupied(mut entry) => (
                entry.get_mut().update(current, next, leadership_policy),
                entry,
            ),
            Entry::Vacant(entry) => (
                true,
                entry.insert_entry(PartitionState::new(current, next, leadership_policy)),
            ),
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
        self.partitions = futures::stream::iter(partition_table.iter_ids().cloned().map(
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
        .buffer_unordered(24)
        .try_filter_map(
            async |(partition_id, partition_state)| match partition_state {
                Some(partition_state) => {
                    Self::note_observed_membership_update(
                        partition_id,
                        &partition_state,
                        &self.replica_set_states,
                    );

                    Ok(Some((partition_id, partition_state)))
                }
                None => Ok(None),
            },
        )
        .try_collect::<HashMap<_, _>>()
        .await?;

        Ok(())
    }

    fn ensure_valid_leaders(
        &mut self,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
    ) {
        for partition_id in partition_table.iter_ids().copied() {
            self.refresh_quarantine_memory(partition_id, cluster_state, legacy_cluster_state);

            // select the leader based on the observed cluster state
            self.select_leader(
                &partition_id,
                cluster_state,
                legacy_cluster_state,
                nodes_config,
            );
        }
    }

    /// A1: rebuilds this controller's cached view of the durable `apply_stalled_since` signal for
    /// `partition_id`'s replica set (current + next, if any). See [`QuarantineMemo`]'s doc for the
    /// update/retention rules. The actual decision logic lives in the pure, independently
    /// unit-tested [`update_quarantine_memory`] -- this method only assembles its inputs.
    fn refresh_quarantine_memory(
        &mut self,
        partition_id: PartitionId,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
    ) {
        let Some(partition) = self.partitions.get(&partition_id) else {
            return;
        };

        let mut relevant_nodes: HashSet<PlainNodeId> =
            partition.current.replica_set().iter().copied().collect();
        if let Some(next) = partition.next.as_ref() {
            relevant_nodes.extend(next.replica_set().iter().copied());
        }

        update_quarantine_memory(
            partition_id,
            &relevant_nodes,
            cluster_state,
            legacy_cluster_state,
            &mut self.quarantine_memory,
        );
    }

    async fn ensure_valid_partition_configuration(
        &mut self,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
    ) -> Result<(), Error> {
        for partition_id in partition_table.iter_ids().copied() {
            let entry = self.partitions.entry(partition_id);

            // make sure that we have a valid partition processor configuration
            let mut occupied_entry = match entry {
                Entry::Occupied(mut entry) if entry.get().current.is_valid() => {
                    let partition_replication = partition_table.replication_property(nodes_config);
                    if Self::requires_reconfiguration(
                        partition_id,
                        entry.get(),
                        &partition_replication,
                        nodes_config,
                        &self.cluster_state,
                    ) {
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
                            ) {
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
                    let partition_replication = partition_table.replication_property(nodes_config);

                    // no or no valid current configuration, pick a valid configuration
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
                            &self.replica_set_states,
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
                cluster_state,
                nodes_config,
                partition_state,
                legacy_cluster_state,
                &self.quarantine_memory,
            ) {
                let partition_configuration_update = Self::complete_reconfiguration(
                    self.metadata_writer.raw_metadata_store_client(),
                    partition_id,
                    occupied_entry.get(),
                )
                .await?;

                if occupied_entry.get_mut().update(
                    partition_configuration_update.current,
                    partition_configuration_update.next,
                    partition_configuration_update.leadership_policy,
                ) {
                    Self::note_observed_membership_update(
                        partition_id,
                        occupied_entry.get(),
                        &self.replica_set_states,
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
    /// * All workers in the current configuration are disabled
    /// * Any of the partition processors in the next configuration is active (== caught up) and
    ///   not quarantined for an apply stall
    ///
    /// Note: We don't complete the reconfiguration if all current nodes are dead for some time,
    /// because we might need any of them to send a partition store snapshot to the next nodes once
    /// we support in-band snapshot exchanges and trimming based on durable lsns.
    fn should_complete_reconfiguration(
        partition_id: PartitionId,
        cluster_state: &ClusterState,
        nodes_config: &NodesConfiguration,
        partition_state: &PartitionState,
        legacy_cluster_state: &LegacyClusterState,
        quarantine_memory: &HashMap<(PartitionId, GenerationalNodeId), QuarantineMemo>,
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
        // configuration, which is possible as soon as a single non-quarantined partition
        // processor from the next configuration has become active
        let any_next_pp_active = next.replica_set().iter().any(|node_id| {
            legacy_cluster_state.is_partition_processor_active(&partition_id, node_id)
                && !cluster_state
                    .get_node_state_and_generation(*node_id)
                    .is_some_and(|(generational_node_id, _)| {
                        quarantine_memory.contains_key(&(partition_id, generational_node_id))
                    })
        });

        all_current_workers_disabled || any_next_pp_active
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
                let (_, _, current, next, leadership_policy) = epoch_metadata.into_inner();

                Ok(Some(PartitionState::new(current, next, leadership_policy)))
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
                        // check whether someone else stored an initial current partition configuration
                        if epoch_metadata.current().version() == Version::INVALID {
                            Ok(epoch_metadata.set_initial_current_configuration(current.clone()))
                        } else {
                            let (_, _, current, next, leadership_policy) =
                                epoch_metadata.into_inner();
                            Err(Box::new(PartitionConfigurationUpdate {
                                current,
                                next,
                                leadership_policy,
                            }))
                        }
                    } else {
                        Ok(EpochMetadata::new(current.clone(), None))
                    }
                },
            )
            .await
        {
            Ok(epoch_metadata) => {
                let (_, _, current, next, leadership_policy) = epoch_metadata.into_inner();
                debug!("Initialized partition {} with {:?}", partition_id, current);
                Ok(PartitionState::new(current, next, leadership_policy))
            }
            Err(ReadModifyWriteError::FailedOperation(concurrent_update)) => {
                Ok(PartitionState::new(
                    concurrent_update.current,
                    concurrent_update.next,
                    concurrent_update.leadership_policy,
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
                            let (_, _, current, next, leadership_policy) =
                                epoch_metadata.into_inner();
                            Err(Box::new(PartitionConfigurationUpdate {
                                current,
                                next,
                                leadership_policy,
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
                let (_, _, current, next, leadership_policy) = epoch_metadata.into_inner();
                Ok(PartitionConfigurationUpdate {
                    current,
                    next,
                    leadership_policy,
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
                        let (_, _, current, next, leadership_policy) = epoch_metadata.into_inner();
                        return Err(Box::new(PartitionConfigurationUpdate {
                            current,
                            next,
                            leadership_policy,
                        }));
                    };

                    match actual_next_version.cmp(&expected_next_version) {
                        Ordering::Less => unreachable!("we should not know about a newer next configuration than the metadata store"),
                        Ordering::Equal => Ok(epoch_metadata.complete_reconfiguration()),
                        Ordering::Greater => {
                            let (_, _, current, next, leadership_policy) = epoch_metadata.into_inner();
                            Err(Box::new(PartitionConfigurationUpdate {
                                current,
                                next,
                                leadership_policy,
                            }))
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
                let (_, _, current, next, leadership_policy) = epoch_metadata.into_inner();
                Ok(PartitionConfigurationUpdate {
                    current,
                    next,
                    leadership_policy,
                })
            }
            Err(ReadModifyWriteError::FailedOperation(concurrent_update)) => {
                Ok(*concurrent_update)
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

    /// Selects a leader based on the leadership policy, observed cluster state, replica set, and
    /// quarantine memory. Delegates the actual decision to the pure, independently unit-tested
    /// [`choose_target_leader`] -- this method's job is only to assemble the [`CandidateView`]s.
    ///
    /// If `freeze` is set, the current target leader is kept unchanged.
    fn select_leader(
        &mut self,
        partition_id: &PartitionId,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
    ) {
        let quarantine_memory = &self.quarantine_memory;
        let Some(partition) = self.partitions.get_mut(partition_id) else {
            return;
        };

        // Freeze: keep the current target leader, do not elect a new one.
        if partition.leadership_policy.freeze.is_some() {
            return;
        }

        let affinity = partition.leadership_policy.affinity.as_ref();

        let candidates =
            partition
                .current
                .replica_set()
                .iter()
                .copied()
                .filter_map(|plain_node_id| {
                    let (node, state) =
                        cluster_state.get_node_state_and_generation(plain_node_id)?;
                    Some(CandidateView {
                        node,
                        alive: state.is_alive(),
                        active: legacy_cluster_state
                            .is_partition_processor_active(partition_id, &plain_node_id),
                        quarantined: quarantine_memory.contains_key(&(*partition_id, node)),
                        matches_affinity: affinity
                            .is_some_and(|a| matches_affinity(plain_node_id, a, nodes_config)),
                    })
                });

        match choose_target_leader(partition.target_leader, candidates) {
            TargetDecision::Keep => {}
            TargetDecision::Set(best) => {
                debug!(
                    "Selecting node {} as partition processor leader for partition {partition_id}",
                    best
                );
                partition.target_leader = Some(best);
            }
            TargetDecision::Clear => {
                debug!(
                    "Clearing target leader for partition {partition_id}: every alive replica is quarantined for an apply stall"
                );
                partition.target_leader = None;
            }
        }
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

/// A1: applies one refresh's worth of updates to `quarantine_memory` for `partition_id`'s
/// `relevant_nodes` (current + next replica set). Pure aside from the `quarantine_memory`
/// out-parameter, so it's unit-testable with plain `ClusterState`/`LegacyClusterState` fixtures
/// and no `Scheduler` at all -- see [`QuarantineMemo`]'s doc for the exact rules being applied.
fn update_quarantine_memory(
    partition_id: PartitionId,
    relevant_nodes: &HashSet<PlainNodeId>,
    cluster_state: &ClusterState,
    legacy_cluster_state: &LegacyClusterState,
    quarantine_memory: &mut HashMap<(PartitionId, GenerationalNodeId), QuarantineMemo>,
) {
    for &plain_node_id in relevant_nodes {
        // No generation info at all -- can't form a key either way; leave any existing memory
        // for this node untouched rather than guessing.
        let Some((generational_node_id, _state)) =
            cluster_state.get_node_state_and_generation(plain_node_id)
        else {
            continue;
        };

        match legacy_cluster_state.apply_stalled_since(&partition_id, &plain_node_id) {
            // Status absent (Dead in legacy, or no entry for this partition): never clears --
            // the worker is authoritative for clearing, not its absence.
            None => {}
            Some(None) => {
                quarantine_memory.remove(&(partition_id, generational_node_id));
            }
            Some(Some(since)) => {
                let key = (partition_id, generational_node_id);
                if quarantine_memory
                    .insert(key, QuarantineMemo { since })
                    .is_none()
                {
                    let memo = quarantine_memory.get(&key).expect("just inserted");
                    debug!(
                        %partition_id, node = %generational_node_id, since = %memo.since,
                        "Quarantining node for partition leadership: apply stall reported"
                    );
                }
            }
        }
    }

    quarantine_memory.retain(|(memo_partition_id, generational_node_id), _| {
        if *memo_partition_id != partition_id {
            return true;
        }
        let plain_node_id = generational_node_id.as_plain();
        if !relevant_nodes.contains(&plain_node_id) {
            return false; // no longer part of this partition's replica set
        }
        match cluster_state.get_node_state_and_generation(plain_node_id) {
            // A newer generation of this node is now observed: the quarantined generation is
            // gone for good, so the memo for it can't be affirmed non-stalled by that node
            // ever again -- drop it.
            Some((current, _)) => current.generation() <= generational_node_id.generation(),
            // No generation info to disprove the stored one; keep conservatively.
            None => true,
        }
    });
}

/// One replica's view for the [`choose_target_leader`] decision.
#[derive(Debug, Clone, Copy)]
struct CandidateView {
    node: GenerationalNodeId,
    alive: bool,
    /// Caught up (`replay_status == Active`).
    active: bool,
    /// Quarantined for an apply stall, per the scheduler's `quarantine_memory`.
    quarantined: bool,
    matches_affinity: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetDecision {
    Keep,
    Set(PlainNodeId),
    Clear,
}

/// The pure leader-selection decision, extracted from [`Scheduler::select_leader`] so it can be
/// unit tested without a `Scheduler`/cluster-state harness.
///
/// Scores each alive, non-quarantined replica in a single pass. Higher score wins:
/// - 3: matches affinity + caught up
/// - 2: caught up (no affinity match)
/// - 1: matches affinity + alive (not caught up)
/// - 0: alive only (baseline)
///
/// A quarantined replica is never eligible to win, but a progressing, non-quarantined
/// `CatchingUp` peer is (it simply scores lower than an `Active` one). If every alive replica is
/// quarantined, `target_leader` is cleared (stops instruction generation) only when `current`
/// itself is one of the quarantined replicas; otherwise (e.g. no alive replicas were observed at
/// all) the existing target is left unchanged, as before this feature existed.
fn choose_target_leader(
    current: Option<PlainNodeId>,
    candidates: impl Iterator<Item = CandidateView>,
) -> TargetDecision {
    let candidates: Vec<CandidateView> = candidates.collect();

    let best = candidates
        .iter()
        .filter(|candidate| candidate.alive && !candidate.quarantined)
        .max_by_key(
            |candidate| match (candidate.matches_affinity, candidate.active) {
                (true, true) => 3u8,
                (false, true) => 2,
                (true, false) => 1,
                (false, false) => 0,
            },
        );

    if let Some(best) = best {
        let best_node = best.node.as_plain();
        return if current == Some(best_node) {
            TargetDecision::Keep
        } else {
            TargetDecision::Set(best_node)
        };
    }

    let current_is_quarantined = current.is_some_and(|current_node| {
        candidates
            .iter()
            .any(|candidate| candidate.node.as_plain() == current_node && candidate.quarantined)
    });

    if current_is_quarantined {
        TargetDecision::Clear
    } else {
        TargetDecision::Keep
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
    use super::*;
    use restate_types::cluster::cluster_state::{
        AliveNode, DeadNode, NodeState as LegacyNodeState, PartitionProcessorStatus, ReplayStatus,
    };
    use restate_types::cluster_state::NodeState as LiveNodeState;
    use restate_types::time::MillisSinceEpoch;
    use std::time::Duration;

    fn candidate(
        node: GenerationalNodeId,
        alive: bool,
        active: bool,
        quarantined: bool,
    ) -> CandidateView {
        CandidateView {
            node,
            alive,
            active,
            quarantined,
            matches_affinity: false,
        }
    }

    #[test]
    fn choose_target_leader_moves_off_quarantined_target_with_active_peer() {
        let target = GenerationalNodeId::new(1, 1);
        let peer = GenerationalNodeId::new(2, 1);
        let candidates = vec![
            candidate(target, true, true, true),
            candidate(peer, true, true, false),
        ];

        let decision = choose_target_leader(Some(target.as_plain()), candidates.into_iter());
        assert_eq!(decision, TargetDecision::Set(peer.as_plain()));
    }

    #[test]
    fn choose_target_leader_moves_to_progressing_catching_up_peer() {
        let target = GenerationalNodeId::new(1, 1);
        let peer = GenerationalNodeId::new(2, 1);
        // Peer is alive, not quarantined, but not yet caught up (CatchingUp) -- still eligible
        // and must be elected since it's the only non-quarantined alive replica.
        let candidates = vec![
            candidate(target, true, true, true),
            candidate(peer, true, false, false),
        ];

        let decision = choose_target_leader(Some(target.as_plain()), candidates.into_iter());
        assert_eq!(decision, TargetDecision::Set(peer.as_plain()));
    }

    #[test]
    fn choose_target_leader_clears_when_all_alive_replicas_quarantined() {
        let target = GenerationalNodeId::new(1, 1);
        let peer = GenerationalNodeId::new(2, 1);
        let candidates = vec![
            candidate(target, true, true, true),
            candidate(peer, true, false, true),
        ];

        let decision = choose_target_leader(Some(target.as_plain()), candidates.into_iter());
        assert_eq!(decision, TargetDecision::Clear);
    }

    #[test]
    fn choose_target_leader_does_not_repick_once_cleared() {
        // With no current target and every alive replica quarantined, there is nothing to elect
        // and nothing to clear -- stays Keep (a no-op).
        let target = GenerationalNodeId::new(1, 1);
        let candidates = vec![candidate(target, true, false, true)];

        let decision = choose_target_leader(None, candidates.into_iter());
        assert_eq!(decision, TargetDecision::Keep);
    }

    #[test]
    fn choose_target_leader_keeps_target_when_no_alive_replicas_observed() {
        // No alive replicas at all (e.g. a transient gossip gap) is different from "all
        // quarantined": we don't know enough to clear, so the existing target is left alone.
        let target = GenerationalNodeId::new(1, 1);
        let candidates = vec![candidate(target, false, false, false)];

        let decision = choose_target_leader(Some(target.as_plain()), candidates.into_iter());
        assert_eq!(decision, TargetDecision::Keep);
    }

    #[test]
    fn choose_target_leader_affirmative_non_stalled_peer_is_eligible() {
        let target = GenerationalNodeId::new(1, 1);
        let peer = GenerationalNodeId::new(2, 1);
        // Both alive, active, and non-quarantined -- an affirmatively-non-stalled peer must be a
        // real candidate for the top spot, not merely tolerated: give it the higher-scoring
        // affinity match so the decision is unambiguous.
        let mut peer_candidate = candidate(peer, true, true, false);
        peer_candidate.matches_affinity = true;
        let candidates = vec![candidate(target, true, true, false), peer_candidate];

        let decision = choose_target_leader(Some(target.as_plain()), candidates.into_iter());
        assert_eq!(decision, TargetDecision::Set(peer.as_plain()));
    }

    #[test]
    fn choose_target_leader_keeps_current_target_when_it_is_the_unambiguous_best() {
        let target = GenerationalNodeId::new(1, 1);
        let peer = GenerationalNodeId::new(2, 1);
        let mut target_candidate = candidate(target, true, true, false);
        target_candidate.matches_affinity = true;
        let candidates = vec![target_candidate, candidate(peer, true, true, false)];

        let decision = choose_target_leader(Some(target.as_plain()), candidates.into_iter());
        assert_eq!(decision, TargetDecision::Keep);
    }

    fn cluster_state_with(nodes: &[(GenerationalNodeId, LiveNodeState)]) -> ClusterState {
        let mut updater = ClusterState::default().updater();
        for &(node_id, state) in nodes {
            updater.upsert_node_state(node_id, state);
        }
        updater.into_cluster_state()
    }

    fn legacy_state_with(
        partition_id: PartitionId,
        nodes: &[(PlainNodeId, Option<Option<MillisSinceEpoch>>)],
    ) -> LegacyClusterState {
        let mut state = LegacyClusterState::empty();
        for &(node_id, apply_stalled_since) in nodes {
            let legacy_node = match apply_stalled_since {
                None => LegacyNodeState::Dead(DeadNode {
                    last_seen_alive: None,
                }),
                Some(apply_stalled_since) => LegacyNodeState::Alive(AliveNode {
                    last_heartbeat_at: MillisSinceEpoch::now(),
                    generational_node_id: node_id.with_generation(1),
                    partitions: [(
                        partition_id,
                        PartitionProcessorStatus {
                            apply_stalled_since,
                            ..PartitionProcessorStatus::default()
                        },
                    )]
                    .into_iter()
                    .collect(),
                    uptime: Duration::ZERO,
                }),
            };
            state.nodes.insert(node_id, legacy_node);
        }
        state
    }

    #[test]
    fn quarantine_rebuilt_after_controller_failover() {
        // A1: a fresh Scheduler (empty quarantine_memory, as after a controller failover) must
        // quarantine a node purely from the first refresh that carries `apply_stalled_since`,
        // without needing any prior persisted state.
        let partition_id = PartitionId::MIN;
        let node = GenerationalNodeId::new(1, 1);
        let relevant = HashSet::from_iter([node.as_plain()]);
        let cluster_state = cluster_state_with(&[(node, LiveNodeState::Alive)]);
        let legacy_state = legacy_state_with(
            partition_id,
            &[(node.as_plain(), Some(Some(MillisSinceEpoch::now())))],
        );
        let mut memory = HashMap::default();

        update_quarantine_memory(
            partition_id,
            &relevant,
            &cluster_state,
            &legacy_state,
            &mut memory,
        );

        assert!(memory.contains_key(&(partition_id, node)));
    }

    #[test]
    fn quarantine_memory_absence_of_status_retains_entry() {
        // Node reported Dead in the legacy view (or missing a partition entry) must never be
        // read as "not stalled" -- the existing memo must survive untouched.
        let partition_id = PartitionId::MIN;
        let node = GenerationalNodeId::new(1, 1);
        let relevant = HashSet::from_iter([node.as_plain()]);
        let cluster_state = cluster_state_with(&[(node, LiveNodeState::Alive)]);
        let mut memory = HashMap::default();
        memory.insert(
            (partition_id, node),
            QuarantineMemo {
                since: MillisSinceEpoch::now(),
            },
        );

        // Node reported Dead this refresh.
        let legacy_state = legacy_state_with(partition_id, &[(node.as_plain(), None)]);
        update_quarantine_memory(
            partition_id,
            &relevant,
            &cluster_state,
            &legacy_state,
            &mut memory,
        );

        assert!(
            memory.contains_key(&(partition_id, node)),
            "absence of status must not clear the memo"
        );
    }

    #[test]
    fn quarantine_memory_affirmative_non_stalled_clears_entry() {
        let partition_id = PartitionId::MIN;
        let node = GenerationalNodeId::new(1, 1);
        let relevant = HashSet::from_iter([node.as_plain()]);
        let cluster_state = cluster_state_with(&[(node, LiveNodeState::Alive)]);
        let mut memory = HashMap::default();
        memory.insert(
            (partition_id, node),
            QuarantineMemo {
                since: MillisSinceEpoch::now(),
            },
        );

        let legacy_state = legacy_state_with(partition_id, &[(node.as_plain(), Some(None))]);
        update_quarantine_memory(
            partition_id,
            &relevant,
            &cluster_state,
            &legacy_state,
            &mut memory,
        );

        assert!(!memory.contains_key(&(partition_id, node)));
    }

    #[test]
    fn quarantine_memory_generation_replacement_drops_entry() {
        let partition_id = PartitionId::MIN;
        let old_generation = GenerationalNodeId::new(1, 1);
        let new_generation = GenerationalNodeId::new(1, 2);
        let relevant = HashSet::from_iter([old_generation.as_plain()]);
        let mut memory = HashMap::default();
        memory.insert(
            (partition_id, old_generation),
            QuarantineMemo {
                since: MillisSinceEpoch::now(),
            },
        );

        // The plain node id is now observed at a newer generation.
        let cluster_state = cluster_state_with(&[(new_generation, LiveNodeState::Alive)]);
        let legacy_state = legacy_state_with(partition_id, &[]);
        update_quarantine_memory(
            partition_id,
            &relevant,
            &cluster_state,
            &legacy_state,
            &mut memory,
        );

        assert!(
            !memory.contains_key(&(partition_id, old_generation)),
            "a superseded generation's memo must be dropped"
        );
    }

    #[test]
    fn quarantine_memory_replica_set_removal_drops_entry() {
        let partition_id = PartitionId::MIN;
        let node = GenerationalNodeId::new(1, 1);
        let mut memory = HashMap::default();
        memory.insert(
            (partition_id, node),
            QuarantineMemo {
                since: MillisSinceEpoch::now(),
            },
        );

        // Node is no longer part of the replica set this refresh.
        let relevant = HashSet::default();
        let cluster_state = cluster_state_with(&[(node, LiveNodeState::Alive)]);
        let legacy_state = legacy_state_with(partition_id, &[]);
        update_quarantine_memory(
            partition_id,
            &relevant,
            &cluster_state,
            &legacy_state,
            &mut memory,
        );

        assert!(!memory.contains_key(&(partition_id, node)));
    }

    #[test]
    fn should_complete_reconfiguration_blocked_by_quarantined_next_replica() {
        let partition_id = PartitionId::MIN;
        let next_node = GenerationalNodeId::new(2, 1);

        let mut current_replica_set = NodeSet::new();
        current_replica_set.insert(PlainNodeId::new(1));
        let current = PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(1),
            current_replica_set,
            Default::default(),
        );

        let mut next_replica_set = NodeSet::new();
        next_replica_set.insert(next_node.as_plain());
        let next = PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(1),
            next_replica_set,
            Default::default(),
        );

        let partition_state = PartitionState::new(current, Some(next), LeadershipPolicy::default());

        let nodes_config = NodesConfiguration::new_for_testing();
        let cluster_state = cluster_state_with(&[(next_node, LiveNodeState::Alive)]);

        // The next replica is Active (caught up) but quarantined.
        let mut legacy_state = LegacyClusterState::empty();
        legacy_state.nodes.insert(
            next_node.as_plain(),
            LegacyNodeState::Alive(AliveNode {
                last_heartbeat_at: MillisSinceEpoch::now(),
                generational_node_id: next_node,
                partitions: [(
                    partition_id,
                    PartitionProcessorStatus {
                        replay_status: ReplayStatus::Active,
                        apply_stalled_since: Some(MillisSinceEpoch::now()),
                        ..PartitionProcessorStatus::default()
                    },
                )]
                .into_iter()
                .collect(),
                uptime: Duration::ZERO,
            }),
        );

        let mut quarantine_memory = HashMap::default();
        quarantine_memory.insert(
            (partition_id, next_node),
            QuarantineMemo {
                since: MillisSinceEpoch::now(),
            },
        );

        assert!(
            !Scheduler::<restate_core::network::FailingConnector>::should_complete_reconfiguration(
                partition_id,
                &cluster_state,
                &nodes_config,
                &partition_state,
                &legacy_state,
                &quarantine_memory,
            ),
            "a quarantined next-replica must not be treated as ready to complete reconfiguration"
        );

        // Once un-quarantined, reconfiguration should be able to complete.
        quarantine_memory.clear();
        assert!(
            Scheduler::<restate_core::network::FailingConnector>::should_complete_reconfiguration(
                partition_id,
                &cluster_state,
                &nodes_config,
                &partition_state,
                &legacy_state,
                &quarantine_memory,
            )
        );
    }
}
