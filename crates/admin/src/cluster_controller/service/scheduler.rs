// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::{Ordering, Reverse};
use std::collections::BTreeMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::time::Duration;

use ahash::HashMap;
use futures::StreamExt;
use tokio::time::Instant;
use tracing::{debug, info, trace};

use restate_core::network::{NetworkSender as _, Networking, Swimlane, TransportConnect};
use restate_core::{Metadata, MetadataWriter, ShutdownError, SyncError, TaskCenter, TaskKind};
use restate_metadata_store::{
    MetadataStoreClient, ReadError, ReadModifyWriteError, ReadWriteError, WriteError,
};
use restate_types::cluster::cluster_state::LegacyClusterState;
use restate_types::cluster_state::ClusterState;
use restate_types::config::{Configuration, LeaderHandoverPolicy};
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::metadata_store::keys::partition_processor_epoch_key;
use restate_types::net::partition_processor_manager::{
    ControlProcessor, ControlProcessors, ProcessorCommand,
};
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role, WorkerState};
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

use super::leader_handover::{
    self, Context, Deferral, DropReason, HandoverKind, HandoverOptions, OrUnknown,
    PartitionHandover, Progress,
};
use crate::cluster_controller::safety_checker::PartitionPlacement;

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
    /// Replay rates and deferred handovers, see [`leader_handover`].
    handover: PartitionHandover,
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
            handover: PartitionHandover::default(),
        }
    }

    /// Alive members of the current and the next replica set.
    fn alive_members<'a>(
        &'a self,
        cluster_state: &'a ClusterState,
    ) -> impl Iterator<Item = PlainNodeId> + 'a {
        self.current
            .replica_set()
            .iter()
            .chain(self.next.iter().flat_map(|next| next.replica_set().iter()))
            .copied()
            .filter(|node_id| cluster_state.is_alive(NodeId::from(*node_id)))
    }

    fn versions(&self) -> leader_handover::ConfigurationVersions {
        (
            self.current.version(),
            self.next.as_ref().map(PartitionConfiguration::version),
        )
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

/// Why a pending reconfiguration can be completed, see
/// [`Scheduler::should_complete_reconfiguration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionReason {
    NextReplicaSetEmpty,
    AllCurrentWorkersDisabled,
    NextReplicaActive,
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

/// Tracks since when worker nodes have been observed as not alive, so that the reconfiguration of
/// replica sets with dead members can be delayed by the reconfiguration grace period.
///
/// The state is only kept in memory and starts empty whenever this node becomes the cluster
/// controller leader. This errs on the side of waiting longer before reconfiguring.
#[derive(Default)]
struct DeadNodes(HashMap<PlainNodeId, Instant>);

impl DeadNodes {
    fn update(&mut self, nodes_config: &NodesConfiguration, cluster_state: &ClusterState) {
        let now = Instant::now();
        self.0 = nodes_config
            .iter_role(Role::Worker)
            .filter(|(node_id, _)| !cluster_state.is_alive((*node_id).into()))
            .map(|(node_id, _)| (node_id, self.0.get(&node_id).copied().unwrap_or(now)))
            .collect();
    }

    fn is_within_grace_period(
        &self,
        node_id: PlainNodeId,
        grace_period: Option<Duration>,
        now: Instant,
    ) -> bool {
        let Some(grace_period) = grace_period else {
            return false;
        };
        // A dead node without an entry died after the last `update` (e.g. while a scheduling pass
        // awaited metadata writes), so its grace period has only just started.
        self.0.get(&node_id).is_none_or(|dead_since| {
            dead_since
                .checked_add(grace_period)
                .is_none_or(|expiry| now < expiry)
        })
    }

    /// Returns the earliest future instant at which a dead node's grace period expires.
    fn next_grace_period_expiry(&self, grace_period: Duration, now: Instant) -> Option<Instant> {
        self.0
            .values()
            .filter_map(|dead_since| dead_since.checked_add(grace_period))
            .filter(|expiry| *expiry > now)
            .min()
    }
}

pub struct Scheduler<T> {
    metadata_writer: MetadataWriter,
    networking: Networking<T>,
    partitions: HashMap<PartitionId, PartitionState>,
    replica_set_states: PartitionReplicaSetStates,
    cluster_state: ClusterState,
    dead_nodes: DeadNodes,
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
            dead_nodes: DeadNodes::default(),
        }
    }

    /// Returns the next instant at which the reconfiguration grace period of a dead node expires.
    /// The scheduler should be re-run at this point to reconfigure the affected partitions.
    pub fn next_grace_period_expiry(&self) -> Option<Instant> {
        let grace_period = Configuration::pinned()
            .admin
            .reconfiguration_grace_period
            .to_non_zero_std()?;
        self.dead_nodes
            .next_grace_period_expiry(grace_period, Instant::now())
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

    /// The placement of the partition as last known to the scheduler, `None` if it hasn't been
    /// loaded yet or if its leadership is frozen and its leader isn't known.
    pub fn partition_placement(
        &self,
        partition_id: PartitionId,
        legacy_cluster_state: &LegacyClusterState,
    ) -> Option<PartitionPlacement> {
        let state = self.partitions.get(&partition_id)?;
        let frozen_leader = match (&state.leadership_policy.freeze, state.target_leader) {
            (None, _) => None,
            (Some(_), Some(leader)) => Some(NodeId::from(leader)),
            // no leader gets selected while frozen, e.g. after a cluster controller failover, so
            // rely on the leader that the replicas have observed
            (Some(_), None) => Some(NodeId::from(Self::observed_leader(
                partition_id,
                state,
                legacy_cluster_state,
            )?)),
        };
        Some(PartitionPlacement::new(
            partition_id,
            &state.current,
            state.next.as_ref(),
            state.placement_policy.is_frozen(),
            frozen_leader,
        ))
    }

    /// The latest leader that the alive replicas of the partition have observed, `None` if no
    /// replica reports one or if the cluster state is stale.
    fn observed_leader(
        partition_id: PartitionId,
        state: &PartitionState,
        legacy_cluster_state: &LegacyClusterState,
    ) -> Option<GenerationalNodeId> {
        if !legacy_cluster_state.is_reliable() {
            return None;
        }
        let is_replica = |node_id| {
            state.current.replica_set().contains(node_id)
                || state
                    .next
                    .as_ref()
                    .is_some_and(|next| next.replica_set().contains(node_id))
        };
        legacy_cluster_state
            .alive_nodes()
            .filter(|node| is_replica(node.generational_node_id.as_plain()))
            .filter_map(|node| {
                let status = node.partitions.get(&partition_id)?;
                Some((
                    status.last_observed_leader_epoch?,
                    status.last_observed_leader_node?,
                ))
            })
            .max_by_key(|(leader_epoch, _)| *leader_epoch)
            .map(|(_, leader)| leader)
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

        self.dead_nodes.update(nodes_config, &self.cluster_state);

        let handover_options = HandoverOptions::from(&Configuration::pinned().admin);
        self.sample_replay_rates(legacy_cluster_state, &handover_options);

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
            &handover_options,
        );
        self.instruct_nodes(legacy_cluster_state)?;

        self.ensure_valid_partition_configuration(
            cluster_state,
            legacy_cluster_state,
            nodes_config,
            partition_table,
            &handover_options,
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

    /// Samples the replay rate of every replica of every partition, so that a rate already exists
    /// when a handover is first checked.
    fn sample_replay_rates(
        &mut self,
        legacy_cluster_state: &LegacyClusterState,
        options: &HandoverOptions,
    ) {
        for (partition_id, partition) in &mut self.partitions {
            partition.handover.rates.sample(
                *partition_id,
                partition.current.replica_set(),
                partition
                    .next
                    .as_ref()
                    .map(PartitionConfiguration::replica_set),
                legacy_cluster_state,
                options.probe_interval,
            );
        }
    }

    fn ensure_valid_leaders(
        &mut self,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
        handover_options: &HandoverOptions,
    ) {
        for partition_id in partition_table.iter_ids() {
            // select the leader based on the observed cluster state
            self.select_leader(
                partition_id,
                cluster_state,
                legacy_cluster_state,
                nodes_config,
                handover_options,
            );
        }
    }

    async fn ensure_valid_partition_configuration(
        &mut self,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        partition_table: &PartitionTable,
        handover_options: &HandoverOptions,
    ) -> Result<(), Error> {
        let mut transitions = PartitionConfigurationTransitions::default();
        let result = self
            .ensure_valid_partition_configuration_inner(
                cluster_state,
                legacy_cluster_state,
                nodes_config,
                partition_table,
                handover_options,
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
        handover_options: &HandoverOptions,
        transitions: &mut PartitionConfigurationTransitions,
    ) -> Result<(), Error> {
        let mut membership_updates = self.replica_set_states.membership_update_batch();
        let grace_period = Configuration::pinned()
            .admin
            .reconfiguration_grace_period
            .to_non_zero_std();

        for partition_id in partition_table.iter_ids().copied() {
            // read per partition since metadata operations of earlier partitions can take a while
            let now = Instant::now();
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
                            |node_id| {
                                self.dead_nodes
                                    .is_within_grace_period(node_id, grace_period, now)
                            },
                        )
                    {
                        trace!("Partition {} requires reconfiguration", partition_id);

                        if let Some(next) = Self::choose_partition_configuration(
                            partition_id,
                            nodes_config,
                            partition_replication,
                            NodeSet::new(),
                            |node_id| self.cluster_state.is_alive(node_id.into()),
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
                        |node_id| self.cluster_state.is_alive(node_id.into()),
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

            let partition_state = occupied_entry.get_mut();
            let completion = Self::should_complete_reconfiguration(
                partition_id,
                nodes_config,
                partition_state,
                legacy_cluster_state,
                &self.cluster_state,
            );

            if Self::gate_reconfiguration_completion(
                partition_id,
                partition_state,
                completion,
                legacy_cluster_state,
                &self.cluster_state,
                handover_options,
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
                handover_options,
            );
        }

        Ok(())
    }

    /// Checks whether a pending reconfiguration should be completed and returns why. Conditions
    /// for doing this are:
    ///
    /// * The next configuration is empty
    /// * All workers in the current configuration are disabled
    /// * Any of the partition processors in the next configuration is active (== caught up) and
    ///   its node is alive
    ///
    /// Note: We don't complete the reconfiguration if all current nodes are dead for some time,
    /// because we might need any of them to send a partition store snapshot to the next nodes once
    /// we support in-band snapshot exchanges and trimming based on durable lsns.
    fn should_complete_reconfiguration(
        partition_id: PartitionId,
        nodes_config: &NodesConfiguration,
        partition_state: &PartitionState,
        legacy_cluster_state: &LegacyClusterState,
        cluster_state: &ClusterState,
    ) -> Option<CompletionReason> {
        // we can only complete the reconfiguration if a next configuration has been set
        let next = partition_state.next.as_ref()?;

        let all_current_workers_disabled = partition_state
            .current
            .replica_set()
            .iter()
            .all(|node_id| nodes_config.get_worker_state(node_id) == WorkerState::Disabled);

        // check whether we can transition from the current configuration to the next
        // configuration, which is possible as soon as a single partition processor from the
        // next configuration has become active. The legacy cluster state is refreshed periodically
        // and can still report a processor as active after its node died. Since dead members of
        // next are kept during the reconfiguration grace period, we also check liveness to avoid
        // completing to a replica set without any live member.
        let any_next_pp_active = next.replica_set().iter().any(|node_id| {
            cluster_state.is_alive((*node_id).into())
                && legacy_cluster_state.is_partition_processor_active(&partition_id, node_id)
        });

        if next.replica_set().is_empty() {
            Some(CompletionReason::NextReplicaSetEmpty)
        } else if all_current_workers_disabled {
            Some(CompletionReason::AllCurrentWorkersDisabled)
        } else if any_next_pp_active {
            Some(CompletionReason::NextReplicaActive)
        } else {
            None
        }
    }

    /// Decides whether a pending reconfiguration completes now, given why it can be completed
    /// (see [`Self::should_complete_reconfiguration`]).
    ///
    /// With the `catch-up-time` leader handover policy, a completion that would remove the
    /// replica in leader mode is deferred until an alive member of the new replica set is close
    /// enough to the log tail. The deferral follows the member that was least lagging when it
    /// started, until another member is close enough or the candidate drops out. All other
    /// completions happen right away.
    fn gate_reconfiguration_completion(
        partition_id: PartitionId,
        partition: &mut PartitionState,
        completion: Option<CompletionReason>,
        legacy_cluster_state: &LegacyClusterState,
        cluster_state: &ClusterState,
        options: &HandoverOptions,
    ) -> bool {
        let leader = partition.target_leader;
        let drop_deferral = |partition: &mut PartitionState, reason| {
            leader_handover::drop_deferral(
                &mut partition.handover.reconfiguration,
                partition_id,
                HandoverKind::Reconfiguration,
                leader,
                reason,
            )
        };

        let Some(completion) = completion else {
            drop_deferral(partition, DropReason::ReconfigurationNotReady);
            return false;
        };
        let ungated = match completion {
            CompletionReason::NextReplicaSetEmpty | CompletionReason::AllCurrentWorkersDisabled => {
                Some(DropReason::ReconfigurationUngated)
            }
            CompletionReason::NextReplicaActive
                if options.policy == LeaderHandoverPolicy::Immediate =>
            {
                Some(DropReason::PolicyImmediate)
            }
            CompletionReason::NextReplicaActive if partition.leadership_policy.freeze.is_some() => {
                Some(DropReason::LeadershipFrozen)
            }
            CompletionReason::NextReplicaActive => None,
        };
        if let Some(reason) = ungated {
            drop_deferral(partition, reason);
            return true;
        }
        // a reconfiguration that can complete always has a next configuration
        let Some(next) = partition.next.as_ref() else {
            return true;
        };

        let is_alive = |node_id: PlainNodeId| cluster_state.is_alive(NodeId::from(node_id));
        let progress = Progress::new(
            partition_id,
            legacy_cluster_state,
            &partition.handover.rates,
            partition.alive_members(cluster_state),
        );
        // The target leader may still be campaigning while the previous leader keeps serving, so
        // gate on the replica that reports leader mode. Without one, the target leader yields the
        // reason why the completion is not gated.
        let current_leader = partition
            .target_leader
            .filter(|leader| {
                legacy_cluster_state.runs_partition_processor_leader(leader, &partition_id)
            })
            .or_else(|| {
                progress.leading(
                    partition
                        .current
                        .replica_set()
                        .iter()
                        .copied()
                        .filter(|node_id| is_alive(*node_id)),
                )
            })
            .or(partition.target_leader);
        let gated_leader = match current_leader {
            None => Err(DropReason::NoLeader),
            Some(leader) if !is_alive(leader) => Err(DropReason::LeaderDied),
            Some(leader)
                if !legacy_cluster_state
                    .runs_partition_processor_leader(&leader, &partition_id) =>
            {
                Err(DropReason::LeaderNotLeading)
            }
            Some(leader) if next.replica_set().contains(leader) => {
                Err(DropReason::LeaderInNewReplicaSet)
            }
            Some(leader) => Ok(leader),
        };
        let leader = match gated_leader {
            Ok(leader) => leader,
            Err(reason) => {
                drop_deferral(partition, reason);
                return true;
            }
        };

        // Keep the deferral's candidate while it is an alive member of the new replica set with a
        // known lag, so that members overtaking each other don't use up catch-up rounds. Switch to
        // the least lagging member once that one is close enough.
        let least_lagging = progress.least_lagging(
            next.replica_set()
                .iter()
                .copied()
                .filter(|node_id| is_alive(*node_id)),
        );
        let least_lagging_close_enough = least_lagging
            .and_then(|node_id| progress.of(node_id))
            .is_some_and(|p| p.is_close_enough(options.max_catch_up_time));
        let Some(candidate) = partition
            .handover
            .reconfiguration
            .as_ref()
            .map(Deferral::candidate)
            .filter(|node_id| {
                !least_lagging_close_enough
                    && next.replica_set().contains(*node_id)
                    && is_alive(*node_id)
                    && progress.lag(*node_id).is_some()
            })
            .or(least_lagging)
        else {
            return true;
        };
        let ctx = Context {
            partition_id,
            kind: HandoverKind::Reconfiguration,
            leader,
            candidate,
            versions: partition.versions(),
            tail: progress.tail(),
        };
        let candidate_progress = progress.of(candidate);
        leader_handover::evaluate(
            &mut partition.handover.reconfiguration,
            &ctx,
            candidate_progress.as_ref(),
            options,
        )
        .is_proceed()
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
                        if epoch_metadata.latest_configuration_version() <= expected_next_version {
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
    ///   longer than the reconfiguration grace period.
    ///
    /// Dead members of the replica set that are still within their grace period are treated as
    /// alive. This avoids reconfiguring partitions when a node is only briefly unavailable (e.g.
    /// during a rolling restart). If all members of the replica set are dead, the grace period is
    /// ignored, since the partition is unavailable until it is reconfigured.
    ///
    /// We check `next` if a reconfiguration is ongoing, otherwise `current`.
    fn requires_reconfiguration(
        partition_id: PartitionId,
        partition_state: &PartitionState,
        default_replication: &ReplicationProperty,
        nodes_config: &NodesConfiguration,
        cluster_state: &ClusterState,
        is_within_grace_period: impl Fn(PlainNodeId) -> bool,
    ) -> bool {
        let target = partition_state
            .next
            .as_ref()
            .unwrap_or(&partition_state.current);
        let any_member_alive = target
            .replica_set()
            .iter()
            .any(|node_id| cluster_state.is_alive((*node_id).into()));

        target.replication() != default_replication
            || Self::choose_partition_configuration(
                partition_id,
                nodes_config,
                default_replication.clone(),
                NodeSet::default(),
                |node_id| {
                    cluster_state.is_alive(node_id.into())
                        || (any_member_alive
                            && target.replica_set().contains(node_id)
                            && is_within_grace_period(node_id))
                },
            )
            .is_some_and(|new_config| !new_config.replica_set().is_equivalent(target.replica_set()))
    }

    fn choose_partition_configuration(
        partition_id: PartitionId,
        nodes_config: &NodesConfiguration,
        partition_replication: ReplicationProperty,
        preferred_nodes: NodeSet,
        is_alive: impl Fn(PlainNodeId) -> bool,
    ) -> Option<PartitionConfiguration> {
        let options =
            SelectorOptions::new(u64::from(partition_id)).with_preferred_nodes(preferred_nodes);
        let filter = |node_id: PlainNodeId, node_config: &NodeConfig| {
            is_alive(node_id) && worker_candidate_filter(node_id, node_config)
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
    /// With the `immediate` leader handover policy, the [preferred replica](Self::preferred_replica)
    /// becomes the target leader right away. With `catch-up-time`, see
    /// [`Self::select_leader_after_catch_up`].
    ///
    /// If `freeze` is set, the current target leader is kept unchanged.
    fn select_leader(
        &mut self,
        partition_id: &PartitionId,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
        handover_options: &HandoverOptions,
    ) {
        let Some(partition) = self.partitions.get_mut(partition_id) else {
            return;
        };

        // Freeze: keep the current target leader, do not elect a new one.
        if partition.leadership_policy.freeze.is_some() {
            leader_handover::drop_deferral(
                &mut partition.handover.leader_switch,
                *partition_id,
                HandoverKind::LeaderSwitch,
                partition.target_leader,
                DropReason::LeadershipFrozen,
            );
            return;
        }

        let best = Self::preferred_replica(
            partition_id,
            partition,
            cluster_state,
            legacy_cluster_state,
            nodes_config,
        );

        if handover_options.policy == LeaderHandoverPolicy::Immediate {
            leader_handover::drop_deferral(
                &mut partition.handover.leader_switch,
                *partition_id,
                HandoverKind::LeaderSwitch,
                partition.target_leader,
                DropReason::PolicyImmediate,
            );

            if let Some(best) = best
                && partition.target_leader != Some(best)
            {
                debug!(
                    "Selecting node {} as partition processor leader for partition {partition_id}: leader handover policy is immediate",
                    best
                );
                partition.target_leader = Some(best);
            }

            // keep the current target leader as we couldn't find any suitable substitute
            return;
        }

        Self::select_leader_after_catch_up(
            *partition_id,
            partition,
            best,
            cluster_state,
            legacy_cluster_state,
            handover_options,
        );
    }

    /// The replica that should lead the partition, irrespective of how far it is behind.
    ///
    /// Scores each alive replica in a single pass. Higher score wins:
    /// - 3: matches affinity + caught up
    /// - 2: caught up (no affinity match)
    /// - 1: matches affinity + alive (not caught up)
    /// - 0: alive only (baseline)
    ///
    /// Ties are broken by the replica set order: the first node with the highest score wins.
    fn preferred_replica(
        partition_id: &PartitionId,
        partition: &PartitionState,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        nodes_config: &NodesConfiguration,
    ) -> Option<PlainNodeId> {
        let affinity = partition.leadership_policy.affinity.as_ref();

        partition
            .current
            .replica_set()
            .iter()
            .copied()
            .filter(|node_id| cluster_state.is_alive(NodeId::from(*node_id)))
            // min_by_key with Reverse picks the *first* node with the highest score (max_by_key
            // would pick the last one), so that the replica set order determines the leader.
            .min_by_key(|node_id| {
                let has_affinity =
                    affinity.is_some_and(|a| matches_affinity(*node_id, a, nodes_config));
                let is_caught_up =
                    legacy_cluster_state.is_partition_processor_active(partition_id, node_id);
                Reverse(match (has_affinity, is_caught_up) {
                    (true, true) => 3u8,
                    (false, true) => 2,
                    (true, false) => 1,
                    (false, false) => 0,
                })
            })
    }

    /// Leader selection with the `catch-up-time` leader handover policy, which hands leadership
    /// to a replica only once it is close to the log tail:
    ///
    /// * No target leader: see [`Self::pick_leader_without_target`].
    /// * Target leader dead: fail over to the preferred replica right away.
    /// * Target leader campaigning (told to lead, not reporting leader mode yet): switch to the
    ///   preferred replica only if it is no further behind.
    /// * Target leader leading: switch to the preferred replica once it is close enough to the
    ///   log tail, see [`leader_handover::evaluate`].
    fn select_leader_after_catch_up(
        partition_id: PartitionId,
        partition: &mut PartitionState,
        preferred: Option<PlainNodeId>,
        cluster_state: &ClusterState,
        legacy_cluster_state: &LegacyClusterState,
        options: &HandoverOptions,
    ) {
        let is_alive = |node_id: PlainNodeId| cluster_state.is_alive(NodeId::from(node_id));
        let drop_deferral = |deferral: &mut _, leader, reason| {
            leader_handover::drop_deferral(
                deferral,
                partition_id,
                HandoverKind::LeaderSwitch,
                leader,
                reason,
            )
        };
        let versions = partition.versions();
        let progress = Progress::new(
            partition_id,
            legacy_cluster_state,
            &partition.handover.rates,
            partition.alive_members(cluster_state),
        );

        let Some(leader) = partition.target_leader else {
            drop_deferral(
                &mut partition.handover.leader_switch,
                None,
                DropReason::NoLeader,
            );
            partition.target_leader = Self::pick_leader_without_target(
                partition_id,
                &partition.current,
                preferred,
                &progress,
                legacy_cluster_state,
                is_alive,
                options,
            );
            return;
        };

        // keep the current target leader if there is no alive replica
        let Some(preferred) = preferred else {
            return;
        };

        if !is_alive(leader) {
            drop_deferral(
                &mut partition.handover.leader_switch,
                Some(leader),
                DropReason::LeaderDied,
            );
            debug!(
                "Selecting node {preferred} as partition processor leader for partition {partition_id}: current leader {leader} is dead"
            );
            partition.target_leader = Some(preferred);
            return;
        }

        if leader == preferred {
            drop_deferral(
                &mut partition.handover.leader_switch,
                Some(leader),
                DropReason::LeaderIsPreferred,
            );
            return;
        }

        if !legacy_cluster_state.runs_partition_processor_leader(&leader, &partition_id) {
            drop_deferral(
                &mut partition.handover.leader_switch,
                Some(leader),
                DropReason::LeaderNotLeading,
            );
            // An unknown lag counts as infinite, so that two unknown lags switch as before.
            let preferred_lag = progress.lag(preferred);
            let leader_lag = progress.lag(leader);
            if preferred_lag.unwrap_or(u64::MAX) <= leader_lag.unwrap_or(u64::MAX) {
                info!(
                    %partition_id,
                    %leader,
                    candidate = %preferred,
                    leader_lag = %OrUnknown(leader_lag),
                    candidate_lag = %OrUnknown(preferred_lag),
                    "Switching campaigning partition processor leader to the preferred replica, which is no further behind"
                );
                partition.target_leader = Some(preferred);
            }
            return;
        }

        if partition
            .handover
            .leader_switch
            .as_ref()
            .is_some_and(|deferral| deferral.candidate() != preferred)
        {
            drop_deferral(
                &mut partition.handover.leader_switch,
                Some(leader),
                DropReason::CandidateNotPreferred,
            );
        }
        let ctx = Context {
            partition_id,
            kind: HandoverKind::LeaderSwitch,
            leader,
            candidate: preferred,
            versions,
            tail: progress.tail(),
        };
        let candidate_progress = progress.of(preferred);
        if leader_handover::evaluate(
            &mut partition.handover.leader_switch,
            &ctx,
            candidate_progress.as_ref(),
            options,
        )
        .is_proceed()
        {
            partition.target_leader = Some(preferred);
        }
    }

    /// Picks a leader for a partition without a target leader, e.g. after a cluster controller
    /// failover or after a reconfiguration removed the leader. Waits for the first status poll,
    /// then keeps an alive replica that reports leader mode. Otherwise it takes the preferred
    /// replica if it is close enough to the log tail, else the least lagging alive replica, else
    /// (no lag known) the preferred replica.
    fn pick_leader_without_target(
        partition_id: PartitionId,
        current: &PartitionConfiguration,
        preferred: Option<PlainNodeId>,
        progress: &Progress<'_>,
        legacy_cluster_state: &LegacyClusterState,
        is_alive: impl Fn(PlainNodeId) -> bool,
        options: &HandoverOptions,
    ) -> Option<PlainNodeId> {
        if legacy_cluster_state.last_refreshed.is_none() {
            debug!(
                %partition_id,
                "Waiting for the first status poll before selecting a partition processor leader"
            );
            return None;
        }

        let alive = || {
            current
                .replica_set()
                .iter()
                .copied()
                .filter(|node_id| is_alive(*node_id))
        };
        if let Some(leading) = progress.leading(alive()) {
            info!(
                %partition_id,
                leader = %leading,
                "Selecting partition processor leader without a current leader: keeping the replica that reports leader mode"
            );
            return Some(leading);
        }

        let preferred = preferred?;
        let (leader, case) = if progress
            .of(preferred)
            .is_some_and(|p| p.is_close_enough(options.max_catch_up_time))
        {
            (preferred, "preferred replica is close enough")
        } else if let Some(least_lagging) = progress
            .least_lagging(alive())
            .filter(|node_id| progress.lag(*node_id).is_some())
        {
            (least_lagging, "least lagging replica")
        } else {
            (preferred, "no lag known, preferred replica")
        };
        info!(
            %partition_id,
            %leader,
            %preferred,
            case,
            lags = %progress.display_lags(alive()),
            "Selecting partition processor leader without a current leader"
        );
        Some(leader)
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
    use restate_core::TestCoreEnvBuilder;
    use restate_core::network::FailingConnector;
    use restate_types::cluster::cluster_state::{
        AliveNode, NodeState as LegacyNodeState, PartitionProcessorStatus, ReplayStatus, RunMode,
    };
    use restate_types::cluster_state::NodeState;
    use restate_types::config::AdminOptions;
    use restate_types::identifiers::LeaderEpoch;
    use restate_types::logs::Lsn;
    use restate_types::metadata::Precondition;
    use restate_types::nodes_config::WorkerConfig;
    use restate_types::partitions::placement_policy::{PlacementFreeze, PlacementPolicy};
    use restate_types::{GenerationalNodeId, RestateVersion};

    use super::*;

    fn configuration(node_id: u32) -> PartitionConfiguration {
        PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(1),
            [PlainNodeId::from(node_id)].into_iter().collect(),
            HashMap::default(),
        )
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
                &ClusterState::default(),
            )
            .is_some()
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

    #[restate_core::test]
    async fn select_leader_picks_first_node_with_highest_score() {
        use restate_types::time::MillisSinceEpoch;

        let builder = TestCoreEnvBuilder::with_incoming_only_connector();
        let mut scheduler = Scheduler::new(
            builder.metadata_writer.clone(),
            builder.networking,
            PartitionReplicaSetStates::default(),
        );
        let partition_id = PartitionId::MIN;
        // replica set order deliberately differs from node id order
        let nodes: Vec<_> = [3, 1, 2].into_iter().map(PlainNodeId::from).collect();
        let current = PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(3),
            nodes.iter().copied().collect(),
            HashMap::default(),
        );
        scheduler.partitions.insert(
            partition_id,
            PartitionState::new(
                current,
                None,
                LeadershipPolicy::default(),
                PlacementPolicy::default(),
            ),
        );

        let cluster_state = ClusterState::default();
        let mut updater = cluster_state.clone().updater();
        for node_id in &nodes {
            updater.upsert_node_state(
                GenerationalNodeId::new(u32::from(*node_id), 1),
                NodeState::Alive,
            );
        }
        let nodes_config = NodesConfiguration::new_for_testing();
        let legacy_cluster_state = |caught_up: &[PlainNodeId]| {
            let mut legacy = LegacyClusterState::empty();
            legacy.last_refreshed = Some(std::time::Instant::now());
            for node_id in &nodes {
                let replay_status = if caught_up.contains(node_id) {
                    ReplayStatus::Active
                } else {
                    ReplayStatus::CatchingUp
                };
                let status = PartitionProcessorStatus {
                    replay_status,
                    ..Default::default()
                };
                legacy.nodes.insert(
                    *node_id,
                    LegacyNodeState::Alive(AliveNode {
                        last_heartbeat_at: MillisSinceEpoch::now(),
                        generational_node_id: GenerationalNodeId::new(u32::from(*node_id), 1),
                        partitions: [(partition_id, status)].into_iter().collect(),
                        uptime: Duration::ZERO,
                    }),
                );
            }
            legacy
        };

        let mut select = |caught_up: &[PlainNodeId]| {
            scheduler.select_leader(
                &partition_id,
                &cluster_state,
                &legacy_cluster_state(caught_up),
                &nodes_config,
                &HandoverOptions::from(&AdminOptions::default()),
            );
            scheduler.partitions[&partition_id].target_leader
        };

        // all tied: first node in the replica set wins
        assert_eq!(select(&nodes), Some(nodes[0]));
        // only the last two are caught up: the first of them wins
        assert_eq!(select(&nodes[1..]), Some(nodes[1]));
        // nobody caught up: all tied again, first node wins
        assert_eq!(select(&[]), Some(nodes[0]));
    }

    #[test]
    fn grace_period_delays_reconfiguration_away_from_dead_members() {
        let partition_id = PartitionId::MIN;
        let replication = ReplicationProperty::new_unchecked(2);
        let grace_period = Some(Duration::from_secs(60));
        let now = Instant::now();
        let expired = now + Duration::from_secs(60);

        let mut nodes_config = NodesConfiguration::new_for_testing();
        for id in 1..=4 {
            nodes_config.upsert_node(
                NodeConfig::builder()
                    .name(format!("node-{id}"))
                    .current_generation(GenerationalNodeId::new(id, 1))
                    .address(format!("unix:/tmp/node-{id}").parse().unwrap())
                    .roles(Role::Worker.into())
                    .worker_config(WorkerConfig {
                        worker_state: WorkerState::Active,
                    })
                    .binary_version(RestateVersion::current())
                    .build(),
            );
        }

        // start from the replica set the scheduler picks when all nodes are alive
        let current = Scheduler::<FailingConnector>::choose_partition_configuration(
            partition_id,
            &nodes_config,
            replication.clone(),
            NodeSet::default(),
            |_| true,
        )
        .unwrap();
        let members: Vec<_> = current.replica_set().iter().copied().collect();
        let state = PartitionState::new(
            current,
            None,
            LeadershipPolicy::default(),
            PlacementPolicy::default(),
        );

        let requires_reconfiguration =
            |alive: &[PlainNodeId], dead_nodes: &DeadNodes, grace_period, now| {
                let cluster_state = ClusterState::default();
                let mut updater = cluster_state.clone().updater();
                for node_id in alive {
                    updater.upsert_node_state(
                        GenerationalNodeId::new(u32::from(*node_id), 1),
                        NodeState::Alive,
                    );
                }
                Scheduler::<FailingConnector>::requires_reconfiguration(
                    partition_id,
                    &state,
                    &replication,
                    &nodes_config,
                    &cluster_state,
                    |node_id| dead_nodes.is_within_grace_period(node_id, grace_period, now),
                )
            };

        // one member is dead
        let dead = members[0];
        let alive: Vec<_> = (1..=4)
            .map(PlainNodeId::from)
            .filter(|id| *id != dead)
            .collect();
        let dead_nodes = DeadNodes([(dead, now)].into_iter().collect());

        assert!(!requires_reconfiguration(
            &alive,
            &dead_nodes,
            grace_period,
            now
        ));
        assert!(requires_reconfiguration(
            &alive,
            &dead_nodes,
            grace_period,
            expired
        ));
        assert!(requires_reconfiguration(&alive, &dead_nodes, None, now));
        // died after the last update of the dead nodes
        assert!(!requires_reconfiguration(
            &alive,
            &DeadNodes::default(),
            grace_period,
            now
        ));

        // all members are dead
        let alive: Vec<_> = (1..=4)
            .map(PlainNodeId::from)
            .filter(|id| !members.contains(id))
            .collect();
        let dead_nodes = DeadNodes(members.iter().map(|id| (*id, now)).collect());
        assert!(requires_reconfiguration(
            &alive,
            &dead_nodes,
            grace_period,
            now
        ));
    }

    const PID: PartitionId = PartitionId::MIN;

    fn n(node_id: u32) -> PlainNodeId {
        PlainNodeId::from(node_id)
    }

    /// The status of a replica `at` seconds into a test, which applied the log up to `applied`
    /// while the log tail (the next free LSN) is `tail`.
    #[derive(Clone, Copy)]
    struct Replica {
        at: u64,
        applied: u64,
        tail: u64,
        replay_status: ReplayStatus,
        leading: bool,
    }

    fn replica(at: u64, applied: u64, tail: u64) -> Replica {
        Replica {
            at,
            applied,
            tail,
            replay_status: ReplayStatus::Active,
            leading: false,
        }
    }

    impl Replica {
        fn leading(self) -> Self {
            Self {
                leading: true,
                ..self
            }
        }

        fn catching_up(self) -> Self {
            Self {
                replay_status: ReplayStatus::CatchingUp,
                ..self
            }
        }

        fn status(self) -> PartitionProcessorStatus {
            PartitionProcessorStatus {
                updated_at: restate_types::time::MillisSinceEpoch::new(1_000_000 + self.at * 1000),
                effective_mode: if self.leading {
                    RunMode::Leader
                } else {
                    RunMode::Follower
                },
                last_observed_leader_epoch: self.leading.then_some(LeaderEpoch::INITIAL),
                last_applied_log_lsn: Some(Lsn::new(self.applied)),
                replay_status: self.replay_status,
                target_tail_lsn: Some(Lsn::new(self.tail)),
                ..Default::default()
            }
        }
    }

    fn replica_set(nodes: &[u32]) -> PartitionConfiguration {
        PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(u8::try_from(nodes.len()).unwrap()),
            nodes.iter().copied().map(PlainNodeId::from).collect(),
            HashMap::default(),
        )
    }

    /// A scheduler with a single partition. Its passes mirror `on_cluster_state_change` without
    /// metadata store writes.
    struct Fixture {
        scheduler: Scheduler<FailingConnector>,
        epoch_metadata: EpochMetadata,
        cluster_state: ClusterState,
        nodes_config: NodesConfiguration,
        options: HandoverOptions,
        polled: bool,
    }

    impl Fixture {
        fn new(current: &[u32], next: Option<&[u32]>, policy: LeaderHandoverPolicy) -> Self {
            let builder = TestCoreEnvBuilder::with_incoming_only_connector();
            let mut scheduler = Scheduler::new(
                builder.metadata_writer.clone(),
                builder.networking,
                PartitionReplicaSetStates::default(),
            );
            let mut epoch_metadata = EpochMetadata::new(replica_set(current), None);
            if let Some(next) = next {
                epoch_metadata = epoch_metadata.reconfigure(replica_set(next));
            }
            let (_, _, current, next, leadership_policy, placement_policy) =
                epoch_metadata.clone().into_inner();
            scheduler.partitions.insert(
                PID,
                PartitionState::new(current, next, leadership_policy, placement_policy),
            );
            let cluster_state = ClusterState::default();
            let mut fixture = Self {
                scheduler,
                epoch_metadata,
                cluster_state,
                nodes_config: NodesConfiguration::new_for_testing(),
                options: HandoverOptions {
                    policy,
                    ..HandoverOptions::from(&AdminOptions::default())
                },
                polled: true,
            };
            for node_id in 1..=4 {
                fixture.set_alive(node_id, true);
            }
            fixture
        }

        fn set_alive(&mut self, node_id: u32, alive: bool) {
            self.cluster_state.clone().updater().upsert_node_state(
                GenerationalNodeId::new(node_id, 1),
                if alive {
                    NodeState::Alive
                } else {
                    NodeState::Dead
                },
            );
        }

        fn state(&self) -> &PartitionState {
            &self.scheduler.partitions[&PID]
        }

        fn restore(&mut self, state: PartitionState) {
            self.scheduler.partitions.insert(PID, state);
        }

        fn set_target_leader(&mut self, node_id: Option<u32>) {
            self.scheduler
                .partitions
                .get_mut(&PID)
                .unwrap()
                .target_leader = node_id.map(n);
        }

        fn completed(&self) -> bool {
            self.state().next.is_none()
        }

        fn select_leader(&mut self, legacy_cluster_state: &LegacyClusterState) {
            self.scheduler.select_leader(
                &PID,
                &self.cluster_state,
                legacy_cluster_state,
                &self.nodes_config,
                &self.options,
            );
        }

        /// One scheduling pass with the given replica statuses; returns the target leader.
        fn pass(&mut self, replicas: &[(u32, Replica)]) -> Option<PlainNodeId> {
            let mut legacy = LegacyClusterState::empty();
            legacy.last_refreshed = self.polled.then(std::time::Instant::now);
            for (node_id, replica) in replicas {
                legacy.nodes.insert(
                    n(*node_id),
                    LegacyNodeState::Alive(AliveNode {
                        last_heartbeat_at: replica.status().updated_at,
                        generational_node_id: GenerationalNodeId::new(*node_id, 1),
                        partitions: [(PID, replica.status())].into_iter().collect(),
                        uptime: Duration::ZERO,
                    }),
                );
            }

            self.scheduler.sample_replay_rates(&legacy, &self.options);
            self.select_leader(&legacy);
            let partition = self.scheduler.partitions.get_mut(&PID).unwrap();
            let completion = Scheduler::<FailingConnector>::should_complete_reconfiguration(
                PID,
                &self.nodes_config,
                partition,
                &legacy,
                &self.cluster_state,
            );
            if Scheduler::<FailingConnector>::gate_reconfiguration_completion(
                PID,
                partition,
                completion,
                &legacy,
                &self.cluster_state,
                &self.options,
            ) {
                let (_, _, current, next, leadership_policy, placement_policy) = self
                    .epoch_metadata
                    .clone()
                    .complete_reconfiguration()
                    .into_inner();
                partition.update(current, next, leadership_policy, placement_policy);
            }
            self.select_leader(&legacy);
            self.state().target_leader
        }
    }

    #[restate_core::test]
    async fn leader_switch_waits_for_catch_up() {
        let mut f = Fixture::new(&[1, 2], None, LeaderHandoverPolicy::CatchUpTime);
        f.set_target_leader(Some(2));
        // N2 leads with lag 0. N1 is first in the replica set and active, but 500k records behind.
        // It replays 30k records/s while the log grows by 10k records/s.
        let at = |t: u64, n1_applied: u64| {
            let tail = 1_000_001 + t * 10_000;
            [
                (1, replica(t, n1_applied, tail)),
                (2, replica(t, tail - 1, tail).leading()),
            ]
        };
        assert_eq!(f.pass(&at(0, 500_000)), Some(n(2)));
        assert!(f.state().handover.leader_switch.is_some());
        assert_eq!(f.pass(&at(5, 650_000)), Some(n(2)));
        // probe: 300k records behind at 30k records/s; passes with the same statuses change nothing
        assert_eq!(f.pass(&at(10, 800_000)), Some(n(2)));
        assert_eq!(f.pass(&at(10, 800_000)), Some(n(2)));
        let deferred = f.state().clone();

        // the current leader dies during the deferral: failover in the same pass
        f.set_alive(2, false);
        assert_eq!(f.pass(&at(11, 830_000)[..1]), Some(n(1)));
        assert!(f.state().handover.leader_switch.is_none());

        // the candidate restarts and stops being the preferred replica
        f.restore(deferred.clone());
        f.set_alive(2, true);
        let [(_, n1), n2] = at(11, 830_000);
        assert_eq!(f.pass(&[(1, n1.catching_up()), n2]), Some(n(2)));
        assert!(f.state().handover.leader_switch.is_none());

        // round 1 ends 100k records behind (3.3 s); N1 takes over once it is close enough
        f.restore(deferred);
        assert_eq!(f.pass(&at(20, 1_100_000)), Some(n(2)));
        assert_eq!(f.pass(&at(24, 1_220_000)), Some(n(1)));
        assert!(f.state().handover.leader_switch.is_none());

        // with the immediate policy, N1 takes over at once
        let mut f = Fixture::new(&[1, 2], None, LeaderHandoverPolicy::Immediate);
        f.set_target_leader(Some(2));
        assert_eq!(f.pass(&at(0, 500_000)), Some(n(1)));
    }

    #[restate_core::test]
    async fn leader_selection_without_leading_target() {
        let mut f = Fixture::new(&[1, 2], None, LeaderHandoverPolicy::CatchUpTime);
        // no leader is assigned before the first status poll
        f.polled = false;
        assert_eq!(
            f.pass(&[(1, replica(0, 100, 101)), (2, replica(0, 100, 101))]),
            None
        );
        f.polled = true;
        // a replica that reports leader mode is kept, although N1 is preferred; as N1 is not
        // close enough, rule 3 defers the switch to it
        assert_eq!(
            f.pass(&[
                (1, replica(0, 50, 101)),
                (2, replica(0, 100, 101).leading())
            ]),
            Some(n(2))
        );
        assert!(f.state().handover.leader_switch.is_some());
        // otherwise the least lagging replica, as the preferred one is not close enough
        f.set_target_leader(None);
        assert_eq!(
            f.pass(&[(1, replica(0, 50, 101)), (2, replica(0, 90, 101))]),
            Some(n(2))
        );
        // N2 campaigns: the preferred N1 takes over only once it is no further behind
        assert_eq!(
            f.pass(&[(1, replica(1, 95, 111)), (2, replica(1, 100, 111))]),
            Some(n(2))
        );
        assert_eq!(
            f.pass(&[(1, replica(2, 110, 121)), (2, replica(2, 110, 121))]),
            Some(n(1))
        );
        // the preferred replica if it is close enough, or if no lag is known
        f.set_target_leader(None);
        assert_eq!(
            f.pass(&[(1, replica(3, 120, 121)), (2, replica(3, 120, 121))]),
            Some(n(1))
        );
        f.set_target_leader(None);
        assert_eq!(f.pass(&[]), Some(n(1)));
    }

    #[restate_core::test]
    async fn reconfiguration_completion_waits_for_catch_up() {
        use LeaderHandoverPolicy::{CatchUpTime, Immediate};

        // N1 leads and is replaced; N2 stays but is 500k records behind; N3 joins
        let far = [
            (1, replica(0, 1_000_000, 1_000_001).leading()),
            (2, replica(0, 500_000, 1_000_001)),
            (3, replica(0, 0, 1_000_001).catching_up()),
        ];
        let close = [
            (1, replica(1, 1_000_100, 1_000_101).leading()),
            (2, replica(1, 1_000_100, 1_000_101)),
            (3, replica(1, 0, 1_000_101).catching_up()),
        ];
        let mut f = Fixture::new(&[1, 2], Some(&[2, 3]), CatchUpTime);
        f.set_target_leader(Some(1));
        assert_eq!(f.pass(&far), Some(n(1)));
        assert!(!f.completed());
        assert!(f.state().handover.reconfiguration.is_some());
        // completed once N2 is close enough, and N2 gets leadership in the same pass
        assert_eq!(f.pass(&close), Some(n(2)));
        assert!(f.completed());
        assert!(f.state().handover.reconfiguration.is_none());

        // the remaining member is close enough at once
        let mut f = Fixture::new(&[1, 2], Some(&[2, 3]), CatchUpTime);
        f.set_target_leader(Some(1));
        assert_eq!(f.pass(&close), Some(n(2)));
        assert!(f.completed());

        // the leader is dead
        let mut f = Fixture::new(&[1, 2], Some(&[2, 3]), CatchUpTime);
        f.set_target_leader(Some(1));
        f.set_alive(1, false);
        assert_eq!(f.pass(&far[1..]), Some(n(2)));
        assert!(f.completed());

        // a follower is replaced
        let mut f = Fixture::new(&[1, 2], Some(&[1, 3]), CatchUpTime);
        f.set_target_leader(Some(1));
        let [n1, n2, (_, n3)] = far;
        assert_eq!(
            f.pass(&[
                n1,
                n2,
                (
                    3,
                    Replica {
                        replay_status: ReplayStatus::Active,
                        ..n3
                    }
                )
            ]),
            Some(n(1))
        );
        assert!(f.completed());

        // both are replaced: held until a member of the new set is close enough
        let both = |t: u64, n3_applied: u64| {
            let tail = 1_000_001 + t * 100;
            [
                (1, replica(t, tail - 1, tail).leading()),
                (2, replica(t, tail - 1, tail)),
                (3, replica(t, n3_applied, tail)),
                (4, replica(t, 0, tail).catching_up()),
            ]
        };
        let mut f = Fixture::new(&[1, 2], Some(&[3, 4]), CatchUpTime);
        f.set_target_leader(Some(1));
        assert_eq!(f.pass(&both(0, 500_000)), Some(n(1)));
        assert!(!f.completed());
        assert_eq!(f.pass(&both(1, 1_000_000)), Some(n(1)));
        assert!(!f.completed());
        assert_eq!(f.pass(&both(2, 1_000_200)), Some(n(3)));
        assert!(f.completed());

        // both are replaced while a leader switch from N2 to N1 goes ahead: the completion waits
        // on behalf of N2, which keeps serving while N1 campaigns
        let mut f = Fixture::new(&[1, 2], Some(&[3, 4]), CatchUpTime);
        f.set_target_leader(Some(2));
        let tail = 1_000_001;
        assert_eq!(
            f.pass(&[
                (1, replica(0, tail - 1, tail)),
                (2, replica(0, tail - 1, tail).leading()),
                (3, replica(0, 500_000, tail)),
                (4, replica(0, 500_000, tail)),
            ]),
            Some(n(1))
        );
        assert!(!f.completed());
        // N3 and N4 overtaking each other don't use up the catch-up rounds
        for t in 1..=10 {
            let tail = 1_000_001 + t * 10_000;
            let (behind, ahead) = (500_000 + t * 20_000, 501_000 + t * 20_000);
            let (n3, n4) = if t % 2 == 0 {
                (ahead, behind)
            } else {
                (behind, ahead)
            };
            assert_eq!(
                f.pass(&[
                    (1, replica(t, tail - 1, tail).leading()),
                    (2, replica(t, tail - 1, tail)),
                    (3, replica(t, n3, tail)),
                    (4, replica(t, n4, tail)),
                ]),
                Some(n(1))
            );
            assert!(!f.completed());
        }
        // N4 takes over from the deferral's candidate N3 once it is close enough
        let tail = 1_110_001;
        assert_eq!(
            f.pass(&[
                (1, replica(11, tail - 1, tail).leading()),
                (2, replica(11, tail - 1, tail)),
                (3, replica(11, 720_000, tail)),
                (4, replica(11, tail - 1, tail)),
            ]),
            Some(n(4))
        );
        assert!(f.completed());

        // with the immediate policy, the reconfiguration completes at once
        let mut f = Fixture::new(&[1, 2], Some(&[2, 3]), Immediate);
        f.set_target_leader(Some(1));
        assert_eq!(f.pass(&far), Some(n(2)));
        assert!(f.completed());
    }
}
