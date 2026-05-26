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

use ahash::{HashMap, HashMapExt};
use futures::StreamExt;
use tracing::{debug, info, trace, warn};

use restate_core::network::{NetworkSender as _, Networking, Swimlane, TransportConnect};
use restate_core::{Metadata, MetadataWriter, ShutdownError, SyncError, TaskCenter, TaskKind};
use restate_metadata_store::{
    MetadataStoreClient, ReadError, ReadModifyWriteError, ReadWriteError, WriteError,
};
use restate_types::cluster::cluster_state::LegacyClusterState;
use restate_types::cluster_state::ClusterState;
use restate_types::config::Configuration;
use restate_types::epoch::EpochMetadata;
use restate_types::identifiers::PartitionId;
use restate_types::locality::LocationScope;
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
use restate_types::replication::{
    DEFAULT_LOAD_BALANCING_TOP_N, NodeSet, ReplicationProperty, extend_top_n_load_balanced,
    hash_node_id,
};
use restate_types::{NodeId, PlainNodeId, Version, Versioned};

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

fn experimental_balanced_placement_enabled() -> bool {
    Configuration::pinned()
        .common
        .experimental_placement_strategy
        .is_balanced_v2()
}

fn experimental_rebalances_when_healthy() -> bool {
    Configuration::pinned()
        .common
        .experimental_placement_rebalance_mode
        .rebalances_when_healthy()
}

fn supports_balanced_placement(replication: &ReplicationProperty) -> bool {
    replication.copies_at_scope(LocationScope::Region).is_none()
        && replication.copies_at_scope(LocationScope::Zone).is_none()
}

fn alive_worker_candidates(
    nodes_config: &NodesConfiguration,
    cluster_state: &ClusterState,
) -> Vec<PlainNodeId> {
    nodes_config
        .iter()
        .filter(|(node_id, node_config)| {
            worker_candidate_filter(*node_id, node_config)
                && cluster_state.is_alive((*node_id).into())
        })
        .map(|(node_id, _)| node_id)
        .collect()
}

fn all_worker_candidates_alive(
    nodes_config: &NodesConfiguration,
    cluster_state: &ClusterState,
) -> bool {
    nodes_config
        .iter()
        .filter(|(node_id, node_config)| worker_candidate_filter(*node_id, node_config))
        .all(|(node_id, _)| cluster_state.is_alive(node_id.into()))
}

fn select_replica_overlap_anchor(
    partition_id: PartitionId,
    current: &PartitionConfiguration,
    planned: &NodeSet,
    candidates: &[PlainNodeId],
    legacy_cluster_state: &LegacyClusterState,
    replica_loads: &HashMap<PlainNodeId, usize>,
) -> Option<PlainNodeId> {
    if planned
        .iter()
        .filter(|node_id| candidates.contains(node_id))
        .any(|node_id| legacy_cluster_state.is_partition_processor_active(&partition_id, node_id))
    {
        return None;
    }

    if let Some(active) = current
        .replica_set()
        .iter()
        .copied()
        .filter(|node_id| candidates.contains(node_id))
        .filter(|node_id| {
            legacy_cluster_state.is_partition_processor_active(&partition_id, node_id)
        })
        .min_by_key(|node_id| {
            (
                replica_loads.get(node_id).copied().unwrap_or_default(),
                Reverse(hash_node_id(u64::from(partition_id), *node_id)),
                u32::from(*node_id),
            )
        })
    {
        return Some(active);
    }

    if current
        .replica_set()
        .iter()
        .any(|node_id| planned.contains(*node_id))
    {
        return None;
    }

    current
        .replica_set()
        .iter()
        .copied()
        .filter(|node_id| candidates.contains(node_id))
        .min_by_key(|node_id| {
            (
                replica_loads.get(node_id).copied().unwrap_or_default(),
                Reverse(hash_node_id(u64::from(partition_id), *node_id)),
                u32::from(*node_id),
            )
        })
}

fn plan_balanced_partition_placements(
    partitions: &HashMap<PartitionId, PartitionState>,
    nodes_config: &NodesConfiguration,
    partition_table: &PartitionTable,
    cluster_state: &ClusterState,
    legacy_cluster_state: &LegacyClusterState,
    partition_replication: &ReplicationProperty,
) -> Option<HashMap<PartitionId, PartitionConfiguration>> {
    if !supports_balanced_placement(partition_replication) {
        warn!(
            replication = %partition_replication,
            "Experimental balanced partition placement only supports flat replication; falling back to legacy placement"
        );
        return None;
    }

    let candidates = alive_worker_candidates(nodes_config, cluster_state);
    let target_size = partition_replication.num_copies() as usize;
    if candidates.len() < target_size {
        warn!(
            candidates = candidates.len(),
            target_size,
            "Experimental balanced partition placement has too few alive worker candidates; falling back to legacy placement"
        );
        return None;
    }

    let rebalance = experimental_rebalances_when_healthy()
        && all_worker_candidates_alive(nodes_config, cluster_state);
    let mut replica_loads = HashMap::<PlainNodeId, usize>::default();
    let mut plan = HashMap::with_capacity(partition_table.num_partitions() as usize);

    // Count every retained member before repairing or balancing. Pending targets contribute the
    // same load before and after completion, so completion itself cannot change the plan.
    for partition_id in partition_table.iter_ids() {
        let Some(state) = partitions.get(partition_id) else {
            continue;
        };
        let existing = state.next.as_ref().unwrap_or(&state.current);
        let retained = if state.current.is_valid() && state.placement_policy.is_frozen() {
            existing.clone()
        } else {
            PartitionConfiguration::new(
                partition_replication.clone(),
                existing
                    .replica_set()
                    .iter()
                    .copied()
                    .filter(|node| {
                        legacy_cluster_state.is_partition_processor_active(partition_id, node)
                    })
                    .chain(existing.replica_set().iter().copied().filter(|node| {
                        !legacy_cluster_state.is_partition_processor_active(partition_id, node)
                    }))
                    .filter(|node| candidates.contains(node))
                    .take(target_size)
                    .collect(),
                HashMap::default(),
            )
        };
        for node in retained.replica_set().iter() {
            *replica_loads.entry(*node).or_default() += 1;
        }
        plan.insert(*partition_id, retained);
    }

    for partition_id in partition_table.iter_ids().copied() {
        if plan
            .get(&partition_id)
            .is_some_and(|config| config.replica_set().len() >= target_size)
        {
            continue;
        }
        if partitions
            .get(&partition_id)
            .is_some_and(|state| state.current.is_valid() && state.placement_policy.is_frozen())
        {
            continue;
        }
        let selected = plan
            .get(&partition_id)
            .map(|config| config.replica_set().clone())
            .unwrap_or_default();
        // Remove this partition's contribution while selecting its replacement.
        for node in selected.iter() {
            *replica_loads
                .get_mut(node)
                .expect("retained member was counted") -= 1;
        }

        let mut replica_set = extend_top_n_load_balanced(
            candidates.iter().copied(),
            u64::from(partition_id),
            target_size,
            selected,
            DEFAULT_LOAD_BALANCING_TOP_N,
            |node_id| replica_loads.get(&node_id).copied().unwrap_or_default(),
        )
        .expect("candidate and target sizes were validated");

        // Keep an active current processor available while cold replacements catch up.
        if let Some(anchor) = partitions.get(&partition_id).and_then(|state| {
            select_replica_overlap_anchor(
                partition_id,
                &state.current,
                &replica_set,
                &candidates,
                legacy_cluster_state,
                &replica_loads,
            )
        }) {
            replica_set = extend_top_n_load_balanced(
                candidates.iter().copied(),
                u64::from(partition_id),
                target_size,
                NodeSet::from_single(anchor),
                DEFAULT_LOAD_BALANCING_TOP_N,
                |node_id| replica_loads.get(&node_id).copied().unwrap_or_default(),
            )
            .expect("the retained replica is an eligible candidate");
        }

        for node_id in replica_set.iter().copied() {
            *replica_loads.entry(node_id).or_default() += 1;
        }

        plan.insert(
            partition_id,
            PartitionConfiguration::new(
                partition_replication.clone(),
                replica_set,
                HashMap::default(),
            ),
        );
    }

    if rebalance {
        for partition_id in partition_table.iter_ids() {
            let Some(state) = partitions.get(partition_id) else {
                continue;
            };
            // Finish repairs and pending transitions before considering a discretionary move.
            if state.placement_policy.is_frozen() || state.next.is_some() {
                continue;
            }
            let config = plan
                .get_mut(partition_id)
                .expect("every partition was planned");
            if state.current.replication() != partition_replication
                || !state
                    .current
                    .replica_set()
                    .is_equivalent(config.replica_set())
            {
                continue;
            }
            let destination = extend_top_n_load_balanced(
                candidates
                    .iter()
                    .copied()
                    .filter(|node| !config.replica_set().contains(*node)),
                u64::from(*partition_id),
                1,
                NodeSet::new(),
                DEFAULT_LOAD_BALANCING_TOP_N,
                |node| replica_loads.get(&node).copied().unwrap_or_default(),
            )
            .and_then(|set| set.iter().next().copied());
            let Some(destination) = destination else {
                continue;
            };
            let active_count = config
                .replica_set()
                .iter()
                .filter(|node| {
                    legacy_cluster_state.is_partition_processor_active(partition_id, node)
                })
                .count();
            if active_count == 0 {
                continue;
            }
            let source = config
                .replica_set()
                .iter()
                .copied()
                .filter(|source| {
                    if replica_loads[source]
                        < replica_loads.get(&destination).copied().unwrap_or_default() + 2
                    {
                        return false;
                    }
                    // Never remove the only reported-active current member, or the only
                    // current member when no processor is reported active.
                    config.replica_set().len() > 1
                        && (active_count != 1
                            || !legacy_cluster_state
                                .is_partition_processor_active(partition_id, source))
                })
                .max_by_key(|source| {
                    (
                        replica_loads[source],
                        Reverse(hash_node_id(u64::from(*partition_id), *source)),
                        Reverse(u32::from(*source)),
                    )
                });
            if let Some(source) = source {
                let mut members = config.replica_set().clone();
                members.remove(source);
                members.insert(destination);
                *replica_loads.get_mut(&source).expect("source was counted") -= 1;
                *replica_loads.entry(destination).or_default() += 1;
                *config = PartitionConfiguration::new(
                    partition_replication.clone(),
                    members,
                    HashMap::default(),
                );
            }
        }
    }

    Some(plan)
}

fn ensure_balanced_leaders(
    partitions: &mut HashMap<PartitionId, PartitionState>,
    cluster_state: &ClusterState,
    legacy_cluster_state: &LegacyClusterState,
    nodes_config: &NodesConfiguration,
    partition_table: &PartitionTable,
    rebalance: bool,
) {
    let mut leader_loads = HashMap::<PlainNodeId, usize>::default();
    let mut retained = HashMap::default();

    // Count fixed leaders first so failovers account for all surviving leadership load.
    for partition_id in partition_table.iter_ids() {
        let Some(partition) = partitions.get(partition_id) else {
            continue;
        };
        let leader = if partition.leadership_policy.freeze.is_some() {
            partition.target_leader
        } else {
            select_balanced_leader(
                partition_id,
                partition,
                cluster_state,
                legacy_cluster_state,
                nodes_config,
                &HashMap::default(),
                true,
            )
            .filter(|leader| {
                is_incumbent_leader(partition_id, *leader, partition, legacy_cluster_state)
            })
        };
        if let Some(leader) = leader {
            retained.insert(*partition_id, leader);
            *leader_loads.entry(leader).or_default() += 1;
        }
    }

    for partition_id in partition_table.iter_ids() {
        let Some(partition) = partitions.get_mut(partition_id) else {
            continue;
        };
        if let Some(leader) = retained.get(partition_id) {
            partition.target_leader = Some(*leader);
            continue;
        }
        if partition.leadership_policy.freeze.is_some() {
            continue;
        }

        let Some(leader) = select_balanced_leader(
            partition_id,
            partition,
            cluster_state,
            legacy_cluster_state,
            nodes_config,
            &leader_loads,
            false,
        ) else {
            continue;
        };

        *leader_loads.entry(leader).or_default() += 1;
        if partition.target_leader != Some(leader) {
            debug!(
                "Selecting node {} as partition processor leader for partition {partition_id}",
                leader
            );
            partition.target_leader = Some(leader);
        }
    }

    if rebalance {
        for partition_id in partition_table.iter_ids() {
            let Some(partition) = partitions.get_mut(partition_id) else {
                continue;
            };
            if partition.leadership_policy.freeze.is_some() {
                continue;
            }
            let Some(source) = partition.target_leader else {
                continue;
            };
            if !legacy_cluster_state.is_partition_processor_active(partition_id, &source) {
                continue;
            }
            let Some(destination) = select_balanced_leader(
                partition_id,
                partition,
                cluster_state,
                legacy_cluster_state,
                nodes_config,
                &leader_loads,
                false,
            ) else {
                continue;
            };
            // A transfer reduces sum(load^2) strictly; ties never cause a leadership change.
            if leader_loads.get(&source).copied().unwrap_or_default()
                >= leader_loads.get(&destination).copied().unwrap_or_default() + 2
            {
                *leader_loads.get_mut(&source).expect("leader was counted") -= 1;
                *leader_loads.entry(destination).or_default() += 1;
                partition.target_leader = Some(destination);
            }
        }
    }
}

fn is_incumbent_leader(
    partition_id: &PartitionId,
    node_id: PlainNodeId,
    partition: &PartitionState,
    legacy_cluster_state: &LegacyClusterState,
) -> bool {
    partition.target_leader == Some(node_id)
        || (partition.target_leader.is_none()
            && legacy_cluster_state.runs_partition_processor_leader(&node_id, partition_id))
}

fn leader_readiness_rank(
    partition_id: &PartitionId,
    node_id: PlainNodeId,
    partition: &PartitionState,
    legacy_cluster_state: &LegacyClusterState,
    nodes_config: &NodesConfiguration,
) -> u8 {
    let has_affinity = partition
        .leadership_policy
        .affinity
        .as_ref()
        .is_some_and(|affinity| matches_affinity(node_id, affinity, nodes_config));
    let is_caught_up = legacy_cluster_state.is_partition_processor_active(partition_id, &node_id);
    match (has_affinity, is_caught_up) {
        (true, true) => 0,
        (false, true) => 1,
        (true, false) => 2,
        (false, false) => 3,
    }
}

fn select_balanced_leader(
    partition_id: &PartitionId,
    partition: &PartitionState,
    cluster_state: &ClusterState,
    legacy_cluster_state: &LegacyClusterState,
    nodes_config: &NodesConfiguration,
    leader_loads: &HashMap<PlainNodeId, usize>,
    preserve_incumbent: bool,
) -> Option<PlainNodeId> {
    partition
        .current
        .replica_set()
        .iter()
        .copied()
        .filter(|node_id| cluster_state.is_alive(NodeId::from(*node_id)))
        .min_by_key(|node_id| {
            (
                leader_readiness_rank(
                    partition_id,
                    *node_id,
                    partition,
                    legacy_cluster_state,
                    nodes_config,
                ),
                preserve_incumbent
                    && !is_incumbent_leader(
                        partition_id,
                        *node_id,
                        partition,
                        legacy_cluster_state,
                    ),
                leader_loads.get(node_id).copied().unwrap_or_default(),
                Reverse(hash_node_id(u64::from(*partition_id), *node_id)),
                u32::from(*node_id),
            )
        })
}

fn requires_reconfiguration_to(
    partition_state: &PartitionState,
    default_replication: &ReplicationProperty,
    planned: &PartitionConfiguration,
) -> bool {
    if let Some(next) = partition_state.next.as_ref() {
        next.replication() != default_replication
            || !next.replica_set().is_equivalent(planned.replica_set())
    } else {
        partition_state.current.replication() != default_replication
            || !partition_state
                .current
                .replica_set()
                .is_equivalent(planned.replica_set())
    }
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
        let partition_replication = partition_table.replication_property(nodes_config);
        if experimental_balanced_placement_enabled()
            && supports_balanced_placement(&partition_replication)
        {
            ensure_balanced_leaders(
                &mut self.partitions,
                cluster_state,
                legacy_cluster_state,
                nodes_config,
                partition_table,
                experimental_rebalances_when_healthy()
                    && all_worker_candidates_alive(nodes_config, cluster_state),
            );
            return;
        }

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

        let partition_replication = partition_table.replication_property(nodes_config);
        let balanced_plan = if experimental_balanced_placement_enabled() {
            plan_balanced_partition_placements(
                &self.partitions,
                nodes_config,
                partition_table,
                cluster_state,
                legacy_cluster_state,
                &partition_replication,
            )
        } else {
            None
        };
        // Replica placement may fall back when too few workers are alive; leader selection
        // must still use the same policy as the first leadership pass.
        let use_balanced_placement = experimental_balanced_placement_enabled()
            && supports_balanced_placement(&partition_replication);

        for partition_id in partition_table.iter_ids().copied() {
            let entry = self.partitions.entry(partition_id);

            // make sure that we have a valid partition processor configuration
            let mut occupied_entry = match entry {
                Entry::Occupied(mut entry) if entry.get().current.is_valid() => {
                    let planned = balanced_plan
                        .as_ref()
                        .and_then(|plan| plan.get(&partition_id));
                    let requires_reconfiguration = planned
                        .map(|planned| {
                            requires_reconfiguration_to(
                                entry.get(),
                                &partition_replication,
                                planned,
                            )
                        })
                        .unwrap_or_else(|| {
                            Self::requires_reconfiguration(
                                partition_id,
                                entry.get(),
                                &partition_replication,
                                nodes_config,
                                &self.cluster_state,
                            )
                        });

                    if !entry.get().placement_policy.is_frozen() && requires_reconfiguration {
                        trace!("Partition {} requires reconfiguration", partition_id);

                        let next = planned.cloned().or_else(|| {
                            Self::choose_partition_configuration(
                                partition_id,
                                nodes_config,
                                partition_replication.clone(),
                                NodeSet::new(),
                                &self.cluster_state,
                            )
                        });

                        if let Some(next) = next {
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
                    // no or no valid current configuration, pick a valid configuration
                    let current = balanced_plan
                        .as_ref()
                        .and_then(|plan| plan.get(&partition_id))
                        .cloned()
                        .or_else(|| {
                            Self::choose_partition_configuration(
                                partition_id,
                                nodes_config,
                                partition_replication.clone(),
                                NodeSet::default(),
                                &self.cluster_state,
                            )
                        });
                    if let Some(current) = current {
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

            if !use_balanced_placement {
                // select the leader based on the observed cluster state
                self.select_leader(
                    &partition_id,
                    cluster_state,
                    legacy_cluster_state,
                    nodes_config,
                );
            }
        }

        if use_balanced_placement {
            ensure_balanced_leaders(
                &mut self.partitions,
                cluster_state,
                legacy_cluster_state,
                nodes_config,
                partition_table,
                experimental_rebalances_when_healthy()
                    && all_worker_candidates_alive(nodes_config, cluster_state),
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

        let best = partition
            .current
            .replica_set()
            .iter()
            .copied()
            .filter(|node_id| cluster_state.is_alive(NodeId::from(*node_id)))
            .max_by_key(|node_id| {
                Reverse(leader_readiness_rank(
                    partition_id,
                    *node_id,
                    partition,
                    legacy_cluster_state,
                    nodes_config,
                ))
            });

        if let Some(best) = best
            && partition.target_leader != Some(best)
        {
            debug!(
                "Selecting node {} as partition processor leader for partition {partition_id}",
                best
            );
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
    use restate_core::network::FailingConnector;
    use restate_types::metadata::Precondition;
    use restate_types::nodes_config::{Role, WorkerConfig};
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

#[cfg(test)]
mod balanced_placement_tests {
    use std::time::Duration;

    use restate_types::cluster::cluster_state::{
        AliveNode, NodeState as LegacyNodeState, PartitionProcessorStatus, ReplayStatus,
    };
    use restate_types::cluster_state::NodeState;
    use restate_types::config::{ExperimentalPlacementRebalanceMode, set_current_config};
    use restate_types::nodes_config::{Role, WorkerConfig, WorkerState};
    use restate_types::partition_table::PartitionReplication;
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{GenerationalNodeId, RestateVersion};

    use super::*;

    fn set_rebalance_mode(mode: ExperimentalPlacementRebalanceMode) {
        let mut config = Configuration::default();
        config.common.experimental_placement_rebalance_mode = mode;
        set_current_config(config);
    }

    fn active_plan(plan: &HashMap<PartitionId, PartitionConfiguration>) -> LegacyClusterState {
        let mut state = LegacyClusterState::empty();
        let nodes: NodeSet = plan
            .values()
            .flat_map(|config| config.replica_set().iter().copied())
            .collect();
        for node in nodes.iter() {
            state.nodes.insert(
                *node,
                LegacyNodeState::Alive(AliveNode {
                    last_heartbeat_at: MillisSinceEpoch::now(),
                    generational_node_id: GenerationalNodeId::new(u32::from(*node), 1),
                    partitions: plan
                        .iter()
                        .filter(|(_, config)| config.replica_set().contains(*node))
                        .map(|(id, _)| {
                            (
                                *id,
                                PartitionProcessorStatus {
                                    replay_status: ReplayStatus::Active,
                                    ..Default::default()
                                },
                            )
                        })
                        .collect(),
                    uptime: Duration::ZERO,
                }),
            );
        }
        state
    }

    fn node_id(id: u32) -> PlainNodeId {
        PlainNodeId::new(id)
    }

    fn node_index(node_id: PlainNodeId) -> usize {
        usize::try_from(u32::from(node_id) - 1).expect("node id should fit into usize")
    }

    fn worker_node(id: u32) -> NodeConfig {
        NodeConfig::builder()
            .name(format!("node-{id}"))
            .current_generation(GenerationalNodeId::new(id, 1))
            .address(format!("unix:/tmp/scheduler-test-{id}").parse().unwrap())
            .roles(Role::Worker.into())
            .worker_config(WorkerConfig {
                worker_state: WorkerState::Active,
            })
            .binary_version(RestateVersion::current())
            .build()
    }

    fn active_worker_nodes(count: u32) -> NodesConfiguration {
        let mut nodes_config = NodesConfiguration::new_for_testing();
        for id in 1..=count {
            nodes_config.upsert_node(worker_node(id));
        }
        nodes_config
    }

    fn alive_cluster_state(count: u32) -> ClusterState {
        cluster_state_with_alive_nodes(1..=count)
    }

    fn cluster_state_with_alive_nodes(alive_nodes: impl IntoIterator<Item = u32>) -> ClusterState {
        let cluster_state = ClusterState::default();
        let mut updater = cluster_state.clone().updater();
        for id in alive_nodes {
            updater.upsert_node_state(GenerationalNodeId::new(id, 1), NodeState::Alive);
        }
        cluster_state
    }

    fn partition_table(partitions: u16, replication: ReplicationProperty) -> PartitionTable {
        let mut builder =
            PartitionTable::with_equally_sized_partitions(Version::MIN, partitions).into_builder();
        builder.set_partition_replication(PartitionReplication::Limit(replication));
        builder.build()
    }

    fn load_range(loads: &[usize]) -> usize {
        loads.iter().max().unwrap() - loads.iter().min().unwrap()
    }

    fn partition_states_from_plan(
        plan: &HashMap<PartitionId, PartitionConfiguration>,
    ) -> HashMap<PartitionId, PartitionState> {
        plan.iter()
            .map(|(partition_id, configuration)| {
                (
                    *partition_id,
                    PartitionState::new(
                        configuration.clone(),
                        None,
                        LeadershipPolicy::default(),
                        PlacementPolicy::default(),
                    ),
                )
            })
            .collect()
    }

    fn count_changed_replica_sets(
        left: &HashMap<PartitionId, PartitionConfiguration>,
        right: &HashMap<PartitionId, PartitionConfiguration>,
    ) -> usize {
        left.iter()
            .filter(|(partition_id, left)| {
                right
                    .get(partition_id)
                    .is_some_and(|right| !left.replica_set().is_equivalent(right.replica_set()))
            })
            .count()
    }

    fn count_replica_sets_containing(
        plan: &HashMap<PartitionId, PartitionConfiguration>,
        node_id: PlainNodeId,
    ) -> usize {
        plan.values()
            .filter(|configuration| configuration.replica_set().contains(node_id))
            .count()
    }

    fn legacy_cluster_state_with_active_processor(
        partition_id: PartitionId,
        active_node: PlainNodeId,
    ) -> LegacyClusterState {
        let status = PartitionProcessorStatus {
            replay_status: ReplayStatus::Active,
            ..PartitionProcessorStatus::default()
        };

        let mut state = LegacyClusterState::empty();
        state.nodes.insert(
            active_node,
            LegacyNodeState::Alive(AliveNode {
                last_heartbeat_at: MillisSinceEpoch::now(),
                generational_node_id: GenerationalNodeId::new(u32::from(active_node), 1),
                partitions: [(partition_id, status)].into_iter().collect(),
                uptime: Duration::ZERO,
            }),
        );
        state
    }

    #[test]
    fn balanced_partition_plan_spreads_replica_load() {
        let nodes_config = active_worker_nodes(5);
        let cluster_state = alive_cluster_state(5);
        let replication = ReplicationProperty::new_unchecked(3);
        let partition_table = partition_table(200, replication.clone());

        let plan = plan_balanced_partition_placements(
            &HashMap::default(),
            &nodes_config,
            &partition_table,
            &cluster_state,
            &LegacyClusterState::empty(),
            &replication,
        )
        .expect("balanced placement should be supported");

        assert_eq!(plan.len(), 200);
        let mut loads = vec![0; 5];
        for configuration in plan.values() {
            assert_eq!(configuration.replica_set().len(), 3);
            for node_id in configuration.replica_set().iter().copied() {
                loads[node_index(node_id)] += 1;
            }
        }
        assert!(
            load_range(&loads) <= 1,
            "replica load should be near-ideal: {loads:?}"
        );
    }

    #[test]
    fn balanced_partition_plan_repairs_down_node_without_cascading_churn() {
        set_rebalance_mode(ExperimentalPlacementRebalanceMode::Rebalance);
        for (node_count, partition_count, rf) in
            std::iter::once((5, 200, 3)).chain([3, 5].into_iter().flat_map(|nodes| {
                [24, 48, 96, 128]
                    .into_iter()
                    .map(move |partitions| (nodes, partitions, 2))
            }))
        {
            let nodes_config = active_worker_nodes(node_count);
            let replication = ReplicationProperty::new_unchecked(rf);
            let partition_table = partition_table(partition_count, replication.clone());

            let all_alive = alive_cluster_state(node_count);
            let initial = plan_balanced_partition_placements(
                &HashMap::default(),
                &nodes_config,
                &partition_table,
                &all_alive,
                &LegacyClusterState::empty(),
                &replication,
            )
            .expect("balanced placement should be supported");
            let partitions_with_down_node =
                count_replica_sets_containing(&initial, node_id(node_count));

            let node_down = cluster_state_with_alive_nodes(1..node_count);
            let repaired = plan_balanced_partition_placements(
                &partition_states_from_plan(&initial),
                &nodes_config,
                &partition_table,
                &node_down,
                &LegacyClusterState::empty(),
                &replication,
            )
            .expect("balanced placement should be supported");
            let repaired_changes = count_changed_replica_sets(&initial, &repaired);

            set_rebalance_mode(ExperimentalPlacementRebalanceMode::RepairOnly);
            let repair_only = plan_balanced_partition_placements(
                &partition_states_from_plan(&repaired),
                &nodes_config,
                &partition_table,
                &all_alive,
                &active_plan(&repaired),
                &replication,
            )
            .unwrap();
            assert_eq!(count_changed_replica_sets(&repaired, &repair_only), 0);
            set_rebalance_mode(ExperimentalPlacementRebalanceMode::Rebalance);
            let unobserved = plan_balanced_partition_placements(
                &partition_states_from_plan(&repaired),
                &nodes_config,
                &partition_table,
                &all_alive,
                &LegacyClusterState::empty(),
                &replication,
            )
            .unwrap();
            assert_eq!(
                count_changed_replica_sets(&repaired, &unobserved),
                0,
                "wait for processor reports before discretionary moves"
            );

            let restored = plan_balanced_partition_placements(
                &partition_states_from_plan(&repaired),
                &nodes_config,
                &partition_table,
                &all_alive,
                &active_plan(&repaired),
                &replication,
            )
            .expect("balanced placement should be supported");
            let up_transition_changes = count_changed_replica_sets(&repaired, &restored);

            assert_eq!(initial.len(), usize::from(partition_count));
            assert_eq!(repaired.len(), initial.len());
            assert_eq!(restored.len(), initial.len());
            assert_eq!(repaired_changes, partitions_with_down_node);
            assert!(
                up_transition_changes
                    <= (usize::from(partition_count) * usize::from(rf))
                        .div_ceil(node_count as usize)
            );
            let loads: Vec<_> = (1..=node_count)
                .map(|id| count_replica_sets_containing(&restored, node_id(id)))
                .collect();
            assert!(
                load_range(&loads) <= 1,
                "restored placement is skewed: {loads:?}"
            );

            let stable = plan_balanced_partition_placements(
                &partition_states_from_plan(&restored),
                &nodes_config,
                &partition_table,
                &all_alive,
                &active_plan(&restored),
                &replication,
            )
            .unwrap();
            assert_eq!(count_changed_replica_sets(&restored, &stable), 0);
        }
    }

    #[test]
    fn balanced_plan_preserves_fair_placements_through_partial_completion() {
        set_rebalance_mode(ExperimentalPlacementRebalanceMode::Rebalance);
        let nodes = active_worker_nodes(3);
        let alive = alive_cluster_state(3);
        let replication = ReplicationProperty::new_unchecked(2);
        let table = partition_table(48, replication.clone());
        let initial = plan_balanced_partition_placements(
            &HashMap::default(),
            &nodes,
            &table,
            &alive,
            &LegacyClusterState::empty(),
            &replication,
        )
        .unwrap();
        // Another equally fair placement need not match the plan for an empty cluster.
        let desired: HashMap<_, _> = initial
            .iter()
            .map(|(id, config)| {
                (
                    *id,
                    PartitionConfiguration::new(
                        replication.clone(),
                        config
                            .replica_set()
                            .iter()
                            .map(|node| node_id(u32::from(*node) % 3 + 1))
                            .collect(),
                        HashMap::default(),
                    ),
                )
            })
            .collect();
        let mut states = partition_states_from_plan(&desired);
        let observed = active_plan(&desired);
        for id in table.iter_ids().filter(|id| u16::from(**id) % 2 == 0) {
            let state = states.get_mut(id).unwrap();
            state.current = initial[id].clone();
            state.next = Some(desired[id].clone());
        }
        let pending = plan_balanced_partition_placements(
            &states,
            &nodes,
            &table,
            &alive,
            &observed,
            &replication,
        )
        .unwrap();
        assert_eq!(count_changed_replica_sets(&desired, &pending), 0);
        let completed = plan_balanced_partition_placements(
            &partition_states_from_plan(&pending),
            &nodes,
            &table,
            &alive,
            &observed,
            &replication,
        )
        .unwrap();
        assert_eq!(count_changed_replica_sets(&pending, &completed), 0);
    }

    #[test]
    fn balanced_plan_accounts_for_frozen_and_pending_placements() {
        use restate_types::partitions::placement_policy::PlacementFreeze;

        let nodes_config = active_worker_nodes(3);
        let cluster_state = alive_cluster_state(3);
        let replication = ReplicationProperty::new_unchecked(1);
        let table = partition_table(24, replication.clone());
        let mut partitions = HashMap::default();
        for id in 16..24 {
            let current = PartitionConfiguration::new(
                replication.clone(),
                NodeSet::from_single(node_id(1)),
                HashMap::default(),
            );
            let mut state = PartitionState::new(
                current,
                None,
                LeadershipPolicy::default(),
                PlacementPolicy::default(),
            );
            if id < 20 {
                state.placement_policy.freeze = Some(PlacementFreeze {
                    reason: "maintenance".to_owned(),
                });
            } else {
                state.next = Some(state.current.clone());
            }
            partitions.insert(PartitionId::from(id), state);
        }
        let plan = plan_balanced_partition_placements(
            &partitions,
            &nodes_config,
            &table,
            &cluster_state,
            &LegacyClusterState::empty(),
            &replication,
        )
        .unwrap();
        let mut loads = [0; 3];
        for (id, config) in &plan {
            if u16::from(*id) >= 16 {
                assert!(config.replica_set().contains(node_id(1)));
            }
            for node in config.replica_set().iter() {
                loads[node_index(*node)] += 1;
            }
        }
        assert_eq!(loads, [8, 8, 8]);
    }

    #[test]
    fn balanced_partition_plan_retains_a_current_replica() {
        set_rebalance_mode(ExperimentalPlacementRebalanceMode::Rebalance);
        let nodes_config = active_worker_nodes(5);
        let cluster_state = alive_cluster_state(5);
        let legacy_cluster_state = LegacyClusterState::empty();
        let replication = ReplicationProperty::new_unchecked(2);
        let partition_table = partition_table(200, replication.clone());

        let ideal = plan_balanced_partition_placements(
            &HashMap::default(),
            &nodes_config,
            &partition_table,
            &cluster_state,
            &legacy_cluster_state,
            &replication,
        )
        .expect("balanced placement should be supported");
        let current = ideal
            .iter()
            .map(|(partition_id, configuration)| {
                let replica_set = (1..=5)
                    .map(node_id)
                    .filter(|node_id| !configuration.replica_set().contains(*node_id))
                    .take(2)
                    .collect();
                (
                    *partition_id,
                    PartitionState::new(
                        PartitionConfiguration::new(
                            replication.clone(),
                            replica_set,
                            HashMap::default(),
                        ),
                        None,
                        LeadershipPolicy::default(),
                        PlacementPolicy::default(),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();

        let plan = plan_balanced_partition_placements(
            &current,
            &nodes_config,
            &partition_table,
            &cluster_state,
            &active_plan(
                &current
                    .iter()
                    .map(|(id, state)| (*id, state.current.clone()))
                    .collect(),
            ),
            &replication,
        )
        .expect("balanced placement should be supported");

        let mut loads = vec![0; 5];
        for (partition_id, configuration) in &plan {
            let current_replica_set = current[partition_id].current.replica_set();
            assert!(
                configuration
                    .replica_set()
                    .iter()
                    .any(|node_id| current_replica_set.contains(*node_id)),
                "partition {partition_id} lost every current replica"
            );
            for node_id in configuration.replica_set().iter().copied() {
                loads[node_index(node_id)] += 1;
            }
        }
        assert!(
            load_range(&loads) <= 1,
            "replica load should remain near-ideal: {loads:?}"
        );
    }

    #[test]
    fn replica_overlap_prefers_a_warm_current_processor() {
        let partition_id = PartitionId::from(1);
        let replication = ReplicationProperty::new_unchecked(2);
        let current = PartitionConfiguration::new(
            replication,
            [node_id(1), node_id(2)].into_iter().collect(),
            HashMap::default(),
        );
        let planned = [node_id(2), node_id(3)].into_iter().collect();
        let candidates = [node_id(1), node_id(2), node_id(3)];
        let replica_loads = HashMap::default();

        let warm_current = legacy_cluster_state_with_active_processor(partition_id, node_id(1));
        assert_eq!(
            select_replica_overlap_anchor(
                partition_id,
                &current,
                &planned,
                &candidates,
                &warm_current,
                &replica_loads,
            ),
            Some(node_id(1)),
            "cold overlap must not displace a warm current processor"
        );

        let warm_overlap = legacy_cluster_state_with_active_processor(partition_id, node_id(2));
        assert_eq!(
            select_replica_overlap_anchor(
                partition_id,
                &current,
                &planned,
                &candidates,
                &warm_overlap,
                &replica_loads,
            ),
            None,
            "an already-planned warm current processor needs no anchor"
        );

        let warm_next = legacy_cluster_state_with_active_processor(partition_id, node_id(3));
        let disjoint = NodeSet::from([3, 4]);
        assert_eq!(
            select_replica_overlap_anchor(
                partition_id,
                &current,
                &disjoint,
                &[node_id(1), node_id(2), node_id(3), node_id(4)],
                &warm_next,
                &replica_loads
            ),
            None,
            "a warm pending member must not be replaced by a cold current member"
        );

        let nodes = active_worker_nodes(3);
        let table = partition_table(2, ReplicationProperty::new_unchecked(2));
        let larger = PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(3),
            NodeSet::from([1, 2, 3]),
            HashMap::default(),
        );
        let states = partition_states_from_plan(&[(partition_id, larger)].into_iter().collect());
        let shrunk = plan_balanced_partition_placements(
            &states,
            &nodes,
            &table,
            &alive_cluster_state(3),
            &warm_next,
            &ReplicationProperty::new_unchecked(2),
        )
        .unwrap();
        assert!(
            shrunk[&partition_id].replica_set().contains(node_id(3)),
            "shrinking replication must retain the warm member"
        );
    }

    #[test]
    fn balanced_leader_selection_moves_leaders_off_overloaded_node() {
        let nodes_config = active_worker_nodes(3);
        let cluster_state = alive_cluster_state(3);
        let replication = ReplicationProperty::new_unchecked(3);
        let partition_table = partition_table(90, replication.clone());
        let replica_set = NodeSet::from_iter([node_id(1), node_id(2), node_id(3)]);
        let mut partitions = HashMap::default();

        for partition_id in partition_table.iter_ids().copied() {
            let mut partition = PartitionState::new(
                PartitionConfiguration::new(
                    replication.clone(),
                    replica_set.clone(),
                    HashMap::default(),
                ),
                None,
                LeadershipPolicy::default(),
                PlacementPolicy::default(),
            );
            partition.target_leader = Some(node_id(1));
            partitions.insert(partition_id, partition);
        }

        let legacy_cluster_state = active_plan(
            &partitions
                .iter()
                .map(|(id, state)| (*id, state.current.clone()))
                .collect(),
        );
        // Repair-only must leave viable leaders alone even when their distribution is skewed.
        ensure_balanced_leaders(
            &mut partitions,
            &cluster_state,
            &legacy_cluster_state,
            &nodes_config,
            &partition_table,
            false,
        );
        assert!(
            partitions
                .values()
                .all(|state| state.target_leader == Some(node_id(1)))
        );

        // A failed node must still trigger failover, with no further movement when it returns.
        let node_down = cluster_state_with_alive_nodes(2..=3);
        ensure_balanced_leaders(
            &mut partitions,
            &node_down,
            &legacy_cluster_state,
            &nodes_config,
            &partition_table,
            false,
        );
        let failed_over: HashMap<_, _> = partitions
            .iter()
            .map(|(id, state)| (*id, state.target_leader))
            .collect();
        assert!(
            failed_over
                .values()
                .all(|leader| leader.is_some() && *leader != Some(node_id(1)))
        );
        ensure_balanced_leaders(
            &mut partitions,
            &cluster_state,
            &legacy_cluster_state,
            &nodes_config,
            &partition_table,
            false,
        );
        assert!(
            partitions
                .iter()
                .all(|(id, state)| state.target_leader == failed_over[id])
        );

        ensure_balanced_leaders(
            &mut partitions,
            &cluster_state,
            &legacy_cluster_state,
            &nodes_config,
            &partition_table,
            true,
        );

        let mut loads = vec![0; 3];
        for partition in partitions.values() {
            let leader = partition.target_leader.expect("leader should be selected");
            loads[node_index(leader)] += 1;
        }

        assert!(
            loads[0] < 90,
            "node 1 should not keep every leader: {loads:?}"
        );
        assert!(
            load_range(&loads) <= 1,
            "leader load should be near-ideal: {loads:?}"
        );
        for state in partitions.values_mut() {
            state.target_leader = state
                .target_leader
                .map(|node| node_id(u32::from(node) % 3 + 1));
        }
        let already_fair: HashMap<_, _> = partitions
            .iter()
            .map(|(id, state)| (*id, state.target_leader))
            .collect();
        ensure_balanced_leaders(
            &mut partitions,
            &cluster_state,
            &legacy_cluster_state,
            &nodes_config,
            &partition_table,
            true,
        );
        assert!(
            partitions
                .iter()
                .all(|(id, state)| state.target_leader == already_fair[id]),
            "equally fair leaders must not be reshuffled"
        );
    }
}
