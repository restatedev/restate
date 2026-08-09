// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ahash::{HashMap, HashMapExt};
use itertools::Itertools;
use metrics::{counter, gauge};
use rand::seq::{IteratorRandom, SliceRandom};
use tokio::time::Instant;
use tracing::{debug, error, warn};

use restate_core::network::{ConnectThrottle, NetworkSender};
use restate_types::cluster_state::ClusterStateUpdater;
use restate_types::config::GossipOptions;
use restate_types::identifiers::PartitionId;
use restate_types::net::node::{ClusterStateReply, Gossip, GossipFlags};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partitions::state::{MembershipState, PartitionReplicaSetStates};
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, PlainNodeId, Version, net};

use crate::metric_definitions::{
    GOSSIP_INSTANCE, GOSSIP_LONELY, GOSSIP_NODES, GOSSIP_RECEIVED, STATE_ALIVE, STATE_DEAD,
    STATE_FAILING_OVER, STATE_SUSPECT,
};

use super::node_state::{Node, NodeState};

const GOSSIP_ATTEMPT_LIMIT: usize = 20;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("This node has been preempted, a higher generation has been observed in nodes configuration. current={} preempted_by={}", .my_node_id, .preempted_by)]
    Preempted {
        my_node_id: GenerationalNodeId,
        preempted_by: GenerationalNodeId,
    },
    #[error("This node has been removed from nodes configuration")]
    NodeRemovedFromConfig,
}

/// General Design Notes
///
/// Gossip can be faster to propagate than nodes configuration metadata between nodes, a new node
/// starting up will likely broadcast its bring-up message before their peers have acquired the
/// newly updated nodes configuration that this node is part of. Therefore, we allow our own state
/// to be updated with nodes that we don't know about yet, but following a certain set of merge
/// rules:
/// 1. If the node's plain id is already recognized by the current configuration, we can only
///    accept the gossip message if the generation is the same or higher.
/// 2. If the gossip message is coming from a node that's been tombstone-ed in the current
///    configuration, we ignore the gossip message.
/// 3. For those unknown nodes, we record the nodes configuration version that arrived with their message
///    and we'll use that to determine if we should keep this node or not when we observe nodes
///    configuration updates. The scenario we want to avoid is that we have a node that we learned
///    about its liveness through gossip, and then we receive a nodes configuration update that
///    doesn't contain this node yet (perhaps the node exists in a couple of versions away) and we
///    decide that this node is unknown and we remove it from our state table. We want to keep this
///    node in our table as long as we didn't observe a configuration update that's higher than the
///    one we learned about from this node's original gossip message.
/// 4. In outgoing gossip messages, we'll only include nodes that _exist_ in our current nodes
///    configuration version.
#[derive(derive_more::Debug)]
pub struct FdState {
    pub(super) my_instance_ts: MillisSinceEpoch,
    in_failover: bool,
    last_processed_nc_version: Version,
    pub(super) my_node_id: GenerationalNodeId,
    #[debug("{:?}", last_gossip_received_at.elapsed())]
    last_gossip_received_at: Instant,
    // show the elapsed in debug
    #[debug("{:?}", last_gossip_tick.elapsed())]
    last_gossip_tick: Instant,
    num_gossip_received: usize,
    node_states: HashMap<PlainNodeId, Node>,
    #[debug(skip)]
    replica_set_states: PartitionReplicaSetStates,
    #[debug(skip)]
    cs_updater: ClusterStateUpdater,
}

impl FdState {
    pub fn new(
        my_node_id: GenerationalNodeId,
        nodes_config: &NodesConfiguration,
        replica_set_states: PartitionReplicaSetStates,
        cs_updater: ClusterStateUpdater,
    ) -> Self {
        let now = Instant::now();
        let mut this = Self {
            my_instance_ts: MillisSinceEpoch::now(),
            in_failover: false,
            last_processed_nc_version: Version::INVALID,
            my_node_id,
            last_gossip_received_at: now,
            last_gossip_tick: now,
            num_gossip_received: 0,
            node_states: HashMap::with_capacity(nodes_config.len()),
            replica_set_states,
            cs_updater,
        };
        // make sure that our state is pre-seeded with our exact generation
        let my_instance_ts = this.my_instance_ts;
        gauge!(GOSSIP_INSTANCE).set(my_instance_ts.as_u64() as f64);
        let my_node = this.get_node_or_insert(my_node_id);
        my_node.maybe_reset(my_node_id, nodes_config.version(), my_instance_ts);

        // we assume that this nodes configuration has this node with the exact generation
        this.refresh_nodes_config(nodes_config)
            .expect("initializing with nodes config that doesn't contain this node");

        this
    }

    pub fn set_failover(&mut self) {
        self.in_failover = true;
    }

    /// Excluding myself
    pub fn peers(&mut self) -> impl Iterator<Item = (PlainNodeId, &mut Node)> {
        let my_node = self.my_node_id.as_plain();
        self.node_states
            .iter_mut()
            .filter_map(move |(node_id, node)| (my_node != *node_id).then_some((*node_id, node)))
    }

    pub fn all_node_states(&self) -> impl Iterator<Item = (GenerationalNodeId, NodeState)> {
        self.node_states
            .values()
            .map(|node| (node.gen_node_id, node.state))
    }

    pub fn partitions(&self) -> impl Iterator<Item = (PartitionId, MembershipState)> {
        self.replica_set_states.iter()
    }

    pub fn refresh_nodes_config(&mut self, nodes_config: &NodesConfiguration) -> Result<(), Error> {
        let my_node_id = self.my_node_id;
        let nc_version = nodes_config.version();
        if nc_version <= self.last_processed_nc_version {
            // save energy, we have already processed this update in a previous run
            return Ok(());
        }

        // Our goal is to synchronize the nodes configuration with the node states
        for (_, config) in nodes_config.iter() {
            // we want to know if this is potentially new node/generation or not
            let node = self.get_node_or_insert(config.current_generation);

            node.maybe_reset(
                config.current_generation,
                nc_version,
                MillisSinceEpoch::UNIX_EPOCH,
            );

            // Are we preempted by a higher generation?
            if my_node_id.is_same_but_different(&node.gen_node_id) {
                return Err(Error::Preempted {
                    my_node_id,
                    preempted_by: node.gen_node_id,
                });
            }
        }

        let mut removed_nodes = Vec::new();
        // remove nodes that are not in the new config
        self.node_states.retain(|node_id, state| {
            let keep = nodes_config.contains(node_id) || state.nc_version_witnessed() > nc_version;
            if !keep {
                removed_nodes.push(*node_id);
            }

            keep
        });

        // we have been removed from the config. We must shutdown.
        if !self.node_states.contains_key(&self.my_node_id.as_plain()) {
            return Err(Error::NodeRemovedFromConfig);
        }

        // update cluster state
        let mut cs_guard = self.cs_updater.write();
        for node in self.node_states.values() {
            cs_guard.upsert_node_state(node.gen_node_id, node.state.into());
        }
        for node_id in removed_nodes {
            cs_guard.remove_node(node_id);
        }

        Ok(())
    }

    pub fn is_stable(&self, opts: &GossipOptions) -> bool {
        // special case for standalone mode
        if self.node_states.len() == 1 {
            return true;
        }
        self.num_gossip_received >= (opts.gossip_fd_stability_threshold.get() as usize)
    }

    /// returns true if there has been at least a gossip interval since the last one.
    pub fn gossip_tick(&mut self, opts: &GossipOptions) -> bool {
        let now = Instant::now();
        assert!(!opts.gossip_tick_interval.is_zero());
        // calculate how many intervals we have missed to compensate for long stalls
        let full_intervals = self
            .last_gossip_tick
            .elapsed()
            .div_duration_f32(*opts.gossip_tick_interval)
            .floor() as u32;
        if full_intervals > 0 {
            for node in self.node_states.values_mut() {
                if node.gen_node_id.as_plain() == self.my_node_id.as_plain() {
                    // keeping ourselves alive
                    node.gossip_age = 0;
                } else {
                    node.gossip_age = node.gossip_age.saturating_add(full_intervals);
                }
            }

            self.last_gossip_tick = now;
            true
        } else {
            false
        }
    }

    pub fn is_lonely(&self, opts: &GossipOptions) -> bool {
        let intervals_since_last_received_gossip = self
            .last_gossip_received_at
            .elapsed()
            .div_duration_f32(*opts.gossip_tick_interval)
            .floor() as u32;
        intervals_since_last_received_gossip > opts.gossip_loneliness_threshold.get()
    }

    pub fn detect_peer_failures(&mut self, opts: &GossipOptions) {
        let is_standalone = self.node_states.len() == 1;

        let changed: Vec<_> = self
            .node_states
            .values_mut()
            .filter_map(|node| {
                let maybe_updated_state =
                    if node.gen_node_id.as_plain() == self.my_node_id.as_plain() {
                        node.maybe_update_state(opts, self.my_node_id, is_standalone)
                    } else {
                        node.maybe_update_state(opts, self.my_node_id, false)
                    };

                match maybe_updated_state {
                    Some(state) if state.is_potentially_alive() => {
                        // reset connection throttle to this node to allow fresh connection attempts to go
                        // through.
                        ConnectThrottle::reset(&restate_core::network::Destination::Node(
                            node.gen_node_id,
                        ));
                        Some(node)
                    }
                    Some(_) => Some(node),
                    None => None,
                }
            })
            .collect();

        // some nodes states have changed
        if !changed.is_empty() {
            let mut guard = self.cs_updater.read();
            for node in changed {
                // update the cluster state with the new state
                guard.set_node_state(node.gen_node_id, node.state.into());
            }
        }
    }

    pub fn update_from_cluster_state_message(
        &mut self,
        opts: &GossipOptions,
        cs_reply: ClusterStateReply,
    ) {
        for incoming_node in cs_reply.nodes {
            if incoming_node.node_id == self.my_node_id {
                // We already know our own state, no need to update it.
                continue;
            }

            if incoming_node.state == net::node::NodeState::Alive {
                let node = self.get_node_or_insert(incoming_node.node_id);
                // reset the age of the node if it's alive
                node.gossip_age = 0;
                node.state = NodeState::Alive;
                self.cs_updater
                    .upsert_node_state(incoming_node.node_id, NodeState::Alive.into());
            }
        }

        for partition in cs_reply.partitions {
            self.replica_set_states.note_observed_membership(
                partition.id,
                partition.current_leader,
                &partition.observed_current_membership,
                &partition.observed_next_membership,
            );
        }
        debug!("{}", self.dump_as_string(opts));
    }

    pub fn update_from_gossip_message(
        &mut self,
        opts: &GossipOptions,
        sender_id: GenerationalNodeId,
        sender_nc_version: Version,
        msg: Gossip,
    ) {
        let is_sender_lonely = msg.flags.intersects(GossipFlags::FeelingLonely);
        let is_sender_in_failover = msg.flags.intersects(GossipFlags::FailingOver);

        // Depending on which flags are set, this could carry info about other nodes
        // or a special message from the sender itself.
        if msg.flags.intersects(GossipFlags::Special) {
            let my_node_id = self.my_node_id;
            let sender_node = self.get_node_or_insert(sender_id);
            let new_state = if msg.flags.intersects(GossipFlags::BringUp) {
                // Note that we do not touch the age in this case, we are not ready to consider this as a
                // suspect node.
                let old_state = sender_node.state;
                sender_node.maybe_reset(sender_id, sender_nc_version, msg.instance_ts);
                let new_state = sender_node.state;
                (old_state != new_state).then_some(new_state)
            } else if msg.flags.intersects(GossipFlags::ReadyToServe) {
                // mark the sender alive
                sender_node.maybe_reset(sender_id, sender_nc_version, msg.instance_ts);
                sender_node.gossip_age = 0;
                sender_node.maybe_update_state(opts, my_node_id, false)
            } else if is_sender_in_failover {
                // sender is failing over
                sender_node.maybe_reset(sender_id, sender_nc_version, msg.instance_ts);
                // We don't update gossip_age of lonely sender as by doing so, we'll falsely propagate
                // that they are alive to others. We know that the node is alive but if it's
                // unreachable by the rest of the cluster, doing so will only cause the cluster to insist
                // that it's alive, preventing failover from this node.
                //
                // That said, we still learn about its view of the world since it's safe to merge
                // our state with its view.
                sender_node.gossip_age = if !is_sender_lonely {
                    0
                } else {
                    sender_node.gossip_age
                };
                sender_node.in_failover = true;
                sender_node.maybe_update_state(opts, my_node_id, false)
            } else {
                // for more flags in the future
                None
            };
            // Update downstream ClusterState
            if let Some(new_state) = new_state {
                self.cs_updater
                    .upsert_node_state(sender_id, new_state.into());
            }
            return;
        }

        // We only count non-broadcast gossip messages since broadcasts don't tell us much about
        // the rest of the cluster. Instead, our budget for stability is to be only consumed by
        // regular gossip messages.
        self.num_gossip_received += 1;
        counter!(GOSSIP_RECEIVED).increment(1);
        self.last_gossip_received_at = Instant::now();

        for incoming_node in msg.nodes.iter() {
            if incoming_node.node_id.as_plain() == self.my_node_id.as_plain() {
                // this is us, we don't need to update our state
                continue;
            }
            // is the node failing over? if so, we need to determine if it's failing over in a
            // higher instance or not.
            let node = self.get_node_or_insert(incoming_node.node_id);
            if incoming_node.instance_ts < node.instance_ts {
                // this is an old instance, we ignore it
                continue;
            }
            if node.gen_node_id.is_newer_than(incoming_node.node_id) {
                // ignore, we know a newer node.
                continue;
            }

            // todo: potentially check if instance_ts is too far in the future
            if node.maybe_reset(
                incoming_node.node_id,
                sender_nc_version,
                incoming_node.instance_ts,
            ) {
                // yes, we have reset
                if node.gen_node_id == sender_id {
                    node.in_failover = is_sender_in_failover;
                    if !is_sender_lonely {
                        // if lonely, we leave age as the default
                        node.gossip_age = 0;
                    }
                } else {
                    // just copy over the state from incoming message
                    node.gossip_age = incoming_node.gossip_age;
                    node.in_failover = incoming_node.in_failover;
                }
            } else if node.gen_node_id == sender_id {
                node.in_failover = is_sender_in_failover;
                if !is_sender_lonely {
                    // if lonely, we leave age as the default
                    node.gossip_age = 0;
                }
            } else {
                // a gossip about a node we know about. Let's merge
                node.gossip_age = node.gossip_age.min(incoming_node.gossip_age);
                node.in_failover |= incoming_node.in_failover;
            }
        }

        if msg.flags.intersects(GossipFlags::Enriched) {
            for partition in msg.partitions.into_iter() {
                self.replica_set_states.note_observed_membership(
                    partition.id,
                    partition.current_leader,
                    &partition.observed_current_membership,
                    &partition.observed_next_membership,
                );
            }
        }

        let left_until_stable = (opts.gossip_fd_stability_threshold.get() as usize)
            .checked_sub(self.num_gossip_received);
        if let Some(left_until_stable) = left_until_stable {
            debug!(peer = %sender_id, "{} Gossips received, {} left until stable.", self.num_gossip_received, left_until_stable);
        }
    }

    pub fn report_stats(&self, opts: &GossipOptions) {
        let mut num_alive = 0;
        let mut num_suspect = 0;
        let mut num_dead = 0;
        let mut num_failing_over = 0;
        for node in self.node_states.values() {
            match node.state {
                NodeState::Alive => num_alive += 1,
                NodeState::Suspect { .. } => num_suspect += 1,
                NodeState::Dead => num_dead += 1,
                NodeState::FailingOver => num_failing_over += 1,
            }
        }

        gauge!(GOSSIP_LONELY).set(self.is_lonely(opts) as u8);
        gauge!(GOSSIP_NODES, "state" => STATE_ALIVE).set(num_alive);
        gauge!(GOSSIP_NODES, "state" => STATE_DEAD).set(num_dead);
        gauge!(GOSSIP_NODES, "state" => STATE_SUSPECT).set(num_suspect);
        gauge!(GOSSIP_NODES, "state" => STATE_FAILING_OVER).set(num_failing_over);
    }

    // Used for debugging purposes
    pub fn dump_as_string(&self, opts: &GossipOptions) -> String {
        // display state in a nice table format
        let mut result = String::new();
        // where is the state?
        result.push_str(&format!(
            "Failure Detector (lonely?={})\n",
            self.is_lonely(opts)
        ));
        // sorted by key
        for (_, node) in self
            .node_states
            .iter()
            .sorted_by_cached_key(|(node_id, _)| **node_id)
        {
            result.push_str(&format!(
                "{}(gossip-age={})\t\t{}\t\t{}\t\n",
                node.gen_node_id,
                node.gossip_age,
                node.state,
                node.instance_ts.as_u64(),
            ));
        }

        result.push_str("Partitions\n");

        // sorted by key
        for (partition_id, partition) in self
            .partitions()
            .sorted_by_cached_key(|(partition_id, _)| **partition_id)
        {
            let leadership = partition.current_leader();
            result.push_str(&format!(
                "{}\t({}{})\t\t{}\t{}\n",
                partition_id,
                leadership.current_leader_epoch,
                if leadership.current_leader.is_valid() {
                    format!(" @ {}", leadership.current_leader)
                } else {
                    String::new()
                },
                partition.observed_current_membership.version,
                if let Some(next) = partition.observed_next_membership.as_ref() {
                    format!("=> {}", next.version)
                } else {
                    String::new()
                },
            ));
        }
        result
    }

    pub fn make_gossip_message(
        &mut self,
        opts: &GossipOptions,
        include_extras: bool,
        nodes_config: &NodesConfiguration,
    ) -> Gossip {
        let mut flags = GossipFlags::empty();

        // do not include extras if we don't have a valid partition table
        if include_extras {
            flags |= GossipFlags::Enriched;
        }

        if self.is_lonely(opts) {
            flags |= GossipFlags::FeelingLonely;
        }

        if self.in_failover {
            // note that we don't use Special here. We just send this flag with every message while
            // we are in failover.
            flags |= GossipFlags::FailingOver;
        }

        // Fill up the nodes but only put nodes that we have witnessed in our nodes configuration
        let nodes = nodes_config
            .iter()
            .map(|(plain_id, _node)| {
                // must exist.
                let state = self
                    .node_states
                    .get(&plain_id)
                    .expect("nodes configuration must have refreshed our state");

                // The value is better be aligned with the flag.
                let in_failover = if plain_id == self.my_node_id.as_plain() {
                    self.in_failover
                } else {
                    state.in_failover
                };
                restate_types::net::node::Node {
                    instance_ts: state.instance_ts,
                    node_id: state.gen_node_id,
                    gossip_age: state.gossip_age,
                    in_failover,
                }
            })
            .collect();

        let partitions = if include_extras {
            self.make_partition_state_list()
        } else {
            Vec::new()
        };

        Gossip {
            instance_ts: self.my_instance_ts,
            sent_at: MillisSinceEpoch::now(),
            flags,
            nodes,
            partitions,
        }
    }

    fn make_partition_state_list(&mut self) -> Vec<net::node::PartitionReplicaSet> {
        self.replica_set_states
            .iter()
            .map(|(id, state)| net::node::PartitionReplicaSet {
                id,
                current_leader: state.current_leader(),
                observed_current_membership: state.observed_current_membership,
                observed_next_membership: state.observed_next_membership,
            })
            .collect()
    }

    pub fn update_my_node_state(&mut self, opts: &GossipOptions) {
        let is_standalone = self.node_states.len() == 1;
        let my_node_id = self.my_node_id;
        if let Some(new_state) =
            self.my_node_mut()
                .maybe_update_state(opts, my_node_id, is_standalone)
        {
            self.cs_updater
                .upsert_node_state(self.my_node_id, new_state.into());
        }
    }

    pub fn can_admit_message(
        &mut self,
        opts: &GossipOptions,
        peer: GenerationalNodeId,
        peer_nc_version: Version,
        msg: &Gossip,
    ) -> bool {
        let now = MillisSinceEpoch::now();
        let max_allowed = opts.gossip_time_skew_threshold.as_millis() + (now.as_u64() as u128);
        // message sent to me from too far the future
        if (msg.sent_at.as_u64() as u128) > max_allowed {
            warn!(
                %peer,
                "Gossip received is too far in the future (now: {}, threshold: {}, sent_at: {})",
                now, opts.gossip_time_skew_threshold, msg.sent_at,
            );
            return false;
        }

        if msg.instance_ts > msg.sent_at {
            error!(
                %peer,
                "Gossip received message with sent_at smaller than instance_ts (sent_at: {}, instance_ts: {})",
                msg.sent_at, msg.instance_ts,
            );
            return false;
        }

        // Message came from an old generation or an old instance_ts
        let node = self.get_node_or_insert(peer);
        node.maybe_reset(peer, peer_nc_version, msg.instance_ts);

        if node.instance_ts > msg.instance_ts {
            debug!(
                %peer,
                "Gossip received is too old (now: {}, threshold: {}, instance_ts: {}, observed_instance_ts: {})",
                now, opts.gossip_time_skew_threshold, msg.instance_ts, node.instance_ts
            );
            return false;
        }

        true
    }

    /// Selects a target node for gossiping. This will return None if no nodes are available to
    /// gossip onto. This is expected if we are the only node in the cluster or if all other peers
    /// have been observed as permanently dead (gone).
    pub fn select_targets_for_gossip(
        &mut self,
        nodes_config: &NodesConfiguration,
        networking: &impl NetworkSender,
    ) -> Vec<&mut Node> {
        // Which nodes are good candidates for gossiping?
        //
        // The selection pool exclude ourself and nodes that are known to be (gone)
        // and nodes that are unknown in our current nodes configuration.
        let mut rng = rand::rng();
        let mut chosen = self
            .node_states
            .iter_mut()
            .filter_map(|(_, node)| {
                let is_known = nodes_config.contains(&node.gen_node_id.as_plain());
                let has_capacity = node.connection(networking).has_capacity();
                let is_me = node.gen_node_id.as_plain() == self.my_node_id.as_plain();
                (is_known && has_capacity && !is_me).then_some(node)
            })
            .sample(&mut rng, GOSSIP_ATTEMPT_LIMIT);
        chosen.shuffle(&mut rng);
        chosen
    }

    /// Returns a mutable reference to the node state, creating it if it doesn't exist.
    pub fn get_node_or_insert(&mut self, node_id: GenerationalNodeId) -> &mut Node {
        self.node_states
            .entry(node_id.as_plain())
            .or_insert_with(|| Node::new(node_id))
    }

    pub fn am_i_alive(&self) -> bool {
        self.node_states
            .get(&self.my_node_id.as_plain())
            .expect("my node must be in FD")
            .state
            == NodeState::Alive
    }

    pub fn node_mut(&mut self, node_id: &PlainNodeId) -> Option<&mut Node> {
        self.node_states.get_mut(node_id)
    }

    fn my_node_mut(&mut self) -> &mut Node {
        self.node_mut(&self.my_node_id.as_plain())
            .expect("my node must be in FD")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::Duration;

    use itertools::Itertools;
    use restate_types::NodeId;
    use restate_types::cluster_state::{ClusterState, NodeState as PublishedNodeState};
    use restate_types::net::address::AdvertisedAddress;
    use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
    use restate_types::{GenerationalNodeId, RestateVersion};

    use super::*;

    const A: GenerationalNodeId = GenerationalNodeId::new(1, 1);
    const B: GenerationalNodeId = GenerationalNodeId::new(2, 1);
    const C: GenerationalNodeId = GenerationalNodeId::new(3, 1);

    /// A compact, deterministic record of one production-state-machine event.
    ///
    /// The test assertions deliberately include the rendered trace, so a failing schedule is
    /// directly replayable from the event sequence without relying on timing-sensitive logs.
    #[derive(Debug)]
    struct TraceEntry {
        elapsed: Duration,
        event: String,
        actor: GenerationalNodeId,
        full_intervals: Option<u32>,
        stable: bool,
        last_direct_receive: Duration,
        peers: Vec<PeerTrace>,
    }

    #[derive(Debug)]
    struct PeerTrace {
        node_id: GenerationalNodeId,
        gossip_age: u32,
        state: NodeState,
        published_state: PublishedNodeState,
    }

    #[derive(Debug)]
    struct FdTrace {
        started_at: Instant,
        entries: Vec<TraceEntry>,
    }

    impl FdTrace {
        fn new() -> Self {
            Self {
                started_at: Instant::now(),
                entries: Vec::new(),
            }
        }

        fn record(
            &mut self,
            event: impl Into<String>,
            actor: GenerationalNodeId,
            full_intervals: Option<u32>,
            stable: bool,
            last_direct_receive: Duration,
            peers: Vec<PeerTrace>,
        ) {
            self.entries.push(TraceEntry {
                elapsed: self.started_at.elapsed(),
                event: event.into(),
                actor,
                full_intervals,
                stable,
                last_direct_receive,
                peers,
            });
        }

        fn render(&self) -> String {
            self.entries
                .iter()
                .enumerate()
                .map(|(index, entry)| {
                    let peers = entry
                        .peers
                        .iter()
                        .map(|peer| {
                            format!(
                                "{} age={} local={} published={:?}",
                                peer.node_id,
                                peer.gossip_age,
                                state_name(peer.state),
                                peer.published_state
                            )
                        })
                        .join(", ");
                    format!(
                        "#{index} +{:?} actor={} event={} full_intervals={:?} stable={} last_direct_receive={:?} [{}]",
                        entry.elapsed,
                        entry.actor,
                        entry.event,
                        entry.full_intervals,
                        entry.stable,
                        entry.last_direct_receive,
                        peers
                    )
                })
                .join("\n")
        }
    }

    fn state_name(state: NodeState) -> &'static str {
        match state {
            NodeState::Alive => "Alive",
            NodeState::Suspect { .. } => "Suspect",
            NodeState::Dead => "Dead",
            NodeState::FailingOver => "FailingOver",
        }
    }

    struct FdActor {
        state: FdState,
        cluster_state: ClusterState,
    }

    impl FdActor {
        fn new(node_id: GenerationalNodeId, nodes_config: &NodesConfiguration) -> Self {
            let cluster_state = ClusterState::default();
            let state = FdState::new(
                node_id,
                nodes_config,
                PartitionReplicaSetStates::default(),
                cluster_state.clone().updater(),
            );
            Self {
                state,
                cluster_state,
            }
        }

        fn peer_state(&self, peer: GenerationalNodeId) -> NodeState {
            self.state.node_states[&peer.as_plain()].state
        }

        fn peer_age(&self, peer: GenerationalNodeId) -> u32 {
            self.state.node_states[&peer.as_plain()].gossip_age
        }

        fn establish_stable_alive_view(&mut self, opts: &GossipOptions) {
            self.state.num_gossip_received = opts.gossip_fd_stability_threshold.get() as usize;
            for node in self.state.node_states.values_mut() {
                node.gossip_age = 0;
                node.state = NodeState::Alive;
                self.state
                    .cs_updater
                    .upsert_node_state(node.gen_node_id, node.state.into());
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum Delivery {
        Queue,
        Drop,
        Duplicate,
    }

    #[derive(Debug)]
    struct QueuedGossip {
        from: GenerationalNodeId,
        message: Gossip,
    }

    /// Deterministically drives production `FdState` instances without a Tokio select loop.
    ///
    /// A caller explicitly chooses every tick and every network outcome. A paused actor simply
    /// receives no `tick()` calls while its mailbox continues accumulating gossip.
    struct FdSimulation {
        opts: GossipOptions,
        nodes_config: NodesConfiguration,
        actors: Vec<FdActor>,
        mailboxes: Vec<VecDeque<QueuedGossip>>,
        trace: FdTrace,
    }

    impl FdSimulation {
        fn stable_three_node_cluster() -> Self {
            let opts = GossipOptions::default();
            let nodes_config = nodes_config();
            let mut actors = [A, B, C]
                .into_iter()
                .map(|node_id| FdActor::new(node_id, &nodes_config))
                .collect_vec();
            for actor in &mut actors {
                actor.establish_stable_alive_view(&opts);
            }

            Self {
                opts,
                nodes_config,
                actors,
                mailboxes: (0..3).map(|_| VecDeque::new()).collect(),
                trace: FdTrace::new(),
            }
        }

        fn actor_index(node_id: GenerationalNodeId) -> usize {
            match node_id {
                A => 0,
                B => 1,
                C => 2,
                _ => panic!("unexpected test node {node_id}"),
            }
        }

        fn actor(&self, node_id: GenerationalNodeId) -> &FdActor {
            &self.actors[Self::actor_index(node_id)]
        }

        fn actor_mut(&mut self, node_id: GenerationalNodeId) -> &mut FdActor {
            &mut self.actors[Self::actor_index(node_id)]
        }

        fn record(
            &mut self,
            event: impl Into<String>,
            actor: GenerationalNodeId,
            full_intervals: Option<u32>,
        ) {
            let fd_actor = self.actor(actor);
            let stable = fd_actor.state.is_stable(&self.opts);
            let last_direct_receive = fd_actor.state.last_gossip_received_at.elapsed();
            let mut peers = fd_actor
                .state
                .node_states
                .values()
                .map(|node| PeerTrace {
                    node_id: node.gen_node_id,
                    gossip_age: node.gossip_age,
                    state: node.state,
                    published_state: fd_actor
                        .cluster_state
                        .get_node_state(NodeId::Generational(node.gen_node_id)),
                })
                .collect_vec();
            peers.sort_unstable_by_key(|peer| peer.node_id);
            self.trace.record(
                event,
                actor,
                full_intervals,
                stable,
                last_direct_receive,
                peers,
            );
        }

        async fn advance_one_interval(&mut self) {
            tokio::time::advance(*self.opts.gossip_tick_interval).await;
        }

        fn tick(&mut self, node_id: GenerationalNodeId) -> u32 {
            let opts = self.opts.clone();
            let full_intervals = self
                .actor(node_id)
                .state
                .last_gossip_tick
                .elapsed()
                .div_duration_f32(*opts.gossip_tick_interval)
                .floor() as u32;
            let interval_passed = {
                let actor = self.actor_mut(node_id);
                let interval_passed = actor.state.gossip_tick(&opts);
                if actor.state.is_stable(&opts) {
                    actor.state.detect_peer_failures(&opts);
                }
                interval_passed
            };
            self.record(
                format!("tick(interval_passed={interval_passed})"),
                node_id,
                Some(full_intervals),
            );
            full_intervals
        }

        fn send_gossip(
            &mut self,
            from: GenerationalNodeId,
            to: GenerationalNodeId,
            delivery: Delivery,
        ) {
            let opts = self.opts.clone();
            let message = self.actors[Self::actor_index(from)]
                .state
                .make_gossip_message(&opts, false, &self.nodes_config);
            let target = Self::actor_index(to);
            match delivery {
                Delivery::Queue => {
                    self.mailboxes[target].push_back(QueuedGossip { from, message });
                }
                Delivery::Drop => {}
                Delivery::Duplicate => {
                    self.mailboxes[target].push_back(QueuedGossip {
                        from,
                        message: message.clone(),
                    });
                    self.mailboxes[target].push_back(QueuedGossip { from, message });
                }
            }
            self.record(format!("gossip {from} -> {to} {delivery:?}"), to, None);
        }

        fn deliver_next_from(&mut self, to: GenerationalNodeId, from: GenerationalNodeId) {
            let opts = self.opts.clone();
            let nodes_config_version = self.nodes_config.version();
            let target = Self::actor_index(to);
            let position = self.mailboxes[target]
                .iter()
                .position(|queued| queued.from == from)
                .expect("the requested gossip message is queued");
            let queued = self.mailboxes[target]
                .remove(position)
                .expect("mailbox entry exists");
            {
                let actor = self.actor_mut(to);
                assert!(actor.state.can_admit_message(
                    &opts,
                    queued.from,
                    nodes_config_version,
                    &queued.message,
                ));
                actor.state.update_from_gossip_message(
                    &opts,
                    queued.from,
                    nodes_config_version,
                    queued.message,
                );
            }
            self.record(format!("deliver gossip {from} -> {to}"), to, None);
        }

        fn trace(&self) -> String {
            self.trace.render()
        }
    }

    fn nodes_config() -> NodesConfiguration {
        let mut nodes_config = NodesConfiguration::new_for_testing();
        for node_id in [A, B, C] {
            nodes_config.upsert_node(
                NodeConfig::builder()
                    .name(format!("node-{node_id}"))
                    .current_generation(node_id)
                    .address(AdvertisedAddress::default())
                    .roles(Role::Admin | Role::Worker)
                    .binary_version(RestateVersion::current())
                    .build(),
            );
        }
        nodes_config
    }

    async fn pause_a_while_b_and_c_continue(sim: &mut FdSimulation, intervals: u32) {
        for _ in 0..intervals {
            sim.advance_one_interval().await;
            keep_b_and_c_healthy(sim);
            sim.send_gossip(B, A, Delivery::Queue);
            sim.send_gossip(C, A, Delivery::Queue);
        }
    }

    fn keep_b_and_c_healthy(sim: &mut FdSimulation) {
        sim.tick(B);
        sim.tick(C);
        sim.send_gossip(B, C, Delivery::Queue);
        sim.deliver_next_from(C, B);
        sim.send_gossip(C, B, Delivery::Queue);
        sim.deliver_next_from(B, C);
    }

    #[tokio::test(start_paused = true)]
    async fn paused_observer_bulk_ages_healthy_peers_to_dead() {
        let mut sim = FdSimulation::stable_three_node_cluster();

        pause_a_while_b_and_c_continue(&mut sim, 12).await;

        assert_eq!(sim.tick(A), 12, "trace:\n{}", sim.trace());
        assert_eq!(sim.actor(A).peer_age(B), 12, "trace:\n{}", sim.trace());
        assert_eq!(sim.actor(A).peer_age(C), 12, "trace:\n{}", sim.trace());
        assert_eq!(
            sim.actor(A).peer_state(B),
            NodeState::Dead,
            "trace:\n{}",
            sim.trace()
        );
        assert_eq!(
            sim.actor(A).peer_state(C),
            NodeState::Dead,
            "trace:\n{}",
            sim.trace()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn every_failure_significant_pause_in_the_incident_range_false_kills_healthy_peers() {
        for intervals in 12..=30 {
            let mut sim = FdSimulation::stable_three_node_cluster();
            pause_a_while_b_and_c_continue(&mut sim, intervals).await;

            assert_eq!(sim.tick(A), intervals, "trace:\n{}", sim.trace());
            assert_eq!(
                sim.actor(A).peer_state(B),
                NodeState::Dead,
                "pause={intervals}; trace:\n{}",
                sim.trace()
            );
            assert_eq!(
                sim.actor(A).peer_state(C),
                NodeState::Dead,
                "pause={intervals}; trace:\n{}",
                sim.trace()
            );
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ResumeEvent {
        DeliverB,
        DeliverC,
        DelayedTick,
    }

    #[tokio::test(start_paused = true)]
    async fn resume_order_permutations_produce_divergent_views() {
        for (schedule, expected) in [
            (
                [
                    ResumeEvent::DeliverB,
                    ResumeEvent::DeliverC,
                    ResumeEvent::DelayedTick,
                ],
                ("Dead", "Dead"),
            ),
            (
                [
                    ResumeEvent::DeliverB,
                    ResumeEvent::DelayedTick,
                    ResumeEvent::DeliverC,
                ],
                ("Suspect", "Suspect"),
            ),
            (
                [
                    ResumeEvent::DeliverC,
                    ResumeEvent::DeliverB,
                    ResumeEvent::DelayedTick,
                ],
                ("Dead", "Dead"),
            ),
            (
                [
                    ResumeEvent::DeliverC,
                    ResumeEvent::DelayedTick,
                    ResumeEvent::DeliverB,
                ],
                ("Suspect", "Suspect"),
            ),
            (
                [
                    ResumeEvent::DelayedTick,
                    ResumeEvent::DeliverB,
                    ResumeEvent::DeliverC,
                ],
                ("Suspect", "Suspect"),
            ),
            (
                [
                    ResumeEvent::DelayedTick,
                    ResumeEvent::DeliverC,
                    ResumeEvent::DeliverB,
                ],
                ("Suspect", "Suspect"),
            ),
        ] {
            let mut sim = FdSimulation::stable_three_node_cluster();
            pause_a_while_b_and_c_continue(&mut sim, 12).await;

            for event in schedule {
                match event {
                    ResumeEvent::DeliverB => sim.deliver_next_from(A, B),
                    ResumeEvent::DeliverC => sim.deliver_next_from(A, C),
                    ResumeEvent::DelayedTick => {
                        assert_eq!(sim.tick(A), 12, "trace:\n{}", sim.trace())
                    }
                }
            }

            sim.advance_one_interval().await;
            assert_eq!(sim.tick(A), 1, "trace:\n{}", sim.trace());
            assert_eq!(
                (
                    state_name(sim.actor(A).peer_state(B)),
                    state_name(sim.actor(A).peer_state(C)),
                ),
                expected,
                "schedule={schedule:?}; trace:\n{}",
                sim.trace()
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn transport_loss_ages_a_scheduled_observer_one_interval_at_a_time() {
        let mut sim = FdSimulation::stable_three_node_cluster();

        for expected_age in 1..=11 {
            sim.advance_one_interval().await;
            assert_eq!(sim.tick(A), 1, "trace:\n{}", sim.trace());
            assert_eq!(
                sim.actor(A).peer_age(B),
                expected_age,
                "trace:\n{}",
                sim.trace()
            );
            assert_eq!(
                sim.actor(A).peer_state(B),
                if expected_age == 11 {
                    NodeState::Dead
                } else {
                    NodeState::Alive
                },
                "trace:\n{}",
                sim.trace()
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn paused_observer_cannot_distinguish_a_silent_peer_from_a_healthy_queued_peer() {
        let mut sim = FdSimulation::stable_three_node_cluster();

        for _ in 0..12 {
            sim.advance_one_interval().await;
            sim.tick(C);
            sim.send_gossip(C, A, Delivery::Queue);
        }

        assert_eq!(sim.tick(A), 12, "trace:\n{}", sim.trace());
        assert_eq!(
            sim.actor(A).peer_state(B),
            NodeState::Dead,
            "trace:\n{}",
            sim.trace()
        );
        assert_eq!(
            sim.actor(A).peer_state(C),
            NodeState::Dead,
            "trace:\n{}",
            sim.trace()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_ticks_create_a_dead_suspect_dead_flap_that_eventually_converges() {
        let mut sim = FdSimulation::stable_three_node_cluster();

        pause_a_while_b_and_c_continue(&mut sim, 12).await;
        sim.tick(A);
        assert_eq!(
            sim.actor(A).peer_state(B),
            NodeState::Dead,
            "trace:\n{}",
            sim.trace()
        );

        sim.deliver_next_from(A, B);
        sim.advance_one_interval().await;
        sim.tick(A);
        assert!(
            matches!(sim.actor(A).peer_state(B), NodeState::Suspect { .. }),
            "trace:\n{}",
            sim.trace()
        );

        for _ in 0..12 {
            sim.advance_one_interval().await;
        }
        sim.tick(A);
        assert_eq!(
            sim.actor(A).peer_state(B),
            NodeState::Dead,
            "trace:\n{}",
            sim.trace()
        );

        sim.send_gossip(B, A, Delivery::Queue);
        sim.deliver_next_from(A, B);
        sim.advance_one_interval().await;
        sim.tick(A);
        assert!(matches!(
            sim.actor(A).peer_state(B),
            NodeState::Suspect { .. }
        ));

        for _ in 0..50 {
            sim.advance_one_interval().await;
            keep_b_and_c_healthy(&mut sim);
            sim.send_gossip(B, A, Delivery::Queue);
            sim.deliver_next_from(A, B);
            sim.tick(A);
        }
        assert_eq!(
            sim.actor(A).peer_state(B),
            NodeState::Alive,
            "trace:\n{}",
            sim.trace()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_gossip_is_explicitly_replayable_without_changing_a_live_view() {
        let mut sim = FdSimulation::stable_three_node_cluster();

        sim.send_gossip(B, A, Delivery::Duplicate);
        sim.deliver_next_from(A, B);
        sim.deliver_next_from(A, B);

        assert_eq!(
            sim.actor(A).peer_state(B),
            NodeState::Alive,
            "trace:\n{}",
            sim.trace()
        );
        assert_eq!(sim.actor(A).peer_age(B), 0, "trace:\n{}", sim.trace());
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_gossip_is_explicitly_replayable() {
        let mut sim = FdSimulation::stable_three_node_cluster();

        sim.send_gossip(B, A, Delivery::Drop);
        for _ in 0..11 {
            sim.advance_one_interval().await;
            sim.tick(A);
        }

        assert_eq!(
            sim.actor(A).peer_state(B),
            NodeState::Dead,
            "trace:\n{}",
            sim.trace()
        );
    }
}
