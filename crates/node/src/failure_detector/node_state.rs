// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::time::Duration;

use tokio::time::Instant;

use restate_core::network::{LazyConnection, NetworkSender, ReplyRx};
use restate_core::network::{SendToken, Swimlane, TrySendError};
use restate_types::config::GossipOptions;
use restate_types::net::node::{ClusterStateReply, GetClusterState, Gossip};
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, Version};

/// The extra time on top of `gossip_suspect_interval` that we'll use for our own self->alive
/// transition.
const SELF_SUSPECT_OFFSET: Duration = Duration::from_millis(600);

/// Node state transitions
///
/// We start by assuming that all nodes are [`NodeState::Dead`]. As we receive gossip messages, we start to
/// realise which nodes are alive and which are not. We have the option to speed up the initial
/// state view by fetching a full ClusterState from a random peer.
///
/// We don't run failure detection until we consider ourselves to be stable. Our view is considered
/// stable after we receive N number of gossip messages.
/// Possible states each node can be in. All nodes initially start as `Dead`, and
/// are moved into `Alive` as long as they have been [`NodeState::Suspect`] for `gossip_suspect_interval'
/// duration.
///
/// State transitions are as follows:
/// - `Dead` -> `Suspect`     When we observe that a dead node is potentially alive
/// - `Suspect` -> `Alive`    If it didn't fall back down to Dead in the last `gossip_suspect_interval` duration
/// - `Suspect -> `Dead`      If the node's gossip age grew above the failure threshold again
/// - `Alive -> `Dead`        If the `Alive` node's gossip age grew above the failure threshold
/// - `Alive -> `FailingOver` Node is performing a graceful shutdown, it's alive during shutdown.
///
/// The same transitions apply to our own node. We start dead and transition through `Suspect`
/// all the way to `Alive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeState {
    /// Node is dead
    #[default]
    Dead,
    /// Node has been known to be alive for some time now.
    Alive,
    /// Node is likely up (it's gossiping), but we'll give it some more time
    /// before promoting it to `Alive`.
    Suspect { suspected_at: Instant },
    /// Node is alive but is performing a graceful shutdown. The exact moment where the node will
    /// terminate is unknown, eventually it'll transition to Dead. The node might get reported Dead
    /// before it completely terminates. We will move it to dead if it's been in this state for
    /// longer than the failure detection threshold anyway.
    FailingOver,
}

impl NodeState {
    pub fn is_potentially_alive(&self) -> bool {
        matches!(self, NodeState::Alive | NodeState::Suspect { .. })
    }
}

impl Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeState::Dead => write!(f, "Dead"),
            NodeState::Alive => write!(f, "Alive"),
            NodeState::Suspect { suspected_at } => {
                write!(f, "Suspect(since {:?} ago)", suspected_at.elapsed())
            }
            NodeState::FailingOver => write!(f, "FailingOver"),
        }
    }
}

impl From<NodeState> for restate_types::cluster_state::NodeState {
    fn from(state: NodeState) -> Self {
        match state {
            NodeState::Dead => restate_types::cluster_state::NodeState::Dead,
            NodeState::Alive => restate_types::cluster_state::NodeState::Alive,
            NodeState::Suspect { .. } => restate_types::cluster_state::NodeState::Dead,
            NodeState::FailingOver => restate_types::cluster_state::NodeState::FailingOver,
        }
    }
}

#[derive(derive_more::Debug)]
pub struct Node {
    pub(super) gen_node_id: GenerationalNodeId,
    pub(super) instance_ts: MillisSinceEpoch,
    pub(super) state: NodeState,
    pub(super) gossip_age: u32,
    pub(super) in_failover: bool,
    /// The version at which we observed the presence of this node. This could be directly acquired
    /// from a nodes configuration object or from the header of a gossip message.
    nc_version_witness: Version,
    // if we have a connection we can use to this node's generation
    // The connection swims on gossip's swimlane.
    #[debug("is_closed?={}, is_none?={}", connection.as_ref().is_some_and(|c| c.is_closed()), connection.is_none())]
    connection: Option<LazyConnection>,
}

impl Node {
    pub fn new(gen_node_id: GenerationalNodeId) -> Self {
        Self {
            gen_node_id,
            instance_ts: MillisSinceEpoch::UNIX_EPOCH,
            state: NodeState::Dead,
            gossip_age: u32::MAX,
            in_failover: false,
            nc_version_witness: Version::INVALID,
            connection: None,
        }
    }
    /// Resets the node state if the generation is higher than the current one.
    ///
    /// It returns `true` if the state was reset, and `false` otherwise.
    pub fn maybe_reset(
        &mut self,
        gen_node_id: GenerationalNodeId,
        nc_version: Version,
        instance_ts: MillisSinceEpoch,
    ) -> bool {
        assert_eq!(self.gen_node_id.as_plain(), gen_node_id.as_plain());
        if gen_node_id.is_newer_than(self.gen_node_id) {
            self.gen_node_id = gen_node_id;
            self.instance_ts = instance_ts;
            self.state = NodeState::Dead;
            self.gossip_age = u32::MAX;
            self.in_failover = false;
            self.nc_version_witness = nc_version;
            self.connection = None;
            true
        } else if instance_ts > self.instance_ts {
            // note that instance_ts can be 0 if the node is not alive yet, we accept that we don't
            // necessarily need to bump the generation when we see a higher instance_ts coming from a
            // gossip message.
            self.instance_ts = instance_ts;
            self.gossip_age = u32::MAX;
            self.in_failover = false;
            // Should we reset the state here to dead?
            // No, because we might have acquired a previous state from an external
            // source (i.e seeding our initial state from a peer) and in those
            // requests we don't see `instance_ts` of peers.
            true
        } else {
            false
        }
    }

    /// Returns the updated state if it was updated. None otherwise.
    ///
    /// `force_alive` is used exclusively to force this node to be alive without transitioning into
    /// suspect state in standalone setups.
    pub fn maybe_update_state(
        &mut self,
        opts: &GossipOptions,
        my_node_id: GenerationalNodeId,
        force_alive: bool,
    ) -> Option<NodeState> {
        // A node is considered dead if any of the following is true:
        // 1. It's not been gossiping for `gossip_failure_threshold` intervals.
        // 2. It's reporting that it's been lonely (not processing gossips) for too long. However,
        //    it's not immediately marked dead, we just don't deduct from its gossip-age when we
        //    receive messages from it.
        // 3. We have lost gossip connection terminally
        let target_state =
            if (self.gossip_age > opts.gossip_failure_threshold.get()) || self.is_gone() {
                NodeState::Dead
            } else if self.in_failover {
                NodeState::FailingOver
            } else {
                NodeState::Alive
            };

        let now = Instant::now();
        let current_state = self.state;

        let gossip_suspect_interval = if self.gen_node_id == my_node_id {
            // for our own node, we delay the transition by 1s to alive to let others observe us
            // as alive before we consider ourselves to be alive. This aids in synchronized
            // startup of nodes to let admin nodes not select themselves prematurely before
            // determining that there are other alive nodes in the cluster.
            *opts.gossip_suspect_interval + SELF_SUSPECT_OFFSET
        } else {
            *opts.gossip_suspect_interval
        };

        let next_state = match (current_state, target_state) {
            (_, NodeState::Suspect { .. }) => unreachable!(),

            (NodeState::Alive, NodeState::Dead) => NodeState::Dead,
            (NodeState::Dead, NodeState::Alive | NodeState::FailingOver) => {
                // we are coming back to life
                if force_alive {
                    NodeState::Alive
                } else if opts.gossip_suspect_interval.is_zero() {
                    target_state
                } else {
                    NodeState::Suspect { suspected_at: now }
                }
            }
            (
                current @ NodeState::Suspect { suspected_at },
                NodeState::Alive | NodeState::FailingOver,
            ) => {
                if force_alive {
                    NodeState::Alive
                } else if suspected_at.elapsed() >= gossip_suspect_interval {
                    target_state
                } else {
                    current
                }
            }
            (NodeState::FailingOver, NodeState::Alive) => {
                // In general it shouldn't happen, but it can happen if we acquired the current
                // state from a source other than failure detector (instance_ts was unknown).
                // In that case, we'll respect FD's view.
                // leaving this warn as a reminder if this case was hit.
                tracing::warn!(
                    "{} transitioned from {} to {}, perhaps our previous state was not acquired via FD?",
                    self.gen_node_id,
                    current_state,
                    NodeState::Alive,
                );
                NodeState::Alive
            }
            (NodeState::Suspect { .. }, NodeState::Dead) => NodeState::Dead,
            (NodeState::FailingOver, NodeState::Dead) => NodeState::Dead,
            (NodeState::Alive, NodeState::Alive) => NodeState::Alive,
            (NodeState::Dead, NodeState::Dead) => NodeState::Dead,
            (_, NodeState::FailingOver) => NodeState::FailingOver,
        };

        // A state transition is about to take place
        if current_state != next_state {
            if matches!(next_state, NodeState::Dead | NodeState::Alive) {
                tracing::info!(
                    "{} transitioned from {} to {} (gossip-age={})",
                    self.gen_node_id,
                    current_state,
                    next_state,
                    self.gossip_age,
                );
            } else {
                tracing::debug!(
                    "{} transitioned from {} to {} (gossip-age={})",
                    self.gen_node_id,
                    current_state,
                    next_state,
                    self.gossip_age,
                );
            }

            self.state = next_state;
            return Some(next_state);
        }
        None
    }

    pub fn nc_version_witnessed(&self) -> Version {
        self.nc_version_witness
    }

    /// Returns a connection reference and it'll spawn one if this is the first encounter
    pub fn connection(&mut self, networking: &impl NetworkSender) -> &LazyConnection {
        self.connection.get_or_insert_with(|| {
            networking.lazy_connect(
                self.gen_node_id,
                Swimlane::Gossip,
                // this buffer is intentionally small to provide fast feedback to failure detector
                // if we cannot connect. It's big enough to carry the bring-up gossip message and a
                // potential GetClusterState request.
                2,
                true,
            )
        })
    }

    pub fn is_gone(&self) -> bool {
        self.connection.as_ref().is_some_and(|c| c.is_closed())
    }

    pub fn send_gossip(
        &mut self,
        networking: &impl NetworkSender,
        msg: Gossip,
    ) -> Result<SendToken, TrySendError<Gossip>> {
        self.connection(networking).try_send_unary(msg, None)
    }

    pub fn send_get_cluster_state(
        &mut self,
        networking: &impl NetworkSender,
    ) -> Result<ReplyRx<ClusterStateReply>, TrySendError<GetClusterState>> {
        self.connection(networking)
            .try_send_rpc(GetClusterState, None)
    }
}
