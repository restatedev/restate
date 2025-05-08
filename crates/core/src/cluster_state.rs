// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use ahash::HashMap;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::watch;

use restate_types::{GenerationalNodeId, NodeId, PlainNodeId};

type Generation = u32;

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum NodeState {
    #[default]
    Dead = 0,
    Alive,
    FailingOver,
}

impl NodeState {
    pub fn is_alive(self) -> bool {
        matches!(self, Self::Alive | Self::FailingOver)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct State {
    generation: Generation,
    state: NodeState,
}

static_assertions::assert_eq_size!(State, u64);

#[derive(Debug, Default)]
struct Node {
    state: watch::Sender<State>,
}

#[derive(Default)]
struct Inner {
    nodes: RwLock<HashMap<PlainNodeId, Node>>,
}

/// Access to liveness information about the cluster.
///
/// `ClusterState` is a view of the state that's acquired by the failure detector.
///
/// At startup all nodes will be marked dead and our own node will transition to `Alive` after a
/// grace period to give the failure detector a chance to establish a good view of the cluster.
///
/// It's generally recommended to watch (my_node_id) until it's `Alive` before relying on the state
/// provided through this structure.
#[derive(Clone, Default)]
pub struct ClusterState {
    inner: Arc<Inner>,
}

/// A guard to perform state updates on the cluster state without changing the node map
/// membership.
pub struct ClusterStateUpdateReadGuard<'a> {
    guard: RwLockReadGuard<'a, HashMap<PlainNodeId, Node>>,
}

impl ClusterStateUpdateReadGuard<'_> {
    /// ignores the node if the plain node ID doesn't exist
    pub fn set_node_state(&mut self, node_id: GenerationalNodeId, new_state: NodeState) -> bool {
        let Some(node) = self.guard.get(&node_id.as_plain()) else {
            return false;
        };

        node.state.send_if_modified(|current_node| {
            if current_node.generation > node_id.generation() {
                // reject the change, generations do not go backwards.
                return false;
            }
            let mut changed = false;
            if current_node.generation != node_id.generation() {
                current_node.generation = node_id.generation();
                changed = true;
            }
            if current_node.state != new_state {
                current_node.state = new_state;
                changed = true;
            }
            changed
        })
    }
}

/// A guard to perform state updates on the cluster state that's allowed to change the node map
/// membership.
pub struct ClusterStateUpdateWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, HashMap<PlainNodeId, Node>>,
}

impl ClusterStateUpdateWriteGuard<'_> {
    /// Removes the node from the map if exists
    pub fn remove_node(&mut self, node_id: PlainNodeId) {
        self.guard.remove(&node_id);
    }

    /// Adds the node to the map if it doesn't exist
    pub fn upsert_node_state(&mut self, node_id: GenerationalNodeId, new_state: NodeState) -> bool {
        let node = self.guard.entry(node_id.as_plain()).or_default();

        node.state.send_if_modified(|current_node| {
            if current_node.generation > node_id.generation() {
                // reject the change, generations do not go backwards.
                return false;
            }
            let mut changed = false;
            if current_node.generation != node_id.generation() {
                current_node.generation = node_id.generation();
                changed = true;
            }
            if current_node.state != new_state {
                current_node.state = new_state;
                changed = true;
            }
            changed
        })
    }
}

pub struct ClusterStateUpdater {
    inner: Arc<Inner>,
}

impl ClusterStateUpdater {
    pub fn into_cluster_state(self) -> ClusterState {
        ClusterState { inner: self.inner }
    }
    /// Acquire a write lock to mutate cluster state node map membership
    pub fn write(&mut self) -> ClusterStateUpdateWriteGuard {
        ClusterStateUpdateWriteGuard {
            guard: self.inner.nodes.write(),
        }
    }

    /// Acquire an updater that can update individual states but won't change node map membership
    pub fn read(&mut self) -> ClusterStateUpdateReadGuard {
        ClusterStateUpdateReadGuard {
            guard: self.inner.nodes.read(),
        }
    }

    /// Returns true if the value was changed
    ///
    /// It'll reject updates if the existing node has higher generation than input value but will
    /// add the node if it doesn't exist
    pub fn upsert_node_state(&mut self, node_id: GenerationalNodeId, new_state: NodeState) -> bool {
        let mut inner = self.write();
        inner.upsert_node_state(node_id, new_state)
    }

    /// Returns true if the value was changed
    ///
    /// Ignores the update if the plain node ID doesn't exist
    pub fn set_node_state(&mut self, node_id: GenerationalNodeId, new_state: NodeState) -> bool {
        let mut inner = self.read();
        inner.set_node_state(node_id, new_state)
    }
}

impl ClusterState {
    pub fn updater(self) -> ClusterStateUpdater {
        ClusterStateUpdater { inner: self.inner }
    }

    pub fn get_node_state(&self, node_id: NodeId) -> NodeState {
        let current = self
            .inner
            .nodes
            .read()
            .get(&node_id.id())
            .map(|n| *n.state.borrow());
        let Some(current) = current else {
            return NodeState::Dead;
        };

        match node_id {
            NodeId::Plain(_) => current.state,
            NodeId::Generational(gen_node_id) if gen_node_id.generation() == current.generation => {
                current.state
            }
            NodeId::Generational(_) => NodeState::Dead,
        }
    }

    /// Returns true if the node is Alive.
    ///
    /// Note: Failing over nodes will not be considered alive by this
    /// call. If you want more precise state, use `get_node_state()` instead.
    pub fn is_alive(&self, node_id: NodeId) -> bool {
        let node_state = self.get_node_state(node_id);
        node_state.is_alive()
    }

    pub fn all(&self) -> Vec<(GenerationalNodeId, NodeState)> {
        self.inner
            .nodes
            .read()
            .iter()
            .map(|(node_id, node)| {
                (
                    node_id.with_generation(node.state.borrow().generation),
                    node.state.borrow().state,
                )
            })
            .collect()
    }

    pub fn all_watches(&self) -> Vec<(PlainNodeId, NodeStateWatch)> {
        self.inner
            .nodes
            .read()
            .iter()
            .map(|(node_id, node)| {
                (
                    *node_id,
                    NodeStateWatch {
                        node_id: *node_id,
                        rx: node.state.subscribe(),
                    },
                )
            })
            .collect()
    }

    /// Creates a watch for this node id
    ///
    /// Note that this doesn't check if this is a valid node id or not, it'll happily create the
    /// watch on a node id that doesn't exist. If the node id became valid at a later stage, it's
    /// guaranteed that this watch will capture it.
    pub fn watch(&self, node_id: PlainNodeId) -> NodeStateWatch {
        let rx = self
            .inner
            .nodes
            .write()
            .entry(node_id)
            .or_default()
            .state
            .subscribe();

        NodeStateWatch { node_id, rx }
    }
}

#[derive(Clone)]
pub struct NodeStateWatch {
    node_id: PlainNodeId,
    rx: watch::Receiver<State>,
}

impl NodeStateWatch {
    /// Returns the current state of the node or dead if it's an unknown node
    pub fn current_state(&self) -> NodeState {
        self.rx.borrow().state
    }

    /// Returns true if the node is alive.
    ///
    /// Note that this doesn't care about which generation the node is currently running
    pub fn is_alive(&self) -> bool {
        self.current_state().is_alive()
    }

    /// Returns true if this exact generation is alive
    pub fn is_generation_alive(&self, generation: u32) -> bool {
        let state = self.rx.borrow();
        if state.generation != generation {
            false
        } else {
            state.state.is_alive()
        }
    }

    pub fn node_id_and_state(&self) -> (GenerationalNodeId, NodeState) {
        let state = self.rx.borrow();
        (self.node_id.with_generation(state.generation), state.state)
    }

    pub fn current_node_id(&self) -> GenerationalNodeId {
        self.node_id.with_generation(self.rx.borrow().generation)
    }

    /// Wait until the condition is true on a certain generation. If the current generation
    /// is different, the condition is checked against NodeState::Dead.
    pub async fn conditional_wait_for(
        &mut self,
        generation: u32,
        mut condition: impl FnMut(NodeState) -> bool,
    ) -> (GenerationalNodeId, NodeState) {
        let result = self
            .rx
            .wait_for(|state| {
                if state.generation != generation {
                    return condition(NodeState::Dead);
                }
                condition(state.state)
            })
            .await;

        match result {
            Ok(current_state) => {
                let current_state = *current_state;
                (
                    self.node_id.with_generation(current_state.generation),
                    current_state.state,
                )
            }
            // the sender will be dropped if the node has been removed from the cluster.
            Err(_) => (self.node_id.with_generation(0), NodeState::Dead),
        }
    }

    /// Wait until the condition is true
    pub async fn wait_for(
        &mut self,
        mut condition: impl FnMut((GenerationalNodeId, NodeState)) -> bool,
    ) -> (GenerationalNodeId, NodeState) {
        let result = self
            .rx
            .wait_for(|state| {
                condition((self.node_id.with_generation(state.generation), state.state))
            })
            .await;

        match result {
            Ok(current_state) => {
                let current_state = *current_state;
                (
                    self.node_id.with_generation(current_state.generation),
                    current_state.state,
                )
            }
            // the sender will be dropped if the node has been removed from the cluster.
            Err(_) => (self.node_id.with_generation(0), NodeState::Dead),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use futures::poll;

    use super::*;

    #[test]
    fn cluster_state_basics() {
        let cluster_state = ClusterState::default();
        let node_id = GenerationalNodeId::new(1, 2);
        let node_state = cluster_state.get_node_state(node_id.into());
        assert_eq!(node_state, NodeState::Dead);
        assert!(!node_state.is_alive());

        let mut updater = cluster_state.clone().updater();
        updater.upsert_node_state(node_id, NodeState::Alive);
        let node_state = cluster_state.get_node_state(node_id.into());
        assert_eq!(node_state, NodeState::Alive);
        // checking an older generation, or a newer generation should yield dead
        assert!(
            !cluster_state
                .get_node_state(GenerationalNodeId::new(1, 1).into())
                .is_alive()
        );

        assert!(
            !cluster_state
                .get_node_state(GenerationalNodeId::new(1, 3).into())
                .is_alive()
        );
    }

    #[test]
    fn cluster_state_ignore_old_generations() {
        let cluster_state = ClusterState::default();
        let node_id = GenerationalNodeId::new(1, 2);
        let mut updater = cluster_state.clone().updater();
        updater.upsert_node_state(node_id, NodeState::Alive);

        let node_state = cluster_state.get_node_state(node_id.into());
        assert_eq!(node_state, NodeState::Alive);

        updater.upsert_node_state(GenerationalNodeId::new(1, 1), NodeState::Dead);
        // query by plain node id, I should see latest gen (2) alive
        let node_state = cluster_state.get_node_state(PlainNodeId::new(1).into());
        assert_eq!(node_state, NodeState::Alive);
        // query by generational
        let node_state = cluster_state.get_node_state(GenerationalNodeId::new(1, 2).into());
        assert_eq!(node_state, NodeState::Alive);

        // query by wrong generation
        let node_state = cluster_state.get_node_state(GenerationalNodeId::new(1, 1).into());
        assert_eq!(node_state, NodeState::Dead);
        let node_state = cluster_state.get_node_state(GenerationalNodeId::new(1, 3).into());
        assert_eq!(node_state, NodeState::Dead);
    }

    // test watching a node state
    #[tokio::test(start_paused = true)]
    async fn watch_node_state() {
        let cluster_state = ClusterState::default();
        let node_id = GenerationalNodeId::new(1, 2);
        let mut updater = cluster_state.clone().updater();
        updater.upsert_node_state(node_id, NodeState::Alive);

        let mut watch = cluster_state.watch(node_id.as_plain());
        assert_eq!(watch.current_state(), NodeState::Alive);
        updater.set_node_state(node_id, NodeState::Dead);
        assert_eq!(watch.current_state(), NodeState::Dead);
        let mut wait_for_alive_fut = std::pin::pin!(
            watch.conditional_wait_for(node_id.generation(), |state| state.is_alive())
        );

        assert_eq!(poll!(&mut wait_for_alive_fut), Poll::Pending);
        // update the state to alive
        updater.set_node_state(node_id, NodeState::Alive);
        // poll aagain, should be ready now
        assert_eq!(
            poll!(&mut wait_for_alive_fut),
            Poll::Ready((node_id, NodeState::Alive))
        );

        // We can watch unknown nodes
        let mut watch = cluster_state.watch(PlainNodeId::new(100));
        assert_eq!(watch.current_state(), NodeState::Dead);
        // on generation 2 we should be alive
        let mut wait_for_alive_fut =
            std::pin::pin!(watch.conditional_wait_for(2, |state| state.is_alive()));

        let watch = cluster_state.watch(PlainNodeId::new(100));

        // pending, current generation is 0
        assert_eq!(poll!(&mut wait_for_alive_fut), Poll::Pending);
        assert_eq!(watch.current_state(), NodeState::Dead);
        assert_eq!(watch.current_node_id().generation(), 0);

        // generation 1 is alive, but we are conditional on gen2
        updater.set_node_state(GenerationalNodeId::new(100, 1), NodeState::Alive);

        // still pending, we'll only resolve if gen=2
        assert_eq!(poll!(&mut wait_for_alive_fut), Poll::Pending);
        updater.set_node_state(GenerationalNodeId::new(100, 2), NodeState::Alive);

        assert_eq!(
            poll!(&mut wait_for_alive_fut),
            Poll::Ready((GenerationalNodeId::new(100, 2), NodeState::Alive))
        );
    }
}
