use std::{collections::BTreeMap, sync::Arc};

use super::{
    worker::{NodeWorker, NodeWorkerHandle, Payload, SendPermit},
    Error, Node, NodeClient, SequencerGlobalState,
};
use restate_core::TaskCenter;
use restate_types::{
    logs::{LogletOffset, SequenceNumber},
    net::log_server::LogletInfo,
    PlainNodeId,
};

#[derive(derive_more::Debug, derive_more::Deref, derive_more::DerefMut)]
#[debug("{:?}", inner.keys().collect::<Vec<&PlainNodeId>>())]
/// Nodes represents the entire cluster of nodes available. This can be shared with
/// multiple Sequencers
pub struct Nodes<C> {
    #[deref]
    #[deref_mut]
    inner: BTreeMap<PlainNodeId, Node<C>>,
}

impl<C, I> From<I> for Nodes<C>
where
    I: IntoIterator<Item = (PlainNodeId, C)>,
{
    fn from(value: I) -> Self {
        let inner: BTreeMap<PlainNodeId, Node<C>> = value
            .into_iter()
            .map(|(key, client)| (key, Node::new(client)))
            .collect();

        Self { inner }
    }
}

#[derive(Debug, Clone)]
pub struct LogletStatus {
    pub sealed: bool,
    pub local_tail: LogletOffset,
    pub inflight_tail: LogletOffset,
    pub trim_point: LogletOffset,
}

impl Default for LogletStatus {
    fn default() -> Self {
        Self {
            sealed: false,
            local_tail: LogletOffset::INVALID,
            inflight_tail: LogletOffset::INVALID,
            trim_point: LogletOffset::INVALID,
        }
    }
}

impl LogletStatus {
    pub fn update(&mut self, info: &LogletInfo) {
        self.sealed = info.sealed;
        self.local_tail = info.local_tail;
        self.trim_point = info.trim_point;
    }
}

#[derive(Debug)]
pub struct LogletNode {
    pub loglet: LogletStatus,
    pub worker: NodeWorkerHandle,
}

/// NodeSet is a subset of Nodes that is maintained internally with each sequencer
#[derive(derive_more::Deref, derive_more::DerefMut, Debug)]
pub struct NodeSet {
    inner: BTreeMap<PlainNodeId, LogletNode>,
}

impl NodeSet {
    /// creates the node set and start the appenders
    pub(crate) fn start<C>(
        tc: &TaskCenter,
        node_set: impl IntoIterator<Item = PlainNodeId>,
        nodes: &Nodes<C>,
        shared: Arc<SequencerGlobalState>,
    ) -> Result<Self, super::Error>
    where
        C: NodeClient + Send + Sync + 'static,
    {
        let mut inner = BTreeMap::new();
        for id in node_set {
            let node = match nodes.get(&id) {
                Some(node) => node.clone(),
                None => return Err(super::Error::InvalidNodeSet),
            };

            let worker = NodeWorker::start(tc, node, 10, Arc::clone(&shared))?;

            inner.insert(
                id,
                LogletNode {
                    loglet: LogletStatus::default(),
                    worker,
                },
            );
        }

        Ok(Self { inner })
    }
}

impl NodeSet {
    pub fn sealed_nodes(&self) -> usize {
        self.values()
            .filter(|n| matches!(n.loglet, LogletStatus { sealed, .. } if sealed))
            .count()
    }
}

#[derive(Debug)]
pub struct ReplicationChecker {
    /// number of nodes to have resolved the write before
    /// assuming this spread is committed
    pub replication_factor: usize,
    /// the nodes included in the spread
    pub node_set: BTreeMap<PlainNodeId, bool>,
}

impl Tracker for ReplicationChecker {
    fn mark_resolved(&mut self, node: &PlainNodeId) -> bool {
        if let Some(value) = self.node_set.get_mut(node) {
            *value = true;
            return true;
        }
        false
    }

    fn is_complete(&self) -> bool {
        self.node_set.values().filter(|v| **v).count() >= self.replication_factor
    }

    fn nodes(&self) -> impl Iterator<Item = &PlainNodeId> {
        self.node_set.keys()
    }
}

pub struct Spread<'a, T> {
    tracker: T,
    permits: Vec<SendPermit<'a>>,
}

impl<'a, T> Spread<'a, T>
where
    T: Tracker,
{
    pub fn enqueue(self, payload: &Arc<Payload>) -> T {
        let Spread { tracker, permits } = self;

        for permit in permits {
            permit.send(Arc::downgrade(payload));
        }

        tracker
    }
}

pub trait Tracker {
    // mark a node as "resolved" for that tracker. returns true
    // if the node is actually resolved
    fn mark_resolved(&mut self, node: &PlainNodeId) -> bool;

    // check if this tracker is resolved as complete
    fn is_complete(&self) -> bool;

    fn nodes(&self) -> impl Iterator<Item = &PlainNodeId>;
}

pub trait SpreadSelect {
    type Tracker: Tracker + Send + Sync + 'static;

    fn select<'a>(
        &mut self,
        global_committed_tail: LogletOffset,
        first_offset: LogletOffset,
        subset: &'a NodeSet,
    ) -> Result<Spread<'a, Self::Tracker>, Error>;
}

pub struct SelectorAll;

impl SpreadSelect for SelectorAll {
    type Tracker = ReplicationChecker;
    fn select<'a>(
        &mut self,
        _global_committed_tail: LogletOffset,
        _first_offset: LogletOffset,
        subset: &'a NodeSet,
    ) -> Result<Spread<'a, Self::Tracker>, Error> {
        let mut node_set = BTreeMap::default();
        let mut permits = Vec::default();

        for (id, node) in subset.iter() {
            let permit = node
                .worker
                .reserve()
                .map_err(|_| Error::TemporaryUnavailable(*id))?;

            permits.push(permit);
            node_set.insert(*id, false);
        }

        Ok(Spread {
            permits,
            tracker: ReplicationChecker {
                replication_factor: node_set.len(),
                node_set,
            },
        })
    }
}

pub struct SelectorSimpleQuorum;

impl SpreadSelect for SelectorSimpleQuorum {
    type Tracker = ReplicationChecker;
    fn select<'a>(
        &mut self,
        global_committed_tail: LogletOffset,
        first_offset: LogletOffset,
        subset: &'a NodeSet,
    ) -> Result<Spread<'a, Self::Tracker>, Error> {
        // a fixed write quorum of N/2+1
        let min = subset.len() / 2 + 1;
        let mut node_set = BTreeMap::default();
        let mut permits = Vec::with_capacity(subset.len());

        // pick nodes at random maybe!
        use rand::seq::SliceRandom;
        let mut all: Vec<&PlainNodeId> = subset.keys().collect();
        all.shuffle(&mut rand::thread_rng());

        for id in all {
            let node = subset.get(id).expect("node must exist in node-set");
            // nodes can't have gaps UNLESS the first offset in the batch
            // is the global_committed_offset.
            if node.loglet.inflight_tail != first_offset && first_offset != global_committed_tail {
                continue;
            }

            let Ok(permit) = node.worker.reserve() else {
                // node is lagging or busy
                continue;
            };

            permits.push(permit);
            node_set.insert(*id, false);
        }

        if permits.len() < min {
            return Err(Error::CannotSatisfySpread);
        }

        Ok(Spread {
            permits,
            tracker: ReplicationChecker {
                replication_factor: min,
                node_set,
            },
        })
    }
}
