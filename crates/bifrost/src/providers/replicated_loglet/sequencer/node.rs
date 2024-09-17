use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{self, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use super::{
    worker::{NodeWorker, NodeWorkerHandle, Payload, SendPermit},
    Error, SequencerGlobalState,
};
use restate_core::{network::NetworkError, TaskCenter};
use restate_types::{
    logs::{LogletOffset, SequenceNumber},
    net::log_server::{LogletInfo, Store},
    replicated_loglet::ReplicationProperty,
    time::MillisSinceEpoch,
    GenerationalNodeId,
};

/// LogletHandler trait abstracts the log-server loglet interface. One of possible implementations
/// is a grpc client to running log server
#[async_trait::async_trait]
pub trait NodeClient {
    async fn enqueue_store(&self, msg: Store) -> Result<(), NetworkError>;
    async fn enqueue_get_loglet_info(&self) -> Result<(), NetworkError>;
}

#[derive(Debug, Default)]
pub struct NodeStatus {
    // todo: this should be monotonic
    last_response_time: atomic::AtomicU64,
}

impl NodeStatus {
    pub(crate) fn touch(&self) {
        // update value with latest timestamp
        self.last_response_time
            .store(MillisSinceEpoch::now().into(), Ordering::Relaxed);
    }

    pub fn last_response_time(&self) -> MillisSinceEpoch {
        self.last_response_time.load(Ordering::Relaxed).into()
    }

    pub fn duration_since_last_response(&self) -> Duration {
        // last_response_time should be monotonic
        self.last_response_time().elapsed()
    }
}

struct NodeInner<C> {
    client: C,
    state: NodeStatus,
}

pub struct Node<C> {
    inner: Arc<NodeInner<C>>,
}

impl<C> Clone for Node<C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C> Node<C> {
    fn new(client: C) -> Self {
        Self {
            inner: Arc::new(NodeInner {
                client,
                state: NodeStatus::default(),
            }),
        }
    }
    pub fn client(&self) -> &C {
        &self.inner.client
    }

    pub fn status(&self) -> &NodeStatus {
        &self.inner.state
    }
}

#[derive(derive_more::Debug, derive_more::Deref, derive_more::DerefMut)]
#[debug("{:?}", inner.keys().collect::<Vec<&GenerationalNodeId>>())]
/// Nodes represents the entire cluster of nodes available. This can be shared with
/// multiple Sequencers
pub struct Nodes<C> {
    #[deref]
    #[deref_mut]
    inner: BTreeMap<GenerationalNodeId, Node<C>>,
}

impl<C, I> From<I> for Nodes<C>
where
    I: IntoIterator<Item = (GenerationalNodeId, C)>,
{
    fn from(value: I) -> Self {
        let inner: BTreeMap<GenerationalNodeId, Node<C>> = value
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
    inner: BTreeMap<GenerationalNodeId, LogletNode>,
}

impl NodeSet {
    /// creates the node set and start the appenders
    pub(crate) fn start<C>(
        tc: &TaskCenter,
        node_set: impl IntoIterator<Item = GenerationalNodeId>,
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
    pub node_set: BTreeMap<GenerationalNodeId, bool>,
}

impl Tracker for ReplicationChecker {
    fn mark_resolved(&mut self, node: &GenerationalNodeId) -> bool {
        if let Some(value) = self.node_set.get_mut(node) {
            *value = true;
            return true;
        }
        false
    }

    fn is_complete(&self) -> bool {
        self.node_set.values().filter(|v| **v).count() >= self.replication_factor
    }

    fn nodes(&self) -> impl Iterator<Item = &GenerationalNodeId> {
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
    fn mark_resolved(&mut self, node: &GenerationalNodeId) -> bool;

    // check if this tracker is resolved as complete
    fn is_complete(&self) -> bool;

    fn nodes(&self) -> impl Iterator<Item = &GenerationalNodeId>;
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
                node_set: node_set,
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
        let mut all: Vec<&GenerationalNodeId> = subset.keys().collect();
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
            node_set.insert(id.clone(), false);
        }

        if permits.len() < min {
            return Err(Error::CannotSatisfySpread);
        }

        Ok(Spread {
            permits,
            tracker: ReplicationChecker {
                replication_factor: min,
                node_set: node_set,
            },
        })
    }
}
