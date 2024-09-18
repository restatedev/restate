use std::{
    sync::{
        atomic::{self, Ordering},
        Arc,
    },
    time::Duration,
};

use restate_core::{network::NetworkError, ShutdownError};

use restate_types::{
    logs::LogletOffset, net::log_server::Store, replicated_loglet::ReplicatedLogletId,
    time::MillisSinceEpoch, GenerationalNodeId, PlainNodeId,
};

mod worker;

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

/// NodeClient trait abstracts the log-server node interface. One of possible implementations
/// is a grpc client.
#[async_trait::async_trait]
pub trait NodeClient {
    async fn enqueue_store(&self, msg: Store) -> Result<(), NetworkError>;
    async fn enqueue_get_loglet_info(&self) -> Result<(), NetworkError>;
}

struct NodeInner<C> {
    client: C,
    state: NodeStatus,
}

/// Clonable node object, provides accessor to the underlying node client and
/// its state
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

    /// gets node client
    pub fn client(&self) -> &C {
        &self.inner.client
    }

    // gets node (general) status
    pub fn status(&self) -> &NodeStatus {
        &self.inner.state
    }
}

/// A sharable part of the sequencer state. This is shared with node workers
#[derive(Debug)]
pub(crate) struct SequencerGlobalState {
    node_id: GenerationalNodeId,
    loglet_id: ReplicatedLogletId,
    global_committed_tail: atomic::AtomicU32,
}

impl SequencerGlobalState {
    pub fn node_id(&self) -> &GenerationalNodeId {
        &self.node_id
    }

    pub fn loglet_id(&self) -> &ReplicatedLogletId {
        &self.loglet_id
    }

    pub fn committed_tail(&self) -> LogletOffset {
        LogletOffset::new(self.global_committed_tail.load(Ordering::Acquire))
    }

    pub(crate) fn set_committed_tail(&self, tail: LogletOffset) {
        self.global_committed_tail
            .fetch_max(tail.into(), Ordering::Release);
    }
}

//todo: improve error names and description
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("cannot satisfy spread")]
    CannotSatisfySpread,
    #[error("malformed batch")]
    MalformedBatch,
    #[error("invalid node set")]
    InvalidNodeSet,
    #[error("node {0} queue is full")]
    TemporaryUnavailable(PlainNodeId),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}
