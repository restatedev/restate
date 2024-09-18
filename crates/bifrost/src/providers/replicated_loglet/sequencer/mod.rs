use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{self, Ordering},
        Arc,
    },
    time::Duration,
};

use node::{LogletStatus, NodeSet, SpreadSelect, Tracker};
use tokio::sync::{mpsc, oneshot};
use worker::{Payload, WorkerEvent};

use restate_core::{
    cancellation_token, network::NetworkError, ShutdownError, TaskCenter, TaskKind,
};

use restate_types::{
    logs::{LogletOffset, Lsn, Record, SequenceNumber, TailState},
    net::log_server::{LogletInfo, Status, Store, Stored},
    replicated_loglet::ReplicatedLogletId,
    time::MillisSinceEpoch,
    GenerationalNodeId, PlainNodeId,
};

use crate::loglet::{LogletCommit, LogletCommitResolver};

mod node;
mod worker;

pub use node::Nodes;

const SUBSCRIPTION_STREAM_SIZE: usize = 64;
const NODE_HEALTH_CHECK: Duration = Duration::from_millis(1000);
const NODE_MAX_LAST_RESPONSE_DURATION: Duration = Duration::from_millis(1000);

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

/// internal commands sent over the [`SequencerHandler`] to sequencer main loop
struct Call<Q, A> {
    request: Q,
    sender: oneshot::Sender<A>,
}

impl<Q, A> Call<Q, A> {
    fn from_request(request: Q) -> (oneshot::Receiver<A>, Self) {
        let (sender, receiver) = oneshot::channel();
        (receiver, Self { request, sender })
    }
}

#[derive(derive_more::Deref)]
struct Event<E> {
    peer: PlainNodeId,
    #[deref]
    event: E,
}

impl<S> Event<S> {
    fn new(peer: PlainNodeId, signal: S) -> Self {
        Self {
            peer,
            event: signal,
        }
    }
}

/// Internal possible calls. This is exclusively used
/// by the SequencerHandler
enum Calls {
    ClusterState(Call<(), ClusterState>),
    /// executed commands
    EnqueueBatch(Call<Arc<[Record]>, Result<LogletCommit, Error>>),
}

// Internal possible events that can be received async from log server
enum Events {
    Stored(Event<Stored>),
    LogletInfo(Event<LogletInfo>),
}

impl Events {
    fn peer(&self) -> &PlainNodeId {
        match self {
            Self::Stored(signal) => &signal.peer,
            Self::LogletInfo(signal) => &signal.peer,
        }
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct Batch<T>
where
    T: Tracker,
{
    pub payload: Arc<Payload>,
    pub tracker: T,
    #[debug(ignore)]
    pub resolver: LogletCommitResolver,
}

/// Main interaction interface with the sequencer state machine
#[derive(Debug, Clone)]
pub struct SequencerHandle {
    /// internal commands channel.
    commands: mpsc::Sender<Calls>,
    /// internal signal channels. Signals are responses that are received
    /// async and sent from log server nodes.
    /// they are separated from the commands, in case we need to block command
    /// processing until a set of responses has been received
    signals: mpsc::Sender<Events>,
}

pub(crate) struct SequencerHandleSink {
    commands: mpsc::Receiver<Calls>,
    events: mpsc::Receiver<Events>,
}

impl SequencerHandle {
    pub(crate) fn pair() -> (SequencerHandle, SequencerHandleSink) {
        // todo: the size of the channel should be 1
        let (commands_sender, commands_receiver) = mpsc::channel::<Calls>(1);
        let (signals_sender, signals_receiver) = mpsc::channel::<Events>(64);
        (
            SequencerHandle {
                commands: commands_sender,
                signals: signals_sender,
            },
            SequencerHandleSink {
                commands: commands_receiver,
                events: signals_receiver,
            },
        )
    }

    pub fn watch_tail(&self) -> futures::stream::BoxStream<'static, TailState<LogletOffset>> {
        unimplemented!()
    }

    pub async fn cluster_state(&self) -> Result<ClusterState, ShutdownError> {
        let (receiver, command) = Call::from_request(());
        self.commands
            .send(Calls::ClusterState(command))
            .await
            .map_err(|_| ShutdownError)?;

        receiver.await.map_err(|_| ShutdownError)
    }

    pub async fn enqueue_batch(&self, payloads: Arc<[Record]>) -> Result<LogletCommit, Error> {
        let (receiver, command) = Call::from_request(payloads);
        self.commands
            .send(Calls::EnqueueBatch(command))
            .await
            .map_err(|_| ShutdownError)?;

        receiver.await.map_err(|_| ShutdownError)?
    }

    pub async fn event_stored(
        &self,
        peer: impl Into<PlainNodeId>,
        payloads: Stored,
    ) -> Result<(), ShutdownError> {
        let signal = Event::new(peer.into(), payloads);
        self.signals
            .send(Events::Stored(signal))
            .await
            .map_err(|_| ShutdownError)
    }

    pub async fn event_loglet_info(
        &self,
        peer: impl Into<PlainNodeId>,
        payloads: LogletInfo,
    ) -> Result<(), ShutdownError> {
        let signal = Event::new(peer.into(), payloads);
        self.signals
            .send(Events::LogletInfo(signal))
            .await
            .map_err(|_| ShutdownError)
    }
}

#[derive(Clone, Debug)]
pub struct ClusterState {
    pub sequencer_id: GenerationalNodeId,
    pub global_committed_tail: LogletOffset,
    pub nodes: BTreeMap<PlainNodeId, LogletStatus>,
}

/// Sequencer inner state machine
///
/// this holds for example, the replica set (log servers)
/// information about global tail, etc...
#[derive(Debug)]
struct SequencerInner<C, P>
where
    P: SpreadSelect,
{
    nodes: node::Nodes<C>,
    node_set: NodeSet,
    replication_policy: P,
    sealed: bool,
    handle: SequencerHandle,
    offset: Lsn,
    inflight_tail: LogletOffset,
    global: Arc<SequencerGlobalState>,
    batches: VecDeque<Batch<P::Tracker>>,
}

pub struct Sequencer;
impl Sequencer {
    pub fn start<C, P>(
        task_center: &TaskCenter,
        node_id: GenerationalNodeId,
        loglet_id: ReplicatedLogletId,
        offset: Lsn,
        nodes: Nodes<C>,
        node_set: Vec<PlainNodeId>,
        replication_policy: P,
    ) -> Result<SequencerHandle, Error>
    where
        C: NodeClient + Send + Sync + 'static,
        P: SpreadSelect + Send + Sync + 'static,
    {
        // - register for all potential response streams from the log-server(s).

        // create a command channel to be used by the sequencer handler. The handler then can be used
        // to call and execute commands on the sequencer directly
        let (handle, commands) = SequencerHandle::pair();

        let sequencer = SequencerInner::new(
            task_center,
            node_id,
            loglet_id,
            nodes,
            node_set,
            replication_policy,
            handle.clone(),
            offset,
        )?;

        task_center.spawn_unmanaged(
            TaskKind::SystemService,
            "leader-sequencer",
            None,
            sequencer.run(commands),
        )?;

        Ok(handle)
    }
}

impl<C, P> SequencerInner<C, P>
where
    C: NodeClient + Send + Sync + 'static,
    P: SpreadSelect,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        task_center: &TaskCenter,
        node_id: GenerationalNodeId,
        loglet_id: ReplicatedLogletId,
        nodes: Nodes<C>,
        node_set: Vec<PlainNodeId>,
        replication_policy: P,
        handle: SequencerHandle,
        offset: Lsn,
    ) -> Result<Self, Error> {
        // shared state with appenders
        let shared = Arc::new(SequencerGlobalState {
            node_id,
            loglet_id,
            global_committed_tail: atomic::AtomicU32::new(LogletOffset::OLDEST.into()),
        });

        // build the node set
        let node_set = NodeSet::start(task_center, node_set, &nodes, Arc::clone(&shared))?;

        //
        Ok(Self {
            nodes,
            node_set,
            replication_policy,
            handle,
            offset,
            inflight_tail: LogletOffset::OLDEST,
            global: shared,
            sealed: false,
            batches: VecDeque::default(),
        })
    }

    async fn run(mut self, mut input: SequencerHandleSink) {
        let shutdown = cancellation_token();

        let mut health = tokio::time::interval(NODE_HEALTH_CHECK);

        // enter main state machine loop
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    break;
                },
                // we rely on the fact that the tick is resolved immediately
                // on first call to send a ping to all nodes in the node set
                _ = health.tick() => {
                    self.health_check().await;
                }
                Some(event) = input.events.recv() => {
                    self.process_event(event).await;
                }
                Some(command) = input.commands.recv() => {
                    self.process_command(command).await;
                }
            }
        }
    }

    /// health check fo the node set, and also try
    async fn health_check(&self) {
        for id in self.node_set.keys() {
            if let Some(node) = self.nodes.get(id) {
                if node.status().duration_since_last_response() < NODE_MAX_LAST_RESPONSE_DURATION {
                    continue;
                }
                // otherwise send a heartbeat and wait for response
                if node.client().enqueue_get_loglet_info().await.is_err() {
                    tracing::warn!(node=%id, "failed to send get-loglet-info to node")
                }
            }
        }
    }

    /// process calls from the SequencerHandler.
    async fn process_command(&mut self, command: Calls) {
        match command {
            Calls::ClusterState(command) => {
                let Call { sender, .. } = command;
                let _ = sender.send(self.cluster_state());
            }
            Calls::EnqueueBatch(command) => {
                let Call { request, sender } = command;

                let _ = sender.send(self.enqueue_batch(request).await);
            }
        }
    }

    /// process calls from the SequencerHandler.
    async fn process_event(&mut self, event: Events) {
        if let Some(node) = self.nodes.get(event.peer()) {
            node.status().touch();
        }

        match event {
            Events::Stored(event) => {
                self.process_stored_event(event).await;
            }
            Events::LogletInfo(signal) => {
                self.process_loglet_info_event(signal);
            }
        }
    }

    fn cluster_state(&self) -> ClusterState {
        ClusterState {
            global_committed_tail: self.global.committed_tail(),
            sequencer_id: self.global.node_id,
            nodes: self
                .node_set
                .iter()
                .map(|(id, node)| (*id, node.loglet.clone()))
                .collect(),
        }
    }

    async fn enqueue_batch(&mut self, records: Arc<[Record]>) -> Result<LogletCommit, Error> {
        if self.sealed {
            // todo: (question) do we return a sealed loglet commit, or error.
            return Ok(LogletCommit::sealed());
        }

        // - create a partial store
        let payload = Arc::new(Payload {
            first_offset: self.inflight_tail,
            records,
        });

        // - compute the new inflight_tail
        let new_inflight_tail = payload.inflight_tail().ok_or(Error::MalformedBatch)?;

        // - get the next spread of nodes from the node set that can satisfy this inflight_tail
        let spread = self.replication_policy.select(
            self.global.committed_tail(),
            self.inflight_tail,
            &self.node_set,
        )?;

        let (commit, resolver) = LogletCommit::deferred();

        let tracker = spread.enqueue(&payload);

        // we need to update the inflight tail of the spread
        for id in tracker.nodes() {
            if let Some(node) = self.node_set.get_mut(id) {
                node.loglet.inflight_tail = new_inflight_tail;
            }
        }

        // - create a batch that will be eventually resolved after we
        // receive all "Stored" commands
        let batch = Batch {
            payload,
            tracker,
            resolver,
        };

        self.inflight_tail = new_inflight_tail;
        self.batches.push_back(batch);

        Ok(commit)
    }

    async fn process_stored_event(&mut self, stored: Event<Stored>) {
        match stored.status {
            Status::Sealed | Status::Sealing => {
                self.sealed = true;
                if let Some(node) = self.node_set.get_mut(&stored.peer) {
                    node.loglet.sealed = true;
                }
                // reject all batches
                // todo: need revision, this might not be always correct
                for batch in self.batches.drain(..) {
                    batch.resolver.sealed();
                }
                return;
            }
            Status::Ok => {
                // store succeeded
                if let Some(node) = self.node_set.get_mut(&stored.peer) {
                    // update node local tail.
                    if stored.local_tail > node.loglet.local_tail {
                        node.loglet.local_tail = stored.local_tail;
                    }

                    // make sure the worker knows about its progress
                    // so it can move on
                    node.worker
                        .notify(WorkerEvent::Stored(stored.event.clone()))
                        .await;
                }
            }
            _ => {
                todo!()
            }
        }

        // fence is the range end of first non resolvable
        // batch in the queue.
        // we start by assuming that all batches are resolvable
        let mut fence = self.batches.len();
        for (index, batch) in self.batches.iter_mut().enumerate() {
            // if the node local tail is smaller that the batch inflight tail then
            // we can safely break from this loop, since every next batch will
            // have a higher inflight tail.
            // it's also safe to unwrap the inflight_tail here since it won't
            // even be in the batches queue if the inflight tail was invalid
            let batch_inflight_tail = batch.payload.inflight_tail().unwrap();
            if stored.local_tail < batch_inflight_tail {
                break;
            }

            batch.tracker.mark_resolved(&stored.peer);
            if !batch.tracker.is_complete() {
                // once fence is set to this index, it will not change
                // until the loop breaks
                fence = std::cmp::min(fence, index);
            }
        }

        for batch in self.batches.drain(..fence) {
            let inflight_tail = batch.payload.inflight_tail().unwrap();
            self.global.set_committed_tail(inflight_tail);
            // todo: (azmy) we probably need to do a release here
            batch.resolver.offset(inflight_tail);
        }
    }

    fn process_loglet_info_event(&mut self, signal: Event<LogletInfo>) {
        let Event {
            peer,
            event: loglet_info,
        } = signal;

        if loglet_info.sealed {
            self.sealed = true;
            // todo: finish sealing ?
        }

        // update last response time
        if let Some(node) = self.nodes.get(&peer) {
            node.status().touch();
        }

        self.node_set.entry(peer).and_modify(|node| {
            node.loglet.update(&loglet_info);
        });
    }
}

/// todo: (azmy) build actual tests this is just experiments
/// over interactions with log-server
#[cfg(test)]
mod test {

    use restate_core::{network::NetworkError, TaskCenterBuilder, TaskKind};
    use restate_types::{
        logs::{LogletOffset, Lsn, Record, SequenceNumber},
        net::log_server::{LogServerResponseHeader, LogletInfo, Status, Store, Stored},
        GenerationalNodeId, PlainNodeId,
    };
    use std::{collections::HashMap, sync::Arc, time::Duration};
    use tokio::sync::Mutex;

    use super::{
        node::{SelectorAll, SelectorSimpleQuorum, SpreadSelect},
        NodeClient, SequencerHandle, SequencerInner,
    };

    struct MockNodeClient {
        id: PlainNodeId,
        handle: SequencerHandle,
        local_tail: Arc<Mutex<LogletOffset>>,
    }

    #[async_trait::async_trait]
    impl NodeClient for MockNodeClient {
        async fn enqueue_store(&self, msg: Store) -> Result<(), NetworkError> {
            // directly respond with stored answer
            let local_tail = msg.last_offset().unwrap() + 1;
            let mut tail = self.local_tail.lock().await;
            *tail = local_tail;
            self.handle
                .event_stored(
                    self.id,
                    Stored {
                        header: LogServerResponseHeader {
                            local_tail,
                            sealed: false,
                            status: Status::Ok,
                        },
                    },
                )
                .await?;

            Ok(())
        }

        async fn enqueue_get_loglet_info(&self) -> Result<(), NetworkError> {
            self.handle
                .event_loglet_info(
                    self.id,
                    LogletInfo {
                        header: LogServerResponseHeader {
                            local_tail: *self.local_tail.lock().await,
                            sealed: false,
                            status: Status::Ok,
                        },
                        trim_point: LogletOffset::OLDEST,
                    },
                )
                .await?;

            Ok(())
        }
    }

    async fn default_setup<P>(size: usize, policy: P) -> SequencerHandle
    where
        P: SpreadSelect + Send + Sync + 'static,
    {
        //crate::setup_panic_handler();
        let tc = TaskCenterBuilder::default_for_tests().build().unwrap();

        let (handle, input) = SequencerHandle::pair();

        let mut nodes = HashMap::with_capacity(size);

        let sequencer_id = GenerationalNodeId::new(1, 1);
        for i in 0..size {
            let id = PlainNodeId::new(i as u32 + 1);
            let node = MockNodeClient {
                id,
                handle: handle.clone(),
                local_tail: Arc::new(Mutex::new(LogletOffset::OLDEST)),
            };
            nodes.insert(id, node);
        }

        let node_set = nodes.keys().copied().collect();

        let sequencer = SequencerInner::new(
            &tc,
            sequencer_id,
            100.into(),
            nodes.into(),
            node_set,
            policy,
            handle.clone(),
            Lsn::OLDEST,
        )
        .unwrap();

        tc.spawn_unmanaged(
            TaskKind::SystemService,
            "sequencer",
            None,
            sequencer.run(input),
        )
        .unwrap();

        handle
    }

    #[tokio::test]
    async fn test_simple_all_replication() {
        let handle = default_setup(2, SelectorAll).await;

        let records = vec![Record::from("hello world"), Record::from("hello human")];
        let resolved = handle.enqueue_batch(Arc::from(records)).await.unwrap();

        println!("waiting for resolved commit");
        let tail = tokio::time::timeout(Duration::from_secs(2), resolved)
            .await
            .unwrap()
            .unwrap();

        let expected = LogletOffset::new(3);
        assert_eq!(tail, expected);

        let state = handle.cluster_state().await.unwrap();
        for state in state.nodes.values() {
            assert_eq!(state.local_tail, expected);
        }
    }

    #[tokio::test]
    async fn test_simple_quorum_replication() {
        let handle = default_setup(3, SelectorSimpleQuorum).await;

        let records = Arc::from(vec![
            Record::from("hello world"),
            Record::from("hello human"),
        ]);

        let resolved = handle.enqueue_batch(Arc::clone(&records)).await.unwrap();

        let tail = tokio::time::timeout(Duration::from_secs(2), resolved)
            .await
            .unwrap()
            .unwrap();

        let expected = LogletOffset::new(3);
        assert_eq!(tail, expected);

        let state_1 = handle.cluster_state().await.unwrap();
        println!("state: {:#?}", state_1);
        // at this point we expect min of 2 nodes to have reached the expected tail
        let mut at_tail = 0;

        assert_eq!(expected, state_1.global_committed_tail);
        for state in state_1.nodes.values() {
            if state.local_tail == expected {
                at_tail += 1;
            }
        }

        assert!(at_tail >= 2);

        // push the next batch!
        // NOTE: since the all nodes is caught up to the global committed tail \
        // next time we do enqueue batch this can end up on a completely different set of nodes
        let resolved = handle.enqueue_batch(Arc::clone(&records)).await.unwrap();

        let expected = expected + records.len() as u32;
        let tail = tokio::time::timeout(Duration::from_secs(2), resolved)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(tail, expected);

        let state_2 = handle.cluster_state().await.unwrap();
        println!("state: {:#?}", state_2);
        // at this point we expect min of 2 nodes to have reached the expected tail
        let mut at_tail = 0;
        assert_eq!(expected, state_2.global_committed_tail);
        for state in state_2.nodes.values() {
            if state.local_tail == expected {
                at_tail += 1;
            }
        }

        assert!(at_tail >= 2);
    }
}
