use common::types::PeerId;
use futures::future::BoxFuture;
use std::convert::Infallible;
use tokio::sync::mpsc;
use tracing::debug;

#[derive(Debug)]
pub(super) struct ShuffleMessage {}

#[derive(Debug)]
pub(crate) struct NewOutboxMessage(u64);

impl NewOutboxMessage {
    pub(crate) fn new(index: u64) -> Self {
        Self(index)
    }
}

#[derive(Debug)]
pub(crate) struct OutboxTruncation(u64);

impl OutboxTruncation {
    pub(crate) fn index(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum NetworkInput {
    Acknowledge(u64),
    Duplicate(u64),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum NetworkOutput {
    Invocation,
    Response,
}

pub(super) trait OutboxReader {
    type Error;

    fn get_next_message(
        &self,
        next_sequence_number: u64,
    ) -> BoxFuture<Result<Option<ShuffleMessage>, Self::Error>>;
}

pub(super) type NetworkSender<T> = mpsc::Sender<T>;

pub(super) type HintSender = mpsc::Sender<NewOutboxMessage>;

pub(super) struct Shuffle<OR, NetSink, CmdSink> {
    peer_id: PeerId,
    _outbox_reader: OR,

    // used to send messages to different partitions
    _network_sink: NetSink,

    network_in_rx: mpsc::Receiver<NetworkInput>,

    // used to tell partition processor about outbox truncations
    _cmd_sink: CmdSink,

    hint_rx: mpsc::Receiver<NewOutboxMessage>,

    // used to create the senders into the shuffle
    network_in_tx: mpsc::Sender<NetworkInput>,
    hint_tx: mpsc::Sender<NewOutboxMessage>,
}

impl<OR, NetSink, CmdSink> Shuffle<OR, NetSink, CmdSink>
where
    OR: OutboxReader,
{
    pub(super) fn new(
        peer_id: PeerId,
        outbox_reader: OR,
        network_sink: NetSink,
        cmd_sink: CmdSink,
    ) -> Self {
        let (network_in_tx, network_in_rx) = mpsc::channel(32);
        let (hint_tx, hint_rx) = mpsc::channel(1);

        Self {
            peer_id,
            _outbox_reader: outbox_reader,
            _network_sink: network_sink,
            network_in_rx,
            network_in_tx,
            _cmd_sink: cmd_sink,
            hint_rx,
            hint_tx,
        }
    }

    pub(super) fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    pub(super) fn create_network_sender(&self) -> NetworkSender<NetworkInput> {
        self.network_in_tx.clone()
    }

    pub(super) fn create_hint_sender(&self) -> HintSender {
        self.hint_tx.clone()
    }

    pub(super) async fn run(self, shutdown_watch: drain::Watch) -> Result<(), Infallible> {
        let Self {
            peer_id,
            mut hint_rx,
            mut network_in_rx,
            ..
        } = self;

        debug!(%peer_id, "Running shuffle");

        let shutdown = shutdown_watch.signaled();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                hint = hint_rx.recv() => {
                    let _hint = hint.expect("Shuffle owns the cmd sender. That's why the channel should never be closed.");
                    todo!("Implement command logic");
                },
                network_msg = network_in_rx.recv() => {
                  let _network_msg = network_msg.expect("Shuffle owns the network in sender. That's why the channel should never be closed.");
                    todo!("Implement network msg logic");
                },
                _ = &mut shutdown => {
                    break;
                }
            }
        }

        debug!(%peer_id, "Stopping shuffle");

        Ok(())
    }
}
