use crate::partition::effects::OutboxMessage;
use crate::partition::shuffle::state_machine::StateMachine;
use common::types::{AckKind, InvocationResponse, PeerId};
use common::utils::GenericError;
use futures::future::BoxFuture;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::debug;

#[derive(Debug)]
pub(crate) struct NewOutboxMessage {
    seq_number: u64,
    message: OutboxMessage,
}

impl NewOutboxMessage {
    pub(crate) fn new(seq_number: u64, message: OutboxMessage) -> Self {
        Self {
            seq_number,
            message,
        }
    }
}

#[derive(Debug)]
pub(crate) struct OutboxTruncation(u64);

impl OutboxTruncation {
    fn new(truncation_index: u64) -> Self {
        Self(truncation_index)
    }

    pub(crate) fn index(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ShuffleInput(pub(crate) AckKind);

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum ShuffleOutput {
    PartitionProcessor(OutboxMessage),
    Ingress(InvocationResponse),
}

#[derive(Debug, thiserror::Error)]
#[error("failed to read outbox: {source:?}")]
pub(super) struct OutboxReaderError {
    source: Option<GenericError>,
}

pub(super) trait OutboxReader {
    fn get_next_message(
        &self,
        next_sequence_number: u64,
    ) -> BoxFuture<Result<Option<(u64, OutboxMessage)>, OutboxReaderError>>;
}

pub(super) type NetworkSender<T> = mpsc::Sender<T>;

pub(super) type HintSender = mpsc::Sender<NewOutboxMessage>;

pub(super) struct Shuffle<OR> {
    peer_id: PeerId,

    outbox_reader: OR,

    // used to send messages to different partitions
    network_tx: mpsc::Sender<ShuffleOutput>,

    network_in_rx: mpsc::Receiver<ShuffleInput>,

    // used to tell partition processor about outbox truncations
    truncation_tx: mpsc::Sender<OutboxTruncation>,

    hint_rx: mpsc::Receiver<NewOutboxMessage>,

    // used to create the senders into the shuffle
    network_in_tx: mpsc::Sender<ShuffleInput>,
    hint_tx: mpsc::Sender<NewOutboxMessage>,
}

impl<OR> Shuffle<OR>
where
    OR: OutboxReader,
{
    pub(super) fn new(
        peer_id: PeerId,
        outbox_reader: OR,
        network_tx: mpsc::Sender<ShuffleOutput>,
        truncation_tx: mpsc::Sender<OutboxTruncation>,
    ) -> Self {
        let (network_in_tx, network_in_rx) = mpsc::channel(32);
        let (hint_tx, hint_rx) = mpsc::channel(1);

        Self {
            peer_id,
            outbox_reader,
            network_tx,
            network_in_rx,
            network_in_tx,
            truncation_tx,
            hint_rx,
            hint_tx,
        }
    }

    pub(super) fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    pub(super) fn create_network_sender(&self) -> NetworkSender<ShuffleInput> {
        self.network_in_tx.clone()
    }

    pub(super) fn create_hint_sender(&self) -> HintSender {
        self.hint_tx.clone()
    }

    pub(super) async fn run(self, shutdown_watch: drain::Watch) -> anyhow::Result<()> {
        let Self {
            peer_id,
            mut hint_rx,
            mut network_in_rx,
            outbox_reader,
            network_tx,
            truncation_tx,
            ..
        } = self;

        debug!(%peer_id, "Running shuffle");

        let shutdown = shutdown_watch.signaled();
        tokio::pin!(shutdown);

        let state_machine = StateMachine::new(
            |next_seq_number| outbox_reader.get_next_message(next_seq_number),
            |msg| network_tx.send(msg),
            &mut hint_rx,
            Duration::from_secs(60),
        );

        tokio::pin!(state_machine);

        loop {
            tokio::select! {
                result = state_machine.as_mut().run() => {
                    result?;
                },
                network_input = network_in_rx.recv() => {
                    let network_input = network_input.expect("Shuffle owns the network in sender. That's why the channel should never be closed.");
                    if let Some(truncation_index) = state_machine.as_mut().on_network_input(network_input) {
                        // this is just a hint which we can drop
                        let _ = truncation_tx.try_send(OutboxTruncation::new(truncation_index));
                    }
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

mod state_machine {
    use crate::partition::effects::OutboxMessage;
    use crate::partition::shuffle::{NewOutboxMessage, ShuffleInput, ShuffleOutput};
    use common::types::AckKind;
    use pin_project::pin_project;
    use std::future::Future;
    use std::marker::PhantomData;
    use std::pin::Pin;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::Sleep;
    use tracing::{debug, trace};

    #[pin_project(project = StateProj)]
    enum State<ReadFuture, SendFuture> {
        Idle,
        ReadingOutbox(#[pin] ReadFuture),
        Sending(#[pin] SendFuture),
        WaitingForAck(#[pin] Sleep),
    }

    #[pin_project]
    pub(super) struct StateMachine<'a, ReadOp, SendOp, ReadFuture, SendFuture, ReadError> {
        current_sequence_number: u64,
        read_operation: ReadOp,
        send_operation: SendOp,
        hint_rx: &'a mut mpsc::Receiver<NewOutboxMessage>,
        retry_timeout: Duration,
        #[pin]
        state: State<ReadFuture, SendFuture>,

        _read_error: PhantomData<ReadError>,
    }

    impl<'a, ReadOp, SendOp, ReadFuture, SendFuture, ReadError>
        StateMachine<'a, ReadOp, SendOp, ReadFuture, SendFuture, ReadError>
    where
        SendFuture: Future<Output = Result<(), mpsc::error::SendError<ShuffleOutput>>>,
        SendOp: Fn(ShuffleOutput) -> SendFuture,
        ReadError: std::error::Error + Send + Sync + 'static,
        ReadFuture: Future<Output = Result<Option<(u64, OutboxMessage)>, ReadError>>,
        ReadOp: Fn(u64) -> ReadFuture,
    {
        pub(super) fn new(
            read_operation: ReadOp,
            send_operation: SendOp,
            hint_rx: &'a mut mpsc::Receiver<NewOutboxMessage>,
            retry_timeout: Duration,
        ) -> Self {
            let current_sequence_number = 0;
            let reading_future = read_operation(current_sequence_number);

            Self {
                current_sequence_number,
                read_operation,
                send_operation,
                hint_rx,
                retry_timeout,
                state: State::ReadingOutbox(reading_future),
                _read_error: Default::default(),
            }
        }

        pub(super) async fn run(self: Pin<&mut Self>) -> Result<(), anyhow::Error> {
            let mut this = self.project();
            loop {
                match this.state.as_mut().project() {
                    StateProj::Idle => {
                        let NewOutboxMessage {
                            seq_number,
                            message,
                        } = this
                            .hint_rx
                            .recv()
                            .await
                            .expect("shuffle is owning the hint sender");

                        if seq_number >= *this.current_sequence_number {
                            *this.current_sequence_number = seq_number;

                            let send_future =
                                (this.send_operation)(ShuffleOutput::PartitionProcessor(message));
                            this.state.set(State::Sending(send_future));
                        } else {
                            let reading_future =
                                (this.read_operation)(*this.current_sequence_number);
                            this.state.set(State::ReadingOutbox(reading_future));
                        }
                    }
                    StateProj::ReadingOutbox(reading_future) => {
                        let reading_result = reading_future.await?;

                        if let Some((seq_number, message)) = reading_result {
                            *this.current_sequence_number = seq_number;

                            let send_future =
                                (this.send_operation)(ShuffleOutput::PartitionProcessor(message));

                            this.state.set(State::Sending(send_future));
                        } else {
                            this.state.set(State::Idle);
                        }
                    }
                    StateProj::Sending(send_future) => {
                        send_future.await?;

                        this.state.set(State::WaitingForAck(tokio::time::sleep(
                            *this.retry_timeout,
                        )));
                    }
                    StateProj::WaitingForAck(sleep) => {
                        sleep.await;

                        debug!(
                            "Did not receive ack for message {} in time. Retry sending it again.",
                            *this.current_sequence_number
                        );
                        // try to send the message again
                        let read_future = (this.read_operation)(*this.current_sequence_number);

                        this.state.set(State::ReadingOutbox(read_future));
                    }
                }
            }
        }

        pub(super) fn on_network_input(
            self: Pin<&mut Self>,
            network_input: ShuffleInput,
        ) -> Option<u64> {
            match network_input.0 {
                AckKind::Acknowledge(seq_number) => {
                    if seq_number >= self.current_sequence_number {
                        trace!("Received acknowledgement for sequence number {seq_number}.");
                        self.read_next_message(seq_number + 1);
                        Some(seq_number)
                    } else {
                        None
                    }
                }
                AckKind::Duplicate(seq_number) => {
                    if seq_number >= self.current_sequence_number {
                        trace!("Message with sequence number {seq_number} is a duplicate.");
                        self.read_next_message(seq_number + 1);
                        Some(seq_number)
                    } else {
                        None
                    }
                }
            }
        }

        fn read_next_message(self: Pin<&mut Self>, next_sequence_number: u64) {
            let mut this = self.project();
            let read_future = (this.read_operation)(next_sequence_number);

            *this.current_sequence_number = next_sequence_number;
            this.state.set(State::ReadingOutbox(read_future));
        }
    }
}
