use crate::partition::shuffle::state_machine::StateMachine;
use futures::future::BoxFuture;
use restate_common::types::{
    AckKind, IngressId, InvocationResponse, MessageIndex, PartitionId, PeerId, ResponseResult,
    ServiceInvocation, ServiceInvocationId,
};
use restate_storage_api::outbox_table::OutboxMessage;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::debug;

#[derive(Debug)]
pub(crate) struct NewOutboxMessage {
    seq_number: MessageIndex,
    message: OutboxMessage,
}

impl NewOutboxMessage {
    pub(crate) fn new(seq_number: MessageIndex, message: OutboxMessage) -> Self {
        Self {
            seq_number,
            message,
        }
    }
}

#[derive(Debug)]
pub(crate) struct OutboxTruncation(MessageIndex);

impl OutboxTruncation {
    fn new(truncation_index: MessageIndex) -> Self {
        Self(truncation_index)
    }

    pub(crate) fn index(&self) -> MessageIndex {
        self.0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ShuffleInput(pub(crate) AckKind);

#[derive(Debug, Clone)]
pub(crate) enum InvocationOrResponse {
    Invocation(ServiceInvocation),
    Response(InvocationResponse),
}

#[derive(Debug, Clone)]
pub(crate) struct IngressResponse {
    pub(crate) _ingress_id: IngressId,
    pub(crate) service_invocation_id: ServiceInvocationId,
    pub(crate) response: ResponseResult,
}

#[derive(Debug, Clone)]
pub(crate) struct ShuffleOutput {
    shuffle_id: PeerId,
    partition_id: PartitionId,
    msg_index: MessageIndex,
    message: ShuffleMessageDestination,
}

impl ShuffleOutput {
    pub(crate) fn new(
        shuffle_id: PeerId,
        partition_id: PartitionId,
        msg_index: MessageIndex,
        message: ShuffleMessageDestination,
    ) -> Self {
        Self {
            shuffle_id,
            partition_id,
            msg_index,
            message,
        }
    }

    pub(crate) fn into_inner(
        self,
    ) -> (PeerId, PartitionId, MessageIndex, ShuffleMessageDestination) {
        (
            self.shuffle_id,
            self.partition_id,
            self.msg_index,
            self.message,
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ShuffleMessageDestination {
    PartitionProcessor(InvocationOrResponse),
    Ingress(IngressResponse),
}

impl From<OutboxMessage> for ShuffleMessageDestination {
    fn from(value: OutboxMessage) -> Self {
        match value {
            OutboxMessage::IngressResponse {
                ingress_id,
                service_invocation_id,
                response,
            } => ShuffleMessageDestination::Ingress(IngressResponse {
                _ingress_id: ingress_id,
                service_invocation_id,
                response,
            }),
            OutboxMessage::ServiceResponse(response) => {
                ShuffleMessageDestination::PartitionProcessor(InvocationOrResponse::Response(
                    response,
                ))
            }
            OutboxMessage::ServiceInvocation(invocation) => {
                ShuffleMessageDestination::PartitionProcessor(InvocationOrResponse::Invocation(
                    invocation,
                ))
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum OutboxReaderError {
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

pub(super) trait OutboxReader {
    fn get_next_message(
        &self,
        next_sequence_number: MessageIndex,
    ) -> BoxFuture<Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>>;
}

pub(super) type NetworkSender<T> = mpsc::Sender<T>;

pub(super) type HintSender = mpsc::Sender<NewOutboxMessage>;

pub(super) struct Shuffle<OR> {
    peer_id: PeerId,
    partition_id: PartitionId,

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
        partition_id: PartitionId,
        outbox_reader: OR,
        network_tx: mpsc::Sender<ShuffleOutput>,
        truncation_tx: mpsc::Sender<OutboxTruncation>,
        channel_size: usize,
    ) -> Self {
        let (network_in_tx, network_in_rx) = mpsc::channel(channel_size);
        let (hint_tx, hint_rx) = mpsc::channel(channel_size);

        Self {
            peer_id,
            partition_id,
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
            partition_id,
            mut hint_rx,
            mut network_in_rx,
            outbox_reader,
            network_tx,
            truncation_tx,
            ..
        } = self;

        debug!(restate.partition.peer = %peer_id, restate.partition.id = %partition_id, "Running shuffle");

        let shutdown = shutdown_watch.signaled();
        tokio::pin!(shutdown);

        let state_machine = StateMachine::new(
            peer_id,
            partition_id,
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
    use crate::partition::shuffle::{NewOutboxMessage, ShuffleInput, ShuffleOutput};
    use pin_project::pin_project;
    use restate_common::types::{AckKind, MessageIndex, PartitionId, PeerId};
    use restate_storage_api::outbox_table::OutboxMessage;
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
        shuffle_id: PeerId,
        partition_id: PartitionId,
        current_sequence_number: MessageIndex,
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
        ReadFuture: Future<Output = Result<Option<(MessageIndex, OutboxMessage)>, ReadError>>,
        ReadOp: Fn(MessageIndex) -> ReadFuture,
    {
        pub(super) fn new(
            shuffle_id: PeerId,
            partition_id: PartitionId,
            read_operation: ReadOp,
            send_operation: SendOp,
            hint_rx: &'a mut mpsc::Receiver<NewOutboxMessage>,
            retry_timeout: Duration,
        ) -> Self {
            let current_sequence_number = 0;
            let reading_future = read_operation(current_sequence_number);

            Self {
                shuffle_id,
                partition_id,
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

                            let send_future = (this.send_operation)(ShuffleOutput::new(
                                *this.shuffle_id,
                                *this.partition_id,
                                seq_number,
                                message.into(),
                            ));
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

                            let send_future = (this.send_operation)(ShuffleOutput::new(
                                *this.shuffle_id,
                                *this.partition_id,
                                seq_number,
                                message.into(),
                            ));

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
        ) -> Option<MessageIndex> {
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
                AckKind::Duplicate { seq_number, .. } => {
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

        fn read_next_message(self: Pin<&mut Self>, next_sequence_number: MessageIndex) {
            let mut this = self.project();
            let read_future = (this.read_operation)(next_sequence_number);

            *this.current_sequence_number = next_sequence_number;
            this.state.set(State::ReadingOutbox(read_future));
        }
    }
}
