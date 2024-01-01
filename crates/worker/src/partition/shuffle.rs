// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::shuffle::state_machine::StateMachine;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::identifiers::{FullInvocationId, IngressDispatcherId, PartitionId, PeerId};
use restate_types::invocation::{
    InvocationResponse, InvocationTermination, ResponseResult, ServiceInvocation,
};
use restate_types::message::{AckKind, MessageIndex};
use std::future::Future;
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
pub(crate) enum PartitionProcessorMessage {
    Invocation(ServiceInvocation),
    Response(InvocationResponse),
    InvocationTermination(InvocationTermination),
}

#[derive(Debug, Clone)]
pub(crate) struct IngressResponse {
    pub(crate) _ingress_dispatcher_id: IngressDispatcherId,
    pub(crate) full_invocation_id: FullInvocationId,
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
    PartitionProcessor(PartitionProcessorMessage),
    Ingress(IngressResponse),
}

impl From<OutboxMessage> for ShuffleMessageDestination {
    fn from(value: OutboxMessage) -> Self {
        match value {
            OutboxMessage::IngressResponse {
                ingress_dispatcher_id: ingress_id,
                full_invocation_id,
                response,
            } => ShuffleMessageDestination::Ingress(IngressResponse {
                _ingress_dispatcher_id: ingress_id,
                full_invocation_id,
                response,
            }),
            OutboxMessage::ServiceResponse(response) => {
                ShuffleMessageDestination::PartitionProcessor(PartitionProcessorMessage::Response(
                    response,
                ))
            }
            OutboxMessage::ServiceInvocation(invocation) => {
                ShuffleMessageDestination::PartitionProcessor(
                    PartitionProcessorMessage::Invocation(invocation),
                )
            }
            OutboxMessage::InvocationTermination(invocation_termination) => {
                ShuffleMessageDestination::PartitionProcessor(
                    PartitionProcessorMessage::InvocationTermination(invocation_termination),
                )
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
    ) -> impl Future<Output = Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>> + Send;
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
    OR: OutboxReader + Send + Sync + 'static,
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
            outbox_reader,
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
    use crate::partition::shuffle;
    use crate::partition::shuffle::{
        NewOutboxMessage, OutboxReaderError, ShuffleInput, ShuffleOutput,
    };
    use pin_project::pin_project;
    use restate_storage_api::outbox_table::OutboxMessage;
    use restate_types::identifiers::{PartitionId, PeerId};
    use restate_types::message::{AckKind, MessageIndex};
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::Sleep;
    use tokio_util::sync::ReusableBoxFuture;
    use tracing::{debug, trace};

    type ReadFuture<OutboxReader> = ReusableBoxFuture<
        'static,
        (
            Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>,
            OutboxReader,
        ),
    >;

    #[pin_project(project = StateProj)]
    enum State<SendFuture> {
        Idle,
        ReadingOutbox,
        Sending(#[pin] SendFuture),
        WaitingForAck(#[pin] Sleep),
    }

    #[pin_project]
    pub(super) struct StateMachine<'a, OutboxReader, SendOp, SendFuture> {
        shuffle_id: PeerId,
        partition_id: PartitionId,
        current_sequence_number: MessageIndex,
        outbox_reader: Option<OutboxReader>,
        read_future: ReadFuture<OutboxReader>,
        send_operation: SendOp,
        hint_rx: &'a mut mpsc::Receiver<NewOutboxMessage>,
        retry_timeout: Duration,
        #[pin]
        state: State<SendFuture>,
    }

    async fn get_next_message<OutboxReader: shuffle::OutboxReader>(
        outbox_reader: OutboxReader,
        sequence_number: MessageIndex,
    ) -> (
        Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>,
        OutboxReader,
    ) {
        let result = outbox_reader.get_next_message(sequence_number).await;

        (result, outbox_reader)
    }

    impl<'a, OutboxReader, SendOp, SendFuture> StateMachine<'a, OutboxReader, SendOp, SendFuture>
    where
        SendFuture: Future<Output = Result<(), mpsc::error::SendError<ShuffleOutput>>>,
        SendOp: Fn(ShuffleOutput) -> SendFuture,
        OutboxReader: shuffle::OutboxReader + Send + Sync + 'static,
    {
        pub(super) fn new(
            shuffle_id: PeerId,
            partition_id: PartitionId,
            outbox_reader: OutboxReader,
            send_operation: SendOp,
            hint_rx: &'a mut mpsc::Receiver<NewOutboxMessage>,
            retry_timeout: Duration,
        ) -> Self {
            let current_sequence_number = 0;
            let reading_future = get_next_message(outbox_reader, current_sequence_number);

            Self {
                shuffle_id,
                partition_id,
                current_sequence_number,
                outbox_reader: None,
                read_future: ReusableBoxFuture::new(reading_future),
                send_operation,
                hint_rx,
                retry_timeout,
                state: State::ReadingOutbox,
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
                        }
                    }
                    StateProj::ReadingOutbox => {
                        let (reading_result, outbox_reader) = this.read_future.get_pin().await;
                        *this.outbox_reader = Some(outbox_reader);

                        if let Some((seq_number, message)) = reading_result? {
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
                                // we have read a message with a sequence number that we have already sent, this can happen
                                // in case of a retry with a concurrent ack
                                this.read_future.set(get_next_message(
                                    this.outbox_reader
                                        .take()
                                        .expect("outbox reader should be available"),
                                    *this.current_sequence_number,
                                ));
                                this.state.set(State::ReadingOutbox);
                            }
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
                        this.read_future.set(get_next_message(
                            this.outbox_reader
                                .take()
                                .expect("outbox reader should be available"),
                            *this.current_sequence_number,
                        ));
                        this.state.set(State::ReadingOutbox);
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
                        self.try_read_next_message(seq_number + 1);
                        Some(seq_number)
                    } else {
                        None
                    }
                }
                AckKind::Duplicate { seq_number, .. } => {
                    if seq_number >= self.current_sequence_number {
                        trace!("Message with sequence number {seq_number} is a duplicate.");
                        self.try_read_next_message(seq_number + 1);
                        Some(seq_number)
                    } else {
                        None
                    }
                }
            }
        }

        fn try_read_next_message(self: Pin<&mut Self>, next_sequence_number: MessageIndex) {
            let mut this = self.project();
            *this.current_sequence_number = next_sequence_number;

            if let Some(outbox_reader) = this.outbox_reader.take() {
                // not in State::ReadingOutbox, so we need to read the next outbox message
                this.state.set(State::ReadingOutbox);
                this.read_future.set(get_next_message(
                    outbox_reader,
                    *this.current_sequence_number,
                ));
            }
        }
    }
}
