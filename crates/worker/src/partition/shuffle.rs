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
use assert2::let_assert;
use async_channel::{TryRecvError, TrySendError};
use restate_bifrost::bifrost;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::dedup::DedupInformation;
use restate_types::identifiers::{
    LeaderEpoch, PartitionId, PartitionKey, PeerId, WithPartitionKey,
};
use restate_types::logs::{LogId, Payload};
use restate_types::message::{AckKind, MessageIndex};
use restate_types::partition_table::FindPartition;
use restate_types::NodeId;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};
use std::future::Future;
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

pub(crate) fn wrap_outbox_message_in_envelope(
    message: OutboxMessage,
    seq_number: MessageIndex,
    shuffle_metadata: &ShuffleMetadata,
) -> Envelope {
    match message {
        OutboxMessage::ServiceInvocation(service_invocation) => {
            let header = create_header(
                service_invocation.fid.partition_key(),
                seq_number,
                shuffle_metadata,
            );
            Envelope::new(header, Command::Invoke(service_invocation))
        }
        OutboxMessage::ServiceResponse(invocation_response) => {
            let header = create_header(
                invocation_response.id.partition_key(),
                seq_number,
                shuffle_metadata,
            );
            Envelope::new(header, Command::InvocationResponse(invocation_response))
        }
        OutboxMessage::InvocationTermination(invocation_termination) => {
            let header = create_header(
                invocation_termination.maybe_fid.partition_key(),
                seq_number,
                shuffle_metadata,
            );
            Envelope::new(header, Command::TerminateInvocation(invocation_termination))
        }
    }
}

fn create_header(
    dest_partition_key: PartitionKey,
    seq_number: MessageIndex,
    shuffle_metadata: &ShuffleMetadata,
) -> Header {
    Header {
        source: Source::Processor {
            partition_id: shuffle_metadata.partition_id,
            partition_key: None,
            leader_epoch: shuffle_metadata.leader_epoch,
            node_id: shuffle_metadata.node_id.id(),
        },
        dest: Destination::Processor {
            partition_key: dest_partition_key,
            dedup: Some(DedupInformation::cross_partition(
                shuffle_metadata.partition_id,
                seq_number,
            )),
        },
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum OutboxReaderError {
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

pub(super) trait OutboxReader {
    fn get_next_message(
        &mut self,
        next_sequence_number: MessageIndex,
    ) -> impl Future<Output = Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>> + Send;

    fn get_message(
        &mut self,
        next_sequence_number: MessageIndex,
    ) -> impl Future<Output = Result<Option<OutboxMessage>, OutboxReaderError>> + Send;
}

/// The hint sender allows to send hints to the shuffle service. If more hints are sent than the
/// channel can store, then the oldest hints will be dropped.
#[derive(Debug, Clone)]
pub(crate) struct HintSender {
    tx: async_channel::Sender<NewOutboxMessage>,

    // receiver to pop the oldest messages from the hint channel
    rx: async_channel::Receiver<NewOutboxMessage>,
}

impl HintSender {
    fn new(
        tx: async_channel::Sender<NewOutboxMessage>,
        rx: async_channel::Receiver<NewOutboxMessage>,
    ) -> Self {
        Self { tx, rx }
    }

    pub(crate) fn send(&self, mut outbox_message: NewOutboxMessage) {
        loop {
            let result = self.tx.try_send(outbox_message);

            outbox_message = match result {
                Ok(_) => break,
                Err(err) => match err {
                    TrySendError::Full(outbox_message) => outbox_message,
                    TrySendError::Closed(_) => {
                        unreachable!("channel should never be closed since we own tx and rx")
                    }
                },
            };

            // pop an element from the hint channel to make space for the new message
            if let Err(err) = self.rx.try_recv() {
                match err {
                    TryRecvError::Empty => {
                        // try again to send since the channel should have capacity now
                    }
                    TryRecvError::Closed => {
                        unreachable!("channel should never be closed since we own tx and rx")
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ShuffleMetadata {
    peer_id: PeerId,
    partition_id: PartitionId,
    leader_epoch: LeaderEpoch,
    node_id: NodeId,
}

impl ShuffleMetadata {
    pub(crate) fn new(
        peer_id: PeerId,
        partition_id: PartitionId,
        leader_epoch: LeaderEpoch,
        node_id: NodeId,
    ) -> Self {
        ShuffleMetadata {
            peer_id,
            partition_id,
            leader_epoch,
            node_id,
        }
    }
}

pub(super) struct Shuffle<OR> {
    metadata: ShuffleMetadata,

    outbox_reader: OR,

    // used to tell partition processor about outbox truncations
    truncation_tx: mpsc::Sender<OutboxTruncation>,

    hint_rx: async_channel::Receiver<NewOutboxMessage>,

    // used to create the senders into the shuffle
    hint_tx: async_channel::Sender<NewOutboxMessage>,
}

impl<OR> Shuffle<OR>
where
    OR: OutboxReader + Send + Sync + 'static,
{
    pub(super) fn new(
        metadata: ShuffleMetadata,
        outbox_reader: OR,
        truncation_tx: mpsc::Sender<OutboxTruncation>,
        channel_size: usize,
    ) -> Self {
        let (hint_tx, hint_rx) = async_channel::bounded(channel_size);

        Self {
            metadata,
            outbox_reader,
            truncation_tx,
            hint_rx,
            hint_tx,
        }
    }

    pub(super) fn create_hint_sender(&self) -> HintSender {
        HintSender::new(self.hint_tx.clone(), self.hint_rx.clone())
    }

    pub(super) async fn run(self, shutdown_watch: drain::Watch) -> anyhow::Result<()> {
        let Self {
            metadata,
            mut hint_rx,
            outbox_reader,
            truncation_tx,
            ..
        } = self;

        debug!(restate.partition.peer = %metadata.peer_id, restate.partition.id = %metadata.partition_id, "Running shuffle");

        let shutdown = shutdown_watch.signaled();
        tokio::pin!(shutdown);

        let peer_id = metadata.peer_id;
        let state_machine = StateMachine::new(
            metadata,
            outbox_reader,
            |msg| async move {
                let_assert!(
                    Destination::Processor { partition_key, .. } = &msg.header.dest,
                    "shuffle can only send messages to a partition processor"
                );

                let partition_id = restate_core::metadata()
                    .partition_table()
                    .find_partition_id(*partition_key)?;
                let log_id = LogId::from(partition_id);

                let payload = Payload::from(msg.encode_with_bincode()?);
                bifrost().append(log_id, payload).await?;

                Ok(())
            },
            &mut hint_rx,
        );

        tokio::pin!(state_machine);

        loop {
            tokio::select! {
                shuffled_message_index = state_machine.as_mut().shuffle_next_message() => {
                    let shuffled_message_index = shuffled_message_index?;

                    // this is just a hint which we can drop
                    let _ = truncation_tx.try_send(OutboxTruncation::new(shuffled_message_index));
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
        wrap_outbox_message_in_envelope, NewOutboxMessage, OutboxReaderError, ShuffleMetadata,
    };
    use pin_project::pin_project;
    use restate_storage_api::outbox_table::OutboxMessage;
    use restate_types::message::MessageIndex;
    use restate_wal_protocol::Envelope;
    use std::cmp::Ordering;
    use std::future::Future;
    use std::pin::Pin;
    use tokio_util::sync::ReusableBoxFuture;
    use tracing::trace;

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
    }

    #[pin_project]
    pub(super) struct StateMachine<'a, OutboxReader, SendOp, SendFuture> {
        metadata: ShuffleMetadata,
        current_sequence_number: MessageIndex,
        outbox_reader: Option<OutboxReader>,
        read_future: ReadFuture<OutboxReader>,
        send_operation: SendOp,
        hint_rx: &'a mut async_channel::Receiver<NewOutboxMessage>,
        #[pin]
        state: State<SendFuture>,
    }

    async fn get_message<OutboxReader: shuffle::OutboxReader>(
        mut outbox_reader: OutboxReader,
        sequence_number: MessageIndex,
    ) -> (
        Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>,
        OutboxReader,
    ) {
        let result = outbox_reader.get_message(sequence_number).await;
        (
            result.map(|opt| opt.map(|m| (sequence_number, m))),
            outbox_reader,
        )
    }

    async fn get_next_message<OutboxReader: shuffle::OutboxReader>(
        mut outbox_reader: OutboxReader,
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
        SendFuture: Future<Output = Result<(), anyhow::Error>>,
        SendOp: Fn(Envelope) -> SendFuture,
        OutboxReader: shuffle::OutboxReader + Send + Sync + 'static,
    {
        pub(super) fn new(
            metadata: ShuffleMetadata,
            outbox_reader: OutboxReader,
            send_operation: SendOp,
            hint_rx: &'a mut async_channel::Receiver<NewOutboxMessage>,
        ) -> Self {
            let current_sequence_number = 0;
            // find the first message from where to start shuffling; everyday I'm shuffling
            // afterwards we assume that the message sequence numbers are consecutive w/o gaps!
            trace!("Starting shuffle. Finding first outbox message.");
            let reading_future = get_next_message(outbox_reader, current_sequence_number);

            Self {
                metadata,
                current_sequence_number,
                outbox_reader: None,
                read_future: ReusableBoxFuture::new(reading_future),
                send_operation,
                hint_rx,
                state: State::ReadingOutbox,
            }
        }

        pub(super) async fn shuffle_next_message(
            self: Pin<&mut Self>,
        ) -> Result<MessageIndex, anyhow::Error> {
            let mut this = self.project();
            loop {
                match this.state.as_mut().project() {
                    StateProj::Idle => {
                        loop {
                            let NewOutboxMessage {
                                seq_number,
                                message,
                            } = this
                                .hint_rx
                                .recv()
                                .await
                                .expect("shuffle is owning the hint sender");

                            match seq_number.cmp(this.current_sequence_number) {
                                Ordering::Equal => {
                                    let send_future =
                                        (this.send_operation)(wrap_outbox_message_in_envelope(
                                            message,
                                            seq_number,
                                            this.metadata,
                                        ));
                                    this.state.set(State::Sending(send_future));
                                    break;
                                }
                                Ordering::Greater => {
                                    // we might have missed some hints, so try again reading the next available outbox message (scan)
                                    this.read_future.set(get_next_message(
                                        this.outbox_reader
                                            .take()
                                            .expect("outbox reader should be available"),
                                        *this.current_sequence_number,
                                    ));
                                    this.state.set(State::ReadingOutbox);
                                    break;
                                }
                                Ordering::Less => {
                                    // this is a hint for a message that we have already sent, so we can ignore it
                                }
                            }
                        }
                    }
                    StateProj::ReadingOutbox => {
                        let (reading_result, outbox_reader) = this.read_future.get_pin().await;
                        *this.outbox_reader = Some(outbox_reader);

                        if let Some((seq_number, message)) = reading_result? {
                            assert!(
                                seq_number >= *this.current_sequence_number,
                                "message sequence numbers must not decrease"
                            );

                            *this.current_sequence_number = seq_number;

                            let send_future = (this.send_operation)(
                                wrap_outbox_message_in_envelope(message, seq_number, this.metadata),
                            );

                            this.state.set(State::Sending(send_future));
                        } else {
                            this.state.set(State::Idle);
                        }
                    }
                    StateProj::Sending(send_future) => {
                        send_future.await?;

                        let successfully_shuffled_sequence_number = *this.current_sequence_number;
                        *this.current_sequence_number += 1;

                        this.read_future.set(get_message(
                            this.outbox_reader
                                .take()
                                .expect("outbox reader should be available"),
                            *this.current_sequence_number,
                        ));
                        this.state.set(State::ReadingOutbox);

                        return Ok(successfully_shuffled_sequence_number);
                    }
                }
            }
        }
    }
}
