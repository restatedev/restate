// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use async_channel::{TryRecvError, TrySendError};
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::debug;

use restate_core::cancellation_watcher;
use restate_core::network::TransportConnect;
use restate_ingestion_client::IngestionClient;
use restate_storage_api::deduplication_table::DedupInformation;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, WithPartitionKey};
use restate_types::message::MessageIndex;
use restate_wal_protocol::{Destination, Envelope, Header, Source};

use crate::partition::types::OutboxMessageExt;

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

pub(crate) fn wrap_outbox_message_in_envelope(
    message: OutboxMessage,
    seq_number: MessageIndex,
    shuffle_metadata: &ShuffleMetadata,
) -> Envelope {
    Envelope::new(
        create_header(message.partition_key(), seq_number, shuffle_metadata),
        message.to_command(),
    )
}

fn create_header(
    dest_partition_key: PartitionKey,
    seq_number: MessageIndex,
    shuffle_metadata: &ShuffleMetadata,
) -> Header {
    Header {
        source: Source::Processor {
            partition_id: None,
            partition_key: None,
            leader_epoch: shuffle_metadata.leader_epoch,
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

#[derive(Debug, Copy, Clone)]
pub(crate) struct ShuffleMetadata {
    partition_id: PartitionId,
    leader_epoch: LeaderEpoch,
}

impl ShuffleMetadata {
    pub(crate) fn new(partition_id: PartitionId, leader_epoch: LeaderEpoch) -> Self {
        ShuffleMetadata {
            partition_id,
            leader_epoch,
        }
    }
}

pub(super) struct Shuffle<T, OR> {
    metadata: ShuffleMetadata,

    outbox_reader: OR,

    ingestion_client: IngestionClient<T, Envelope>,

    // used to tell partition processor about outbox truncations
    truncation_tx: mpsc::Sender<OutboxTruncation>,

    hint_rx: async_channel::Receiver<NewOutboxMessage>,

    // used to create the senders into the shuffle
    hint_tx: async_channel::Sender<NewOutboxMessage>,
}

impl<T, OR> Shuffle<T, OR>
where
    T: TransportConnect,
    OR: OutboxReader + Send + Sync + 'static,
{
    pub(super) fn new(
        metadata: ShuffleMetadata,
        outbox_reader: OR,
        truncation_tx: mpsc::Sender<OutboxTruncation>,
        channel_size: usize,
        ingestion_client: IngestionClient<T, Envelope>,
    ) -> Self {
        let (hint_tx, hint_rx) = async_channel::bounded(channel_size);

        Self {
            metadata,
            outbox_reader,
            truncation_tx,
            hint_rx,
            hint_tx,
            ingestion_client,
        }
    }

    pub(super) fn create_hint_sender(&self) -> HintSender {
        HintSender::new(self.hint_tx.clone(), self.hint_rx.clone())
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        let Self {
            metadata,
            hint_rx,
            outbox_reader,
            truncation_tx,
            ingestion_client,
            ..
        } = self;

        debug!(restate.partition.id = %metadata.partition_id, "Running shuffle");

        let stream =
            shuffle_stream::ShuffleStream::new(metadata, ingestion_client, outbox_reader, hint_rx);

        let mut stream = std::pin::pin!(stream);

        loop {
            tokio::select! {
                Some(shuffled_message_index) = stream.next() => {
                    let shuffled_message_index = shuffled_message_index?;

                    // this is just a hint which we can drop
                    let _ = truncation_tx.try_send(OutboxTruncation::new(shuffled_message_index));
                },
                _ = cancellation_watcher() => {
                    break;
                }
            }
        }

        debug!("Stopping shuffle");

        Ok(())
    }
}

mod shuffle_stream {
    use std::{
        cmp::Ordering,
        collections::VecDeque,
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::{FutureExt, Stream, StreamExt, ready};
    use pin_project::pin_project;
    use tokio_util::sync::ReusableBoxFuture;

    use restate_core::network::TransportConnect;
    use restate_ingestion_client::{IngestFuture, IngestionClient, RecordCommit};
    use restate_storage_api::outbox_table::OutboxMessage;
    use restate_types::{identifiers::WithPartitionKey, message::MessageIndex};
    use restate_wal_protocol::Envelope;

    use crate::partition::shuffle::{
        NewOutboxMessage, OutboxReaderError, ShuffleMetadata, wrap_outbox_message_in_envelope,
    };

    type ReadFuture<OutboxReader> = ReusableBoxFuture<
        'static,
        (
            Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>,
            OutboxReader,
        ),
    >;

    async fn get_next_message<OutboxReader: super::OutboxReader>(
        mut outbox_reader: OutboxReader,
        next_sequence_number: MessageIndex,
    ) -> (
        Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError>,
        OutboxReader,
    ) {
        let result = outbox_reader.get_next_message(next_sequence_number).await;
        (result, outbox_reader)
    }

    #[pin_project(project = StateProj)]
    enum State {
        Idle,
        ReadingOutbox,
        Ingesting { ingest: IngestFuture, sn: u64 },
    }

    #[pin_project]
    pub struct ShuffleStream<T, R> {
        metadata: ShuffleMetadata,
        ingestion: IngestionClient<T, Envelope>,
        #[pin]
        hint_rx: async_channel::Receiver<NewOutboxMessage>,
        reader: Option<R>,
        read_fut: ReadFuture<R>,
        inflight: VecDeque<RecordCommit<MessageIndex>>,
        next_sequence_number: MessageIndex,
        #[pin]
        state: State,
    }

    impl<T, R> ShuffleStream<T, R>
    where
        T: TransportConnect,
        R: super::OutboxReader + Send + Sync + 'static,
    {
        pub fn new(
            metadata: ShuffleMetadata,
            ingestion: IngestionClient<T, Envelope>,
            reader: R,
            hint_rx: async_channel::Receiver<NewOutboxMessage>,
        ) -> Self {
            Self {
                metadata,
                ingestion,
                hint_rx,
                reader: None,
                read_fut: ReusableBoxFuture::new(get_next_message(reader, 0)),
                inflight: VecDeque::new(),
                next_sequence_number: 0,
                state: State::ReadingOutbox,
            }
        }

        fn inner_poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<anyhow::Error> {
            let mut this = self.project();

            loop {
                match this.state.as_mut().project() {
                    StateProj::Idle => {
                        loop {
                            let NewOutboxMessage {
                                seq_number: sn,
                                message,
                            } = ready!(this.hint_rx.poll_next_unpin(cx))
                                .expect("shuffle is owning the hint sender");

                            match sn.cmp(this.next_sequence_number) {
                                Ordering::Equal => {
                                    let envelope =
                                        wrap_outbox_message_in_envelope(message, sn, this.metadata);
                                    this.state.set(State::Ingesting {
                                        ingest: this
                                            .ingestion
                                            .ingest(envelope.partition_key(), envelope),
                                        sn,
                                    });
                                    break;
                                }
                                Ordering::Greater => {
                                    // Missed hints; we need to do an outbox scan
                                    this.read_fut.set(get_next_message(
                                        this.reader.take().unwrap(),
                                        *this.next_sequence_number,
                                    ));
                                    this.state.set(State::ReadingOutbox);
                                    break;
                                }
                                Ordering::Less => {
                                    // Already processed ... skip
                                }
                            }
                        }
                    }
                    StateProj::Ingesting { ingest, sn } => {
                        let sn = *sn;
                        let result = ready!(ingest.poll_unpin(cx));
                        let commit_token = match result {
                            Ok(commit_token) => commit_token,
                            Err(err) => return Poll::Ready(err.into()),
                        };
                        this.inflight.push_back(commit_token.map(|_| sn));

                        // read next message
                        *this.next_sequence_number = sn + 1;
                        this.read_fut.set(get_next_message(
                            this.reader.take().unwrap(),
                            *this.next_sequence_number,
                        ));
                        this.state.set(State::ReadingOutbox);
                    }
                    StateProj::ReadingOutbox => {
                        let (result, reader) = ready!(this.read_fut.poll_unpin(cx));
                        *this.reader = Some(reader);

                        match result {
                            Ok(None) => {
                                this.state.set(State::Idle);
                            }
                            Ok(Some((sn, message))) => {
                                let envelope =
                                    wrap_outbox_message_in_envelope(message, sn, this.metadata);
                                this.state.set(State::Ingesting {
                                    ingest: this
                                        .ingestion
                                        .ingest(envelope.partition_key(), envelope),
                                    sn,
                                });
                            }
                            Err(err) => return Poll::Ready(err.into()),
                        }
                    }
                }
            }
        }
    }

    impl<T, R> Stream for ShuffleStream<T, R>
    where
        T: TransportConnect,
        R: super::OutboxReader + Send + Sync + 'static,
    {
        type Item = Result<MessageIndex, anyhow::Error>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Poll::Ready(err) = self.as_mut().inner_poll_next(cx) {
                return Poll::Ready(Some(Err(err)));
            }

            let this = self.project();
            let mut latest_committed_sn = None;
            loop {
                let Some(head) = this.inflight.front_mut() else {
                    break;
                };

                match head.poll_unpin(cx) {
                    Poll::Pending => break,
                    Poll::Ready(Ok(sn)) => latest_committed_sn = Some(sn),
                    Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                }
                this.inflight.pop_front();
            }

            match latest_committed_sn {
                Some(sn) => Poll::Ready(Some(Ok(sn))),
                None => Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::{Context, anyhow};
    use assert2::let_assert;
    use futures::StreamExt;
    use restate_core::partitions::PartitionRouting;
    use restate_ingestion_client::IngestionClient;
    use restate_types::net::RpcRequest;
    use restate_types::net::ingest::{ReceivedIngestRequest, ResponseStatus};
    use restate_types::net::partition_processor::PartitionLeaderService;
    use restate_types::partitions::state::{LeadershipState, PartitionReplicaSetStates};
    use restate_types::storage::StorageCodec;
    use test_log::test;
    use tokio::sync::mpsc;

    use restate_core::network::{
        BackPressureMode, FailingConnector, ServiceMessage, ServiceStream,
    };
    use restate_core::{TaskCenter, TaskKind, TestCoreEnv, TestCoreEnvBuilder};
    use restate_storage_api::StorageError;
    use restate_storage_api::outbox_table::OutboxMessage;
    use restate_types::Version;
    use restate_types::identifiers::{InvocationId, LeaderEpoch, PartitionId};
    use restate_types::invocation::ServiceInvocation;
    use restate_types::message::MessageIndex;
    use restate_types::partition_table::PartitionTable;
    use restate_wal_protocol::{Command, Envelope};

    use crate::partition::shuffle::{OutboxReader, OutboxReaderError, Shuffle, ShuffleMetadata};

    struct MockOutboxReader {
        base_offset: MessageIndex,
        // there can be holes in our records
        records: Vec<Option<ServiceInvocation>>,
    }

    impl MockOutboxReader {
        fn new(base_offset: MessageIndex, records: Vec<Option<ServiceInvocation>>) -> Self {
            Self {
                base_offset,
                records,
            }
        }

        fn subslice_from_index(
            &self,
            starting_index: MessageIndex,
        ) -> &[Option<ServiceInvocation>] {
            if starting_index < self.base_offset {
                <&[Option<ServiceInvocation>]>::default()
            } else {
                self.records
                    .get((starting_index - self.base_offset) as usize..)
                    .unwrap_or_default()
            }
        }
    }

    impl OutboxReader for MockOutboxReader {
        async fn get_next_message(
            &mut self,
            next_sequence_number: MessageIndex,
        ) -> Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError> {
            let next_sequence_number = next_sequence_number.max(self.base_offset);
            let records = self.subslice_from_index(next_sequence_number);
            let next_some_index = records.iter().position(|m| m.is_some());

            Ok(next_some_index.map(|index| {
                (
                    next_sequence_number + u64::try_from(index).expect("usize fits in u64"),
                    OutboxMessage::ServiceInvocation(
                        records
                            .get(index)
                            .expect("subslice entry should exist")
                            .clone()
                            .map(Box::new)
                            .expect("message should exist"),
                    ),
                )
            }))
        }
    }

    /// Outbox reader which is used to let the shuffler fail in a controlled manner so that we
    /// can simulate restarts.
    struct FailingOutboxReader {
        records: Vec<Option<ServiceInvocation>>,
        fail_index: MessageIndex,
    }

    impl FailingOutboxReader {
        fn new(records: Vec<Option<ServiceInvocation>>, fail_index: MessageIndex) -> Self {
            Self {
                records,
                fail_index,
            }
        }

        fn check_fail(&self, next_sequence_number: MessageIndex) -> Result<(), OutboxReaderError> {
            if next_sequence_number >= self.fail_index {
                return Err(OutboxReaderError::Storage(StorageError::Generic(anyhow!(
                    "test error"
                ))));
            }

            Ok(())
        }
    }

    impl OutboxReader for Arc<FailingOutboxReader> {
        async fn get_next_message(
            &mut self,
            next_sequence_number: MessageIndex,
        ) -> Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError> {
            let next_sequence_number = next_sequence_number as usize;
            let offset_records = self.records.get(next_sequence_number..).unwrap_or_default();
            let next_some_index = offset_records
                .iter()
                .position(|record| record.is_some())
                .unwrap_or(offset_records.len())
                + next_sequence_number;

            self.check_fail(u64::try_from(next_some_index).expect("usize fits in u64"))?;

            Ok(self.records.get(next_some_index).map(|record| {
                (
                    u64::try_from(next_some_index).expect("usize fits in u64"),
                    OutboxMessage::ServiceInvocation(
                        record.clone().map(Box::new).expect("record must exist"),
                    ),
                )
            }))
        }
    }

    async fn collect_invoke_commands_until(
        stream: &mut ServiceStream<PartitionLeaderService>,
        last_invocation_id: InvocationId,
    ) -> anyhow::Result<Vec<ServiceInvocation>> {
        let mut messages = Vec::new();
        let mut stream = std::pin::pin!(stream);

        'out: while let Some(ServiceMessage::Rpc(incoming)) = stream.next().await {
            assert_eq!(incoming.msg_type(), ReceivedIngestRequest::TYPE);
            let incoming = incoming.into_typed::<ReceivedIngestRequest>();
            let (r, body) = incoming.split();
            r.send(ResponseStatus::Ack.into());
            for mut record in body.records {
                let envelope = StorageCodec::decode::<Envelope, _>(&mut record.record)
                    .context("Failed to decode envelope")?;

                let_assert!(Command::Invoke(service_invocation) = envelope.command);
                let invocation_id = service_invocation.invocation_id;
                messages.push(*service_invocation);

                if last_invocation_id == invocation_id {
                    break 'out;
                }
            }
        }

        Ok(messages)
    }

    fn assert_received_invoke_commands(
        received_invokes: Vec<ServiceInvocation>,
        expected_invokes: Vec<Option<ServiceInvocation>>,
    ) {
        // remove Nones
        let expected_messages = expected_invokes.iter().flatten();

        // received_messages can theoretically contain duplicate messages
        let mut received_messages = received_invokes.iter();

        for expected_message in expected_messages {
            let mut message_found = false;
            for received_message in received_messages.by_ref() {
                if received_message == expected_message {
                    message_found = true;
                    break;
                }
            }

            assert!(
                message_found,
                "Expected message {expected_message:?} was not found in received messages"
            );
        }
    }

    struct ShuffleEnv<OR> {
        #[allow(dead_code)]
        env: TestCoreEnv<FailingConnector>,
        stream: ServiceStream<PartitionLeaderService>,
        ingress: IngestionClient<FailingConnector, Envelope>,
        shuffle: Shuffle<FailingConnector, OR>,
    }

    async fn create_shuffle_env<OR: OutboxReader + Send + Sync + 'static>(
        outbox_reader: OR,
    ) -> ShuffleEnv<OR> {
        // set numbers of partitions to 1 to easily find all sent messages by the shuffle
        let mut builder = TestCoreEnvBuilder::with_incoming_only_connector().set_partition_table(
            PartitionTable::with_equally_sized_partitions(Version::MIN, 1),
        );

        let metadata = ShuffleMetadata::new(PartitionId::from(0), LeaderEpoch::from(0));

        let partition_replica_set_states = PartitionReplicaSetStates::default();

        partition_replica_set_states.note_observed_leader(
            0.into(),
            LeadershipState {
                current_leader: builder.my_node_id,
                current_leader_epoch: LeaderEpoch::INITIAL,
            },
        );

        let svc = builder
            .router_builder
            .register_service::<PartitionLeaderService>(10, BackPressureMode::PushBack);

        let env = builder.build().await;

        let stream = svc.start();

        let ingress = IngestionClient::new(
            env.networking.clone(),
            env.metadata.updateable_partition_table(),
            PartitionRouting::new(partition_replica_set_states, TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(),
            None,
        );

        let (truncation_tx, _truncation_rx) = mpsc::channel(1);

        let shuffle = Shuffle::new(metadata, outbox_reader, truncation_tx, 1, ingress.clone());

        ShuffleEnv {
            env,
            stream,
            ingress,
            shuffle,
        }
    }

    #[test(restate_core::test)]
    async fn shuffle_consecutive_outbox() -> anyhow::Result<()> {
        let expected_messages = iter::repeat_with(|| Some(ServiceInvocation::mock()))
            .take(10)
            .collect::<Vec<_>>();

        let last_invocation_id = expected_messages
            .last()
            .and_then(|msg| {
                msg.as_ref()
                    .map(|service_invocation| service_invocation.invocation_id)
            })
            .expect("service invocation should be present");

        let outbox_reader = MockOutboxReader::new(42, expected_messages.clone());
        let mut shuffle_env = create_shuffle_env(outbox_reader).await;

        TaskCenter::spawn_child(TaskKind::Shuffle, "shuffle", shuffle_env.shuffle.run())?;

        let messages =
            collect_invoke_commands_until(&mut shuffle_env.stream, last_invocation_id).await?;

        assert_received_invoke_commands(messages, expected_messages);

        Ok(())
    }

    #[test(restate_core::test)]
    async fn shuffle_holey_outbox() -> anyhow::Result<()> {
        let expected_messages = vec![
            Some(ServiceInvocation::mock()),
            None,
            None,
            Some(ServiceInvocation::mock()),
            Some(ServiceInvocation::mock()),
        ];

        let last_invocation_id = expected_messages
            .last()
            .and_then(|msg| {
                msg.as_ref()
                    .map(|service_invocation| service_invocation.invocation_id)
            })
            .expect("service invocation should be present");

        let outbox_reader = MockOutboxReader::new(42, expected_messages.clone());
        let mut shuffle_env = create_shuffle_env(outbox_reader).await;

        TaskCenter::spawn_child(TaskKind::Shuffle, "shuffle", shuffle_env.shuffle.run())?;

        let messages =
            collect_invoke_commands_until(&mut shuffle_env.stream, last_invocation_id).await?;

        assert_received_invoke_commands(messages, expected_messages);

        Ok(())
    }

    #[test(restate_core::test)]
    async fn shuffle_with_restarts() -> anyhow::Result<()> {
        let expected_messages: Vec<_> = iter::repeat_with(|| Some(ServiceInvocation::mock()))
            .take(100)
            .collect();

        let last_invocation_id = expected_messages
            .last()
            .and_then(|msg| {
                msg.as_ref()
                    .map(|service_invocation| service_invocation.invocation_id)
            })
            .expect("service invocation should be present");

        let mut outbox_reader = Arc::new(FailingOutboxReader::new(expected_messages.clone(), 10));
        let mut shuffle_env = create_shuffle_env(Arc::clone(&outbox_reader)).await;
        let total_restarts = Arc::new(AtomicUsize::new(0));

        let shuffle_task = TaskCenter::spawn_child(TaskKind::Shuffle, "shuffle", {
            let total_restarts = Arc::clone(&total_restarts);
            async move {
                let mut shuffle = shuffle_env.shuffle;
                let metadata = shuffle.metadata;
                let truncation_tx = shuffle.truncation_tx.clone();
                let mut processed_range = 0;
                let mut num_restarts = 0;

                // restart shuffle on failures and update failing outbox reader
                while shuffle.run().await.is_err() {
                    num_restarts += 1;
                    // update the failing outbox reader to make a bit more progress and delete some of the delivered records
                    {
                        let outbox_reader = Arc::get_mut(&mut outbox_reader)
                            .expect("only one reference should exist");

                        // leave the first entry to generate some holes
                        for idx in (processed_range + 1)..outbox_reader.fail_index {
                            outbox_reader.records
                                [usize::try_from(idx).expect("index should fit in usize")] = None;
                        }

                        processed_range = outbox_reader.fail_index;
                        outbox_reader.fail_index += 10;
                    }

                    shuffle = Shuffle::new(
                        metadata,
                        Arc::clone(&outbox_reader),
                        truncation_tx.clone(),
                        1,
                        shuffle_env.ingress.clone(),
                    );
                }

                total_restarts.store(num_restarts, Ordering::Relaxed);

                Ok(())
            }
        })?;

        let messages =
            collect_invoke_commands_until(&mut shuffle_env.stream, last_invocation_id).await?;

        assert_received_invoke_commands(messages, expected_messages);

        let shuffle_task = TaskCenter::current()
            .cancel_task(shuffle_task)
            .expect("should exist");
        shuffle_task.await?;

        // make sure that we have restarted the shuffle
        assert!(
            total_restarts.load(Ordering::Relaxed) > 0,
            "expecting the shuffle to be restarted a couple of times"
        );

        Ok(())
    }
}
