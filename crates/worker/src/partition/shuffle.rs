// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use metrics::{counter, histogram};
use restate_types::config::Configuration;
use tokio::sync::mpsc;
use tracing::debug;

use restate_bifrost::Bifrost;
use restate_core::cancellation_token;
use restate_core::network::TransportConnect;
use restate_ingestion_client::IngestionClient;
use restate_storage_api::deduplication_table::DedupInformation;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, WithPartitionKey};
use restate_types::message::MessageIndex;
use restate_wal_protocol::{Destination, Envelope, Header, Source};

use crate::metric_definitions::{
    PARTITION_LABEL, PARTITION_SHUFFLE_INFLIGHT_COUNT, PARTITION_SHUFFLE_MESSAGE_COUNT,
};
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
    inner: ShuffleInner<T, OR>,
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
        bifrost: &Bifrost,
        ingestion_client: IngestionClient<T, Envelope>,
    ) -> Self {
        let batch_ingestion = Configuration::pinned()
            .common
            .experimental_shuffler_batch_ingestion;

        let inner = if batch_ingestion {
            debug!("Using Shuffler experimental batch ingestion mechanism");
            ShuffleInner::IngestionClient(IngestionClientShuffleInner::new(
                metadata,
                outbox_reader,
                truncation_tx,
                channel_size,
                ingestion_client,
            ))
        } else {
            debug!("Using Shuffler legacy ingestion mechanism");
            ShuffleInner::Legacy(LegacyShuffleInner::new(
                metadata,
                outbox_reader,
                truncation_tx,
                channel_size,
                bifrost.clone(),
            ))
        };

        Self { inner }
    }

    pub(super) fn create_hint_sender(&self) -> HintSender {
        match &self.inner {
            ShuffleInner::Legacy(inner) => inner.create_hint_sender(),
            ShuffleInner::IngestionClient(inner) => inner.create_hint_sender(),
        }
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        match self.inner {
            ShuffleInner::Legacy(inner) => inner.run().await,
            ShuffleInner::IngestionClient(inner) => inner.run().await,
        }
    }
}

enum ShuffleInner<T, OR> {
    Legacy(LegacyShuffleInner<OR>),
    IngestionClient(IngestionClientShuffleInner<T, OR>),
}

struct IngestionClientShuffleInner<T, OR> {
    metadata: ShuffleMetadata,
    outbox_reader: OR,
    ingestion_client: IngestionClient<T, Envelope>,
    // used to tell partition processor about outbox truncations
    truncation_tx: mpsc::Sender<OutboxTruncation>,
    hint_rx: async_channel::Receiver<NewOutboxMessage>,
    // used to create the senders into the shuffle
    hint_tx: async_channel::Sender<NewOutboxMessage>,
}

impl<T, OR> IngestionClientShuffleInner<T, OR>
where
    T: TransportConnect,
    OR: OutboxReader + Send + Sync + 'static,
{
    pub fn new(
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

    fn create_hint_sender(&self) -> HintSender {
        HintSender::new(self.hint_tx.clone(), self.hint_rx.clone())
    }

    async fn run(self) -> anyhow::Result<()> {
        let result = cancellation_token()
            .run_until_cancelled(self.shuffle())
            .await
            .unwrap_or(Ok(()));

        debug!("Stopping shuffle");

        result
    }

    async fn shuffle(self) -> anyhow::Result<()> {
        use futures::future::OptionFuture;
        use std::collections::VecDeque;

        let Self {
            metadata,
            hint_rx,
            outbox_reader,
            truncation_tx,
            ingestion_client,
            ..
        } = self;

        debug!(restate.partition.id = %metadata.partition_id, "Running shuffle");

        let mut state_machine =
            state_machine::StateMachine::new(metadata, ingestion_client, outbox_reader, hint_rx);

        let mut inflight = VecDeque::new();

        let ingested_counter = counter!(
            PARTITION_SHUFFLE_MESSAGE_COUNT,
            PARTITION_LABEL => self.metadata.partition_id.to_string(),
        );

        let inflight_count = histogram!(
            PARTITION_SHUFFLE_INFLIGHT_COUNT,
            PARTITION_LABEL => self.metadata.partition_id.to_string(),
        );

        loop {
            inflight_count.record(inflight.len() as f64);
            let head = OptionFuture::from(inflight.front_mut());
            tokio::select! {
                commit_token = state_machine.shuffle_next_message() => {
                    let commit_token = commit_token?;
                    inflight.push_back(commit_token);
                }
                Some(committed) = head => {
                    let message_index = committed?;
                    _ = inflight.pop_front();
                    ingested_counter.increment(1);
                    let _ = truncation_tx.try_send(OutboxTruncation::new(message_index));
                }
            }
        }
    }
}

struct LegacyShuffleInner<OR> {
    metadata: ShuffleMetadata,
    outbox_reader: OR,
    bifrost: Bifrost,
    // used to tell partition processor about outbox truncations
    truncation_tx: mpsc::Sender<OutboxTruncation>,
    hint_rx: async_channel::Receiver<NewOutboxMessage>,
    // used to create the senders into the shuffle
    hint_tx: async_channel::Sender<NewOutboxMessage>,
}

impl<OR> LegacyShuffleInner<OR>
where
    OR: OutboxReader + Send + Sync + 'static,
{
    pub(super) fn new(
        metadata: ShuffleMetadata,
        outbox_reader: OR,
        truncation_tx: mpsc::Sender<OutboxTruncation>,
        channel_size: usize,
        bifrost: Bifrost,
    ) -> Self {
        let (hint_tx, hint_rx) = async_channel::bounded(channel_size);

        Self {
            metadata,
            outbox_reader,
            truncation_tx,
            hint_rx,
            hint_tx,
            bifrost,
        }
    }

    fn create_hint_sender(&self) -> HintSender {
        HintSender::new(self.hint_tx.clone(), self.hint_rx.clone())
    }

    async fn run(self) -> anyhow::Result<()> {
        let result = cancellation_token()
            .run_until_cancelled(self.shuffle())
            .await
            .unwrap_or(Ok(()));

        debug!("Stopping shuffle");

        result
    }

    async fn shuffle(self) -> anyhow::Result<()> {
        use std::sync::Arc;

        let Self {
            metadata,
            mut hint_rx,
            outbox_reader,
            truncation_tx,
            bifrost,
            ..
        } = self;

        debug!(restate.partition.id = %metadata.partition_id, "Running shuffle");

        let ingested_counter = counter!(
            PARTITION_SHUFFLE_MESSAGE_COUNT,
            PARTITION_LABEL => self.metadata.partition_id.to_string(),
        );

        let state_machine = legacy_state_machine::StateMachine::new(
            metadata,
            outbox_reader,
            move |msg| {
                let bifrost = bifrost.clone();
                let ingested_counter = ingested_counter.clone();
                async move {
                    restate_bifrost::append_to_bifrost(&bifrost, Arc::new(msg)).await?;
                    ingested_counter.increment(1);
                    Ok(())
                }
            },
            &mut hint_rx,
        );

        tokio::pin!(state_machine);

        loop {
            let shuffled_message_index = state_machine.as_mut().shuffle_next_message().await?;

            // this is just a hint which we can drop
            let _ = truncation_tx.try_send(OutboxTruncation::new(shuffled_message_index));
        }
    }
}

mod state_machine {
    use std::cmp::Ordering;

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

    enum State {
        Idle,
        ReadingOutbox,
        Ingesting { ingest: IngestFuture, sn: u64 },
    }

    pub struct StateMachine<T, R> {
        metadata: ShuffleMetadata,
        ingestion: IngestionClient<T, Envelope>,
        hint_rx: async_channel::Receiver<NewOutboxMessage>,
        reader: Option<R>,
        read_fut: ReadFuture<R>,
        next_sequence_number: MessageIndex,
        state: State,
    }

    impl<T, R> StateMachine<T, R>
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
                next_sequence_number: 0,
                state: State::ReadingOutbox,
            }
        }

        pub async fn shuffle_next_message(&mut self) -> anyhow::Result<RecordCommit<u64>> {
            loop {
                match &mut self.state {
                    State::Idle => {
                        let NewOutboxMessage {
                            seq_number: sn,
                            message,
                        } = self
                            .hint_rx
                            .recv()
                            .await
                            .expect("shuffle is owning the hint sender");

                        match sn.cmp(&self.next_sequence_number) {
                            Ordering::Equal => {
                                let envelope =
                                    wrap_outbox_message_in_envelope(message, sn, &self.metadata);
                                self.state = State::Ingesting {
                                    ingest: self
                                        .ingestion
                                        .ingest(envelope.partition_key(), envelope),
                                    sn,
                                };
                            }
                            Ordering::Greater => {
                                // Missed hints; we need to do an outbox scan
                                self.read_fut.set(get_next_message(
                                    self.reader.take().unwrap(),
                                    self.next_sequence_number,
                                ));
                                self.state = State::ReadingOutbox;
                            }
                            Ordering::Less => {
                                // this is a hint for a message that we have already sent, so we can ignore it
                            }
                        }
                    }
                    State::Ingesting { ingest, sn } => {
                        let sn = *sn;
                        let commit_token = ingest.await?.map(|_| sn);

                        // read next message
                        self.next_sequence_number = sn + 1;
                        self.read_fut.set(get_next_message(
                            self.reader.take().unwrap(),
                            self.next_sequence_number,
                        ));
                        self.state = State::ReadingOutbox;

                        return Ok(commit_token);
                    }
                    State::ReadingOutbox => {
                        let (result, reader) = self.read_fut.get_pin().await;
                        self.reader = Some(reader);

                        match result? {
                            None => {
                                self.state = State::Idle;
                            }
                            Some((sn, message)) => {
                                assert!(
                                    sn >= self.next_sequence_number,
                                    "message sequence numbers must not decrease"
                                );
                                let envelope =
                                    wrap_outbox_message_in_envelope(message, sn, &self.metadata);
                                self.state = State::Ingesting {
                                    ingest: self
                                        .ingestion
                                        .ingest(envelope.partition_key(), envelope),
                                    sn,
                                };
                            }
                        }
                    }
                }
            }
        }
    }
}

mod legacy_state_machine {
    use pin_project::pin_project;
    use std::cmp::Ordering;
    use std::future::Future;
    use std::pin::Pin;
    use tokio_util::sync::ReusableBoxFuture;
    use tracing::trace;

    use restate_storage_api::outbox_table::OutboxMessage;
    use restate_types::message::MessageIndex;
    use restate_wal_protocol::Envelope;

    use crate::partition::shuffle;
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
                                let envelope = wrap_outbox_message_in_envelope(
                                    message.clone(),
                                    seq_number,
                                    this.metadata,
                                );
                                let send_future = (this.send_operation)(envelope);
                                this.state.set(State::Sending(send_future));
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
                            }
                            Ordering::Less => {
                                // this is a hint for a message that we have already sent, so we can ignore it
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

                            let envelope =
                                wrap_outbox_message_in_envelope(message, seq_number, this.metadata);
                            let send_future = (this.send_operation)(envelope);

                            this.state.set(State::Sending(send_future));
                        } else {
                            this.state.set(State::Idle);
                        }
                    }
                    StateProj::Sending(send_future) => {
                        send_future.await?;

                        let successfully_shuffled_sequence_number = *this.current_sequence_number;
                        *this.current_sequence_number += 1;

                        this.read_future.set(get_next_message(
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

#[cfg(test)]
mod ingestion_client_tests {
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
    use restate_memory::MemoryBudget;
    use restate_storage_api::StorageError;
    use restate_storage_api::outbox_table::OutboxMessage;
    use restate_types::Version;
    use restate_types::identifiers::{InvocationId, LeaderEpoch, PartitionId};
    use restate_types::invocation::ServiceInvocation;
    use restate_types::message::MessageIndex;
    use restate_types::partition_table::PartitionTable;
    use restate_wal_protocol::{Command, Envelope};

    use crate::partition::shuffle::{
        IngestionClientShuffleInner, OutboxReader, OutboxReaderError, ShuffleMetadata,
    };

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
        ingestion: IngestionClient<FailingConnector, Envelope>,
        shuffle: IngestionClientShuffleInner<FailingConnector, OR>,
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

        // Original test used buffer_size=10, BackPressureMode::PushBack
        // Using a small pool for tests (~20KB)
        let pool = MemoryBudget::new("test-partition-leader", 20 * 1024, NonZeroUsize::MIN);
        let svc = builder
            .router_builder
            .register_service::<PartitionLeaderService>(pool, BackPressureMode::PushBack);

        let env = builder.build().await;

        let stream = svc.start();

        let ingestion = IngestionClient::new(
            env.networking.clone(),
            env.metadata.updateable_partition_table(),
            PartitionRouting::new(partition_replica_set_states, TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(),
            None,
        );

        let (truncation_tx, _truncation_rx) = mpsc::channel(1);

        let shuffle = IngestionClientShuffleInner::new(
            metadata,
            outbox_reader,
            truncation_tx,
            1,
            ingestion.clone(),
        );

        ShuffleEnv {
            env,
            stream,
            ingestion,
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

                    shuffle = IngestionClientShuffleInner::new(
                        metadata,
                        Arc::clone(&outbox_reader),
                        truncation_tx.clone(),
                        1,
                        shuffle_env.ingestion.clone(),
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

#[cfg(test)]
mod legacy_tests {
    use std::iter;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::anyhow;
    use assert2::let_assert;
    use futures::{Stream, StreamExt};
    use test_log::test;
    use tokio::sync::mpsc;

    use restate_bifrost::{Bifrost, LogEntry};
    use restate_core::network::FailingConnector;
    use restate_core::{TaskCenter, TaskKind, TestCoreEnv, TestCoreEnvBuilder};
    use restate_storage_api::StorageError;
    use restate_storage_api::outbox_table::OutboxMessage;
    use restate_types::Version;
    use restate_types::identifiers::{InvocationId, LeaderEpoch, PartitionId};
    use restate_types::invocation::ServiceInvocation;
    use restate_types::logs::{KeyFilter, LogId, Lsn, SequenceNumber};
    use restate_types::message::MessageIndex;
    use restate_types::partition_table::PartitionTable;
    use restate_wal_protocol::{Command, Envelope};

    use crate::partition::shuffle::{
        LegacyShuffleInner, OutboxReader, OutboxReaderError, ShuffleMetadata,
    };

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
        stream: impl Stream<Item = restate_bifrost::Result<LogEntry>>,
        last_invocation_id: InvocationId,
    ) -> anyhow::Result<Vec<ServiceInvocation>> {
        let mut messages = Vec::new();
        let mut stream = std::pin::pin!(stream);

        while let Some(record) = stream.next().await {
            let record = record?;

            if let Some(envelope) = record.try_decode::<Envelope>().transpose()? {
                let_assert!(Command::Invoke(service_invocation) = envelope.command);
                let invocation_id = service_invocation.invocation_id;
                messages.push(*service_invocation);

                if last_invocation_id == invocation_id {
                    break;
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
        bifrost: Bifrost,
        shuffle: LegacyShuffleInner<OR>,
    }

    async fn create_shuffle_env<OR: OutboxReader + Send + Sync + 'static>(
        outbox_reader: OR,
    ) -> ShuffleEnv<OR> {
        // set numbers of partitions to 1 to easily find all sent messages by the shuffle
        let env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_partition_table(PartitionTable::with_equally_sized_partitions(
                Version::MIN,
                1,
            ))
            .build()
            .await;
        let metadata = ShuffleMetadata::new(PartitionId::from(0), LeaderEpoch::from(0));

        let (truncation_tx, _truncation_rx) = mpsc::channel(1);

        let bifrost = Bifrost::init_in_memory(env.metadata_writer.clone()).await;
        let shuffle =
            LegacyShuffleInner::new(metadata, outbox_reader, truncation_tx, 1, bifrost.clone());

        ShuffleEnv {
            env,
            bifrost,
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
        let shuffle_env = create_shuffle_env(outbox_reader).await;

        let partition_id = shuffle_env.shuffle.metadata.partition_id;
        TaskCenter::spawn_child(TaskKind::Shuffle, "shuffle", shuffle_env.shuffle.run())?;
        let reader = shuffle_env.bifrost.create_reader(
            LogId::from(partition_id),
            KeyFilter::Any,
            Lsn::OLDEST,
            Lsn::MAX,
        )?;

        let messages = collect_invoke_commands_until(reader, last_invocation_id).await?;

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
        let shuffle_env = create_shuffle_env(outbox_reader).await;

        let partition_id = shuffle_env.shuffle.metadata.partition_id;
        TaskCenter::spawn_child(TaskKind::Shuffle, "shuffle", shuffle_env.shuffle.run())?;
        let reader = shuffle_env.bifrost.create_reader(
            LogId::from(partition_id),
            KeyFilter::Any,
            Lsn::OLDEST,
            Lsn::MAX,
        )?;

        let messages = collect_invoke_commands_until(reader, last_invocation_id).await?;

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
        let shuffle_env = create_shuffle_env(Arc::clone(&outbox_reader)).await;
        let total_restarts = Arc::new(AtomicUsize::new(0));

        let partition_id = shuffle_env.shuffle.metadata.partition_id;
        let reader = shuffle_env.bifrost.create_reader(
            LogId::from(partition_id),
            KeyFilter::Any,
            Lsn::INVALID,
            Lsn::MAX,
        )?;

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

                    shuffle = LegacyShuffleInner::new(
                        metadata,
                        Arc::clone(&outbox_reader),
                        truncation_tx.clone(),
                        1,
                        shuffle_env.bifrost.clone(),
                    );
                }

                total_restarts.store(num_restarts, Ordering::Relaxed);

                Ok(())
            }
        })?;

        let messages = collect_invoke_commands_until(reader, last_invocation_id).await?;

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
