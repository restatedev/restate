// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::loglet::{
    AppendError, Loglet, LogletBase, LogletOffset, LogletProvider, LogletProviderFactory,
    LogletReadStream, OperationError, SendableLogletReadStream,
};
use crate::record::ErasedInputRecord;
use crate::{Error, Header, LogEntry, Record, TailState};
use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{stream, Future, Stream, StreamExt};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use restate_types::config::KafkaLogletOptions;
use restate_types::flexbuffers_storage_encode_decode;
use restate_types::logs::metadata::{LogletParams, ProviderKind, SegmentIndex};
use restate_types::logs::{KeyFilter, Keys, LogId, SequenceNumber};
use restate_types::retries::RetryPolicy;
use restate_types::storage::{PolyBytes, StorageCodec};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task;
use std::task::{ready, Poll};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::debug;

pub struct KafkaLogletProviderFactory {
    topic: String,
    client_config: ClientConfig,
}

impl KafkaLogletProviderFactory {
    pub fn new(kafka_loglet_options: &KafkaLogletOptions) -> Self {
        let mut client_config = ClientConfig::default();

        client_config.set(
            "metadata.broker.list",
            kafka_loglet_options.brokers.join(","),
        );
        client_config.set("group.id", "restate");
        client_config.set("broker.address.family", "v4");

        Self {
            topic: kafka_loglet_options.topic.clone(),
            client_config,
        }
    }
}

#[async_trait]
impl LogletProviderFactory for KafkaLogletProviderFactory {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Kafka
    }

    async fn create(
        self: Box<Self>,
    ) -> crate::Result<Arc<dyn LogletProvider + 'static>, OperationError> {
        debug!("Started Kafka loglet provider");
        Ok(Arc::new(KafkaLogletProvider {
            topic: self.topic,
            client_config: self.client_config,
            loglets: Mutex::default(),
        }))
    }
}

pub struct KafkaLogletProvider {
    topic: String,
    client_config: ClientConfig,
    loglets: Mutex<BTreeMap<i32, Arc<KafkaLoglet>>>,
}

#[async_trait]
impl LogletProvider for KafkaLogletProvider {
    async fn get_loglet(
        &self,
        log_id: LogId,
        _segment_index: SegmentIndex,
        _params: &LogletParams,
    ) -> crate::Result<Arc<dyn Loglet<Offset = LogletOffset>>> {
        let partition: i32 = i32::try_from(u64::from(log_id)).expect("log_id should fit into i32");

        let mut guard = self.loglets.lock().await;

        if let Some(loglet) = guard.get(&partition) {
            Ok(Arc::clone(loglet) as Arc<dyn Loglet>)
        } else {
            let producer = self
                .client_config
                .create()
                .map_err(|err| Error::Generic(Arc::new(err.into())))?;

            let loglet = Arc::new(KafkaLoglet::new(
                self.topic.clone(),
                partition,
                self.client_config.clone(),
                producer,
            ));

            guard.insert(partition, Arc::clone(&loglet));

            Ok(loglet as Arc<dyn Loglet>)
        }
    }
}

pub struct KafkaLoglet {
    topic: String,
    partition: i32,
    client_config: ClientConfig,
    producer: FutureProducer,
}

impl Debug for KafkaLoglet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaLoglet")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("client_config", &self.client_config)
            .finish()
    }
}

impl KafkaLoglet {
    fn new(
        topic: String,
        partition: i32,
        client_config: ClientConfig,
        producer: FutureProducer,
    ) -> Self {
        Self {
            topic,
            partition,
            client_config,
            producer,
        }
    }

    async fn append_batch(
        &self,
        records: Arc<[ErasedInputRecord]>,
    ) -> crate::Result<LogletOffset, AppendError> {
        let mut serde_buffer = BytesMut::with_capacity(1024);
        let mut last_offset = 0;

        for record in records.as_ref() {
            StorageCodec::encode(record.body.as_ref(), &mut serde_buffer)
                .map_err(AppendError::terminal)?;
            let payload = KafkaPayload {
                header: record.header.clone(),
                keys: record.keys.clone(),
                payload: serde_buffer.split().freeze(),
            };
            StorageCodec::encode(&payload, &mut serde_buffer).map_err(AppendError::terminal)?;
            let data = serde_buffer.split().freeze();
            let record: FutureRecord<(), _> = FutureRecord::to(&self.topic)
                .partition(self.partition)
                .payload(data.chunk());
            let (partition, offset) = self
                .producer
                .send(record, Timeout::Never)
                .await
                .map_err(|(err, _)| AppendError::terminal(err))?;

            assert_eq!(self.partition, partition);

            last_offset = offset;
        }

        Ok(Self::kafka_offset_to_loglet_offset(last_offset))
    }

    fn kafka_offset_to_loglet_offset(offset: i64) -> LogletOffset {
        LogletOffset::from((offset + 1) as u64)
    }

    fn loglet_offset_to_kafka_offset(offset: LogletOffset) -> i64 {
        offset.0.saturating_sub(1) as i64
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct KafkaPayload {
    header: Header,
    keys: Keys,
    payload: Bytes,
}

flexbuffers_storage_encode_decode!(KafkaPayload);

#[async_trait]
impl LogletBase for KafkaLoglet {
    type Offset = LogletOffset;

    async fn create_read_stream(
        self: Arc<Self>,
        _filter: KeyFilter,
        from: Self::Offset,
        _to: Option<Self::Offset>,
    ) -> Result<SendableLogletReadStream<Self::Offset>, OperationError> {
        let consumer = initialize_consumer(&self.client_config, &self.topic, self.partition)?;
        seek(
            &consumer,
            &self.topic,
            self.partition,
            Offset::Offset(Self::loglet_offset_to_kafka_offset(from)),
        )
        .await?;

        Ok(Box::pin(KafkaLogletReadStream::new(consumer)))
    }

    fn watch_tail(&self) -> BoxStream<'static, TailState<Self::Offset>> {
        stream::repeat(TailState::Open(Self::Offset::INVALID)).boxed()
    }

    async fn append_batch(
        &self,
        payloads: Arc<[ErasedInputRecord]>,
    ) -> crate::Result<Self::Offset, AppendError> {
        let last_offset = self.append_batch(payloads).await?;

        Ok(last_offset)
    }

    async fn find_tail(&self) -> crate::Result<TailState<Self::Offset>, OperationError> {
        Ok(TailState::Open(Self::Offset::OLDEST))
    }

    async fn get_trim_point(&self) -> crate::Result<Option<Self::Offset>, OperationError> {
        Ok(None)
    }

    async fn trim(&self, _trim_point: Self::Offset) -> crate::Result<(), OperationError> {
        Ok(())
    }

    async fn seal(&self) -> crate::Result<(), OperationError> {
        Err(OperationError::terminal(UnsupportedError))
    }
}

fn initialize_consumer(
    client_config: &ClientConfig,
    topic: &str,
    partition: i32,
) -> Result<StreamConsumer, OperationError> {
    let consumer: StreamConsumer = client_config.create().map_err(OperationError::terminal)?;
    let mut topic_partition_list = TopicPartitionList::new();
    topic_partition_list.add_partition(topic, partition);

    consumer
        .assign(&topic_partition_list)
        .map_err(OperationError::terminal)?;

    Ok(consumer)
}

async fn seek(
    consumer: &StreamConsumer,
    topic: &str,
    partition: i32,
    offset: Offset,
) -> crate::Result<(), OperationError> {
    let retry_policy = RetryPolicy::exponential(Duration::from_millis(10), 2.0, Some(10), None);
    retry_policy
        .retry(|| async {
            consumer
                .seek(topic, partition, offset, Timeout::Never)
                .map_err(OperationError::terminal)
        })
        .await?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("unsupported operation")]
struct UnsupportedError;

struct KafkaLogletReadStream {
    consumer: StreamConsumer,
}

impl KafkaLogletReadStream {
    fn new(consumer: StreamConsumer) -> Self {
        Self { consumer }
    }
}

impl Stream for KafkaLogletReadStream {
    type Item = crate::Result<LogEntry<LogletOffset>, OperationError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let next_message = std::pin::pin!(self.consumer.recv());
        let ready = ready!(next_message.poll(cx));

        let result = match ready {
            Ok(message) => {
                let payload = StorageCodec::decode::<KafkaPayload, _>(
                    &mut message.payload().unwrap_or_default(),
                );

                match payload {
                    Ok(payload) => {
                        let record = Record::from_parts(
                            payload.header,
                            payload.keys,
                            PolyBytes::Bytes(payload.payload),
                        );
                        Ok(LogEntry::new_data(
                            KafkaLoglet::kafka_offset_to_loglet_offset(message.offset()),
                            record,
                        ))
                    }
                    Err(err) => Err(OperationError::terminal(err)),
                }
            }
            Err(err) => Err(OperationError::terminal(err)),
        };

        Poll::Ready(Some(result))
    }
}

impl LogletReadStream<LogletOffset> for KafkaLogletReadStream {
    fn read_pointer(&self) -> LogletOffset {
        todo!()
    }

    fn is_terminated(&self) -> bool {
        todo!()
    }
}
