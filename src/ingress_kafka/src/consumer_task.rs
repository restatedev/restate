// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message as _;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::{ClientConfig, Message};
use restate_ingress_dispatcher::{
    AckReceiver, IngressDeduplicationId, IngressRequest, IngressRequestSender,
};
use restate_types::identifiers::{FullInvocationId, InvocationUuid};
use std::collections::HashMap;
use std::io::Write;
use tokio::sync::oneshot;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    #[error("ingress dispatcher channel is closed")]
    IngressDispatcherClosed,
}

type MessageConsumer = StreamConsumer<DefaultConsumerContext>;

#[derive(Debug, Clone)]
pub enum MessageDispatcherType {
    DispatchEvent,
    DispatchKeyedEvent,
}

impl MessageDispatcherType {
    fn convert_to_service_invocation(
        &self,
        service_name: &str,
        method_name: &str,
        source_name: &str,
        consumer_group_id: &str,
        msg: &impl Message,
    ) -> Result<(IngressRequest, AckReceiver), Error> {
        let dedup_id = Self::generate_deduplication_id(consumer_group_id, msg);
        let ordering_key = MessageDispatcherType::generate_ordering_key(consumer_group_id, msg);

        Ok(match self {
            MessageDispatcherType::DispatchEvent {} => {
                let pb_event = restate_pb::restate::Event {
                    ordering_key: ordering_key.clone(),
                    payload: Bytes::copy_from_slice(
                        msg.payload().expect("There must be a payload!"),
                    ),
                    source: source_name.to_string(),
                    attributes: MessageDispatcherType::generate_events_attributes(msg),
                };

                let fid = FullInvocationId::generate(service_name, ordering_key);

                IngressRequest::background_invocation(
                    fid,
                    method_name,
                    pb_event.encode_to_vec(),
                    Default::default(),
                    Some(dedup_id),
                )
            }
            MessageDispatcherType::DispatchKeyedEvent => {
                let event_key =
                    MessageDispatcherType::generate_restate_key(msg.key().unwrap_or_default());

                let pb_event = restate_pb::restate::KeyedEvent {
                    key: event_key.clone(),
                    payload: Bytes::copy_from_slice(
                        msg.payload().expect("There must be a payload!"),
                    ),
                    source: source_name.to_string(),
                    attributes: MessageDispatcherType::generate_events_attributes(msg),
                };

                // For keyed events, we dispatch them through the Proxy service, to avoid scattering the offset info throughout all the partitions
                let proxy_fid =
                    FullInvocationId::generate(restate_pb::PROXY_SERVICE_NAME, ordering_key);

                IngressRequest::background_invocation(
                    proxy_fid,
                    restate_pb::PROXY_PROXY_THROUGH_METHOD_NAME,
                    restate_pb::restate::internal::ProxyThroughRequest {
                        target_service: service_name.to_string(),
                        target_method: method_name.to_string(),
                        target_key: event_key,
                        target_invocation_uuid: Bytes::copy_from_slice(
                            InvocationUuid::now_v7().as_bytes(),
                        ),
                        input: pb_event.encode_to_vec().into(),
                    }
                    .encode_to_vec(),
                    Default::default(),
                    Some(dedup_id),
                )
            }
        })
    }

    fn generate_ordering_key(ordering_key_prefix: &str, msg: &impl Message) -> Bytes {
        let mut buf = BytesMut::new();
        write!(
            (&mut buf).writer(),
            "{ordering_key_prefix}-{}-{}",
            msg.topic(),
            msg.partition()
        )
        .expect("Writing to an in memory buffer should be infallible!");

        Self::generate_restate_key(buf.freeze())
    }

    fn generate_restate_key(key: impl Buf) -> Bytes {
        // Because this needs to be a valid Restate key, we need to prepend it with its length to make it
        // look like it was extracted using the RestateKeyExtractor
        // This is done to ensure all the other operations on the key will work correctly (e.g. key to json)
        let key_len = key.remaining();
        let mut buf =
            BytesMut::with_capacity(prost::encoding::encoded_len_varint(key_len as u64) + key_len);
        prost::encoding::encode_varint(key_len as u64, &mut buf);
        buf.put(key);
        buf.freeze()
    }

    fn generate_events_attributes(msg: &impl Message) -> HashMap<String, String> {
        let mut attributes = HashMap::with_capacity(3);
        attributes.insert("kafka.offset".to_string(), msg.offset().to_string());
        attributes.insert("kafka.topic".to_string(), msg.topic().to_string());
        attributes.insert("kafka.partition".to_string(), msg.partition().to_string());
        if let Some(timestamp) = msg.timestamp().to_millis() {
            attributes.insert("kafka.timestamp".to_string(), timestamp.to_string());
        }
        attributes
    }

    fn generate_deduplication_id(
        consumer_group: &str,
        msg: &impl Message,
    ) -> IngressDeduplicationId {
        (
            format!("{consumer_group}-{}-{}", msg.topic(), msg.partition()),
            msg.offset() as u64,
        )
    }
}

#[derive(Debug, Clone)]
pub struct MessageSender {
    service_name: String,
    method_name: String,
    source_name: String,
    message_dispatcher_type: MessageDispatcherType,
    tx: IngressRequestSender,
}

impl MessageSender {
    pub fn new(
        service_name: String,
        method_name: String,
        source_name: String,
        message_dispatcher_type: MessageDispatcherType,
        tx: IngressRequestSender,
    ) -> Self {
        Self {
            service_name,
            method_name,
            source_name,
            message_dispatcher_type,
            tx,
        }
    }

    async fn send(
        &mut self,
        consumer_group_id: &str,
        msg: &BorrowedMessage<'_>,
    ) -> Result<(), Error> {
        let (req, rx) = self.message_dispatcher_type.convert_to_service_invocation(
            &self.service_name,
            &self.method_name,
            &self.source_name,
            consumer_group_id,
            msg,
        )?;

        self.tx
            .send(req)
            .map_err(|_| Error::IngressDispatcherClosed)?;
        rx.await.map_err(|_| Error::IngressDispatcherClosed)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ConsumerTask {
    client_config: ClientConfig,
    topics: Vec<String>,
    sender: MessageSender,
}

impl ConsumerTask {
    pub fn new(client_config: ClientConfig, topics: Vec<String>, sender: MessageSender) -> Self {
        Self {
            client_config,
            topics,
            sender,
        }
    }

    pub async fn run(mut self, mut rx: oneshot::Receiver<()>) -> Result<(), Error> {
        // Create the consumer and subscribe to the topic
        let consumer_group_id = self
            .client_config
            .get("group.id")
            .expect("group.id must be set")
            .to_string();
        let consumer: MessageConsumer = self.client_config.create()?;
        let topics: Vec<&str> = self.topics.iter().map(|x| &**x).collect();
        consumer.subscribe(&topics)?;

        loop {
            tokio::select! {
                res = consumer.recv() => {
                    let msg = res?;
                    self.sender.send(&consumer_group_id, &msg).await?;
                    // This method tells rdkafka that we have processed this message,
                    // so its offset can be safely committed.
                    // rdkafka periodically commits these offsets asynchronously, with a period configurable
                    // with auto.commit.interval.ms
                    consumer.store_offset_from_message(&msg)?;
                }
                _ = &mut rx => {
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use base64::prelude::*;
    use rdkafka::message::OwnedMessage;
    use rdkafka::Timestamp;
    use restate_pb::mocks::EVENT_HANDLER_SERVICE_NAME;
    use restate_schema_api::endpoint::EndpointMetadata;
    use restate_schema_api::key::{RestateKeyConverter, ServiceInstanceType};
    use restate_schema_impl::{Schemas, ServiceRegistrationRequest};

    fn schemas_mock() -> Schemas {
        let schemas = Schemas::default();

        schemas
            .apply_updates(
                schemas
                    .compute_new_endpoint_updates(
                        EndpointMetadata::mock_with_uri("http://localhost:8080"),
                        vec![ServiceRegistrationRequest::new(
                            EVENT_HANDLER_SERVICE_NAME.to_string(),
                            ServiceInstanceType::keyed_with_scalar_key([("Handle", 1)]),
                        )],
                        restate_pb::mocks::DESCRIPTOR_POOL.clone(),
                        false,
                    )
                    .unwrap(),
            )
            .unwrap();

        schemas
    }

    #[test]
    fn generated_event_is_well_formed() {
        let msg_dispatcher_type = MessageDispatcherType::DispatchEvent {};
        let schemas = schemas_mock();
        let msg = OwnedMessage::new(
            Some(b"123".to_vec()),
            None,
            "abc".to_string(),
            Timestamp::now(),
            10,
            50,
            None,
        );
        let source_name = "my-source";
        let consumer_group = "my-consumer-group";

        let (ingress_req, _) = msg_dispatcher_type
            .convert_to_service_invocation(
                EVENT_HANDLER_SERVICE_NAME,
                "Handle",
                source_name,
                consumer_group,
                &msg,
            )
            .unwrap();
        let (actual_fid, method_name, mut payload, _, actual_dedup_id, _) =
            ingress_req.expect_dedupable_background_invocation();

        assert_eq!(
            actual_fid.service_id.service_name,
            EVENT_HANDLER_SERVICE_NAME
        );
        assert_eq!(method_name, "Handle");

        // This should have been converted to a base 64 encoded key
        let actual_json_key = schemas
            .key_to_json(
                EVENT_HANDLER_SERVICE_NAME,
                actual_fid.service_id.key.clone(),
            )
            .unwrap();
        let actual_key = String::from_utf8(
            BASE64_STANDARD
                .decode(actual_json_key.as_str().unwrap())
                .unwrap(),
        )
        .unwrap();
        assert_eq!(actual_key, "my-consumer-group-abc-10");

        // Event contains the original payload
        let actual_payload = restate_pb::restate::Event::decode(&mut payload)
            .unwrap()
            .payload;
        assert_eq!(actual_payload, msg.payload().unwrap());

        // Assert deduplication id
        let expected_dedup_id =
            MessageDispatcherType::generate_deduplication_id(consumer_group, &msg);
        assert_eq!(actual_dedup_id, expected_dedup_id);
    }
}
