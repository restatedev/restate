// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message as _;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use restate_ingress_dispatcher::{AckReceiver, IngressRequest, IngressRequestSender};
use restate_types::identifiers::FullInvocationId;

// Remove and replace with proper error type
pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type MessageConsumer = StreamConsumer<DefaultConsumerContext>;

pub enum MessageDispatcherType {
    DispatchEvent { ordering_key_prefix: String },
    DispatchKeyedEvent,
}

impl MessageDispatcherType {
    fn convert_to_service_invocation(
        &self,
        service_name: &str,
        method_name: &str,
        source_name: &str,
        msg: &BorrowedMessage<'_>,
    ) -> Result<(AckReceiver, IngressRequest), Error> {
        Ok(match self {
            MessageDispatcherType::DispatchEvent {
                ordering_key_prefix,
            } => {
                let key = MessageDispatcherType::generate_ordering_key(&ordering_key_prefix, msg);

                let pb_event = restate_pb::restate::Event {
                    payload: Bytes::copy_from_slice(
                        msg.payload().expect("There must be a payload!"),
                    ),
                    source: source_name.to_string(),
                    // TODO add attributes
                    ..Default::default()
                };

                let fid = FullInvocationId::generate(service_name, key);

                IngressRequest::background_invocation(
                    fid,
                    method_name,
                    pb_event.encode_to_vec(),
                    Default::default(),
                )
            }
            MessageDispatcherType::DispatchKeyedEvent => {
                let key =
                    MessageDispatcherType::generate_restate_key(msg.key().unwrap_or_default());

                let pb_event = restate_pb::restate::KeyedEvent {
                    key: key.clone(),
                    payload: Bytes::copy_from_slice(
                        msg.payload().expect("There must be a payload!"),
                    ),
                    source: source_name.to_string(),
                    // TODO add attributes
                    ..Default::default()
                };

                let fid = FullInvocationId::generate(service_name, key);

                IngressRequest::background_invocation(
                    fid,
                    method_name,
                    pb_event.encode_to_vec(),
                    Default::default(),
                )
            }
        })
    }

    // TODO add a test with key to json for this!
    fn generate_ordering_key(ordering_key_prefix: &String, msg: &BorrowedMessage<'_>) -> Bytes {
        Self::generate_restate_key(
            format!(
                "{ordering_key_prefix}-{}-{}-{}",
                msg.topic(),
                msg.partition(),
                msg.offset()
            )
            .as_bytes(),
        )
    }

    fn generate_restate_key(key: &[u8]) -> Bytes {
        // Because this needs to be a valid Restate key, we need to prepend it with its length to make it
        // look like it was extracted using the RestateKeyExtractor
        // This is done to ensure all the other operations on the key will work correctly (e.g. key to json)
        let mut buf = BytesMut::with_capacity(
            prost::encoding::encoded_len_varint(key.len() as u64) + key.len(),
        );
        prost::encoding::encode_varint(key.len() as u64, &mut buf);
        buf.put_slice(key);
        return buf.freeze();
    }
}

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

    async fn send(&mut self, msg: &BorrowedMessage<'_>) -> Result<(), Error> {
        let (rx, req) = self.message_dispatcher_type.convert_to_service_invocation(
            &self.service_name,
            &self.method_name,
            &self.source_name,
            &msg,
        )?;

        self.tx.send(req)?;
        Ok(rx.await?)
    }
}

pub struct ConsumerTask {
    consumer: MessageConsumer,
    sender: MessageSender,
}

impl ConsumerTask {
    // TODO Validate Client config!
    // It shouldn't be done here, but in the meta/schema api when creating the subscription,
    // to fail fast in case of wrong subscription config.
    // Validation constraints:
    // If no group.id, create one and notify it back
    // "enable.auto.commit" == "true"
    // "enable.auto.offset.store" == "false"
    pub fn new(consumer: MessageConsumer, sender: MessageSender) -> Self {
        Self { consumer, sender }
    }

    pub async fn run(mut self) -> Result<(), Error> {
        loop {
            let msg = self.consumer.recv().await?;
            self.sender.send(&msg).await?;

            // The offset will be auto-committed wrt auto.commit.interval.ms config option
            self.consumer.store_offset_from_message(&msg)?;
        }
    }
}
