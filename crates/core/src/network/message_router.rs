// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;

use ahash::HashMap;
use async_trait::async_trait;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{trace, warn};

use restate_types::net::ProtocolVersion;
use restate_types::net::TargetName;
use restate_types::net::codec::{Targeted, WireDecode};

use super::protobuf::network::message::BinaryMessage;
use super::{Incoming, RouterError};
use crate::TaskCenter;

pub type MessageStream<T> = Pin<Box<dyn Stream<Item = Incoming<T>> + Send + Sync + 'static>>;

/// Implement this trait to process network messages for a specific target
/// (e.g. TargetName = METADATA_MANAGER).
pub trait MessageHandler {
    type MessageType: WireDecode + Targeted;
    /// Process the request and return the response asynchronously.
    fn on_message(
        &self,
        msg: Incoming<Self::MessageType>,
    ) -> impl std::future::Future<Output = ()> + Send;
}

impl<T> MessageHandler for Arc<T>
where
    T: MessageHandler,
{
    type MessageType = T::MessageType;

    fn on_message(
        &self,
        msg: Incoming<Self::MessageType>,
    ) -> impl std::future::Future<Output = ()> + Send {
        (**self).on_message(msg)
    }
}

impl<T> MessageHandler for Box<T>
where
    T: MessageHandler,
{
    type MessageType = T::MessageType;

    fn on_message(
        &self,
        msg: Incoming<Self::MessageType>,
    ) -> impl std::future::Future<Output = ()> + Send {
        (**self).on_message(msg)
    }
}

/// A low-level handler trait.
#[async_trait]
pub trait Handler: Send {
    /// Deserialize and process the message asynchronously.
    async fn call(
        &self,
        message: Incoming<BinaryMessage>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), RouterError>;
}

#[async_trait]
impl<T> Handler for Arc<T>
where
    T: Handler + Send + Sync + 'static,
{
    async fn call(
        &self,
        message: Incoming<BinaryMessage>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), RouterError> {
        (**self).call(message, protocol_version).await
    }
}

#[async_trait]
impl<T> Handler for Box<T>
where
    T: Handler + Send + Sync + 'static,
{
    async fn call(
        &self,
        message: Incoming<BinaryMessage>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), RouterError> {
        (**self).call(message, protocol_version).await
    }
}

#[derive(Default)]
pub struct MessageRouter {
    handlers: HashMap<TargetName, Box<dyn Handler + Send + Sync>>,
}

#[async_trait]
impl Handler for MessageRouter {
    /// Process the request and return the response asynchronously.
    async fn call(
        &self,
        message: Incoming<BinaryMessage>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), RouterError> {
        let target = message.body().target();
        let Some(handler) = self.handlers.get(&target) else {
            return Err(RouterError::NotRegisteredTarget(target.to_string()));
        };
        handler.call(message, protocol_version).await?;
        Ok(())
    }
}

#[derive(Default)]
pub struct MessageRouterBuilder {
    handlers: HashMap<TargetName, Box<dyn Handler + Send + Sync>>,
}

impl MessageRouterBuilder {
    /// Attach a handler that implements [`MessageHandler`] to receive messages
    /// for the associated target.
    #[track_caller]
    pub fn add_message_handler<H>(&mut self, handler: H) -> &mut Self
    where
        H: MessageHandler + Send + Sync + 'static,
    {
        let wrapped = MessageHandlerWrapper { inner: handler };
        let target = H::MessageType::TARGET;
        if self.handlers.insert(target, Box::new(wrapped)).is_some() {
            panic!("Handler for target {target} has been registered already!");
        }
        self
    }

    /// Attach a handler that receives all messages targeting a certain [`TargetName`].
    #[track_caller]
    pub fn add_raw_handler(
        &mut self,
        target: TargetName,
        handler: Box<dyn Handler + Send + Sync>,
    ) -> &mut Self {
        if self.handlers.insert(target, handler).is_some() {
            panic!("Handler for target {target} has been registered already!");
        }
        self
    }

    /// Subscribe to a stream of messages for a specific target. This enables consumers of messages
    /// to use async stream API to process messages of a given target as an alternative to the
    /// message callback-style API as in `add_message_handler`.
    #[track_caller]
    pub fn subscribe_to_stream<M>(&mut self, buffer_size: usize) -> MessageStream<M>
    where
        M: WireDecode + Targeted + Send + Sync + 'static,
    {
        let (tx, rx) = mpsc::channel(buffer_size);

        let wrapped = StreamHandlerWrapper { sender: tx };
        let target = M::TARGET;
        if self.handlers.insert(target, Box::new(wrapped)).is_some() {
            panic!("Handler for target {target} has been registered already!");
        }
        Box::pin(ReceiverStream::new(rx))
    }

    /// Finalize this builder and return the message router that can be attached to
    /// [`crate::ConnectionManager`]
    pub fn build(self) -> MessageRouter {
        MessageRouter {
            handlers: self.handlers,
        }
    }
}

struct MessageHandlerWrapper<H> {
    inner: H,
}

#[async_trait]
impl<H> Handler for MessageHandlerWrapper<H>
where
    H: MessageHandler + Send + Sync + 'static,
{
    /// Process the request and return the response asynchronously.
    async fn call(
        &self,
        message: Incoming<BinaryMessage>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), RouterError> {
        let message = message.try_map(|mut m| {
            #[cfg(debug_assertions)]
            let decode_start = tokio::time::Instant::now();

            let res = <H::MessageType as WireDecode>::try_decode(&mut m.payload, protocol_version)
                .map_err(|e| RouterError::Other(e.into()));
            #[cfg(debug_assertions)]
            {
                use super::metric_definitions::NETWORK_MESSAGE_DECODE_DURATION;
                metrics::histogram!(
                    NETWORK_MESSAGE_DECODE_DURATION,
                    "target" => H::MessageType::TARGET.as_str_name()
                )
                .record(decode_start.elapsed());
            }
            res
        })?;
        self.inner.on_message(message).await;
        Ok(())
    }
}

struct StreamHandlerWrapper<M>
where
    M: WireDecode + Targeted + Send + 'static,
{
    sender: mpsc::Sender<Incoming<M>>,
}

#[async_trait]
impl<M> Handler for StreamHandlerWrapper<M>
where
    M: WireDecode + Targeted + Send + 'static,
{
    /// Process the request and return the response asynchronously.
    async fn call(
        &self,
        message: Incoming<BinaryMessage>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), RouterError> {
        let message = message.try_map(|mut m| {
            <M as WireDecode>::try_decode(&mut m.payload, protocol_version)
                .map_err(|e| RouterError::Other(e.into()))
        })?;
        if let Err(e) = self.sender.send(message).await {
            // Can be benign if we are shutting down
            if !TaskCenter::is_shutdown_requested() {
                warn!(
                    "Failed to send message for target {} to stream: {}",
                    M::TARGET,
                    e
                );
            } else {
                trace!(
                    "Blackholed message {} since handler stream has been dropped",
                    M::TARGET
                );
            }
        }
        Ok(())
    }
}

static_assertions::assert_impl_all!(MessageRouter: Send);
