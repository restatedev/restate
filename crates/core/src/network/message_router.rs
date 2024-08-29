// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;

use restate_types::net::codec::{Targeted, WireDecode};
use restate_types::net::CodecError;
use restate_types::net::ProtocolVersion;
use restate_types::net::TargetName;
use restate_types::protobuf::node::message::BinaryMessage;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

use crate::is_cancellation_requested;

use super::{Incoming, RouterError};

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

/// A low-level handler trait.
#[async_trait]
pub trait Handler: Send + Sync {
    type Error;
    /// Deserialize and process the message asynchronously.
    async fn call(
        &self,
        message: Incoming<BinaryMessage>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), Self::Error>;
}

#[derive(Clone, Default)]
pub struct MessageRouter(Arc<MessageRouterInner>);

#[derive(Default)]
struct MessageRouterInner {
    handlers: HashMap<TargetName, Box<dyn Handler<Error = CodecError> + Send + Sync>>,
}

#[async_trait]
impl Handler for MessageRouter {
    type Error = RouterError;
    /// Process the request and return the response asynchronously.
    async fn call(
        &self,
        message: Incoming<BinaryMessage>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), Self::Error> {
        let target = message.target();
        let Some(handler) = self.0.handlers.get(&target) else {
            return Err(RouterError::NotRegisteredTarget(target.to_string()));
        };
        handler.call(message, protocol_version).await?;
        Ok(())
    }
}

#[derive(Default)]
pub struct MessageRouterBuilder {
    handlers: HashMap<TargetName, Box<dyn Handler<Error = CodecError> + Send + Sync>>,
}

impl MessageRouterBuilder {
    /// Attach a handler that implements [`MessageHandler`] to receive messages
    /// for the associated target.
    #[track_caller]
    pub fn add_message_handler<H>(&mut self, handler: H)
    where
        H: MessageHandler + Send + Sync + 'static,
    {
        let wrapped = MessageHandlerWrapper { inner: handler };
        let target = H::MessageType::TARGET;
        if self.handlers.insert(target, Box::new(wrapped)).is_some() {
            panic!("Handler for target {} has been registered already!", target);
        }
    }

    /// Subscribe to a stream of messages for a specific target. This enables consumers of messages
    /// to use async stream API to process messages of a given target as an alternative to the
    /// message callback-style API as in `add_message_handler`.
    #[track_caller]
    pub fn subscribe_to_stream<M>(
        &mut self,
        buffer_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Incoming<M>> + Send + Sync + 'static>>
    where
        M: WireDecode + Targeted + Send + Sync + 'static,
    {
        let (tx, rx) = mpsc::channel(buffer_size);

        let wrapped = StreamHandlerWrapper { sender: tx };
        let target = M::TARGET;
        if self.handlers.insert(target, Box::new(wrapped)).is_some() {
            panic!("Handler for target {} has been registered already!", target);
        }
        Box::pin(ReceiverStream::new(rx))
    }

    /// Finalize this builder and return the message router that can be attached to
    /// [`crate::ConnectionManager`]
    pub fn build(self) -> MessageRouter {
        MessageRouter(Arc::new(MessageRouterInner {
            handlers: self.handlers,
        }))
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
    type Error = CodecError;
    /// Process the request and return the response asynchronously.
    async fn call(
        &self,
        message: Incoming<BinaryMessage>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), Self::Error> {
        let message = message.try_map(|mut m| {
            <H::MessageType as WireDecode>::decode(&mut m.payload, protocol_version)
        })?;
        self.inner.on_message(message).await;
        Ok(())
    }
}

struct StreamHandlerWrapper<M>
where
    M: WireDecode + Targeted + Send + Sync + 'static,
{
    sender: mpsc::Sender<Incoming<M>>,
}

#[async_trait]
impl<M> Handler for StreamHandlerWrapper<M>
where
    M: WireDecode + Targeted + Send + Sync + 'static,
{
    type Error = CodecError;
    /// Process the request and return the response asynchronously.
    async fn call(
        &self,
        message: Incoming<BinaryMessage>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), Self::Error> {
        let message =
            message.try_map(|mut m| <M as WireDecode>::decode(&mut m.payload, protocol_version))?;
        if let Err(e) = self.sender.send(message).await {
            // Can be benign if we are shutting down
            if !is_cancellation_requested() {
                warn!(
                    "Failed to send message for target {} to stream: {}",
                    M::TARGET,
                    e
                );
            }
        }
        Ok(())
    }
}

static_assertions::assert_impl_all!(MessageRouter: Send, Sync);
