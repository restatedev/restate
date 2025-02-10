// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Weak};
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use futures::stream::BoxStream;
use futures::StreamExt;
use restate_types::NodeId;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{error, warn};

use restate_types::net::codec::{Targeted, WireDecode, WireEncode};
use restate_types::net::RpcRequest;

type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;

use super::{
    HasConnection, Incoming, MessageHandler, MessageRouterBuilder, NetworkError, NetworkSendError,
    NetworkSender, Networking, Outgoing, TransportConnect,
};
use crate::{cancellation_watcher, ShutdownError};

/// A router for sending and receiving RPC messages through Networking
///
/// It's responsible for keeping track of in-flight requests, correlating responses, and dropping
/// tracking tokens if caller dropped the future.
///
/// This type is designed to be used by senders of RpcRequest(s).
pub struct RpcRouter<T>
where
    T: RpcRequest,
{
    response_tracker: ResponseTracker<T::ResponseMessage>,
}

impl<T: RpcRequest> Clone for RpcRouter<T> {
    fn clone(&self) -> Self {
        RpcRouter {
            response_tracker: self.response_tracker.clone(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub enum RpcError<T> {
    SendError(#[from] NetworkSendError<T>),
    Shutdown(#[from] ShutdownError),
}

impl<T> RpcRouter<T>
where
    T: RpcRequest + WireEncode + Send + Sync + 'static,
    T::ResponseMessage: WireDecode + Send + Sync + 'static,
{
    pub fn new(router_builder: &mut MessageRouterBuilder) -> Self {
        let response_tracker = ResponseTracker::<T::ResponseMessage>::default();
        router_builder.add_message_handler(response_tracker.clone());
        Self { response_tracker }
    }

    pub async fn call(
        &self,
        network_sender: &impl NetworkSender,
        peer: impl Into<NodeId>,
        msg: T,
    ) -> Result<Incoming<T::ResponseMessage>, RpcError<T>> {
        let outgoing = Outgoing::new(peer, msg);
        let token = self
            .response_tracker
            .register(&outgoing)
            .expect("msg-id is registered once");

        network_sender.send(outgoing).await.map_err(|e| {
            RpcError::SendError(NetworkSendError::new(
                Outgoing::into_body(e.original),
                e.source,
            ))
        })?;
        token
            .recv()
            .await
            .map_err(|_| RpcError::Shutdown(ShutdownError))
    }

    /// Does not perform retries
    pub async fn call_timeout<X: TransportConnect>(
        &self,
        networking: &Networking<X>,
        peer: impl Into<NodeId>,
        msg: T,
        timeout: Duration,
    ) -> Result<Incoming<T::ResponseMessage>, NetworkError> {
        let start = Instant::now();
        let peer = peer.into();
        let connection = tokio::time::timeout(timeout, networking.node_connection(peer))
            .await
            .map_err(|_| NetworkError::Timeout(start.elapsed()))??;
        let outgoing = Outgoing::new(peer, msg).assign_connection(connection);
        self.call_outgoing_timeout(outgoing, timeout - start.elapsed())
            .await
    }

    /// Use this method when you have a connection associated with the outgoing request
    pub async fn call_on_connection(
        &self,
        outgoing: Outgoing<T, HasConnection>,
    ) -> Result<Incoming<T::ResponseMessage>, RpcError<Outgoing<T, HasConnection>>> {
        let token = self
            .response_tracker
            .register(&outgoing)
            .expect("msg-id is registered once");

        outgoing.send().await.map_err(RpcError::SendError)?;
        token
            .recv()
            .await
            .map_err(|_| RpcError::Shutdown(ShutdownError))
    }

    /// Use this method when you have a connection associated with the outgoing request
    ///
    /// Timeout encompasses the time it takes to send the request and the time it takes to receive
    /// the response.
    pub async fn call_outgoing_timeout(
        &self,
        outgoing: Outgoing<T, HasConnection>,
        timeout: Duration,
    ) -> Result<Incoming<T::ResponseMessage>, NetworkError> {
        let token = self
            .response_tracker
            .register(&outgoing)
            .expect("msg-id is registered once");

        let start = Instant::now();
        outgoing.send_timeout(timeout).await.map_err(|e| e.source)?;
        let remaining = timeout - start.elapsed();
        tokio::time::timeout(remaining, token.recv())
            .await
            .map_err(|_| NetworkError::Timeout(start.elapsed()))?
            // We only fail here if the response tracker was dropped.
            .map_err(NetworkError::Shutdown)
    }

    pub async fn send_on_connection(
        &self,
        outgoing: Outgoing<T, HasConnection>,
    ) -> Result<RpcToken<T::ResponseMessage>, NetworkSendError<Outgoing<T, HasConnection>>> {
        let token = self
            .response_tracker
            .register(&outgoing)
            .expect("msg-id is registered once");

        outgoing.send().await?;
        Ok(token)
    }

    pub fn num_in_flight(&self) -> usize {
        self.response_tracker.num_in_flight()
    }
}

/// A tracker for responses but can be used to track responses for requests that were dispatched
/// via other mechanisms (e.g. ingress flow)
pub struct ResponseTracker<T>
where
    T: Targeted,
{
    in_flight: Arc<DashMap<u64, RpcTokenSender<T>>>,
}

impl<T> Clone for ResponseTracker<T>
where
    T: Targeted,
{
    fn clone(&self) -> Self {
        Self {
            in_flight: Arc::clone(&self.in_flight),
        }
    }
}

impl<T> Default for ResponseTracker<T>
where
    T: Targeted,
{
    fn default() -> Self {
        Self {
            in_flight: Default::default(),
        }
    }
}

impl<T> ResponseTracker<T>
where
    T: Targeted,
{
    pub fn num_in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Returns None if an in-flight request holds the same msg_id.
    pub fn register<M, S>(&self, outgoing: &Outgoing<M, S>) -> Option<RpcToken<T>> {
        self.register_raw(outgoing.msg_id())
    }

    /// Handle a message through this response tracker. Returns None on success or Some(incoming)
    /// if the message doesn't correspond to an in-flight request.
    pub fn handle_message(&self, msg: Incoming<T>) -> Option<Incoming<T>> {
        let Some(original_msg_id) = msg.in_response_to() else {
            warn!(
                message_target = msg.body().kind(),
                "received a message with a `in_response_to` field unset! The message will be dropped",
            );
            return None;
        };
        // find the token and send, message is dropped on the floor if no valid match exist for the
        // msg id.
        if let Some((_, token)) = self.in_flight.remove(&original_msg_id) {
            let _ = token.sender.send(msg);
            None
        } else {
            Some(msg)
        }
    }

    fn register_raw(&self, msg_id: u64) -> Option<RpcToken<T>> {
        match self.in_flight.entry(msg_id) {
            Entry::Occupied(entry) => {
                error!(
                    "msg_id {:?} was already in-flight when this rpc was issued, this is an indicator that the msg_id is not unique across RPC calls",
                    entry.key()
                );
                None
            }
            Entry::Vacant(entry) => {
                let (sender, receiver) = oneshot::channel();
                entry.insert(RpcTokenSender { sender });

                Some(RpcToken {
                    msg_id,
                    router: Arc::downgrade(&self.in_flight),
                    receiver: Some(receiver),
                })
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionAwareRpcError<T> {
    #[error("connection closed")]
    ConnectionClosed,
    #[error("cannot establish connection to peer {0}: {1}")]
    CannotEstablishConnectionToPeer(NodeId, #[source] NetworkError),
    #[error(transparent)]
    SendError(#[from] NetworkSendError<T>),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

impl<T> From<RpcError<T>> for ConnectionAwareRpcError<T> {
    fn from(value: RpcError<T>) -> Self {
        match value {
            RpcError::SendError(err) => ConnectionAwareRpcError::SendError(err),
            RpcError::Shutdown(err) => ConnectionAwareRpcError::Shutdown(err),
        }
    }
}

/// Decorator of the [`RpcRouter`] which allows to monitor the state of connections on which
/// messages are sent. This can be useful to detect if a response won't be received because the
/// connection was closed. This requires that the recipient sends responses back to the sender on
/// the very same connection.
#[derive(Clone)]
pub struct ConnectionAwareRpcRouter<T>
where
    T: RpcRequest,
{
    rpc_router: RpcRouter<T>,
}

impl<T> ConnectionAwareRpcRouter<T>
where
    T: RpcRequest + WireEncode + Send + Sync + 'static,
    T::ResponseMessage: WireDecode + Send + Sync + 'static,
{
    pub fn new(router_builder: &mut MessageRouterBuilder) -> Self {
        ConnectionAwareRpcRouter::from_rpc_router(RpcRouter::new(router_builder))
    }

    pub fn from_rpc_router(rpc_router: RpcRouter<T>) -> Self {
        ConnectionAwareRpcRouter { rpc_router }
    }

    pub async fn call<C: TransportConnect>(
        &self,
        networking: &Networking<C>,
        peer: impl Into<NodeId>,
        msg: T,
    ) -> Result<Incoming<T::ResponseMessage>, ConnectionAwareRpcError<Outgoing<T, HasConnection>>>
    {
        let peer = peer.into();
        let connection = networking
            .node_connection(peer)
            .await
            .map_err(|e| ConnectionAwareRpcError::CannotEstablishConnectionToPeer(peer, e))?;
        let connection_closed = connection.closed();
        let outgoing = Outgoing::new(peer, msg).assign_connection(connection);

        tokio::select! {
            response = self.rpc_router.call_on_connection(outgoing) => {
                response.map_err(Into::into)
            },
            _ = connection_closed => {
                Err(ConnectionAwareRpcError::ConnectionClosed)
            }
        }
    }
}

pub struct StreamingResponseTracker<T>
where
    T: Targeted,
{
    flight_tracker: ResponseTracker<T>,
    incoming_messages: BoxStream<'static, Incoming<T>>,
}

impl<T> StreamingResponseTracker<T>
where
    T: Targeted,
{
    pub fn new(incoming_messages: BoxStream<'static, Incoming<T>>) -> Self {
        let flight_tracker = ResponseTracker::default();
        Self {
            flight_tracker,
            incoming_messages,
        }
    }

    /// Returns None if an in-flight request holds the same msg_id.
    pub fn register<M, S>(&self, outgoing: &Outgoing<M, S>) -> Option<RpcToken<T>> {
        self.flight_tracker.register(outgoing)
    }

    pub fn register_raw(&self, msg_id: u64) -> Option<RpcToken<T>> {
        self.flight_tracker.register_raw(msg_id)
    }

    /// Handles the next message. This will **return** the message if no correlated request is
    /// in-flight. Otherwise, it's handled by the corresponding token receiver.
    pub async fn handle_next_or_get(&mut self) -> Option<Incoming<T>> {
        tokio::select! {
            Some(message) = self.incoming_messages.next() => {
                self.flight_tracker.handle_message(message)
            },
            _ = cancellation_watcher() => { None },
            else => { None } ,
        }
    }
}

struct RpcTokenSender<T> {
    sender: oneshot::Sender<Incoming<T>>,
}

pub struct RpcToken<T>
where
    T: Targeted,
{
    msg_id: u64,
    router: Weak<DashMap<u64, RpcTokenSender<T>>>,
    // This is Option to get around Rust's borrow checker rules when a type implements the Drop
    // trait. Without this, we cannot move receiver out.
    receiver: Option<oneshot::Receiver<Incoming<T>>>,
}

impl<T> RpcToken<T>
where
    T: Targeted,
{
    pub fn msg_id(&self) -> u64 {
        self.msg_id
    }

    /// Awaits the response to come for the associated request. Cancellation safe.
    pub async fn recv(mut self) -> Result<Incoming<T>, ShutdownError> {
        let receiver = std::mem::take(&mut self.receiver);
        let res = match receiver {
            Some(receiver) => {
                tokio::select! {
                    _ = cancellation_watcher() => {
                        return Err(ShutdownError);
                    },
                    res = receiver => {
                        res.map_err(|_| ShutdownError)
                    }
                }
            }
            // Should never happen unless token was created with None which shouldn't be possible
            None => Err(ShutdownError),
        };
        // If we have received something, we don't need to run drop() since the flight tracker has
        // already removed the sender token.
        std::mem::forget(self);
        res
    }
}

impl<T> Drop for RpcToken<T>
where
    T: Targeted,
{
    fn drop(&mut self) {
        // if the router is gone, we can't do anything.
        let Some(router) = self.router.upgrade() else {
            return;
        };
        let _ = router.remove(&self.msg_id);
    }
}

impl<T> MessageHandler for ResponseTracker<T>
where
    T: WireDecode + Targeted,
{
    type MessageType = T;

    fn on_message(
        &self,
        msg: Incoming<Self::MessageType>,
    ) -> impl std::future::Future<Output = ()> + Send {
        self.handle_message(msg);
        std::future::ready(())
    }
}

#[cfg(test)]
mod test {
    use crate::network::{PeerMetadataVersion, WeakConnection};

    use super::*;
    use futures::future::join_all;
    use restate_types::net::codec::encode_default;
    use restate_types::net::{CodecError, TargetName};
    use restate_types::GenerationalNodeId;
    use serde::{Deserialize, Serialize};
    use tokio::sync::Barrier;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestRequest {
        text: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestResponse {
        text: String,
    }

    impl Targeted for TestRequest {
        const TARGET: TargetName = TargetName::Unknown;
        fn kind(&self) -> &'static str {
            "TestRequest"
        }
    }

    impl RpcRequest for TestRequest {
        type ResponseMessage = TestResponse;
    }

    impl Targeted for TestResponse {
        const TARGET: TargetName = TargetName::Unknown;
        fn kind(&self) -> &'static str {
            "TestMessage"
        }
    }

    impl WireEncode for TestResponse {
        fn encode<B: bytes::BufMut>(
            self,
            buf: &mut B,
            protocol_version: restate_types::net::ProtocolVersion,
        ) -> Result<(), CodecError> {
            encode_default(self, buf, protocol_version)
        }
    }

    impl WireDecode for TestResponse {
        fn decode(
            _: impl bytes::Buf,
            _: restate_types::net::ProtocolVersion,
        ) -> Result<Self, CodecError>
        where
            Self: Sized,
        {
            unimplemented!()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_rpc_flight_tracker_drop() {
        let tracker = ResponseTracker::<TestResponse>::default();
        assert_eq!(tracker.num_in_flight(), 0);
        let token = tracker.register_raw(1).unwrap();
        assert_eq!(tracker.num_in_flight(), 1);
        drop(token);
        assert_eq!(tracker.num_in_flight(), 0);

        let token = tracker.register_raw(1).unwrap();
        assert_eq!(tracker.num_in_flight(), 1);
        // receive with timeout, this should drop the token
        let start = tokio::time::Instant::now();
        let dur = std::time::Duration::from_millis(500);
        let res = tokio::time::timeout(dur, token.recv()).await;
        assert!(res.is_err());
        assert!(start.elapsed() >= dur);
        assert_eq!(tracker.num_in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_rpc_flight_tracker_send_recv() {
        let tracker = ResponseTracker::<TestResponse>::default();
        assert_eq!(tracker.num_in_flight(), 0);
        let token = tracker.register_raw(1).unwrap();
        assert_eq!(tracker.num_in_flight(), 1);

        // dropped on the floor
        tracker
            .on_message(Incoming::from_parts(
                TestResponse {
                    text: "test".to_string(),
                },
                WeakConnection::new_closed(GenerationalNodeId::new(1, 1)),
                1,
                Some(42),
                PeerMetadataVersion::default(),
            ))
            .await;

        assert_eq!(tracker.num_in_flight(), 1);

        let maybe_msg = tracker.handle_message(Incoming::from_parts(
            TestResponse {
                text: "test".to_string(),
            },
            WeakConnection::new_closed(GenerationalNodeId::new(1, 1)),
            1,
            Some(42),
            PeerMetadataVersion::default(),
        ));
        assert!(maybe_msg.is_some());

        assert_eq!(tracker.num_in_flight(), 1);

        // matches msg id
        tracker
            .on_message(Incoming::from_parts(
                TestResponse {
                    text: "a very real message".to_string(),
                },
                WeakConnection::new_closed(GenerationalNodeId::new(1, 1)),
                1,
                Some(1),
                PeerMetadataVersion::default(),
            ))
            .await;

        // sender token is dropped
        assert_eq!(tracker.num_in_flight(), 0);

        let msg = token.recv().await.unwrap();
        assert_eq!(Some(1), msg.in_response_to());
        assert_eq!(GenerationalNodeId::new(1, 1), msg.peer());
        assert_eq!("a very real message", msg.body().text);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_response_tracker_modifications() {
        let num_responses = 10000;
        let response_tracker = ResponseTracker::default();

        let rpc_tokens: Vec<RpcToken<TestResponse>> = (0..num_responses)
            .map(|idx| {
                response_tracker
                    .register_raw(idx)
                    .expect("first time created")
            })
            .collect();

        let barrier = Arc::new(Barrier::new((2 * num_responses) as usize));

        for idx in 0..num_responses {
            let response_tracker_handle_message = response_tracker.clone();
            let barrier_handle_message = Arc::clone(&barrier);

            tokio::spawn(async move {
                barrier_handle_message.wait().await;
                response_tracker_handle_message.handle_message(Incoming::from_parts(
                    TestResponse {
                        text: format!("{idx}"),
                    },
                    WeakConnection::new_closed(GenerationalNodeId::new(0, 0)),
                    1,
                    Some(idx),
                    PeerMetadataVersion::default(),
                ));
            });

            let response_tracker_new_token = response_tracker.clone();
            let barrier_new_token = Arc::clone(&barrier);

            tokio::spawn(async move {
                barrier_new_token.wait().await;
                response_tracker_new_token.register_raw(idx);
            });
        }

        let results = join_all(rpc_tokens.into_iter().map(|rpc_token| async {
            rpc_token
                .recv()
                .await
                .expect("should complete successfully")
        }))
        .await;

        for result in results {
            assert_eq!(
                Some(result.body().text.parse::<u64>().expect("valid u64")),
                result.in_response_to()
            );
        }
    }
}
