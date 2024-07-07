// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::Weak;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::instrument;

use super::metric_definitions::CONNECTION_SEND_DURATION;
use super::metric_definitions::MESSAGE_SENT;
use super::NetworkError;
use super::ProtocolError;
use crate::Metadata;
use restate_types::net::codec::Targeted;
use restate_types::net::codec::{serialize_message, WireEncode};
use restate_types::net::ProtocolVersion;
use restate_types::protobuf::node::message;
use restate_types::protobuf::node::message::Body;
use restate_types::protobuf::node::Header;
use restate_types::protobuf::node::Message;
use restate_types::GenerationalNodeId;

/// A single streaming connection with a channel to the peer. A connection can be
/// opened by either ends of the connection and has no direction. Any connection
/// can be used to send or receive from a peer.
///
/// The primary owned of a connection is the running reactor, all other components
/// should hold a Weak<Connection> if caching access to a certain connection is
/// needed.
pub(crate) struct Connection {
    /// Connection identifier, randomly generated on this end of the connection.
    pub(crate) cid: u64,
    pub(crate) peer: GenerationalNodeId,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) sender: mpsc::Sender<Message>,
    pub(crate) created: Instant,
    pub(crate) metadata: Metadata,
}

impl Connection {
    pub fn new(
        peer: GenerationalNodeId,
        protocol_version: ProtocolVersion,
        sender: mpsc::Sender<Message>,
        metadata: Metadata,
    ) -> Self {
        Self {
            cid: rand::random(),
            peer,
            protocol_version,
            sender,
            created: std::time::Instant::now(),
            metadata,
        }
    }

    /// The current negotiated protocol version of the connection
    pub fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Best-effort delivery of signals on the connection.
    pub fn send_control_frame(&self, control: message::ConnectionControl) {
        let msg = Message {
            header: None,
            body: Some(control.into()),
        };
        let _ = self.sender.try_send(msg);
    }

    /// A handle that sends messages through that connection. This hides the
    /// wire protocol from the user and guarantees order of messages.
    pub fn sender(self: &Arc<Self>) -> ConnectionSender {
        ConnectionSender {
            connection: Arc::downgrade(self),
        }
    }

    /// Send a message on this connection. This returns Ok(()) when the message is:
    /// - Successfully serialized to the wire format based on the negotiated protocol
    /// - Serialized message was enqueued on the send buffer of the socket
    ///
    /// That means that this is not a guarantee that the message has been sent
    /// over the network or that the peer has received it.
    ///
    /// If this is needed, the caller must design the wire protocol with a
    /// request/response state machine and perform retries on other nodes/connections if needed.
    ///
    /// This roughly maps to the semantics of a POSIX write/send socket operation.
    ///
    /// This doesn't auto-retry connection resets or send errors, this is up to the user
    /// for retrying externally.
    #[instrument(skip_all, fields(peer_node_id = %self.peer, target_service = ?message.target(), msg = ?message.kind()))]
    pub async fn send<M>(&self, message: M) -> Result<(), NetworkError>
    where
        M: WireEncode + Targeted,
    {
        let send_start = Instant::now();
        let (header, body) = self.create_message(message)?;
        let res = self
            .sender
            .send(Message::new(header, body))
            .await
            .map_err(|_| NetworkError::ConnectionClosed);

        if res.is_ok() {
            MESSAGE_SENT.increment(1);
            CONNECTION_SEND_DURATION.record(send_start.elapsed());
        }

        res
    }

    fn create_message<M>(&self, message: M) -> Result<(Header, Body), NetworkError>
    where
        M: WireEncode + Targeted,
    {
        let header = Header::new(
            self.metadata.nodes_config_version(),
            self.metadata.logs_version(),
            self.metadata.schema_version(),
            self.metadata.partition_table_version(),
        );
        let body =
            serialize_message(message, self.protocol_version).map_err(ProtocolError::Codec)?;
        Ok((header, body))
    }

    /// Tries sending a message on this connection. If there is no capacity, it will fail. Apart
    /// from this, the method behaves similarly to [`Connection::send`].
    #[instrument(skip_all, fields(peer_node_id = %self.peer, target_service = ?message.target(), msg = ?message.kind()))]
    pub fn try_send<M>(&self, message: M) -> Result<(), NetworkError>
    where
        M: WireEncode + Targeted,
    {
        let send_start = Instant::now();
        let (header, body) = self.create_message(message)?;
        let res = self
            .sender
            .try_send(Message::new(header, body))
            .map_err(|err| match err {
                TrySendError::Full(_) => NetworkError::Full,
                TrySendError::Closed(_) => NetworkError::ConnectionClosed,
            });

        if res.is_ok() {
            MESSAGE_SENT.increment(1);
            CONNECTION_SEND_DURATION.record(send_start.elapsed());
        }

        res
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.cid == other.cid && self.peer == other.peer
    }
}

/// A handle to send messages through a connection. It's safe and cheap to hold
/// and clone objects of this even if the connection has been dropped.
#[derive(Clone)]
pub struct ConnectionSender {
    connection: Weak<Connection>,
}

impl ConnectionSender {
    /// See [`Connection::send`].
    pub async fn send<M>(&self, message: M) -> Result<(), NetworkError>
    where
        M: WireEncode + Targeted,
    {
        self.connection
            .upgrade()
            .ok_or(NetworkError::ConnectionClosed)?
            .send(message)
            .await
    }

    /// See [`Connection::try_send`].
    pub fn try_send<M>(&self, message: M) -> Result<(), NetworkError>
    where
        M: WireEncode + Targeted,
    {
        self.connection
            .upgrade()
            .ok_or(NetworkError::ConnectionClosed)?
            .try_send(message)
    }
}

static_assertions::assert_impl_all!(ConnectionSender: Send, Sync);
