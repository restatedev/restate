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

use tokio::sync::mpsc;
use tracing::instrument;

use restate_core::metadata;
use restate_core::network::NetworkSendError;
use restate_node_protocol::codec::serialize_message;
use restate_node_protocol::codec::Targeted;
use restate_node_protocol::codec::WireSerde;
use restate_node_protocol::common::ProtocolVersion;
use restate_node_protocol::node::message;
use restate_node_protocol::node::Header;
use restate_node_protocol::node::Message;
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
    pub(crate) created: std::time::Instant,
}

impl Connection {
    pub fn new(
        peer: GenerationalNodeId,
        protocol_version: ProtocolVersion,
        sender: mpsc::Sender<Message>,
    ) -> Self {
        Self {
            cid: rand::random(),
            peer,
            protocol_version,
            sender,
            created: std::time::Instant::now(),
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
            peer: self.peer,
            connection: Arc::downgrade(self),
            protocol_version: self.protocol_version,
        }
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
    peer: GenerationalNodeId,
    connection: Weak<Connection>,
    protocol_version: ProtocolVersion,
}

impl ConnectionSender {
    /// Send a message on this connection. This returns Ok(()) when the message is:
    /// - Successfully serialized to the wire format based on the negotiated protocol
    /// - Serialized message was enqueued on the send buffer of the socket
    ///
    /// That means that this is not a guarantee that the message has been sent
    /// over the network or that the peer have received it.
    ///
    /// If this is needed, the caller must design the wire protocol with a
    /// request/response state machine and perform retries on other nodes/connections if needed.
    ///
    /// This roughly maps to the semantics of a POSIX write/send socket operation.
    ///
    /// This doesn't auto-retry connection resets or send errors, this is up to the user
    /// for retrying externally.
    #[instrument(skip_all, fields(peer_node_id = %self.peer, target_service = ?message.target(), msg = ?message.kind()))]
    pub async fn send<M>(&self, message: M) -> Result<(), NetworkSendError>
    where
        M: WireSerde + Targeted,
    {
        let header = Header::new(metadata().nodes_config_version());
        let body = serialize_message(message, self.protocol_version)?;
        self.connection
            .upgrade()
            .ok_or(NetworkSendError::ConnectionClosed)?
            .sender
            .send(Message::new(header, body))
            .await
            .map_err(|_| NetworkSendError::ConnectionClosed)
    }
}

static_assertions::assert_impl_all!(ConnectionSender: Send, Sync);
