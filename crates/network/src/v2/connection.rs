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
use restate_node_protocol::common::ProtocolVersion;
use restate_node_protocol::node::message;
use restate_node_protocol::node::Header;
use restate_node_protocol::node::Message;
use restate_node_protocol::NetworkMessage;
use restate_types::GenerationalNodeId;

use super::codec::serialize_message;
use super::error::NetworkError;

// represents an incoming single streaming connection with a channel to the peer.
pub(crate) struct Connection {
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

    pub fn sender(self: &Arc<Self>) -> ConnectionSender {
        ConnectionSender {
            peer: self.peer,
            _connection: Arc::downgrade(self),
            sender: self.sender.clone(),
            protocol_version: self.protocol_version,
        }
    }
}

/// Does not handle connection reset errors. Users of this struct are responsible
/// for retrying externally.
pub struct ConnectionSender {
    peer: GenerationalNodeId,
    _connection: Weak<Connection>,
    sender: mpsc::Sender<Message>,
    protocol_version: ProtocolVersion,
}

impl ConnectionSender {
    #[instrument(skip_all, fields(peer_node_id = %self.peer, msg = ?message.kind()))]
    pub async fn send(&self, message: &NetworkMessage) -> Result<(), NetworkError> {
        let header = Header::new(metadata().nodes_config_version());
        let body = serialize_message(message, self.protocol_version)?;
        self.sender
            .send(Message::new(header, body))
            .await
            .map_err(|_| NetworkError::ConnectionClosed)
    }
}
