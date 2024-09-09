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

use enum_map::{enum_map, EnumMap};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::instrument;

use restate_types::live::Live;
use restate_types::logs::metadata::Logs;
use restate_types::net::codec::Targeted;
use restate_types::net::codec::{serialize_message, WireEncode};
use restate_types::net::metadata::MetadataKind;
use restate_types::net::ProtocolVersion;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::node::message;
use restate_types::protobuf::node::Header;
use restate_types::protobuf::node::Message;
use restate_types::schema::Schema;
use restate_types::{GenerationalNodeId, Version, Versioned};

use super::metric_definitions::CONNECTION_SEND_DURATION;
use super::metric_definitions::MESSAGE_SENT;
use super::NetworkError;
use super::NetworkSendError;
use super::Outgoing;
use super::ProtocolError;
use crate::network::connection_manager::MetadataVersions;
use crate::Metadata;

/// A single streaming connection with a channel to the peer. A connection can be
/// opened by either ends of the connection and has no direction. Any connection
/// can be used to send or receive from a peer.
///
/// The primary owner of a connection is the running reactor, all other components
/// should hold a Weak<Connection> if caching access to a certain connection is
/// needed.
pub struct Connection {
    /// Connection identifier, randomly generated on this end of the connection.
    pub(crate) cid: u64,
    pub(crate) peer: GenerationalNodeId,
    pub(crate) protocol_version: ProtocolVersion,
    pub(crate) sender: mpsc::Sender<Message>,
    pub(crate) created: Instant,
}

impl Connection {
    pub(crate) fn new(
        peer: GenerationalNodeId,
        protocol_version: ProtocolVersion,
        sender: mpsc::Sender<Message>,
    ) -> Self {
        Self {
            cid: rand::random(),
            peer,
            protocol_version,
            sender,
            created: Instant::now(),
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn new_fake(
        peer: GenerationalNodeId,
        protocol_version: ProtocolVersion,
        sender: mpsc::Sender<Message>,
    ) -> Arc<Self> {
        Arc::new(Self::new(peer, protocol_version, sender))
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
    pub fn sender(self: &Arc<Self>, metadata: &Metadata) -> ConnectionSender {
        ConnectionSender {
            connection: Arc::downgrade(self),
            nodes_config: metadata.updateable_nodes_config(),
            schema: metadata.updateable_schema(),
            logs: metadata.updateable_logs_metadata(),
            partition_table: metadata.updateable_partition_table(),
            metadata_versions: MetadataVersions::default(),
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
    #[instrument(level = "trace", skip_all, fields(peer_node_id = %self.peer, target_service = ?message.target(), msg = ?message.kind()))]
    pub async fn send<M>(
        &self,
        message: Outgoing<M>,
        metadata_versions: HeaderMetadataVersions,
    ) -> Result<(), NetworkSendError<M>>
    where
        M: WireEncode + Targeted,
    {
        let send_start = Instant::now();
        // do not serialize if we can't acquire capacity
        let permit = match self.sender.reserve().await {
            Ok(permit) => permit,
            Err(_) => {
                return Err(NetworkSendError::new(
                    message,
                    NetworkError::ConnectionClosed,
                ))
            }
        };

        let serialized_msg = match self.create_message(&message, metadata_versions) {
            Ok(m) => m,
            Err(e) => return Err(NetworkSendError::new(message, e)),
        };
        permit.send(serialized_msg);
        MESSAGE_SENT.increment(1);
        CONNECTION_SEND_DURATION.record(send_start.elapsed());
        Ok(())
    }

    fn create_message<M>(
        &self,
        message: &Outgoing<M>,
        metadata_versions: HeaderMetadataVersions,
    ) -> Result<Message, NetworkError>
    where
        M: WireEncode + Targeted,
    {
        let header = Header::new(
            metadata_versions[MetadataKind::NodesConfiguration]
                .expect("nodes configuration version must be set"),
            metadata_versions[MetadataKind::Logs],
            metadata_versions[MetadataKind::Schema],
            metadata_versions[MetadataKind::PartitionTable],
            message.msg_id(),
            message.in_response_to(),
        );
        let body = serialize_message(message.body(), self.protocol_version)
            .map_err(ProtocolError::Codec)?;
        Ok(Message::new(header, body))
    }

    /// Tries sending a message on this connection. If there is no capacity, it will fail. Apart
    /// from this, the method behaves similarly to [`Connection::send`].
    #[instrument(skip_all, fields(peer_node_id = %self.peer, target_service = ?message.target(), msg = ?message.kind()))]
    pub fn try_send<M>(
        &self,
        message: Outgoing<M>,
        metadata_versions: HeaderMetadataVersions,
    ) -> Result<(), NetworkSendError<M>>
    where
        M: WireEncode + Targeted,
    {
        let send_start = Instant::now();
        // do not serialize if we can't acquire capacity
        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(TrySendError::Full(_)) => {
                return Err(NetworkSendError::new(message, NetworkError::Full))
            }
            Err(TrySendError::Closed(_)) => {
                return Err(NetworkSendError::new(
                    message,
                    NetworkError::ConnectionClosed,
                ))
            }
        };

        let serialized_msg = match self.create_message(&message, metadata_versions) {
            Ok(m) => m,
            Err(e) => return Err(NetworkSendError::new(message, e)),
        };
        permit.send(serialized_msg);
        MESSAGE_SENT.increment(1);
        CONNECTION_SEND_DURATION.record(send_start.elapsed());
        Ok(())
    }
}

#[derive(derive_more::Index)]
pub(crate) struct HeaderMetadataVersions {
    #[index]
    versions: EnumMap<MetadataKind, Option<Version>>,
}

impl Default for HeaderMetadataVersions {
    // Used primarily in tests
    fn default() -> Self {
        let versions = enum_map! {
            MetadataKind::NodesConfiguration => Some(Version::MIN),
            MetadataKind::Schema => None,
            MetadataKind::Logs => None,
            MetadataKind::PartitionTable => None,
        };
        Self { versions }
    }
}

impl HeaderMetadataVersions {
    pub fn from_metadata(metadata: &Metadata) -> Self {
        let versions = enum_map! {
            MetadataKind::NodesConfiguration => Some(metadata.nodes_config_version()),
            MetadataKind::Schema => Some(metadata.schema_version()),
            MetadataKind::Logs => Some(metadata.logs_version()),
            MetadataKind::PartitionTable => Some(metadata.partition_table_version()),
        };
        Self { versions }
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.cid == other.cid && self.peer == other.peer
    }
}

/// A handle to send messages through a connection. It's safe to hold and clone objects of this
/// even if the connection has been dropped. Cloning and holding comes at the cost of caching
/// all existing metadata which is not for free.
#[derive(Clone)]
pub struct ConnectionSender {
    connection: Weak<Connection>,
    nodes_config: Live<NodesConfiguration>,
    schema: Live<Schema>,
    logs: Live<Logs>,
    partition_table: Live<PartitionTable>,
    metadata_versions: MetadataVersions,
}

impl ConnectionSender {
    /// See [`Connection::send`].
    pub async fn send<M>(&mut self, message: Outgoing<M>) -> Result<(), NetworkSendError<M>>
    where
        M: WireEncode + Targeted,
    {
        let Some(connection) = self.connection.upgrade() else {
            return Err(NetworkSendError::new(
                message,
                NetworkError::ConnectionClosed,
            ));
        };
        connection
            .send(message, self.header_metadata_versions())
            .await
    }

    /// See [`Connection::try_send`].
    pub fn try_send<M>(&mut self, message: Outgoing<M>) -> Result<(), NetworkSendError<M>>
    where
        M: WireEncode + Targeted,
    {
        let Some(connection) = self.connection.upgrade() else {
            return Err(NetworkSendError::new(
                message,
                NetworkError::ConnectionClosed,
            ));
        };
        connection.try_send(message, self.header_metadata_versions())
    }

    fn header_metadata_versions(&mut self) -> HeaderMetadataVersions {
        let mut version_updates = self.metadata_versions.update(
            None,
            Some(self.partition_table.live_load().version()),
            Some(self.schema.live_load().version()),
            Some(self.logs.live_load().version()),
        );
        version_updates[MetadataKind::NodesConfiguration] =
            Some(self.nodes_config.live_load().version());

        HeaderMetadataVersions {
            versions: version_updates,
        }
    }
}

static_assertions::assert_impl_all!(ConnectionSender: Send, Sync);
