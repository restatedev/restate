// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::net::codec::{Targeted, WireEncode};

use super::{NetworkSendError, Outgoing};

/// Send NetworkMessage to nodes
pub trait NetworkSender: Send + Sync + Clone {
    /// Send a message to a peer node. Order of messages is not guaranteed since underlying
    /// implementations might load balance message writes across multiple connections or re-order
    /// messages in-flight based on priority. If ordered delivery is required, then use
    /// [`restate_network::ConnectionSender`] instead.
    ///
    /// Establishing connections is handled internally with basic retries for straight-forward
    /// failures.
    ///
    /// If `to` is a NodeID with generation, then it's guaranteed that messages will be sent to
    /// this particular generation, otherwise, it'll be routed to the latest generation available
    /// in nodes configuration at the time of the call. This might return
    /// [[`NetworkSendError::OldPeerGeneration`]] if the node is not the latest generation.
    ///
    /// It returns Ok(()) when the message is:
    /// - Successfully serialized to the wire format based on the negotiated protocol
    /// - Serialized message was enqueued on the send buffer of the socket
    ///
    /// That means that this is not a guarantee that the message has been sent
    /// over the network or that the peer have received it.
    fn send<M>(
        &self,
        message: Outgoing<M>,
    ) -> impl std::future::Future<Output = Result<(), NetworkSendError<M>>> + Send
    where
        M: WireEncode + Targeted + Send + Sync;
}
