// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};

use restate_types::net::codec::{Targeted, WireEncode};
use restate_types::net::RpcRequest;
use restate_types::{GenerationalNodeId, NodeId};

use crate::with_metadata;

use super::connection::{Connection, HeaderMetadataVersions};
use super::{NetworkError, NetworkSendError};

static NEXT_MSG_ID: AtomicU64 = const { AtomicU64::new(1) };

/// generate a new unique message id for this node
#[inline(always)]
pub(crate) fn generate_msg_id() -> u64 {
    NEXT_MSG_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// A wrapper for incoming messages that includes the sender information
#[derive(Debug, Clone)]
pub struct Incoming<M> {
    peer: GenerationalNodeId,
    msg_id: u64,
    connection: Weak<Connection>,
    body: M,
    in_response_to: Option<u64>,
}

impl<M> Deref for Incoming<M> {
    type Target = M;
    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

impl<M> Incoming<M> {
    pub(crate) fn from_parts(
        peer: GenerationalNodeId,
        body: M,
        connection: Weak<Connection>,
        msg_id: u64,
        in_response_to: Option<u64>,
    ) -> Self {
        Self {
            peer,
            msg_id,
            connection,
            body,
            in_response_to,
        }
    }
}

impl<M> Incoming<M> {
    pub fn peer(&self) -> GenerationalNodeId {
        self.peer
    }

    pub fn msg_id(&self) -> u64 {
        self.msg_id
    }

    pub fn split(self) -> (GenerationalNodeId, M) {
        (self.peer, self.body)
    }

    pub fn body(&self) -> &M {
        &self.body
    }

    pub fn into_body(self) -> M {
        self.body
    }

    pub fn try_map<O, E>(self, f: impl FnOnce(M) -> Result<O, E>) -> Result<Incoming<O>, E> {
        Ok(Incoming {
            peer: self.peer,
            msg_id: self.msg_id,
            connection: self.connection,
            body: f(self.body)?,
            in_response_to: self.in_response_to,
        })
    }

    pub fn map<O>(self, f: impl FnOnce(M) -> O) -> Incoming<O> {
        Incoming {
            peer: self.peer,
            msg_id: self.msg_id,
            connection: self.connection,
            body: f(self.body),
            in_response_to: self.in_response_to,
        }
    }

    pub fn in_response_to(&self) -> Option<u64> {
        self.in_response_to
    }

    pub fn prepare_response<O>(&self, body: O) -> Outgoing<O> {
        Outgoing {
            peer: self.peer.into(),
            connection: self.connection.clone(),
            msg_id: generate_msg_id(),
            body,
            in_response_to: Some(self.msg_id),
        }
    }

    /// Sends a response on the same connection where we received the request. This will
    /// fail with [`NetworkError::ConnectionClosed`] if the connection is terminated.
    ///
    /// This fails immediately with [`NetworkError::Full`] if connection stream is out of capacity.
    pub fn try_respond<O: Targeted + WireEncode>(
        &self,
        response: O,
    ) -> Result<(), NetworkSendError<O>> {
        let (connection, versions, response) = self.respond_prep_inner(response)?;
        connection.try_send(response, versions)
    }

    /// Sends a response on the same connection where we received the request. This will
    /// fail with [`NetworkError::ConnectionClosed`] if the connection is terminated.
    ///
    /// This blocks until there is capacity on the connection stream.
    pub async fn respond<O: Targeted + WireEncode>(
        &self,
        response: O,
    ) -> Result<(), NetworkSendError<O>> {
        let (connection, versions, response) = self.respond_prep_inner(response)?;
        connection.send(response, versions).await
    }

    fn respond_prep_inner<O>(
        &self,
        response: O,
    ) -> Result<(Arc<Connection>, HeaderMetadataVersions, Outgoing<O>), NetworkSendError<O>> {
        let response = self.prepare_response(response);

        let connection = match self.connection.upgrade() {
            Some(connection) => connection,
            None => {
                return Err(NetworkSendError::new(
                    response,
                    NetworkError::ConnectionClosed,
                ));
            }
        };
        let versions = with_metadata(HeaderMetadataVersions::from_metadata);
        Ok((connection, versions, response))
    }
}

impl<M: RpcRequest> Incoming<M> {
    pub fn prepare_rpc_response(
        &self,
        response: M::ResponseMessage,
    ) -> Outgoing<M::ResponseMessage> {
        self.prepare_response(response)
    }

    /// Sends a response on the same connection where we received the request. This will
    /// fail with [`NetworkError::ConnectionClosed`] if the connection is terminated.
    ///
    /// This fails immediately with [`NetworkError::Full`] if connection stream is out of capacity.
    pub fn try_respond_rpc(
        &self,
        response: M::ResponseMessage,
    ) -> Result<(), NetworkSendError<M::ResponseMessage>> {
        let (connection, versions, response) = self.respond_prep_inner(response)?;
        connection.try_send(response, versions)
    }

    /// Sends a response on the same connection where we received the request. This will
    /// fail with [`NetworkError::ConnectionClosed`] if the connection is terminated.
    ///
    /// This blocks until there is capacity on the connection stream.
    pub async fn respond_rpc(
        &self,
        response: M::ResponseMessage,
    ) -> Result<(), NetworkSendError<M::ResponseMessage>> {
        let (connection, versions, response) = self.respond_prep_inner(response)?;
        connection.send(response, versions).await
    }
}

/// A wrapper for outgoing messages that includes the correlation information if a message is in
/// response to a request.
#[derive(Debug, Clone)]
pub struct Outgoing<M> {
    peer: NodeId,
    msg_id: u64,
    connection: Weak<Connection>,
    body: M,
    in_response_to: Option<u64>,
}

impl<M> Deref for Outgoing<M> {
    type Target = M;
    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

impl<M: Targeted> Outgoing<M> {
    pub fn new(peer: impl Into<NodeId>, body: M) -> Self {
        let msg_id = generate_msg_id();
        Self {
            peer: peer.into(),
            msg_id,
            connection: Weak::new(),
            body,
            in_response_to: None,
        }
    }

    pub fn from_parts(peer: NodeId, body: M, msg_id: u64, in_response_to: Option<u64>) -> Self {
        Self {
            peer,
            msg_id,
            connection: Weak::new(),
            body,
            in_response_to,
        }
    }
}

impl<M> Outgoing<M> {
    pub fn peer(&self) -> NodeId {
        self.peer
    }
    pub fn set_peer(&mut self, peer: impl Into<NodeId>) {
        self.peer = peer.into();
        // unset connection
        self.reset_connection();
    }

    pub fn msg_id(&self) -> u64 {
        self.msg_id
    }

    pub(crate) fn get_connection(&self) -> Option<Arc<Connection>> {
        self.connection.upgrade()
    }

    pub(crate) fn reset_connection(&mut self) {
        self.connection = Weak::new();
    }

    pub fn body(&self) -> &M {
        &self.body
    }

    pub fn into_body(self) -> M {
        self.body
    }

    pub fn in_response_to(&self) -> Option<u64> {
        self.in_response_to
    }

    pub fn try_map<O, E>(self, f: impl FnOnce(M) -> Result<O, E>) -> Result<Outgoing<O>, E> {
        Ok(Outgoing {
            peer: self.peer,
            msg_id: self.msg_id,
            connection: self.connection,
            body: f(self.body)?,
            in_response_to: self.in_response_to,
        })
    }

    pub fn map<O>(self, f: impl FnOnce(M) -> O) -> Outgoing<O> {
        Outgoing {
            peer: self.peer,
            msg_id: self.msg_id,
            connection: self.connection,
            body: f(self.body),
            in_response_to: self.in_response_to,
        }
    }
}
