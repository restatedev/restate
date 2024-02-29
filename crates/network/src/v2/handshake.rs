// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use futures::Stream;
use restate_node_protocol::common::{ProtocolVersion, CURRENT_PROTOCOL_VERSION};
use restate_node_protocol::node::{message, Header, Hello, Message, Welcome};
use tokio_stream::StreamExt;

use crate::error::ProtocolError;

pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);

pub async fn wait_for_hello<S>(incoming: &mut S) -> Result<(Header, Hello), ProtocolError>
where
    S: Stream<Item = Result<Message, ProtocolError>> + Unpin,
{
    let maybe_hello = tokio::time::timeout(HANDSHAKE_TIMEOUT, incoming.next())
        .await
        .map_err(|_| {
            ProtocolError::HandshakeTimeout("Hello message wasn't received within deadline")
        })?;

    let Some(maybe_hello) = maybe_hello else {
        return Err(ProtocolError::PeerDropped);
    };

    let maybe_hello = maybe_hello.map_err(|_| ProtocolError::PeerDropped)?;

    let Some(header) = maybe_hello.header else {
        return Err(ProtocolError::HandshakeFailed(
            "Header should always be set",
        ));
    };

    let Some(maybe_hello) = maybe_hello.body else {
        return Err(ProtocolError::HandshakeFailed(
            "Hello is expected on handshake",
        ));
    };

    // only hello is allowed
    let message::Body::Hello(hello) = maybe_hello else {
        return Err(ProtocolError::HandshakeFailed(
            "Only hello is allowed in handshake!",
        ));
    };
    Ok((header, hello))
}

pub fn negotiate_protocol_version(hello: &Hello) -> Result<ProtocolVersion, ProtocolError> {
    let selected_proto_version =
        std::cmp::min(CURRENT_PROTOCOL_VERSION, hello.max_protocol_version());
    if !selected_proto_version.is_supported() {
        // We cannot support peer's protocol version
        return Err(ProtocolError::UnsupportedVersion(
            hello.max_protocol_version,
        ));
    }

    // Invariant safety net.
    // protocol version must be a value between the min/max version supported by the client.
    // The server has minimum and maximum as well.
    if selected_proto_version < hello.min_protocol_version()
        || selected_proto_version > hello.max_protocol_version()
    {
        // The client cannot support our protocol version
        return Err(ProtocolError::UnsupportedVersion(
            hello.max_protocol_version,
        ));
    }
    Ok(selected_proto_version)
}

pub async fn wait_for_welcome<S>(
    response_stream: &mut S,
) -> Result<(Header, Welcome), ProtocolError>
where
    S: Stream<Item = Result<Message, ProtocolError>> + Unpin,
{
    // first thing we expect is Welcome.
    let maybe_welcome = tokio::time::timeout(HANDSHAKE_TIMEOUT, response_stream.next())
        .await
        .map_err(|_| ProtocolError::HandshakeTimeout("No Welcome received within deadline"))?;

    let Some(maybe_welcome) = maybe_welcome else {
        return Err(ProtocolError::HandshakeFailed("No Welcome received"));
    };

    let maybe_welcome = maybe_welcome.map_err(|_| ProtocolError::PeerDropped)?;

    let Some(header) = maybe_welcome.header else {
        return Err(ProtocolError::HandshakeFailed(
            "Header should always be set",
        ));
    };

    let Some(maybe_welcome) = maybe_welcome.body else {
        return Err(ProtocolError::HandshakeFailed(
            "Welcome is expected on handshake",
        ));
    };

    // only welcome is allowed
    let message::Body::Welcome(welcome) = maybe_welcome else {
        return Err(ProtocolError::HandshakeFailed(
            "Only hello is allowed in handshake!",
        ));
    };

    Ok((header, welcome))
}
