// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use tokio_stream::StreamExt;

use restate_types::net::{CURRENT_PROTOCOL_VERSION, ProtocolVersion};

use super::HandshakeError;
use super::protobuf::network::{Header, Hello, Message, Welcome, message};

pub async fn wait_for_hello<S>(
    incoming: &mut S,
    timeout: Duration,
) -> Result<(Header, Hello), HandshakeError>
where
    S: Stream<Item = Message> + Unpin,
{
    let maybe_hello = tokio::time::timeout(timeout, incoming.next())
        .await
        .map_err(|_| {
            HandshakeError::Timeout("Hello message wasn't received within deadline".to_owned())
        })?;

    let Some(maybe_hello) = maybe_hello else {
        return Err(HandshakeError::PeerDropped);
    };

    let Some(header) = maybe_hello.header else {
        return Err(HandshakeError::Failed(
            "Header should always be set".to_owned(),
        ));
    };

    let Some(maybe_hello) = maybe_hello.body else {
        return Err(HandshakeError::Failed(
            "Hello is expected on handshake".to_owned(),
        ));
    };

    // only hello is allowed
    let message::Body::Hello(hello) = maybe_hello else {
        return Err(HandshakeError::Failed(
            "Only hello is allowed in handshake!".to_owned(),
        ));
    };
    Ok((header, hello))
}

pub fn negotiate_protocol_version(hello: &Hello) -> Result<ProtocolVersion, HandshakeError> {
    let selected_proto_version_number =
        std::cmp::min(CURRENT_PROTOCOL_VERSION as i32, hello.max_protocol_version);

    let selected_proto_version = ProtocolVersion::try_from(selected_proto_version_number)
        .map_err(|_| HandshakeError::UnsupportedVersion(selected_proto_version_number))?;

    if !selected_proto_version.is_supported() {
        // We cannot support peer's protocol version
        return Err(HandshakeError::UnsupportedVersion(
            hello.max_protocol_version,
        ));
    }

    // Invariant safety net.
    // protocol version must be a value between the min/max version supported by the client.
    // The server has minimum and maximum as well.
    if selected_proto_version_number < hello.min_protocol_version
        || selected_proto_version_number > hello.max_protocol_version
    {
        // The client cannot support our protocol version
        return Err(HandshakeError::UnsupportedVersion(
            hello.max_protocol_version,
        ));
    }
    Ok(selected_proto_version)
}

pub async fn wait_for_welcome<S>(
    response_stream: &mut S,
    timeout: Duration,
) -> Result<(Header, Welcome), HandshakeError>
where
    S: Stream<Item = Message> + Unpin,
{
    // first thing we expect is Welcome.
    let maybe_welcome = tokio::time::timeout(timeout, response_stream.next())
        .await
        .map_err(|_| HandshakeError::Timeout("No Welcome received within deadline".to_owned()))?;

    let Some(maybe_welcome) = maybe_welcome else {
        return Err(HandshakeError::Failed("No Welcome received".to_owned()));
    };

    let Some(header) = maybe_welcome.header else {
        return Err(HandshakeError::Failed(
            "Header should always be set".to_owned(),
        ));
    };

    let Some(maybe_welcome) = maybe_welcome.body else {
        return Err(HandshakeError::Failed(
            "Welcome is expected on handshake".to_owned(),
        ));
    };

    // only welcome is allowed
    let message::Body::Welcome(welcome) = maybe_welcome else {
        return Err(HandshakeError::Failed(
            "Only Welcome is allowed in handshake!".to_owned(),
        ));
    };

    Ok((header, welcome))
}
