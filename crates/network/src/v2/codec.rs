// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_node_protocol::common::ProtocolVersion;
use restate_node_protocol::node::message;
use restate_node_protocol::NetworkMessage;

use crate::error::ProtocolError;

pub fn deserialize_message(
    msg: message::Body,
    _protocol_version: ProtocolVersion,
) -> Result<NetworkMessage, ProtocolError> {
    let message::Body::Bincoded(bincoded) = msg else {
        // at the moment, we only support bincoded messages
        return Err(ProtocolError::Codec(
            "Cannot deserialize message, message is not bincoded".to_owned(),
        ));
    };
    let kind = bincoded.kind();
    let (msg, _): (NetworkMessage, _) =
        bincode::serde::decode_from_slice(&bincoded.payload, bincode::config::standard())
            .map_err(|e| ProtocolError::Codec(e.to_string()))?;
    debug_assert_eq!(kind, msg.kind());
    Ok(msg)
}

pub fn serialize_message(
    msg: &NetworkMessage,
    _protocol_version: ProtocolVersion,
) -> Result<message::Body, ProtocolError> {
    Ok(message::Body::Bincoded(message::BinaryMessage {
        kind: msg.kind().into(),
        payload: bincode::serde::encode_to_vec(msg, bincode::config::standard())
            .map_err(|e| ProtocolError::Codec(e.to_string()))?
            .into(),
    }))
}
