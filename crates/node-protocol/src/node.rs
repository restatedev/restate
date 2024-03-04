// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::GenerationalNodeId;

use crate::common::{ProtocolVersion, CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION};

use self::message::{BinaryMessage, ConnectionControl, Signal};

include!(concat!(env!("OUT_DIR"), "/dev.restate.node.rs"));

pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/node_descriptor.bin"));

impl Hello {
    pub fn new(my_node_id: GenerationalNodeId, cluster_name: String) -> Self {
        Self {
            min_protocol_version: MIN_SUPPORTED_PROTOCOL_VERSION.into(),
            max_protocol_version: CURRENT_PROTOCOL_VERSION.into(),
            my_node_id: Some(my_node_id.into()),
            cluster_name,
        }
    }
}

impl Header {
    pub fn new(nodes_config_version: restate_types::Version) -> Self {
        Self {
            my_nodes_config_version: Some(nodes_config_version.into()),
        }
    }
}

impl Welcome {
    pub fn new(my_node_id: GenerationalNodeId, protocol_version: ProtocolVersion) -> Self {
        Self {
            my_node_id: Some(my_node_id.into()),
            protocol_version: protocol_version.into(),
        }
    }
}

impl Message {
    pub fn new(header: Header, body: impl Into<self::message::Body>) -> Self {
        Self {
            header: Some(header),
            body: Some(body.into()),
        }
    }
}

impl From<Hello> for message::Body {
    fn from(value: Hello) -> Self {
        message::Body::Hello(value)
    }
}

impl From<Welcome> for message::Body {
    fn from(value: Welcome) -> Self {
        message::Body::Welcome(value)
    }
}

impl From<ConnectionControl> for message::Body {
    fn from(value: ConnectionControl) -> Self {
        message::Body::ConnectionControl(value)
    }
}

impl From<BinaryMessage> for message::Body {
    fn from(value: BinaryMessage) -> Self {
        message::Body::Encoded(value)
    }
}

impl ConnectionControl {
    pub fn connection_reset() -> Self {
        Self {
            signal: message::Signal::DrainConnection.into(),
            message: "Connection is draining and will be dropped".to_owned(),
        }
    }
    pub fn shutdown() -> Self {
        Self {
            signal: message::Signal::Shutdown.into(),
            message: "Node is shutting down".to_owned(),
        }
    }
    pub fn codec_error(message: impl Into<String>) -> Self {
        Self {
            signal: message::Signal::CodecError.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Signal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str_name())
    }
}
