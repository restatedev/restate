// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/common_descriptor.bin"));

pub mod common {
    use crate::net::{CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION};

    include!(concat!(env!("OUT_DIR"), "/restate.common.rs"));

    impl ProtocolVersion {
        pub fn is_supported(&self) -> bool {
            *self >= MIN_SUPPORTED_PROTOCOL_VERSION && *self <= CURRENT_PROTOCOL_VERSION
        }
    }

    impl std::fmt::Display for NodeId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if let Some(generation) = self.generation {
                write!(f, "N{}:{}", self.id, generation)
            } else {
                write!(f, "N{}", self.id)
            }
        }
    }

    impl std::fmt::Display for Lsn {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.value)
        }
    }

    impl std::fmt::Display for LeaderEpoch {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "e{}", self.value)
        }
    }
}

pub mod cluster {
    include!(concat!(env!("OUT_DIR"), "/restate.cluster.rs"));

    impl std::fmt::Display for RunMode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let o = match self {
                RunMode::Unknown => "UNKNOWN",
                RunMode::Leader => "Leader",
                RunMode::Follower => "Follower",
            };
            write!(f, "{}", o)
        }
    }
}

pub mod node {
    use crate::GenerationalNodeId;

    use crate::net::{ProtocolVersion, CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION};

    use self::message::{BinaryMessage, ConnectionControl, Signal};

    include!(concat!(env!("OUT_DIR"), "/restate.node.rs"));

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
        pub fn new(
            nodes_config_version: crate::Version,
            logs_version: Option<crate::Version>,
            schema_version: Option<crate::Version>,
            partition_table_version: Option<crate::Version>,
            msg_id: u64,
            in_response_to: Option<u64>,
        ) -> Self {
            Self {
                my_nodes_config_version: Some(nodes_config_version.into()),
                my_logs_version: logs_version.map(Into::into),
                my_schema_version: schema_version.map(Into::into),
                my_partition_table_version: partition_table_version.map(Into::into),
                msg_id,
                in_response_to,
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
}
