// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod core_node_svc {
    tonic::include_proto!("restate.core_node_svc");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("core_node_svc_descriptor");
}

pub mod network {
    tonic::include_proto!("restate.network");

    use opentelemetry::propagation::{Extractor, Injector};

    use restate_types::GenerationalNodeId;

    use restate_types::net::{
        CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION, ProtocolVersion,
    };

    use self::message::{BinaryMessage, ConnectionControl, Signal};

    impl Hello {
        pub fn new(
            my_node_id: Option<GenerationalNodeId>,
            cluster_name: String,
            direction: ConnectionDirection,
        ) -> Self {
            Self {
                direction: direction.into(),
                min_protocol_version: MIN_SUPPORTED_PROTOCOL_VERSION.into(),
                max_protocol_version: CURRENT_PROTOCOL_VERSION.into(),
                my_node_id: my_node_id.map(Into::into),
                cluster_name,
            }
        }
    }

    impl Injector for SpanContext {
        fn set(&mut self, key: &str, value: String) {
            self.fields.insert(key.to_owned(), value);
        }
    }

    impl Extractor for SpanContext {
        fn get(&self, key: &str) -> Option<&str> {
            self.fields.get(key).map(|v| v.as_str())
        }

        fn keys(&self) -> Vec<&str> {
            self.fields.keys().map(|k| k.as_str()).collect()
        }
    }
    impl Header {
        #[cfg(any(test, feature = "test-util"))]
        pub fn new(
            nodes_config_version: restate_types::Version,
            logs_version: Option<restate_types::Version>,
            schema_version: Option<restate_types::Version>,
            partition_table_version: Option<restate_types::Version>,
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
                span_context: None,
            }
        }
    }

    impl Welcome {
        pub fn new(
            my_node_id: GenerationalNodeId,
            protocol_version: ProtocolVersion,
            direction_ack: ConnectionDirection,
        ) -> Self {
            Self {
                my_node_id: Some(my_node_id.into()),
                protocol_version: protocol_version.into(),
                direction_ack: direction_ack.into(),
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

    impl self::message::Body {
        pub fn try_as_binary_body(
            self,
            _protocol_version: ProtocolVersion,
        ) -> anyhow::Result<BinaryMessage> {
            let message::Body::Encoded(binary) = self else {
                return Err(anyhow::anyhow!(
                    "Cannot deserialize message, message is not of type BinaryMessage",
                ));
            };
            Ok(binary)
        }
    }
}
