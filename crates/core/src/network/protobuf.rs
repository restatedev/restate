// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
    use restate_types::net::metadata::MetadataKind;
    use restate_types::nodes_config::ClusterFingerprint;

    use restate_types::net::{
        CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION, ProtocolVersion,
    };

    use self::message::{ConnectionControl, Signal};

    impl std::fmt::Display for ConnectionDirection {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.as_str_name())
        }
    }

    impl std::fmt::Display for Swimlane {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.as_str_name())
        }
    }

    impl Hello {
        #[must_use]
        pub fn new(
            my_node_id: Option<GenerationalNodeId>,
            cluster_name: String,
            cluster_fingerprint: ClusterFingerprint,
            direction: ConnectionDirection,
            swimlane: Swimlane,
        ) -> Self {
            Self {
                direction: direction.into(),
                min_protocol_version: MIN_SUPPORTED_PROTOCOL_VERSION.into(),
                max_protocol_version: CURRENT_PROTOCOL_VERSION.into(),
                my_node_id: my_node_id.map(Into::into),
                cluster_name,
                cluster_fingerprint: cluster_fingerprint.to_u64(),
                swimlane: swimlane.into(),
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
            self.fields.get(key).map(String::as_str)
        }

        fn keys(&self) -> Vec<&str> {
            self.fields.keys().map(String::as_str).collect()
        }
    }

    impl Header {
        /// Returns the version of the metadata for the given kind.
        #[must_use]
        pub fn metadata_version(&self, kind: MetadataKind) -> restate_types::Version {
            match kind {
                MetadataKind::NodesConfiguration => self.my_nodes_config_version.into(),
                MetadataKind::Schema => self.my_schema_version.into(),
                MetadataKind::PartitionTable => self.my_partition_table_version.into(),
                MetadataKind::Logs => self.my_logs_version.into(),
            }
        }
    }

    impl Welcome {
        #[must_use]
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
        #[must_use]
        pub fn new(header: Header, body: impl Into<self::message::Body>) -> Self {
            Self {
                header: Some(header),
                body: Some(body.into()),
            }
        }
    }

    impl ConnectionControl {
        #[must_use]
        pub fn connection_reset() -> Self {
            Self {
                signal: message::Signal::DrainConnection.into(),
                message: "Connection is draining and will be dropped".to_owned(),
            }
        }
        #[must_use]
        pub fn shutdown() -> Self {
            Self {
                signal: message::Signal::Shutdown.into(),
                message: "Node is shutting down".to_owned(),
            }
        }
        #[must_use]
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

    impl Ping {
        pub fn flip(self) -> Pong {
            Pong {
                timestamp: self.timestamp,
            }
        }
    }
}
