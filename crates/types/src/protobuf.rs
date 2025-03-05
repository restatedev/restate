// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
    use crate::Merge;
    use crate::net::{CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION};

    include!(concat!(env!("OUT_DIR"), "/restate.common.rs"));

    impl ProtocolVersion {
        pub fn is_supported(&self) -> bool {
            *self >= MIN_SUPPORTED_PROTOCOL_VERSION && *self <= CURRENT_PROTOCOL_VERSION
        }
    }

    impl From<crate::GenerationalNodeId> for GenerationalNodeId {
        fn from(value: crate::GenerationalNodeId) -> Self {
            Self {
                id: value.id(),
                generation: value.generation(),
            }
        }
    }

    impl std::fmt::Display for GenerationalNodeId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(&crate::GenerationalNodeId::from(*self), f)
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

    impl MetadataServerStatus {
        /// Returns true if the metadata store is running which means that it is either a member or
        /// a standby node.
        pub fn is_running(&self) -> bool {
            matches!(
                self,
                MetadataServerStatus::Member | MetadataServerStatus::Standby
            )
        }
    }

    impl Merge for NodeStatus {
        fn merge(&mut self, other: Self) -> bool {
            if other > *self {
                *self = other;
                return true;
            }
            false
        }
    }
}

pub mod cluster {
    use crate::partition_table::PartitionReplication;

    include!(concat!(env!("OUT_DIR"), "/restate.cluster.rs"));

    impl std::fmt::Display for RunMode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let o = match self {
                RunMode::Unknown => "UNKNOWN",
                RunMode::Leader => "Leader",
                RunMode::Follower => "Follower",
            };
            write!(f, "{o}")
        }
    }

    impl From<crate::replication::ReplicationProperty> for ReplicationProperty {
        fn from(value: crate::replication::ReplicationProperty) -> Self {
            ReplicationProperty {
                replication_property: value.to_string(),
            }
        }
    }

    impl TryFrom<ReplicationProperty> for crate::replication::ReplicationProperty {
        type Error = anyhow::Error;

        fn try_from(value: ReplicationProperty) -> Result<Self, Self::Error> {
            value.replication_property.parse()
        }
    }

    impl TryFrom<Option<ReplicationProperty>> for PartitionReplication {
        type Error = anyhow::Error;

        fn try_from(value: Option<ReplicationProperty>) -> Result<Self, Self::Error> {
            Ok(value
                .map(TryFrom::try_from)
                .transpose()?
                .map_or(PartitionReplication::Everywhere, |p| {
                    PartitionReplication::Limit(p)
                }))
        }
    }

    impl From<PartitionReplication> for Option<ReplicationProperty> {
        fn from(value: PartitionReplication) -> Self {
            match value {
                PartitionReplication::Everywhere => None,
                PartitionReplication::Limit(replication_property) => {
                    Some(replication_property.into())
                }
            }
        }
    }

    impl ClusterConfiguration {
        pub fn into_inner(self) -> (u32, Option<ReplicationProperty>, Option<BifrostProvider>) {
            (
                self.num_partitions,
                self.partition_replication,
                self.bifrost_provider,
            )
        }
    }

    impl BifrostProvider {
        pub fn into_inner(self) -> (String, Option<ReplicationProperty>, u32) {
            (
                self.provider,
                self.replication_property,
                self.target_nodeset_size,
            )
        }
    }
}

pub mod node {
    use opentelemetry::global;
    use opentelemetry::propagation::{Extractor, Injector};
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    use crate::GenerationalNodeId;

    use crate::net::{CURRENT_PROTOCOL_VERSION, MIN_SUPPORTED_PROTOCOL_VERSION, ProtocolVersion};

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
        pub fn new(
            nodes_config_version: crate::Version,
            logs_version: Option<crate::Version>,
            schema_version: Option<crate::Version>,
            partition_table_version: Option<crate::Version>,
            msg_id: u64,
            in_response_to: Option<u64>,
        ) -> Self {
            let span_context = global::get_text_map_propagator(|propagator| {
                let context = tracing::Span::current().context();
                let mut span_context = SpanContext::default();
                propagator.inject_context(&context, &mut span_context);
                if span_context.fields.is_empty() {
                    None
                } else {
                    Some(span_context)
                }
            });

            Self {
                my_nodes_config_version: Some(nodes_config_version.into()),
                my_logs_version: logs_version.map(Into::into),
                my_schema_version: schema_version.map(Into::into),
                my_partition_table_version: partition_table_version.map(Into::into),
                msg_id,
                in_response_to,
                span_context,
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

pub mod log_server_common {

    include!(concat!(env!("OUT_DIR"), "/restate.log_server_common.rs"));

    impl std::fmt::Display for RecordStatus {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(&crate::net::log_server::RecordStatus::from(*self as i32), f)
        }
    }
}

pub mod metadata {
    use crate::errors::ConversionError;

    include!(concat!(env!("OUT_DIR"), "/restate.metadata.rs"));

    impl TryFrom<VersionedValue> for crate::metadata::VersionedValue {
        type Error = ConversionError;

        fn try_from(value: VersionedValue) -> Result<Self, Self::Error> {
            let version = value
                .version
                .ok_or_else(|| ConversionError::missing_field("version"))?;
            Ok(crate::metadata::VersionedValue::new(
                version.into(),
                value.bytes,
            ))
        }
    }

    impl From<crate::metadata::VersionedValue> for VersionedValue {
        fn from(value: crate::metadata::VersionedValue) -> Self {
            VersionedValue {
                version: Some(value.version.into()),
                bytes: value.value,
            }
        }
    }

    impl From<crate::metadata::Precondition> for Precondition {
        fn from(value: crate::metadata::Precondition) -> Self {
            match value {
                crate::metadata::Precondition::None => Precondition {
                    kind: PreconditionKind::None.into(),
                    version: None,
                },
                crate::metadata::Precondition::DoesNotExist => Precondition {
                    kind: PreconditionKind::DoesNotExist.into(),
                    version: None,
                },
                crate::metadata::Precondition::MatchesVersion(version) => Precondition {
                    kind: PreconditionKind::MatchesVersion.into(),
                    version: Some(version.into()),
                },
            }
        }
    }

    impl TryFrom<Precondition> for crate::metadata::Precondition {
        type Error = ConversionError;

        fn try_from(value: Precondition) -> Result<Self, Self::Error> {
            match value.kind() {
                PreconditionKind::Unknown => {
                    Err(ConversionError::invalid_data("unknown precondition kind"))
                }
                PreconditionKind::None => Ok(crate::metadata::Precondition::None),
                PreconditionKind::DoesNotExist => Ok(crate::metadata::Precondition::DoesNotExist),
                PreconditionKind::MatchesVersion => {
                    Ok(crate::metadata::Precondition::MatchesVersion(
                        value
                            .version
                            .ok_or_else(|| ConversionError::missing_field("version"))?
                            .into(),
                    ))
                }
            }
        }
    }
}
