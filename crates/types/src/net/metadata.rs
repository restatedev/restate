// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use bytes::Bytes;
use enum_map::Enum;
use prost_dto::{FromProst, IntoProst};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strum::EnumIter;

use crate::Version;
use crate::Versioned;
use crate::logs::metadata::Logs;
use crate::metadata::GlobalMetadata;
use crate::net::ServiceTag;
use crate::net::{define_rpc, define_service};
use crate::nodes_config::NodesConfiguration;
use crate::partition_table::PartitionTable;
use crate::schema::Schema;

use super::ProtocolVersion;
use super::codec;
use super::codec::EncodeError;
use super::codec::WireDecode;
use super::codec::WireEncode;

pub struct MetadataManagerService;
define_service! {
    @service = MetadataManagerService,
    @tag = ServiceTag::MetadataManagerService,
}

define_rpc! {
    @request = GetMetadataRequest,
    @response = MetadataUpdate,
    @service = MetadataManagerService,
}

// -- Custom encoding logic to handle compatibility with V1 protocol
//
impl WireEncode for GetMetadataRequest {
    fn encode_to_bytes(&self, _protocol_version: ProtocolVersion) -> Result<Bytes, EncodeError> {
        Ok(Bytes::from(codec::encode_as_flexbuffers(self)))
    }
}

impl WireDecode for GetMetadataRequest {
    type Error = anyhow::Error;
    fn try_decode(
        buf: impl bytes::Buf,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        codec::decode_as_flexbuffers(buf, protocol_version)
    }
}

impl WireEncode for MetadataUpdate {
    fn encode_to_bytes(&self, protocol_version: ProtocolVersion) -> Result<Bytes, EncodeError> {
        match protocol_version {
            ProtocolVersion::Unknown
            | ProtocolVersion::V2
            | ProtocolVersion::V3
            | ProtocolVersion::V4 => Ok(Bytes::from(codec::encode_as_flexbuffers(self))),
            ProtocolVersion::V6 => Ok(wire::metadata_update_wire_encode(self, protocol_version)
                .expect("encode to succeed")),
        }
    }
}

impl WireDecode for MetadataUpdate {
    type Error = anyhow::Error;
    fn try_decode(
        buf: impl bytes::Buf,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        match protocol_version {
            ProtocolVersion::Unknown
            | ProtocolVersion::V2
            | ProtocolVersion::V3
            | ProtocolVersion::V4 => codec::decode_as_flexbuffers(buf, protocol_version),
            ProtocolVersion::V6 => Ok(wire::metadata_update_wire_decode(buf, protocol_version)?),
        }
    }
}

/// The kind of versioned metadata that can be synchronized across nodes.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Enum,
    EnumIter,
    Serialize,
    Deserialize,
    derive_more::Display,
    strum::EnumCount,
    IntoProst,
    FromProst,
)]
#[prost(target = "crate::protobuf::common::MetadataKind")]
pub enum MetadataKind {
    NodesConfiguration,
    Schema,
    PartitionTable,
    Logs,
}

// todo remove once prost_dto supports TryFromProst
impl TryFrom<crate::protobuf::common::MetadataKind> for MetadataKind {
    type Error = anyhow::Error;

    fn try_from(value: crate::protobuf::common::MetadataKind) -> Result<Self, Self::Error> {
        match value {
            crate::protobuf::common::MetadataKind::Unknown => bail!("unknown metadata kind"),
            crate::protobuf::common::MetadataKind::NodesConfiguration => {
                Ok(MetadataKind::NodesConfiguration)
            }
            crate::protobuf::common::MetadataKind::Schema => Ok(MetadataKind::Schema),
            crate::protobuf::common::MetadataKind::PartitionTable => {
                Ok(MetadataKind::PartitionTable)
            }
            crate::protobuf::common::MetadataKind::Logs => Ok(MetadataKind::Logs),
        }
    }
}

// TODO(azmy): serde Serialize and Deserialize is only implemented for
// backward compatibility with Wire ProtocolVersion < V6. Once
// protocol versions < V6 are deprecated (possibly Restate v1.9.0)
// we SHOULD drop those implementations.
#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum MetadataContainer {
    NodesConfiguration(Arc<NodesConfiguration>),
    PartitionTable(Arc<PartitionTable>),
    Logs(Arc<Logs>),
    Schema(Arc<Schema>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMetadataRequest {
    pub metadata_kind: MetadataKind,
    pub min_version: Option<crate::Version>,
}

// TODO(azmy): serde Serialize and Deserialize is only implemented for
// backward compatibility with Wire ProtocolVersion < V6. Once
// protocol versions < V6 are deprecated (possibly Restate v1.9.0)
// we SHOULD drop those implementations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataUpdate {
    pub container: MetadataContainer,
}

impl MetadataContainer {
    pub fn version(&self) -> Version {
        match self {
            MetadataContainer::NodesConfiguration(c) => c.version(),
            MetadataContainer::PartitionTable(p) => p.version(),
            MetadataContainer::Logs(l) => l.version(),
            MetadataContainer::Schema(s) => s.version(),
        }
    }

    pub fn kind(&self) -> MetadataKind {
        match self {
            MetadataContainer::NodesConfiguration(_) => MetadataKind::NodesConfiguration,
            MetadataContainer::PartitionTable(_) => MetadataKind::PartitionTable,
            MetadataContainer::Logs(_) => MetadataKind::Logs,
            MetadataContainer::Schema(_) => MetadataKind::Schema,
        }
    }

    pub fn extract<T>(self) -> Option<Arc<T>>
    where
        T: GlobalMetadata + Extraction<Output = T>,
    {
        T::extract_as_global_metadata(self)
    }
}

pub trait Extraction: GlobalMetadata {
    type Output;

    fn extract_as_global_metadata(v: MetadataContainer) -> Option<Arc<Self::Output>>;
}

impl Extraction for NodesConfiguration {
    type Output = NodesConfiguration;

    fn extract_as_global_metadata(value: MetadataContainer) -> Option<Arc<Self::Output>> {
        if let MetadataContainer::NodesConfiguration(v) = value {
            Some(v)
        } else {
            None
        }
    }
}

impl Extraction for Schema {
    type Output = Schema;

    fn extract_as_global_metadata(value: MetadataContainer) -> Option<Arc<Self::Output>> {
        if let MetadataContainer::Schema(v) = value {
            Some(v)
        } else {
            None
        }
    }
}

impl Extraction for PartitionTable {
    type Output = PartitionTable;

    fn extract_as_global_metadata(value: MetadataContainer) -> Option<Arc<Self::Output>> {
        if let MetadataContainer::PartitionTable(v) = value {
            Some(v)
        } else {
            None
        }
    }
}

impl Extraction for Logs {
    type Output = Logs;

    fn extract_as_global_metadata(value: MetadataContainer) -> Option<Arc<Self::Output>> {
        if let MetadataContainer::Logs(v) = value {
            Some(v)
        } else {
            None
        }
    }
}

mod wire {
    use std::{ops::Deref, sync::Arc};

    use bilrost::{Message, OwnedMessage};
    use bytes::{Bytes, BytesMut};

    use restate_platform::storage::{StorageCodecKind, StorageDecodeError, StorageEncodeError};

    use super::MetadataUpdate;
    use crate::{
        net::{ProtocolVersion, metadata::MetadataContainer},
        schema::SchemaUnindexed,
        storage::{self},
    };

    /// A duplicate of the super::MetadataKind just to make the bilrost
    /// encoding separate from the public types.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
    enum MetadataKind {
        #[bilrost(0)]
        NodesConfiguration,
        #[bilrost(1)]
        PartitionTable,
        #[bilrost(2)]
        Logs,
        #[bilrost(3)]
        Schema,
    }

    #[derive(Debug, Clone, bilrost::Message)]
    struct MetadataUpdateHeader {
        #[bilrost(1)]
        kind: MetadataKind,
    }

    pub fn metadata_update_wire_encode(
        value: &MetadataUpdate,
        protocol_version: ProtocolVersion,
    ) -> Result<Bytes, StorageEncodeError> {
        assert_eq!(protocol_version, ProtocolVersion::V6);

        let mut bytes = BytesMut::new();

        let kind = match &value.container {
            MetadataContainer::NodesConfiguration(_) => MetadataKind::NodesConfiguration,
            MetadataContainer::PartitionTable(_) => MetadataKind::PartitionTable,
            MetadataContainer::Logs(_) => MetadataKind::Logs,
            MetadataContainer::Schema(_) => MetadataKind::Schema,
        };

        let header = MetadataUpdateHeader { kind };
        let len = header.encoded_len();

        bytes.reserve(len + bilrost::encoding::encoded_len_varint(len as u64));
        header
            .encode_length_delimited(&mut bytes)
            .map_err(|err| StorageEncodeError::EncodeValue(err.into()))?;

        match &value.container {
            MetadataContainer::NodesConfiguration(value) => {
                storage::encode::encode_serde(
                    value,
                    &mut bytes,
                    StorageCodecKind::FlexbuffersSerde,
                )?;
            }
            MetadataContainer::PartitionTable(value) => {
                storage::encode::encode_serde(
                    value,
                    &mut bytes,
                    StorageCodecKind::FlexbuffersSerde,
                )?;
            }
            MetadataContainer::Logs(value) => {
                storage::encode::encode_serde(
                    value,
                    &mut bytes,
                    StorageCodecKind::FlexbuffersSerde,
                )?;
            }
            MetadataContainer::Schema(value) => {
                storage::encode::encode_bilrost_zstd(value.as_ref().deref(), &mut bytes)?;
            }
        };

        Ok(bytes.freeze())
    }

    pub fn metadata_update_wire_decode(
        mut buf: impl bytes::Buf,
        protocol_version: ProtocolVersion,
    ) -> Result<MetadataUpdate, StorageDecodeError> {
        assert_eq!(protocol_version, ProtocolVersion::V6);

        let header = MetadataUpdateHeader::decode_length_delimited(&mut buf)
            .map_err(|err| StorageDecodeError::DecodeValue(err.into()))?;

        let container = match header.kind {
            MetadataKind::NodesConfiguration => {
                let value =
                    storage::decode::decode_serde(&mut buf, StorageCodecKind::FlexbuffersSerde)?;
                MetadataContainer::NodesConfiguration(Arc::new(value))
            }
            MetadataKind::PartitionTable => {
                let value =
                    storage::decode::decode_serde(&mut buf, StorageCodecKind::FlexbuffersSerde)?;
                MetadataContainer::PartitionTable(Arc::new(value))
            }
            MetadataKind::Logs => {
                let value =
                    storage::decode::decode_serde(&mut buf, StorageCodecKind::FlexbuffersSerde)?;
                MetadataContainer::Logs(Arc::new(value))
            }
            MetadataKind::Schema => {
                let value: SchemaUnindexed = storage::decode::decode_bilrost_zstd(&mut buf)?;
                value
                    .verify()
                    .map_err(|err| StorageDecodeError::DecodeValue(err.into()))?;
                MetadataContainer::Schema(Arc::new(value.into()))
            }
        };

        Ok(MetadataUpdate { container })
    }
}

#[cfg(test)]
mod tests {
    use restate_platform::storage::{StorageCodecKind, StorageEncode};

    use super::*;

    #[test]
    fn metadata_update_wire_roundtrip() {
        let mut nodes_config = NodesConfiguration::new_for_testing();
        nodes_config.set_version(Version::from(7));
        let partition_table = PartitionTable::with_equally_sized_partitions(Version::from(3), 4);

        let containers = [
            MetadataContainer::NodesConfiguration(Arc::new(nodes_config.clone())),
            MetadataContainer::PartitionTable(Arc::new(partition_table.clone())),
            MetadataContainer::Logs(Arc::new(Logs::default())),
            MetadataContainer::Schema(Arc::new(Schema::default())),
        ];

        for protocol_version in [ProtocolVersion::V4, ProtocolVersion::V6] {
            for container in &containers {
                let update = MetadataUpdate {
                    container: container.clone(),
                };
                let bytes = update.encode_to_bytes(protocol_version).unwrap();
                let decoded = MetadataUpdate::try_decode(bytes, protocol_version).unwrap();

                assert_eq!(decoded.container.kind(), container.kind());
                assert_eq!(decoded.container.version(), container.version());

                match decoded.container {
                    MetadataContainer::NodesConfiguration(decoded) => {
                        assert_eq!(decoded.cluster_name(), nodes_config.cluster_name());
                        assert_eq!(
                            decoded.cluster_fingerprint(),
                            nodes_config.cluster_fingerprint()
                        );
                    }
                    MetadataContainer::PartitionTable(decoded) => {
                        assert_eq!(*decoded, partition_table);
                    }
                    MetadataContainer::Logs(_) | MetadataContainer::Schema(_) => {}
                }
            }
        }
    }

    /// Guards against a metadata type's default storage codec drifting away
    /// from the codecs the wire protocol expects for it.
    ///
    /// A failure here is not a correctness issue, but it means the wire
    /// protocol is no longer sending that object with its most efficient codec.
    /// To fix it, bump the wire protocol version, switch that object to the new
    /// codec on the wire, and then update the expected codecs below.
    #[test]
    fn storage_encoding_matches_wire_encoding() {
        let supported_codecs = vec![
            (
                "NodesConfiguration",
                NodesConfiguration::default().default_codec(),
                vec![StorageCodecKind::FlexbuffersSerde],
            ),
            (
                "PartitionTable",
                PartitionTable::default().default_codec(),
                vec![StorageCodecKind::FlexbuffersSerde],
            ),
            (
                "Logs",
                Logs::default().default_codec(),
                vec![StorageCodecKind::FlexbuffersSerde],
            ),
            (
                "Schema",
                Schema::default().default_codec(),
                vec![
                    // todo(azmy): Remove the flexbuffer support once zstd-bilrost-encoding becomes
                    // default storage codec for schema
                    StorageCodecKind::FlexbuffersSerde,
                    StorageCodecKind::ZstdBilrostDefault,
                ],
            ),
        ];

        for (metadata_kind, current, supported) in supported_codecs {
            assert!(
                supported.contains(&current),
                "StorageCodecKind for {metadata_kind} has diverged from the supported wire protocol (storage: {current}), supported: {supported:?})"
            );
        }
    }
}
