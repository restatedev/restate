// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use restate_encoding::BilrostAs;
use restate_encoding::NetSerde;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strum::EnumIter;

use crate::Version;
use crate::Versioned;
use crate::logs::metadata::Logs;
use crate::metadata::GlobalMetadata;
use crate::net::ServiceTag;
use crate::net::{default_wire_codec, define_rpc, define_service};
use crate::nodes_config::NodesConfiguration;
use crate::partition_table::PartitionTable;
use crate::schema::Schema;

use super::ProtocolVersion;
use super::codec;
use super::codec::WireDecode;
use super::codec::WireEncode;

// NOTE: this type is kept for V1 message fabric compatibility
// V2 fabric uses `GetMetadataRequest` and `MetadataUpdate` directly
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    derive_more::From,
    derive_more::IsVariant,
    strum::IntoStaticStr,
)]
pub enum MetadataMessage {
    GetMetadataRequest(GetMetadataRequest),
    MetadataUpdate(MetadataUpdate),
}

default_wire_codec!(MetadataMessage);

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
    fn encode_to_bytes(&self, protocol_version: ProtocolVersion) -> Bytes {
        if let ProtocolVersion::V1 = protocol_version {
            // V1 protocol sends `GetMetadataRequest` as a `MetadataMessage`
            return Bytes::from(crate::net::codec::encode_as_flexbuffers(
                MetadataMessage::GetMetadataRequest(self.clone()),
                protocol_version,
            ));
        }

        codec::encode_as_bilrost(self, protocol_version)
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
        if let ProtocolVersion::V1 = protocol_version {
            // V1 protocol sends `GetMetadataRequest` as a `MetadataMessage`
            let message =
                crate::net::codec::decode_as_flexbuffers::<MetadataMessage>(buf, protocol_version)?;
            if let MetadataMessage::GetMetadataRequest(request) = message {
                return Ok(request);
            }
            bail!("Invalid message type");
        }

        codec::decode_as_bilrost(buf, protocol_version)
    }
}

impl WireEncode for MetadataUpdate {
    fn encode_to_bytes(&self, protocol_version: ProtocolVersion) -> Bytes {
        if let ProtocolVersion::V1 = protocol_version {
            // V1 protocol sends `MetadataUpdate` as a `MetadataMessage`
            return Bytes::from(codec::encode_as_flexbuffers(
                MetadataMessage::MetadataUpdate(self.clone()),
                protocol_version,
            ));
        }

        codec::encode_as_bilrost(self, protocol_version)
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
        if let ProtocolVersion::V1 = protocol_version {
            // V1 protocol sends `MetadataUpdate` as a `MetadataMessage`
            let message =
                crate::net::codec::decode_as_flexbuffers::<MetadataMessage>(buf, protocol_version)?;
            if let MetadataMessage::MetadataUpdate(update) = message {
                return Ok(update);
            }
            bail!("Invalid message type");
        }
        codec::decode_as_bilrost(buf, protocol_version)
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
    bilrost::Enumeration,
    NetSerde,
)]
#[prost(target = "crate::protobuf::common::MetadataKind")]
pub enum MetadataKind {
    Unknown = 0,
    NodesConfiguration = 1,
    Schema = 2,
    PartitionTable = 3,
    Logs = 4,
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

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From, Default, BilrostAs, NetSerde)]
#[bilrost_as(dto::MetadataContainer)]
pub enum MetadataContainer {
    #[default]
    Unknown,
    NodesConfiguration(Arc<NodesConfiguration>),
    PartitionTable(Arc<PartitionTable>),
    Logs(Arc<Logs>),
    Schema(Arc<Schema>),
}

#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message, NetSerde)]
pub struct GetMetadataRequest {
    #[bilrost(1)]
    pub metadata_kind: MetadataKind,
    #[bilrost(2)]
    pub min_version: Option<crate::Version>,
}

#[derive(Debug, Clone, Serialize, Deserialize, bilrost::Message, NetSerde)]
pub struct MetadataUpdate {
    #[bilrost(1)]
    pub container: MetadataContainer,
}

impl MetadataContainer {
    pub fn version(&self) -> Version {
        match self {
            MetadataContainer::Unknown => unreachable!("invalid metadata container kind"),
            MetadataContainer::NodesConfiguration(c) => c.version(),
            MetadataContainer::PartitionTable(p) => p.version(),
            MetadataContainer::Logs(l) => l.version(),
            MetadataContainer::Schema(s) => s.version(),
        }
    }

    pub fn kind(&self) -> MetadataKind {
        match self {
            MetadataContainer::Unknown => unreachable!("invalid metadata container kind"),
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

pub mod dto {

    use std::{borrow::Cow, sync::Arc};

    use bilrost::{Message, OwnedMessage};
    use bytes::Bytes;

    use crate::{
        logs::metadata::Logs, nodes_config::NodesConfiguration, partition_table::PartitionTable,
        schema::Schema,
    };

    #[derive(Debug, Clone, bilrost::Oneof)]
    enum MetadataContainerInner {
        Unknown,
        #[bilrost(1)]
        NodesConfiguration(Bytes),
        #[bilrost(2)]
        PartitionTable(Bytes),
        #[bilrost(3)]
        Logs(Bytes),
        #[bilrost(4)]
        Schema(Bytes),
    }

    #[derive(bilrost::Message)]
    pub(super) struct MetadataContainer {
        #[bilrost(oneof(1, 2, 3, 4))]
        inner: MetadataContainerInner,
    }

    #[derive(bilrost::Message)]
    struct LogsWrapper<'a>(#[bilrost(1)] Cow<'a, Logs>);

    impl From<&super::MetadataContainer> for MetadataContainer {
        fn from(value: &super::MetadataContainer) -> Self {
            // we avoid copying the inner value (T from the Arc<T>) by directly
            // serializing it directly to a byte array and then
            // send this data instead.
            let inner = match value {
                super::MetadataContainer::Unknown => MetadataContainerInner::Unknown,
                super::MetadataContainer::NodesConfiguration(inner) => {
                    MetadataContainerInner::NodesConfiguration(
                        inner.encode_fast().into_vec().into(),
                    )
                }
                super::MetadataContainer::PartitionTable(inner) => {
                    MetadataContainerInner::PartitionTable(inner.encode_fast().into_vec().into())
                }
                super::MetadataContainer::Logs(inner) => {
                    MetadataContainerInner::Logs(inner.encode_fast().into_vec().into())
                }
                super::MetadataContainer::Schema(inner) => {
                    MetadataContainerInner::Schema(inner.encode_fast().into_vec().into())
                }
            };

            Self { inner }
        }
    }

    impl From<MetadataContainer> for super::MetadataContainer {
        fn from(value: MetadataContainer) -> Self {
            match value.inner {
                MetadataContainerInner::Unknown => Self::Unknown,
                MetadataContainerInner::NodesConfiguration(inner) => Self::NodesConfiguration(
                    Arc::new(NodesConfiguration::decode(inner).expect("valid nodes config")),
                ),
                MetadataContainerInner::PartitionTable(inner) => Self::PartitionTable(Arc::new(
                    PartitionTable::decode(inner).expect("valid partition table"),
                )),
                MetadataContainerInner::Logs(inner) => {
                    Self::Logs(Arc::new(Logs::decode(inner).expect("valid logs")))
                }
                MetadataContainerInner::Schema(inner) => {
                    Self::Schema(Arc::new(Schema::decode(inner).expect("valid schema")))
                }
            }
        }
    }
}
