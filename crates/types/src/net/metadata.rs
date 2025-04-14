// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Buf;
use enum_map::Enum;
use prost_dto::{FromProst, IntoProst};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strum::EnumIter;

use crate::Version;
use crate::Versioned;
use crate::errors::ConversionError;
use crate::logs::metadata::Logs;
use crate::net::TargetName;
use crate::net::define_message;
use crate::nodes_config::NodesConfiguration;
use crate::partition_table::PartitionTable;
use crate::protobuf::net::metadata as proto;
use crate::schema::Schema;

use super::RpcRequest;
use super::codec::V2Convertible;

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

impl From<MetadataMessage> for proto::MetadataMessage {
    fn from(value: MetadataMessage) -> Self {
        use crate::protobuf::common::MetadataKind as ProtoMetadataKind;
        use proto::metadata_message::MessageKind;

        fn to_vec<T: Serialize>(input: T) -> Vec<u8> {
            flexbuffers::to_vec(input).expect("network message serde can't fail")
        }

        match value {
            MetadataMessage::GetMetadataRequest(get) => Self {
                message_kind: MessageKind::Get.into(),
                metadata_kind: get.metadata_kind.into(),
                min_version: get.min_version.map(Into::into),

                payload: None,
            },
            MetadataMessage::MetadataUpdate(update) => {
                let (metadata_kind, payload) = match update.container {
                    MetadataContainer::Logs(payload) => (ProtoMetadataKind::Logs, to_vec(payload)),
                    MetadataContainer::NodesConfiguration(payload) => {
                        (ProtoMetadataKind::NodesConfiguration, to_vec(payload))
                    }
                    MetadataContainer::PartitionTable(payload) => {
                        (ProtoMetadataKind::PartitionTable, to_vec(payload))
                    }
                    MetadataContainer::Schema(payload) => {
                        (ProtoMetadataKind::Schema, to_vec(payload))
                    }
                };

                Self {
                    message_kind: MessageKind::Update.into(),
                    min_version: None,
                    metadata_kind: metadata_kind.into(),
                    payload: Some(payload.into()),
                }
            }
        }
    }
}

impl TryFrom<proto::MetadataMessage> for MetadataMessage {
    type Error = ConversionError;

    fn try_from(value: proto::MetadataMessage) -> Result<Self, Self::Error> {
        use crate::protobuf::common::MetadataKind as ProtoMetadataKind;
        use proto::metadata_message::MessageKind;

        let metadata_kind = value.metadata_kind();

        let result = match value.message_kind() {
            MessageKind::Get => Self::GetMetadataRequest(GetMetadataRequest {
                metadata_kind: value.metadata_kind().try_into()?,
                min_version: value.min_version.map(Into::into),
            }),
            MessageKind::Update => {
                let payload = value
                    .payload
                    .ok_or_else(|| ConversionError::missing_field("payload"))?;

                Self::MetadataUpdate(MetadataUpdate {
                    container: {
                        match metadata_kind {
                            ProtoMetadataKind::Logs => MetadataContainer::Logs(
                                flexbuffers::from_slice(payload.chunk())
                                    .map_err(|_| ConversionError::invalid_data("payload"))?,
                            ),
                            ProtoMetadataKind::NodesConfiguration => {
                                MetadataContainer::NodesConfiguration(
                                    flexbuffers::from_slice(payload.chunk())
                                        .map_err(|_| ConversionError::invalid_data("payload"))?,
                                )
                            }
                            ProtoMetadataKind::PartitionTable => MetadataContainer::PartitionTable(
                                flexbuffers::from_slice(payload.chunk())
                                    .map_err(|_| ConversionError::invalid_data("payload"))?,
                            ),
                            ProtoMetadataKind::Schema => MetadataContainer::Schema(
                                flexbuffers::from_slice(payload.chunk())
                                    .map_err(|_| ConversionError::invalid_data("payload"))?,
                            ),
                            ProtoMetadataKind::Unknown => {
                                return Err(ConversionError::InvalidData("metadata_kind"));
                            }
                        }
                    },
                })
            }
            _ => return Err(ConversionError::invalid_data("message_kind")),
        };

        Ok(result)
    }
}

impl V2Convertible for MetadataMessage {
    type Target = proto::MetadataMessage;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

impl RpcRequest for MetadataMessage {
    type ResponseMessage = MetadataMessage;
}

define_message! {
    @message = MetadataMessage,
    @target = TargetName::MetadataManager,
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
    type Error = ConversionError;

    fn try_from(value: crate::protobuf::common::MetadataKind) -> Result<Self, Self::Error> {
        match value {
            crate::protobuf::common::MetadataKind::Unknown => {
                Err(ConversionError::invalid_data("kind"))
            }
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
}
