// Copyright (c) 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use bytes::Bytes;
use prost::{DecodeError, Message};

use crate::errors::ConversionError;
use crate::protobuf::common::MetadataServerStatus;
use crate::protobuf::metadata as protobuf;
use crate::{flexbuffers_storage_encode_decode, PlainNodeId, Version};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VersionedValue {
    pub version: Version,
    pub value: Bytes,
}

impl VersionedValue {
    pub fn new(version: Version, value: Bytes) -> Self {
        Self { version, value }
    }
}

flexbuffers_storage_encode_decode!(VersionedValue);

impl From<VersionedValue> for protobuf::VersionedValue {
    fn from(value: VersionedValue) -> protobuf::VersionedValue {
        protobuf::VersionedValue {
            version: Some(value.version.into()),
            bytes: value.value,
        }
    }
}

impl TryFrom<protobuf::VersionedValue> for VersionedValue {
    type Error = ConversionError;

    fn try_from(value: protobuf::VersionedValue) -> Result<Self, Self::Error> {
        let version = value
            .version
            .ok_or_else(|| ConversionError::missing_field("version"))?;
        Ok(VersionedValue::new(version.into(), value.bytes))
    }
}

/// Preconditions for the write operations of the [`MetadataStore`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Precondition {
    /// No precondition
    None,
    /// Key-value pair must not exist for the write operation to succeed.
    DoesNotExist,
    /// Key-value pair must have the provided [`Version`] for the write operation to succeed.
    MatchesVersion(Version),
}

impl From<Precondition> for protobuf::Precondition {
    fn from(value: Precondition) -> Self {
        match value {
            Precondition::None => protobuf::Precondition {
                kind: protobuf::PreconditionKind::None.into(),
                version: None,
            },
            Precondition::DoesNotExist => protobuf::Precondition {
                kind: protobuf::PreconditionKind::DoesNotExist.into(),
                version: None,
            },
            Precondition::MatchesVersion(version) => protobuf::Precondition {
                kind: protobuf::PreconditionKind::MatchesVersion.into(),
                version: Some(version.into()),
            },
        }
    }
}

impl TryFrom<protobuf::Precondition> for Precondition {
    type Error = ConversionError;

    fn try_from(value: protobuf::Precondition) -> Result<Self, Self::Error> {
        match value.kind() {
            protobuf::PreconditionKind::Unknown => {
                Err(ConversionError::invalid_data("unknown precondition kind"))
            }
            protobuf::PreconditionKind::None => Ok(Precondition::None),
            protobuf::PreconditionKind::DoesNotExist => Ok(Precondition::DoesNotExist),
            protobuf::PreconditionKind::MatchesVersion => Ok(Precondition::MatchesVersion(
                value
                    .version
                    .ok_or_else(|| ConversionError::missing_field("version"))?
                    .into(),
            )),
        }
    }
}

/// Identifier to detect the loss of a disk.
pub type StorageId = u64;

#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    prost_dto::IntoProst,
    prost_dto::FromProst,
)]
#[prost(target = "crate::protobuf::metadata::MemberId")]
pub struct MemberId {
    pub node_id: PlainNodeId,
    pub storage_id: StorageId,
}

impl MemberId {
    pub fn new(node_id: PlainNodeId, storage_id: StorageId) -> Self {
        MemberId {
            node_id,
            storage_id,
        }
    }
}

impl Display for MemberId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:x}", self.node_id, self.storage_id & 0x0ffff)
    }
}

/// Status summary of the metadata store.
#[derive(Clone, Debug, Default)]
pub enum MetadataStoreSummary {
    #[default]
    Starting,
    Provisioning,
    Standby,
    Member {
        leader: Option<PlainNodeId>,
        configuration: MetadataServerConfiguration,
        raft: RaftSummary,
        snapshot: Option<SnapshotSummary>,
    },
}

impl From<MetadataStoreSummary> for protobuf::MetadataStoreSummary {
    fn from(value: MetadataStoreSummary) -> Self {
        match value {
            MetadataStoreSummary::Starting => protobuf::MetadataStoreSummary {
                status: MetadataServerStatus::StartingUp.into(),
                configuration: None,
                leader: None,
                raft: None,
                snapshot: None,
            },
            MetadataStoreSummary::Provisioning => protobuf::MetadataStoreSummary {
                status: MetadataServerStatus::AwaitingProvisioning.into(),
                configuration: None,
                leader: None,
                raft: None,
                snapshot: None,
            },
            MetadataStoreSummary::Standby => protobuf::MetadataStoreSummary {
                status: MetadataServerStatus::Standby.into(),
                configuration: None,
                leader: None,
                raft: None,
                snapshot: None,
            },
            MetadataStoreSummary::Member {
                configuration,
                leader,
                raft,
                snapshot,
            } => protobuf::MetadataStoreSummary {
                status: MetadataServerStatus::Member.into(),
                configuration: Some(protobuf::MetadataServerConfiguration::from(configuration)),
                leader: leader.map(u32::from),
                raft: Some(protobuf::RaftSummary::from(raft)),
                snapshot: snapshot.map(protobuf::SnapshotSummary::from),
            },
        }
    }
}

#[derive(Clone, Debug, prost_dto::IntoProst, prost_dto::FromProst)]
#[prost(target = "crate::protobuf::metadata::RaftSummary")]
pub struct RaftSummary {
    pub term: u64,
    pub committed: u64,
    pub applied: u64,
    pub first_index: u64,
    pub last_index: u64,
}

#[derive(Clone, Debug, prost_dto::IntoProst, prost_dto::FromProst)]
#[prost(target = "crate::protobuf::metadata::SnapshotSummary")]
pub struct SnapshotSummary {
    pub index: u64,
    // size in bytes
    pub size: u64,
}

#[derive(Clone, Debug, prost_dto::IntoProst, prost_dto::FromProst)]
#[prost(target = "crate::protobuf::metadata::MetadataServerConfiguration")]
pub struct MetadataServerConfiguration {
    #[prost(required)]
    pub version: Version,
    pub members: HashMap<PlainNodeId, StorageId>,
}

impl Default for MetadataServerConfiguration {
    fn default() -> Self {
        MetadataServerConfiguration {
            version: Version::INVALID,
            members: HashMap::default(),
        }
    }
}

impl MetadataServerConfiguration {
    pub fn encode_to_vec(self) -> Vec<u8> {
        protobuf::MetadataServerConfiguration::from(self).encode_to_vec()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        protobuf::MetadataServerConfiguration::decode(buf).map(Self::from)
    }
}
