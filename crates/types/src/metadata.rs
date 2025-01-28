// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use bytes::Bytes;

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
        leader: Option<MemberId>,
        configuration: MetadataServerConfiguration,
        raft: RaftSummary,
        snapshot: Option<SnapshotSummary>,
    },
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
