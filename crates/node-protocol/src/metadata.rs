// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use enum_map::Enum;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::FixedPartitionTable;
use serde::{Deserialize, Serialize};
use strum_macros::EnumIter;

use crate::codec::{Targeted, WireSerde};
use crate::common::ProtocolVersion;
use crate::common::TargetName;
use crate::CodecError;

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    derive_more::From,
    strum_macros::EnumIs,
    strum_macros::IntoStaticStr,
)]
pub enum MetadataMessage {
    GetMetadataRequest(GetMetadataRequest),
    MetadataUpdate(MetadataUpdate),
}

impl Targeted for MetadataMessage {
    const TARGET: TargetName = TargetName::MetadataManager;

    fn kind(&self) -> &'static str {
        self.into()
    }
}

impl WireSerde for MetadataMessage {
    fn encode(&self, _protocol_version: ProtocolVersion) -> Result<Bytes, CodecError> {
        // serialize message to bytes
        Ok(bincode::serde::encode_to_vec(self, bincode::config::standard())?.into())
    }

    fn decode(payload: Bytes, _protocol_version: ProtocolVersion) -> Result<Self, CodecError> {
        let (output, _) = bincode::serde::decode_from_slice(&payload, bincode::config::standard())?;
        Ok(output)
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
    strum_macros::Display,
)]
pub enum MetadataKind {
    NodesConfiguration,
    Schema,
    PartitionTable,
    Logs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataContainer {
    NodesConfiguration(NodesConfiguration),
    PartitionTable(FixedPartitionTable),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMetadataRequest {
    pub metadata_kind: MetadataKind,
    pub min_version: Option<restate_types::Version>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataUpdate {
    pub container: MetadataContainer,
}

impl MetadataContainer {
    pub fn kind(&self) -> MetadataKind {
        match self {
            MetadataContainer::NodesConfiguration(_) => MetadataKind::NodesConfiguration,
            MetadataContainer::PartitionTable(_) => MetadataKind::PartitionTable,
        }
    }
}

impl From<NodesConfiguration> for MetadataContainer {
    fn from(value: NodesConfiguration) -> Self {
        MetadataContainer::NodesConfiguration(value)
    }
}

impl From<FixedPartitionTable> for MetadataContainer {
    fn from(value: FixedPartitionTable) -> Self {
        MetadataContainer::PartitionTable(value)
    }
}
