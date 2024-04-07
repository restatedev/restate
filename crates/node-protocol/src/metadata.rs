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
pub use restate_schema::{SchemaInformation, UpdatingSchemaInformation};
use restate_types::logs::metadata::Logs;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::FixedPartitionTable;
use serde::{Deserialize, Serialize};
use strum_macros::EnumIter;

use crate::codec::{decode_default, encode_default, Targeted, WireSerde};
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
    fn encode(&self, protocol_version: ProtocolVersion) -> Result<Bytes, CodecError> {
        encode_default(self, protocol_version)
    }

    fn decode(payload: Bytes, protocol_version: ProtocolVersion) -> Result<Self, CodecError> {
        decode_default(payload, protocol_version)
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
    Schemas,
    PartitionTable,
    Logs,
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum MetadataContainer {
    NodesConfiguration(NodesConfiguration),
    PartitionTable(FixedPartitionTable),
    Logs(Logs),
    SchemaRegistry(SchemaInformation),
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
            MetadataContainer::Logs(_) => MetadataKind::Logs,
            MetadataContainer::SchemaRegistry(_) => MetadataKind::Schemas,
        }
    }
}
