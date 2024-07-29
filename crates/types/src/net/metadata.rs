// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enum_map::Enum;
use serde::{Deserialize, Serialize};
use strum_macros::EnumIter;

use crate::logs::metadata::Logs;
use crate::net::TargetName;
use crate::nodes_config::NodesConfiguration;
use crate::partition_table::FixedPartitionTable;
use crate::schema::Schema;

use crate::net::define_message;

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
    strum_macros::Display,
    strum_macros::EnumCount,
)]
pub enum MetadataKind {
    NodesConfiguration,
    Schema,
    PartitionTable,
    Logs,
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum MetadataContainer {
    NodesConfiguration(NodesConfiguration),
    PartitionTable(FixedPartitionTable),
    Logs(Logs),
    Schema(Schema),
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
    pub fn kind(&self) -> MetadataKind {
        match self {
            MetadataContainer::NodesConfiguration(_) => MetadataKind::NodesConfiguration,
            MetadataContainer::PartitionTable(_) => MetadataKind::PartitionTable,
            MetadataContainer::Logs(_) => MetadataKind::Logs,
            MetadataContainer::Schema(_) => MetadataKind::Schema,
        }
    }
}
