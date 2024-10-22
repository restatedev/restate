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
use strum::EnumIter;

use crate::logs::metadata::Logs;
use crate::net::define_message;
use crate::net::TargetName;
use crate::nodes_config::NodesConfiguration;
use crate::partition_table::PartitionTable;
use crate::schema::Schema;
use crate::Version;
use crate::Versioned;

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
    PartitionTable(PartitionTable),
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
