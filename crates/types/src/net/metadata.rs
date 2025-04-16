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
use enum_map::Enum;
use prost_dto::{FromProst, IntoProst};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use strum::EnumIter;

use crate::Version;
use crate::Versioned;
use crate::logs::metadata::Logs;
use crate::metadata::GlobalMetadata;
use crate::net::TargetName;
use crate::net::define_message;
use crate::nodes_config::NodesConfiguration;
use crate::partition_table::PartitionTable;
use crate::schema::Schema;

use super::RpcRequest;

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
