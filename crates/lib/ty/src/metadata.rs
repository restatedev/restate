// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use prost_dto::{FromProst, IntoProst};
use serde::{Deserialize, Serialize};

use crate::errors::GenericError;

/// The kind of versioned metadata that can be synchronized across nodes.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    derive_more::Display,
    strum::EnumCount,
    IntoProst,
    FromProst,
)]
#[prost(target = "crate::protobuf::MetadataKind")]
pub enum MetadataKind {
    NodesConfiguration,
    Schema,
    PartitionTable,
    Logs,
}

// todo remove once prost_dto supports TryFromProst
impl TryFrom<crate::protobuf::MetadataKind> for MetadataKind {
    type Error = GenericError;

    fn try_from(value: crate::protobuf::MetadataKind) -> Result<Self, Self::Error> {
        match value {
            crate::protobuf::MetadataKind::Unknown => Err("unknown metadata kind".into()),
            crate::protobuf::MetadataKind::NodesConfiguration => {
                Ok(MetadataKind::NodesConfiguration)
            }
            crate::protobuf::MetadataKind::Schema => Ok(MetadataKind::Schema),
            crate::protobuf::MetadataKind::PartitionTable => Ok(MetadataKind::PartitionTable),
            crate::protobuf::MetadataKind::Logs => Ok(MetadataKind::Logs),
        }
    }
}
