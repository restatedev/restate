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
use strum_macros::EnumIter;
/// A type used for versioned metadata.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
    derive_more::AddAssign,
)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[display(fmt = "v{}", _0)]
pub struct Version(u32);

impl Version {
    pub const INVALID: Version = Version(0);
    pub const MIN: Version = Version(1);
}

impl Default for Version {
    fn default() -> Self {
        Self::MIN
    }
}

/// The kind of versioned metadata that can be synchronized across nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Enum, EnumIter)]
pub enum MetadataKind {
    NodesConfiguration,
    Schema,
    PartitionTable,
    Logs,
}
