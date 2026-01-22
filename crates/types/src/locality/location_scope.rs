// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// [`LocationScope`] specifies the location of a node in the cluster. The location
/// is expressed by a set of hierarchical scopes. Restate assumes the cluster topology
/// to be a tree-like structure.
#[derive(
    Debug,
    Copy,
    Clone,
    Hash,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    strum::EnumIter,
    strum::Display,
    strum::EnumString,
    serde::Serialize,
    serde::Deserialize,
    bilrost::Enumeration,
)]
#[serde(rename_all = "kebab-case")]
#[strum(ascii_case_insensitive)]
#[repr(u8)]
pub enum LocationScope {
    /// Special; Indicating the smallest scope (an individual node)
    Node = 0,

    // Actual scopes representing the location of a node
    Zone = 1,
    Region = 2,

    // Special; Includes all lower-level scopes.
    #[strum(disabled)]
    Root = 3,
}

impl LocationScope {
    /// Returns None if self is already the largest scope, i.e. `Root`
    pub const fn next_greater_scope(self) -> Option<Self> {
        // we know `self + 1` won't overflow
        let next = (self as u8) + 1;
        Self::from_u8(next)
    }

    /// Returns None if self is already the smallest scope, i.e. `Node`
    pub const fn next_smaller_scope(self) -> Option<Self> {
        if matches!(self, Self::Node) {
            None
        } else {
            Self::from_u8(self as u8 - 1)
        }
    }

    pub const fn is_special(self) -> bool {
        matches!(self, LocationScope::Root | LocationScope::Node)
    }

    // Returns the number of non-special scopes.
    pub const fn num_scopes() -> usize {
        LocationScope::Root as usize - 1
    }

    #[inline]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Node),
            1 => Some(Self::Zone),
            2 => Some(Self::Region),
            3 => Some(Self::Root),
            _ => None,
        }
    }
}
