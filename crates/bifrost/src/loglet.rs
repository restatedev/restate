// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Remove after fleshing the code out.
#![allow(dead_code)]

use enum_map::Enum;

use crate::Lsn;

/// An enum with the list of supported loglet providers.
/// For each variant we must have a corresponding implementation of the
/// [`crate::loglet::Loglet`] trait
#[derive(
    Debug,
    Clone,
    Hash,
    Eq,
    PartialEq,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    Enum,
    strum_macros::EnumIter,
)]
pub enum ProviderKind {
    /// A file-backed loglet.
    File,
    #[cfg(test)]
    Memory,
}

pub fn provider_default_config(_kind: ProviderKind) -> serde_json::Value {
    serde_json::Value::Null
}

// Inner loglet offset
#[derive(Debug, Clone, derive_more::From, derive_more::Display)]
pub struct LogletOffset(u64);

impl LogletOffset {
    /// Converts a loglet offset into the virtual address (LSN). The base_lsn is
    /// the starting address of the segment.
    pub fn into_lsn(self, base_lsn: Lsn) -> Lsn {
        // This assumes that this will not overflow. That's not guaranteed to always be the
        // case but it should be extremely rare that it'd be okay to just wrap in this case.
        Lsn(base_lsn.0.wrapping_add(self.0))
    }
}
