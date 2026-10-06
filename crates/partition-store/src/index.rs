// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Identifies a secondary index in persisted keys.
pub trait SecondaryIndex {
    const INDEX_ID: IndexId;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[allow(clippy::enum_variant_names)]
pub enum IndexId {
    // InvocationByStatus = 1,
}

impl IndexId {
    pub const fn as_u32(self) -> u32 {
        match self {
            // IndexId::InvocationByStatus => 1,
        }
    }
    pub const fn from_u32(_value: u32) -> Option<Self> {
        // No secondary indexes are registered yet.
        None
    }
}
