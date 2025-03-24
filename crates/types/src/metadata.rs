// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;

use crate::{Version, flexbuffers_storage_encode_decode};

#[derive(derive_more::Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VersionedValue {
    pub version: Version,
    #[debug(skip)]
    pub value: Bytes,
}

impl VersionedValue {
    pub fn new(version: Version, value: Bytes) -> Self {
        Self { version, value }
    }
}

flexbuffers_storage_encode_decode!(VersionedValue);

/// Preconditions for the write operations of the [`MetadataStore`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_more::Display)]
pub enum Precondition {
    /// No precondition
    None,
    /// Key-value pair must not exist for the write operation to succeed.
    DoesNotExist,
    /// Key-value pair must have the provided [`Version`] for the write operation to succeed.
    MatchesVersion(Version),
}
