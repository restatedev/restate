// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;

use restate_ty::metadata::MetadataKind;
use restate_ty::storage::{StorageDecode, StorageEncode};

use crate::net::metadata::MetadataContainer;
use crate::{Version, Versioned, flexbuffers_storage_encode_decode};

/// A trait all metadata types managed by metadata manager.
pub trait GlobalMetadata: Versioned + StorageEncode + StorageDecode {
    /// The key for this metadata type in metadata store
    const KEY: &'static str;
    /// Returns the kind of metadata.
    const KIND: MetadataKind;

    /// Wrap into MetadataContainer
    fn into_container(self: Arc<Self>) -> MetadataContainer;

    /// Runs an optional validation step before replacing the previous value with the incoming
    /// (self) one.
    fn validate_update(&self, _previous: Option<&Arc<Self>>) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[derive(derive_more::Debug, Clone, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct VersionedValue {
    #[bilrost(1)]
    pub version: Version,
    #[debug(skip)]
    #[bilrost(2)]
    pub value: Bytes,
}

impl VersionedValue {
    pub fn new(version: Version, value: Bytes) -> Self {
        Self { version, value }
    }
}

flexbuffers_storage_encode_decode!(VersionedValue);

/// Preconditions for the write operations of the [`MetadataStore`].
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, derive_more::Display)]
pub enum Precondition {
    /// No precondition
    None,
    /// Key-value pair must not exist for the write operation to succeed.
    DoesNotExist,
    /// Key-value pair must have the provided [`Version`] for the write operation to succeed.
    MatchesVersion(Version),
}
