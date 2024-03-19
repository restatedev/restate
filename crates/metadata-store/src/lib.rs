// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

mod grpc_svc;
mod local;

use async_trait::async_trait;
use bytes::Bytes;
use bytestring::ByteString;
use restate_types::errors::GenericError;
use restate_types::{Version, Versioned};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("network error: {0}")]
    Network(GenericError),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("deserialization failed: {0}")]
    Deserialization(GenericError),
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    #[error("network error: {0}")]
    Network(GenericError),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("serialization failed: {0}")]
    Serialization(GenericError),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VersionedValue {
    pub version: Version,
    pub value: Bytes,
}

impl VersionedValue {
    pub fn new(version: Version, value: Bytes) -> Self {
        Self { version, value }
    }
}

/// Preconditions for the write operations of the [`MetadataStore`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Precondition {
    /// No precondition
    None,
    /// Key-value pair must not exist for the write operation to succeed.
    DoesNotExist,
    /// Key-value pair must have the provided [`Version`] for the write operation to succeed.
    MatchesVersion(Version),
}

/// Metadata store abstraction. The metadata store implementations need to support linearizable
/// reads and atomic compare and swap operations.
#[async_trait]
pub trait MetadataStore {
    /// Gets the value and its current version for the given key. If key-value pair is not present,
    /// then return [`None`].
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError>;

    /// Gets the current version for the given key. If key-value pair is not present, then return
    /// [`None`].
    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError>;

    /// Puts the versioned value under the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError>;

    /// Deletes the key-value pair for the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError>;
}

/// Metadata store client which allows storing [`Versioned`] values into a [`MetadataStore`].
#[derive(Clone)]
struct MetadataStoreClient {
    // premature optimization? Maybe introduce trait object once we have multiple implementations?
    inner: Arc<dyn MetadataStore + Send + Sync>,
}

impl MetadataStoreClient {
    fn new<S>(metadata_store: S) -> Self
    where
        S: MetadataStore + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(metadata_store),
        }
    }

    async fn get<T: Versioned + DeserializeOwned>(
        &self,
        key: ByteString,
    ) -> Result<Option<T>, ReadError> {
        let value = self.inner.get(key).await?;

        if let Some(versioned_value) = value {
            // todo add proper format version
            let (value, _) = bincode::serde::decode_from_slice::<T, _>(
                versioned_value.value.as_ref(),
                bincode::config::standard(),
            )
            .map_err(|err| ReadError::Deserialization(err.into()))?;

            assert_eq!(
                versioned_value.version,
                value.version(),
                "versions must align"
            );

            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        self.inner.get_version(key).await
    }

    async fn put<T>(
        &self,
        key: ByteString,
        value: T,
        precondition: Precondition,
    ) -> Result<(), WriteError>
    where
        T: Versioned + Serialize,
    {
        let version = value.version();

        // todo add proper format version
        let value = bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|err| WriteError::Serialization(err.into()))?;

        self.inner
            .put(
                key,
                VersionedValue::new(version, value.into()),
                precondition,
            )
            .await
    }

    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        self.inner.delete(key, precondition).await
    }
}

static_assertions::assert_impl_all!(MetadataStoreClient: Send, Sync, Clone);
