// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod providers;
mod test_util;

#[cfg(any(test, feature = "test-util"))]
use crate::metadata_store::test_util::InMemoryMetadataStore;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use restate_types::errors::GenericError;
use restate_types::retries::RetryPolicy;
use restate_types::storage::{StorageCodec, StorageDecode, StorageEncode};
use restate_types::{flexbuffers_storage_encode_decode, Version, Versioned};
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("network error: {0}")]
    Network(GenericError),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("codec error: {0}")]
    Codec(GenericError),
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    #[error("network error: {0}")]
    Network(GenericError),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("codec error: {0}")]
    Codec(GenericError),
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

flexbuffers_storage_encode_decode!(VersionedValue);

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
pub struct MetadataStoreClient {
    // premature optimization? Maybe introduce trait object once we have multiple implementations?
    inner: Arc<dyn MetadataStore + Send + Sync>,
    backoff_policy: Option<RetryPolicy>,
}

impl MetadataStoreClient {
    pub fn new<S>(metadata_store: S, backoff_policy: Option<RetryPolicy>) -> Self
    where
        S: MetadataStore + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(metadata_store),
            backoff_policy,
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn new_in_memory() -> Self {
        MetadataStoreClient::new(
            InMemoryMetadataStore::default(),
            None, // not needed since no concurrent modifications happening
        )
    }

    /// Gets the value and its current version for the given key. If key-value pair is not present,
    /// then return [`None`].
    pub async fn get<T: Versioned + StorageDecode>(
        &self,
        key: ByteString,
    ) -> Result<Option<T>, ReadError> {
        let value = self.inner.get(key).await?;

        if let Some(mut versioned_value) = value {
            let value = StorageCodec::decode::<T, _>(&mut versioned_value.value)
                .map_err(|err| ReadError::Codec(err.into()))?;

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

    /// Gets the current version for the given key. If key-value pair is not present, then return
    /// [`None`].
    pub async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        self.inner.get_version(key).await
    }

    /// Puts the versioned value under the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    pub async fn put<T>(
        &self,
        key: ByteString,
        value: &T,
        precondition: Precondition,
    ) -> Result<(), WriteError>
    where
        T: Versioned + StorageEncode,
    {
        let version = value.version();

        let mut buf = BytesMut::default();
        StorageCodec::encode(value, &mut buf).map_err(|err| WriteError::Codec(err.into()))?;

        self.inner
            .put(
                key,
                VersionedValue::new(version, buf.freeze()),
                precondition,
            )
            .await
    }

    /// Deletes the key-value pair for the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    pub async fn delete(
        &self,
        key: ByteString,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        self.inner.delete(key, precondition).await
    }

    /// Gets the value under the specified key or inserts a new value if it is not present into the
    /// metadata store.
    ///
    /// This method won't overwrite an existing value that is stored in the metadata store.
    pub async fn get_or_insert<T, F>(
        &self,
        key: ByteString,
        mut init: F,
    ) -> Result<T, ReadWriteError>
    where
        T: Versioned + StorageEncode + StorageDecode,
        F: FnMut() -> T,
    {
        let mut backoff_policy = self.backoff_policy.as_ref().map(|p| p.iter());

        loop {
            let value = self.get::<T>(key.clone()).await?;

            if let Some(value) = value {
                return Ok(value);
            } else {
                let init_value = init();
                let precondition = Precondition::DoesNotExist;

                match self.put(key.clone(), &init_value, precondition).await {
                    Ok(()) => return Ok(init_value),
                    Err(WriteError::FailedPrecondition(msg)) => {
                        if let Some(backoff) = backoff_policy.as_mut().and_then(|p| p.next()) {
                            debug!(
                                "Concurrent value update: {msg}; retrying in '{}'",
                                humantime::format_duration(backoff)
                            );
                            tokio::time::sleep(backoff).await;
                        } else {
                            return Err(ReadWriteError::RetriesExhausted(key));
                        }
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }
    }

    /// Reads the value under the given key from the metadata store, then modifies it and writes
    /// the result back to the metadata store. The write only succeeds if the stored value has not
    /// been modified in the meantime. If this should happen, then the read-modify-write cycle is
    /// retried.
    pub async fn read_modify_write<T, F, E>(
        &self,
        key: ByteString,
        mut modify: F,
    ) -> Result<T, ReadModifyWriteError<E>>
    where
        T: Versioned + StorageEncode + StorageDecode,
        F: FnMut(Option<T>) -> Result<T, E>,
    {
        let mut backoff_policy = self.backoff_policy.as_ref().map(|p| p.iter());

        loop {
            let old_value = self
                .get::<T>(key.clone())
                .await
                .map_err(ReadWriteError::from)?;

            let precondition = old_value
                .as_ref()
                .map(|c| Precondition::MatchesVersion(c.version()))
                .unwrap_or(Precondition::DoesNotExist);

            let result = modify(old_value);

            match result {
                Ok(new_value) => match self.put(key.clone(), &new_value, precondition).await {
                    Ok(()) => return Ok(new_value),
                    Err(WriteError::FailedPrecondition(msg)) => {
                        if let Some(backoff) = backoff_policy.as_mut().and_then(|p| p.next()) {
                            debug!(
                                "Concurrent value update: {msg}; retrying in '{}'",
                                humantime::format_duration(backoff)
                            );
                            tokio::time::sleep(backoff).await;
                        } else {
                            return Err(ReadWriteError::RetriesExhausted(key).into());
                        }
                    }
                    Err(err) => return Err(ReadModifyWriteError::ReadWrite(err.into())),
                },
                Err(err) => return Err(ReadModifyWriteError::FailedOperation(err)),
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadWriteError {
    #[error("network error: {0}")]
    Network(GenericError),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("codec error: {0}")]
    Codec(GenericError),
    #[error("retries for operation on key '{0}' exhausted")]
    RetriesExhausted(ByteString),
}

#[derive(Debug, thiserror::Error)]
pub enum ReadModifyWriteError<E = String> {
    #[error(transparent)]
    ReadWrite(#[from] ReadWriteError),
    #[error("failed read-modify-write operation: {0}")]
    FailedOperation(E),
}

impl<E> ReadModifyWriteError<E>
where
    E: From<ReadWriteError>,
{
    pub fn transpose(self) -> E {
        match self {
            ReadModifyWriteError::ReadWrite(err) => err.into(),
            ReadModifyWriteError::FailedOperation(err) => err,
        }
    }
}

impl From<ReadError> for ReadWriteError {
    fn from(value: ReadError) -> Self {
        match value {
            ReadError::Network(err) => ReadWriteError::Network(err),
            ReadError::Internal(msg) => ReadWriteError::Internal(msg),
            ReadError::Codec(err) => ReadWriteError::Codec(err),
        }
    }
}

impl From<WriteError> for ReadWriteError {
    fn from(value: WriteError) -> Self {
        match value {
            WriteError::FailedPrecondition(_) => {
                unreachable!("failed preconditions should be treated separately")
            }
            WriteError::Network(err) => ReadWriteError::Network(err),
            WriteError::Internal(msg) => ReadWriteError::Internal(msg),
            WriteError::Codec(err) => ReadWriteError::Codec(err),
        }
    }
}

pub trait MetadataStoreClientError {
    fn is_network_error(&self) -> bool;
}

impl<E> MetadataStoreClientError for ReadModifyWriteError<E> {
    fn is_network_error(&self) -> bool {
        match self {
            ReadModifyWriteError::ReadWrite(err) => err.is_network_error(),
            ReadModifyWriteError::FailedOperation(_) => false,
        }
    }
}

impl MetadataStoreClientError for ReadWriteError {
    fn is_network_error(&self) -> bool {
        match self {
            ReadWriteError::Network(_) => true,
            ReadWriteError::Internal(_) => false,
            ReadWriteError::Codec(_) => false,
            ReadWriteError::RetriesExhausted(_) => false,
        }
    }
}

impl MetadataStoreClientError for WriteError {
    fn is_network_error(&self) -> bool {
        match self {
            WriteError::FailedPrecondition(_) => false,
            WriteError::Network(_) => true,
            WriteError::Internal(_) => false,
            WriteError::Codec(_) => false,
        }
    }
}

static_assertions::assert_impl_all!(MetadataStoreClient: Send, Sync, Clone);
