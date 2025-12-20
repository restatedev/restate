// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use bytestring::ByteString;
use metrics::{counter, histogram};
use tokio::time::Instant;
use tracing::{debug, warn};

use restate_time_util::DurationExt;
use restate_types::errors::{
    BoxedMaybeRetryableError, GenericError, IntoMaybeRetryable, MaybeRetryableError,
};
use restate_types::metadata::{GlobalMetadata, Precondition, VersionedValue};
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::retries::RetryPolicy;
use restate_types::schema::Schema;
use restate_types::storage::{StorageCodec, StorageDecode, StorageEncode, StorageEncodeError};
use restate_types::{Version, Versioned};

use crate::metric_definitions::{
    METADATA_CLIENT_GET_DURATION, METADATA_CLIENT_GET_TOTAL, METADATA_CLIENT_GET_VERSION_DURATION,
    METADATA_CLIENT_GET_VERSION_TOTAL, METADATA_CLIENT_PUT_DURATION, METADATA_CLIENT_PUT_TOTAL,
    STATUS_COMPLETED, STATUS_FAILED, describe_metrics,
};
#[cfg(feature = "test-util")]
use crate::test_util::InMemoryMetadataStore;

const METADATA_SIZE_WARNING: usize = 4 * 1024 * 1024; //4MB

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("codec error: {0}")]
    Codec(GenericError),
    #[error("other error: {0}")]
    Other(BoxedMaybeRetryableError),
}

impl ReadError {
    pub fn retryable<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Box::new(error.into_retryable()))
    }

    pub fn terminal<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Box::new(error.into_terminal()))
    }

    pub fn other<E: MaybeRetryableError + Send + Sync>(error: E) -> Self {
        Self::Other(Box::new(error))
    }
}

impl MaybeRetryableError for ReadError {
    fn retryable(&self) -> bool {
        match self {
            ReadError::Other(err) => err.retryable(),
            ReadError::Codec(_) => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    #[error("other error: {0}")]
    Other(BoxedMaybeRetryableError),
    #[error("codec error: {0}")]
    Codec(GenericError),
}

impl WriteError {
    pub fn retryable<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Box::new(error.into_retryable()))
    }

    pub fn terminal<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Box::new(error.into_terminal()))
    }

    pub fn other<E: MaybeRetryableError + Send + Sync>(error: E) -> Self {
        Self::Other(Box::new(error))
    }
}

impl MaybeRetryableError for WriteError {
    fn retryable(&self) -> bool {
        match self {
            WriteError::Other(err) => err.retryable(),
            WriteError::Codec(_) => false,
            WriteError::FailedPrecondition(_) => false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    #[error("other error: {0}")]
    Other(BoxedMaybeRetryableError),
    #[error("codec error: {0}")]
    Codec(GenericError),
    #[error("provisioning is not supported: {0}")]
    NotSupported(String),
}

impl ProvisionError {
    pub fn retryable<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Box::new(error.into_retryable()))
    }

    pub fn terminal<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Box::new(error.into_terminal()))
    }

    pub fn other<E: MaybeRetryableError + Send + Sync>(error: E) -> Self {
        Self::Other(Box::new(error))
    }
}

impl MaybeRetryableError for ProvisionError {
    fn retryable(&self) -> bool {
        match self {
            ProvisionError::Other(err) => err.retryable(),
            ProvisionError::Codec(_) => false,
            ProvisionError::NotSupported(_) => false,
        }
    }
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

    /// Tries to provision the metadata store with the provided [`NodesConfiguration`]. Returns
    /// `true` if the metadata store was newly provisioned. Returns `false` if the metadata store
    /// is already provisioned.
    ///
    /// # Important
    /// The implementation should be able to handle repeated calls as provisioning is an idempotent
    /// operation.
    async fn provision(
        &self,
        nodes_configuration: &NodesConfiguration,
    ) -> Result<bool, ProvisionError>;
}

/// A provisioned metadata store does not need to be explicitly provisioned. Therefore, a provision
/// call is translated into a put command.
#[async_trait]
pub trait ProvisionedMetadataStore {
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

#[async_trait]
impl<T: ProvisionedMetadataStore + Sync> MetadataStore for T {
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        self.get(key).await
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        self.get_version(key).await
    }

    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        self.put(key, value, precondition).await
    }

    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        self.delete(key, precondition).await
    }

    async fn provision(
        &self,
        nodes_configuration: &NodesConfiguration,
    ) -> Result<bool, ProvisionError> {
        let versioned_value = serialize_value(nodes_configuration)
            .map_err(|err| ProvisionError::Codec(err.into()))?;
        match self
            .put(
                NODES_CONFIG_KEY.clone(),
                versioned_value,
                Precondition::DoesNotExist,
            )
            .await
        {
            Ok(()) => Ok(true),
            Err(err) => match err {
                WriteError::FailedPrecondition(_) => Ok(false),
                WriteError::Other(err) => Err(ProvisionError::Other(err)),
                WriteError::Codec(err) => Err(ProvisionError::Codec(err)),
            },
        }
    }
}

/// Metadata store client which allows storing [`Versioned`] values into a [`MetadataStore`].
#[derive(Clone)]
pub struct MetadataStoreClient {
    inner: Arc<dyn MetadataStore + Send + Sync>,
    backoff_policy: Option<RetryPolicy>,
}

impl MetadataStoreClient {
    pub fn new<S>(metadata_store: S, backoff_policy: Option<RetryPolicy>) -> Self
    where
        S: MetadataStore + Send + Sync + 'static,
    {
        describe_metrics();
        Self {
            inner: Arc::new(metadata_store),
            backoff_policy,
        }
    }

    pub fn inner(&self) -> Arc<dyn MetadataStore + Send + Sync> {
        Arc::clone(&self.inner)
    }

    #[cfg(feature = "test-util")]
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
        let start_time = Instant::now();
        let result = {
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
        };

        let status = if result.is_ok() {
            STATUS_COMPLETED
        } else {
            STATUS_FAILED
        };

        histogram!(METADATA_CLIENT_GET_DURATION).record(start_time.elapsed());
        counter!(METADATA_CLIENT_GET_TOTAL, "status" => status).increment(1);

        result
    }

    /// Gets the current version for the given key. If key-value pair is not present, then return
    /// [`None`].
    pub async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let start_time = Instant::now();
        let result = self.inner.get_version(key).await;

        let status = if result.is_ok() {
            STATUS_COMPLETED
        } else {
            STATUS_FAILED
        };

        histogram!(METADATA_CLIENT_GET_VERSION_DURATION).record(start_time.elapsed());
        counter!(METADATA_CLIENT_GET_VERSION_TOTAL, "status" => status).increment(1);

        result
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
        let start_time = Instant::now();
        let result = {
            let versioned_value =
                serialize_value(value).map_err(|err| WriteError::Codec(err.into()))?;

            self.warn_if_oversized(&key, &versioned_value);
            self.inner.put(key, versioned_value, precondition).await
        };

        let status = if result.is_ok() {
            STATUS_COMPLETED
        } else {
            STATUS_FAILED
        };

        histogram!(METADATA_CLIENT_PUT_DURATION).record(start_time.elapsed());
        counter!(METADATA_CLIENT_PUT_TOTAL, "status" => status).increment(1);

        result
    }

    /// Deletes the key-value pair for the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    ///
    /// # Important
    /// Parts of the system expect that the version of versioned values increases monotonically
    /// (e.g. the metadata manager when exchanging metadata information, the raft server when
    /// rejecting write requests early). Therefore, you should only delete key-value pairs if you
    /// are certain that these values won't be used in the future ever again!
    #[cfg(any(test, feature = "test-util"))]
    pub async fn delete(
        &self,
        key: ByteString,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        let start_time = Instant::now();
        let result = self.inner.delete(key, precondition).await;

        let status = if result.is_ok() {
            STATUS_COMPLETED
        } else {
            STATUS_FAILED
        };

        histogram!(crate::metric_definitions::METADATA_CLIENT_DELETE_DURATION)
            .record(start_time.elapsed());
        counter!(crate::metric_definitions::METADATA_CLIENT_DELETE_TOTAL, "status" => status)
            .increment(1);

        result
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
                                backoff.friendly()
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
                                backoff.friendly()
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

    pub async fn provision(
        &self,
        nodes_configuration: &NodesConfiguration,
    ) -> Result<bool, ProvisionError> {
        self.inner.provision(nodes_configuration).await
    }

    fn warn_if_oversized(&self, key: &ByteString, value: &VersionedValue) {
        let size = value.value.len();

        if size < METADATA_SIZE_WARNING {
            return;
        }

        match &key[..] {
            Schema::KEY => {
                warn!(
                    "Schema metadata is {size} bytes (soft limit {METADATA_SIZE_WARNING}). \
                    Remove unused deployments or services to keep metadata manageable."
                );
            }
            NodesConfiguration::KEY => {
                warn!(
                    "Nodes metadata is {size} bytes (soft limit {METADATA_SIZE_WARNING}). \
                    Remove dead nodes to keep metadata manageable."
                );
            }
            _ => {
                warn!(
                    "Metadata entry '{key}' is {size} bytes, above the soft limit of {METADATA_SIZE_WARNING} bytes."
                );
            }
        }
    }
}

pub fn serialize_value<T: Versioned + StorageEncode>(
    value: &T,
) -> Result<VersionedValue, StorageEncodeError> {
    let version = value.version();
    let mut buf = BytesMut::default();
    StorageCodec::encode(value, &mut buf)?;
    let versioned_value = VersionedValue::new(version, buf.freeze());
    Ok(versioned_value)
}

#[derive(Debug, thiserror::Error)]
pub enum ReadWriteError {
    #[error("other error: {0}")]
    Other(BoxedMaybeRetryableError),
    #[error("codec error: {0}")]
    Codec(GenericError),
    #[error("retries for operation on key '{0}' exhausted")]
    RetriesExhausted(ByteString),
}

impl MaybeRetryableError for ReadWriteError {
    fn retryable(&self) -> bool {
        match self {
            ReadWriteError::Other(err) => err.retryable(),
            ReadWriteError::Codec(_) => false,
            ReadWriteError::RetriesExhausted(_) => true,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadModifyWriteError<E = String> {
    #[error(transparent)]
    ReadWrite(#[from] ReadWriteError),
    #[error("failed read-modify-write operation: {0}")]
    FailedOperation(E),
}

impl<E> MaybeRetryableError for ReadModifyWriteError<E>
where
    E: std::fmt::Display + std::fmt::Debug + 'static,
{
    fn retryable(&self) -> bool {
        match self {
            ReadModifyWriteError::ReadWrite(err) => err.retryable(),
            ReadModifyWriteError::FailedOperation(_) => false,
        }
    }
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
            ReadError::Other(err) => ReadWriteError::Other(err),
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
            WriteError::Other(err) => ReadWriteError::Other(err),
            WriteError::Codec(err) => ReadWriteError::Codec(err),
        }
    }
}

static_assertions::assert_impl_all!(MetadataStoreClient: Send, Sync, Clone);

pub async fn retry_on_retryable_error<Fn, Fut, T, E, P>(
    retry_policy: P,
    mut action: Fn,
) -> Result<T, RetryError<E>>
where
    P: Into<RetryPolicy>,
    Fn: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: MaybeRetryableError,
{
    let retry_policy = retry_policy.into();
    let mut retry_iter = retry_policy.iter();

    loop {
        match action().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if err.retryable() {
                    if let Some(delay) = retry_iter.next() {
                        tokio::time::sleep(delay).await;
                    } else {
                        return Err(RetryError::RetriesExhausted(err));
                    }
                } else {
                    return Err(RetryError::NotRetryable(err));
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RetryError<E> {
    #[error("retries exhausted: {0}")]
    RetriesExhausted(E),
    #[error(transparent)]
    NotRetryable(E),
}

impl<E> RetryError<E> {
    pub fn into_inner(self) -> E {
        match self {
            RetryError::RetriesExhausted(err) => err,
            RetryError::NotRetryable(err) => err,
        }
    }

    pub fn map<F>(self, mapper: impl Fn(E) -> F) -> RetryError<F> {
        match self {
            RetryError::RetriesExhausted(err) => RetryError::RetriesExhausted(mapper(err)),
            RetryError::NotRetryable(err) => RetryError::NotRetryable(mapper(err)),
        }
    }
}
