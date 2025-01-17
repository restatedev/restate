// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod grpc;
mod grpc_svc;
pub mod local;

use bytestring::ByteString;
use restate_core::metadata_store::VersionedValue;
pub use restate_core::metadata_store::{
    MetadataStoreClient, Precondition, ReadError, ReadModifyWriteError, WriteError,
};
use restate_core::ShutdownError;
use restate_types::errors::GenericError;
use restate_types::storage::{StorageDecodeError, StorageEncodeError};
use restate_types::Version;
use tokio::sync::{mpsc, oneshot};

pub type BoxedMetadataStoreService = Box<dyn MetadataStoreService>;

pub type RequestSender = mpsc::Sender<MetadataStoreRequest>;
pub type RequestReceiver = mpsc::Receiver<MetadataStoreRequest>;

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error("storage error: {0}")]
    Storage(#[from] GenericError),
    #[error("failed precondition: {0}")]
    FailedPrecondition(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("encode error: {0}")]
    Encode(#[from] StorageEncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] StorageDecodeError),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("system is shutting down")]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Generic(#[from] GenericError),
}

#[async_trait::async_trait]
pub trait MetadataStoreServiceBoxed {
    async fn run_boxed(self: Box<Self>) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl<T: MetadataStoreService> MetadataStoreServiceBoxed for T {
    async fn run_boxed(self: Box<Self>) -> Result<(), Error> {
        (*self).run().await
    }
}

#[async_trait::async_trait]
pub trait MetadataStoreService: MetadataStoreServiceBoxed + Send {
    async fn run(self) -> Result<(), Error>;

    fn boxed(self) -> BoxedMetadataStoreService
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

#[async_trait::async_trait]
impl<T: MetadataStoreService + ?Sized> MetadataStoreService for Box<T> {
    async fn run(self) -> Result<(), Error> {
        self.run_boxed().await
    }
}

#[derive(Debug)]
pub enum MetadataStoreRequest {
    Get {
        key: ByteString,
        result_tx: oneshot::Sender<Result<Option<VersionedValue>, RequestError>>,
    },
    GetVersion {
        key: ByteString,
        result_tx: oneshot::Sender<Result<Option<Version>, RequestError>>,
    },
    Put {
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
    Delete {
        key: ByteString,
        precondition: Precondition,
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
}

impl RequestError {
    fn kv_pair_exists() -> Self {
        RequestError::FailedPrecondition("key-value pair already exists".to_owned())
    }

    fn version_mismatch(expected: Version, actual: Option<Version>) -> Self {
        RequestError::FailedPrecondition(format!(
            "Expected version '{expected}' but found version '{actual:?}'"
        ))
    }
}
