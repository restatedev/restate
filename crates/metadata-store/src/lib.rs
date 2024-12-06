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
mod kv_memory_storage;
pub mod local;
mod network;
pub mod omnipaxos;
pub mod raft;
mod util;

use assert2::let_assert;
use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use restate_core::metadata_store::VersionedValue;
pub use restate_core::metadata_store::{
    MetadataStoreClient, Precondition, ReadError, ReadModifyWriteError, WriteError,
};
use restate_core::ShutdownError;
use restate_types::errors::GenericError;
use restate_types::storage::{StorageCodec, StorageDecodeError, StorageEncodeError};
use restate_types::{flexbuffers_storage_encode_decode, Version};
use tokio::sync::{mpsc, oneshot};
use ulid::Ulid;

pub type BoxedMetadataStoreService = Box<dyn MetadataStoreService>;

pub type RequestSender = mpsc::Sender<MetadataStoreRequest>;
pub type RequestReceiver = mpsc::Receiver<MetadataStoreRequest>;

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error("internal error: {0}")]
    Internal(GenericError),
    #[error("service currently unavailable: {0}")]
    Unavailable(GenericError),
    #[error("failed precondition: {0}")]
    FailedPrecondition(#[from] PreconditionViolation),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("encode error: {0}")]
    Encode(#[from] StorageEncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] StorageDecodeError),
}

#[derive(Debug, thiserror::Error)]
pub enum PreconditionViolation {
    #[error("key-value pair already exists")]
    Exists,
    #[error("expected version '{expected}' but found version '{actual:?}'")]
    VersionMismatch {
        expected: Version,
        actual: Option<Version>,
    },
}

impl PreconditionViolation {
    fn kv_pair_exists() -> Self {
        PreconditionViolation::Exists
    }

    fn version_mismatch(expected: Version, actual: Option<Version>) -> Self {
        PreconditionViolation::VersionMismatch { expected, actual }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("error while running server grpc reflection service: {0}")]
    GrpcReflection(#[from] tonic_reflection::server::Error),
    #[error("system is shutting down")]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Generic(#[from] GenericError),
}

impl Error {
    pub fn generic(err: impl Into<GenericError>) -> Error {
        Error::Generic(err.into())
    }
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

impl MetadataStoreRequest {
    fn split_request(self) -> (Callback, Request) {
        let (request_kind, callback_kind) = match self {
            MetadataStoreRequest::Get { key, result_tx } => {
                (RequestKind::Get { key }, CallbackKind::Get { result_tx })
            }
            MetadataStoreRequest::GetVersion { key, result_tx } => (
                RequestKind::GetVersion { key },
                CallbackKind::GetVersion { result_tx },
            ),
            MetadataStoreRequest::Put {
                key,
                value,
                precondition,
                result_tx,
            } => (
                RequestKind::Put {
                    key,
                    value,
                    precondition,
                },
                CallbackKind::Put { result_tx },
            ),
            MetadataStoreRequest::Delete {
                key,
                precondition,
                result_tx,
            } => (
                RequestKind::Delete { key, precondition },
                CallbackKind::Delete { result_tx },
            ),
        };

        let request_id = Ulid::new();

        let callback = Callback {
            request_id,
            kind: callback_kind,
        };

        let request = Request {
            request_id,
            kind: request_kind,
        };

        (callback, request)
    }
}

struct Callback {
    request_id: Ulid,
    kind: CallbackKind,
}

impl Callback {
    fn fail(self, err: impl Into<RequestError>) {
        match self.kind {
            CallbackKind::Get { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
            CallbackKind::GetVersion { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
            CallbackKind::Put { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
            CallbackKind::Delete { result_tx } => {
                // err only if the oneshot receiver has gone away
                let _ = result_tx.send(Err(err.into()));
            }
        };
    }

    fn complete_get(self, result: Option<VersionedValue>) {
        let_assert!(
            CallbackKind::Get { result_tx } = self.kind,
            "expected 'Get' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(Ok(result));
    }

    fn complete_get_version(self, result: Option<Version>) {
        let_assert!(
            CallbackKind::GetVersion { result_tx } = self.kind,
            "expected 'GetVersion' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(Ok(result));
    }

    fn complete_put(self, result: Result<(), RequestError>) {
        let_assert!(
            CallbackKind::Put { result_tx } = self.kind,
            "expected 'Put' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(result);
    }

    fn complete_delete(self, result: Result<(), RequestError>) {
        let_assert!(
            CallbackKind::Delete { result_tx } = self.kind,
            "expected 'Delete' callback"
        );
        // err if caller has gone
        let _ = result_tx.send(result);
    }
}

enum CallbackKind {
    Get {
        result_tx: oneshot::Sender<Result<Option<VersionedValue>, RequestError>>,
    },
    GetVersion {
        result_tx: oneshot::Sender<Result<Option<Version>, RequestError>>,
    },
    Put {
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
    Delete {
        result_tx: oneshot::Sender<Result<(), RequestError>>,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Request {
    request_id: Ulid,
    kind: RequestKind,
}

flexbuffers_storage_encode_decode!(Request);

impl Request {
    fn encode_to_vec(&self) -> Result<Vec<u8>, StorageEncodeError> {
        let mut buffer = BytesMut::new();
        // todo: Removing support for BufMut requires an extra copy from BytesMut to Vec :-(
        StorageCodec::encode(self, &mut buffer)?;
        Ok(buffer.to_vec())
    }

    fn decode_from_bytes(mut bytes: Bytes) -> Result<Self, StorageDecodeError> {
        StorageCodec::decode::<Request, _>(&mut bytes)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum RequestKind {
    Get {
        key: ByteString,
    },
    GetVersion {
        key: ByteString,
    },
    Put {
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    },
    Delete {
        key: ByteString,
        precondition: Precondition,
    },
}
