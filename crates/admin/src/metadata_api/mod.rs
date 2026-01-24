// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::HashSet,
    str::FromStr,
    sync::{Arc, LazyLock},
};

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    response::{AppendHeaders, IntoResponse, Response},
    routing::{delete, get, head, put},
};
use bytestring::ByteString;
use http::{HeaderMap, StatusCode, header::ToStrError};

use restate_metadata_store::{MetadataStore, MetadataStoreClient, ReadError, WriteError};
use restate_types::{
    Version,
    metadata::{Precondition, VersionedValue},
    metadata_store::keys,
};

/// ETag header.
const HEADER_ETAG: &str = "ETag";

/// If-Match header. Possible values are
/// - *: no precondition
/// - does-not-exist: key must not exist
/// - <version>: Match given etag
const HEADER_IF_MATCH: &str = "If-Match";

static PROTECTED_KEYS: LazyLock<HashSet<ByteString>> = LazyLock::new(|| {
    [
        keys::PARTITION_TABLE_KEY.clone(),
        keys::BIFROST_CONFIG_KEY.clone(),
        keys::NODES_CONFIG_KEY.clone(),
        keys::SCHEMA_INFORMATION_KEY.clone(),
    ]
    .into()
});

fn is_protected(key: &ByteString) -> bool {
    PROTECTED_KEYS.contains(key) || key.starts_with(keys::PARTITION_PROCESSOR_EPOCH_PREFIX)
}

#[derive(Clone, derive_more::Deref)]
struct MetadataStoreState {
    inner: Arc<dyn MetadataStore + Send + Sync>,
}

pub fn router(metadata_store_client: &MetadataStoreClient) -> Router {
    let state = MetadataStoreState {
        inner: metadata_store_client.inner(),
    };

    Router::new()
        .route("/metadata/{key}", get(get_key))
        .route("/metadata/{key}", head(get_key_version))
        .route("/metadata/{key}", put(put_key))
        .route("/metadata/{key}", delete(delete_key))
        .with_state(state)
}

/// Deletes a key from the metadata store
///
/// Requires `If-Match` header
async fn delete_key(
    Path(key): Path<String>,
    State(store): State<MetadataStoreState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StoreApiError> {
    let key = key.into();

    if is_protected(&key) {
        return Err(StoreApiError::ProtectedKey);
    }

    // todo(azmy): implement both PreconditionHeader and VersionHeader
    // as extractors after updating axium to version > 0.8
    let precondition: IfMatch = headers
        .get(HEADER_IF_MATCH)
        .ok_or(IfMatchError::MissingPrecondition)?
        .to_str()?
        .parse()?;

    store.delete(key, precondition.into()).await?;
    Ok(())
}

/// Puts a key/value pair
///
/// Requires `If-Match` header
/// Required `ETag` header
async fn put_key(
    Path(key): Path<String>,
    State(store): State<MetadataStoreState>,
    headers: HeaderMap,
    value: Bytes,
) -> Result<impl IntoResponse, StoreApiError> {
    // todo(azmy): implement both PreconditionHeader and VersionHeader
    // as extractors after updating axium to version > 0.8
    let key = key.into();

    if is_protected(&key) {
        return Err(StoreApiError::ProtectedKey);
    }

    let precondition: IfMatch = headers
        .get(HEADER_IF_MATCH)
        .ok_or(IfMatchError::MissingPrecondition)?
        .to_str()?
        .parse()?;

    let version: ETagHeader = headers
        .get(HEADER_ETAG)
        .ok_or(ETagError::MissingETag)?
        .to_str()?
        .parse()?;

    let versioned = VersionedValue {
        version: version.into(),
        value,
    };

    store.put(key, versioned, precondition.into()).await?;
    Ok(())
}

/// Gets a key from the metadata store
async fn get_key(
    Path(key): Path<String>,
    State(store): State<MetadataStoreState>,
) -> Result<impl IntoResponse, StoreApiError> {
    let value = store
        .get(key.into())
        .await?
        .ok_or(StoreApiError::NotFound)?;

    Ok(VersionedValueResponse::from(value))
}

/// Gets a key version from the metadata store
async fn get_key_version(
    Path(key): Path<String>,
    State(store): State<MetadataStoreState>,
) -> Result<impl IntoResponse, HeadError> {
    let version = store
        .get_version(key.into())
        .await
        .map_err(StoreApiError::from)?
        .ok_or(StoreApiError::NotFound)?;

    Ok(AppendHeaders([(
        HEADER_ETAG,
        u32::from(version).to_string(),
    )]))
}

#[derive(Debug, thiserror::Error)]
enum StoreApiError {
    #[error(transparent)]
    ReadError(#[from] ReadError),
    #[error(transparent)]
    WriteError(#[from] WriteError),
    #[error("Key not found")]
    NotFound,

    #[error(transparent)]
    ToStrError(#[from] ToStrError),
    #[error(transparent)]
    PreconditionError(#[from] IfMatchError),
    #[error(transparent)]
    VersionError(#[from] ETagError),
    #[error("Key is protected")]
    ProtectedKey,
}

impl StoreApiError {
    fn code(&self) -> StatusCode {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::WriteError(WriteError::FailedPrecondition(_)) => StatusCode::PRECONDITION_FAILED,
            Self::ProtectedKey => StatusCode::UNAUTHORIZED,
            Self::ReadError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::WriteError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ToStrError(_) | Self::PreconditionError(_) | Self::VersionError(_) => {
                StatusCode::BAD_REQUEST
            }
        }
    }
}

impl IntoResponse for StoreApiError {
    fn into_response(self) -> Response {
        (self.code(), self.to_string()).into_response()
    }
}

// We can't return a body as a response to Head requests
#[derive(derive_more::From)]
struct HeadError(StoreApiError);

impl IntoResponse for HeadError {
    fn into_response(self) -> Response {
        self.0.code().into_response()
    }
}

#[derive(derive_more::From)]
struct VersionedValueResponse(VersionedValue);

impl IntoResponse for VersionedValueResponse {
    fn into_response(self) -> Response {
        (
            AppendHeaders([(HEADER_ETAG, u32::from(self.0.version).to_string())]),
            self.0.value,
        )
            .into_response()
    }
}

#[derive(Debug, thiserror::Error)]
enum IfMatchError {
    #[error("Missing If-Match header")]
    MissingPrecondition,
    #[error("Invalid If-Match header")]
    InvalidPrecondition,
}

struct IfMatch(Precondition);

impl FromStr for IfMatch {
    type Err = IfMatchError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = match s.to_lowercase().as_str() {
            "*" => Precondition::None,
            "does-not-exist" => Precondition::DoesNotExist,
            version => {
                let version: u32 = version
                    .parse()
                    .map_err(|_| IfMatchError::InvalidPrecondition)?;

                Precondition::MatchesVersion(version.into())
            }
        };

        Ok(Self(inner))
    }
}

impl From<IfMatch> for Precondition {
    fn from(value: IfMatch) -> Self {
        value.0
    }
}

#[derive(Debug, thiserror::Error)]
enum ETagError {
    #[error("Missing ETag header")]
    MissingETag,

    #[error("Invalid ETag header")]
    InvalidETag,
}

struct ETagHeader(Version);

impl FromStr for ETagHeader {
    type Err = ETagError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let version: u32 = s.parse().map_err(|_| ETagError::InvalidETag)?;

        Ok(ETagHeader(version.into()))
    }
}

impl From<ETagHeader> for Version {
    fn from(value: ETagHeader) -> Self {
        value.0
    }
}
