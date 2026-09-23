// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use bytes::Bytes;
use bytestring::ByteString;
use object_store::AttributeValue;

use restate_types::errors::GenericError;

#[derive(Debug, thiserror::Error)]
pub enum VersionRepositoryError {
    #[error("already exists")]
    AlreadyExists,
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("not found")]
    NotFound,
    #[error("Network {0}")]
    Network(GenericError),
    #[error("Unexpected condition {0}")]
    UnexpectedCondition(String),
    #[error("Encoding error")]
    Encoding(#[from] EncodingError),
}

/// The version of a stored value that conditional writes must match.
///
/// Object stores differ in what they match on: S3 compares the ETag, while GCS compares the
/// object generation, which object_store exposes as the object's `version`. Both are kept
/// when the store reports them.
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub(crate) struct Tag {
    e_tag: ByteString,
    version: Option<ByteString>,
}

impl From<String> for Tag {
    fn from(e_tag: String) -> Self {
        Tag::new(e_tag, None)
    }
}

impl Tag {
    pub(crate) fn new(e_tag: String, version: Option<String>) -> Self {
        Tag {
            e_tag: e_tag.into(),
            version: version.map(Into::into),
        }
    }

    pub(crate) fn e_tag(&self) -> &str {
        &self.e_tag
    }

    pub(crate) fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    pub(crate) fn as_string(&self) -> String {
        self.e_tag.to_string()
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum EncodingError {
    #[error("Unknown encoding '{0}'")]
    UnknownEncoding(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ValueEncoding {
    Cbor,
    // default since version v1.6
    #[default]
    Bilrost,
}

impl FromStr for ValueEncoding {
    type Err = EncodingError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "binary/bilrost+v1" => Ok(Self::Bilrost),
            "binary/cbor" => Ok(Self::Cbor),
            _ => Err(EncodingError::UnknownEncoding(s.to_owned())),
        }
    }
}

impl From<ValueEncoding> for AttributeValue {
    fn from(value: ValueEncoding) -> Self {
        match value {
            ValueEncoding::Bilrost => AttributeValue::from("binary/bilrost+v1"),
            ValueEncoding::Cbor => AttributeValue::from("binary/cbor"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TaggedValue {
    pub tag: Tag,
    pub content: Content,
}

impl TaggedValue {
    #[cfg(test)]
    pub(crate) fn into_inner(self) -> (Tag, Content) {
        (self.tag, self.content)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Content {
    pub encoding: ValueEncoding,
    pub bytes: Bytes,
}

#[async_trait::async_trait]
pub(crate) trait VersionRepository: Sync + Send + 'static {
    async fn create(
        &self,
        key: ByteString,
        content: Content,
    ) -> Result<Tag, VersionRepositoryError>;

    async fn get(&self, key: ByteString) -> Result<TaggedValue, VersionRepositoryError>;

    async fn put_if_tag_matches(
        &self,
        key: ByteString,
        expected: Tag,
        content: Content,
    ) -> Result<Tag, VersionRepositoryError>;

    async fn put(&self, key: ByteString, content: Content) -> Result<Tag, VersionRepositoryError>;

    async fn delete(&self, key: ByteString) -> Result<(), VersionRepositoryError>;

    async fn delete_if_tag_matches(
        &self,
        key: ByteString,
        expected_tag: Tag,
    ) -> Result<(), VersionRepositoryError>;
}
