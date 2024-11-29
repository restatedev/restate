// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use bytestring::ByteString;

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
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub(crate) struct Tag(ByteString);

impl From<String> for Tag {
    fn from(value: String) -> Self {
        Tag(value.into())
    }
}

impl Tag {
    pub(crate) fn as_string(&self) -> String {
        self.0.to_string()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TaggedValue {
    pub tag: Tag,
    pub bytes: Bytes,
}

impl TaggedValue {
    #[cfg(test)]
    pub(crate) fn into_inner(self) -> (Tag, Bytes) {
        (self.tag, self.bytes)
    }
}

#[async_trait::async_trait]
pub(crate) trait VersionRepository: Sync + Send + 'static {
    async fn create(&self, key: ByteString, content: Bytes) -> Result<Tag, VersionRepositoryError>;

    async fn get(&self, key: ByteString) -> Result<TaggedValue, VersionRepositoryError>;

    async fn put_if_tag_matches(
        &self,
        key: ByteString,
        expected: Tag,
        new_content: Bytes,
    ) -> Result<Tag, VersionRepositoryError>;

    async fn put(&self, key: ByteString, new_content: Bytes)
        -> Result<Tag, VersionRepositoryError>;

    async fn delete(&self, key: ByteString) -> Result<(), VersionRepositoryError>;

    async fn delete_if_tag_matches(
        &self,
        key: ByteString,
        expected_tag: Tag,
    ) -> Result<(), VersionRepositoryError>;
}
