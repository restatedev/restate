// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytestring::ByteString;
use tracing::{debug, instrument};

use super::version_repository::{
    Content, Tag, TaggedValue, VersionRepository, VersionRepositoryError,
};

/// Simple in-memory VersionRepository for tests.
/// Generates monotonically increasing tags.
#[derive(Clone)]
pub struct InMemoryVersionRepository(Arc<Mutex<Inner>>);

struct Inner {
    storage: HashMap<ByteString, (Tag, Content)>,
    next_tag: u64,
}

impl InMemoryVersionRepository {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Inner {
            storage: HashMap::new(),
            next_tag: 1,
        })))
    }
}

impl Default for InMemoryVersionRepository {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl VersionRepository for InMemoryVersionRepository {
    #[instrument(level = "trace", skip(self, content), err)]
    async fn create(
        &self,
        key: ByteString,
        content: Content,
    ) -> Result<Tag, VersionRepositoryError> {
        debug!(%key, size = content.bytes.len(), "create");

        let mut inner = self.0.lock().unwrap();
        if inner.storage.contains_key(&key) {
            return Err(VersionRepositoryError::AlreadyExists);
        }
        let tag = Tag::from(format!("tag_{}", inner.next_tag));
        inner.next_tag += 1;
        inner.storage.insert(key, (tag.clone(), content));
        Ok(tag)
    }

    #[instrument(level = "trace", skip(self), err)]
    async fn get(&self, key: ByteString) -> Result<TaggedValue, VersionRepositoryError> {
        debug!(%key, "get");

        let inner = self.0.lock().unwrap();
        match inner.storage.get(&key) {
            Some((tag, content)) => Ok(TaggedValue {
                tag: tag.clone(),
                content: content.clone(),
            }),
            None => Err(VersionRepositoryError::NotFound),
        }
    }

    #[instrument(level = "trace", skip(self, content), err)]
    async fn put_if_tag_matches(
        &self,
        key: ByteString,
        expected: Tag,
        content: Content,
    ) -> Result<Tag, VersionRepositoryError> {
        debug!(%key, ?expected, size = content.bytes.len(), "put_if_tag_matches");

        let mut inner = self.0.lock().unwrap();
        match inner.storage.get(&key) {
            Some((current_tag, _)) if current_tag == &expected => {
                let new_tag = Tag::from(format!("tag_{}", inner.next_tag));
                inner.next_tag += 1;
                inner.storage.insert(key, (new_tag.clone(), content));
                Ok(new_tag)
            }
            Some(_) => Err(VersionRepositoryError::PreconditionFailed),
            None => Err(VersionRepositoryError::NotFound),
        }
    }

    #[instrument(level = "trace", skip(self, content), err)]
    async fn put(&self, key: ByteString, content: Content) -> Result<Tag, VersionRepositoryError> {
        debug!(%key, size = content.bytes.len(), "put");

        let mut inner = self.0.lock().unwrap();
        let tag = Tag::from(format!("tag_{}", inner.next_tag));
        inner.next_tag += 1;
        inner.storage.insert(key, (tag.clone(), content));
        Ok(tag)
    }

    #[instrument(level = "trace", skip(self), err)]
    async fn delete(&self, key: ByteString) -> Result<(), VersionRepositoryError> {
        debug!(%key, "delete");

        let mut inner = self.0.lock().unwrap();
        if inner.storage.remove(&key).is_some() {
            Ok(())
        } else {
            Err(VersionRepositoryError::NotFound)
        }
    }

    #[instrument(level = "trace", skip(self), err)]
    async fn delete_if_tag_matches(
        &self,
        key: ByteString,
        expected_tag: Tag,
    ) -> Result<(), VersionRepositoryError> {
        debug!(%key, ?expected_tag, "delete_if_tag_matches");

        let mut inner = self.0.lock().unwrap();
        match inner.storage.get(&key) {
            Some((current_tag, _)) if current_tag == &expected_tag => {
                inner.storage.remove(&key);
                Ok(())
            }
            Some(_) => Err(VersionRepositoryError::PreconditionFailed),
            None => Err(VersionRepositoryError::NotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objstore::version_repository::ValueEncoding;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_create_and_get() {
        let repo = InMemoryVersionRepository::new();
        let key = ByteString::from_static("test_key");
        let content = Content {
            encoding: ValueEncoding::Cbor,
            bytes: Bytes::from_static(b"test_value"),
        };

        let tag = repo.create(key.clone(), content.clone()).await.unwrap();

        let tagged_value = repo.get(key).await.unwrap();
        assert_eq!(tagged_value.tag, tag);
        assert_eq!(tagged_value.content.bytes, content.bytes);
        assert_eq!(tagged_value.content.encoding, content.encoding);
    }

    #[tokio::test]
    async fn test_create_already_exists() {
        let repo = InMemoryVersionRepository::new();
        let key = ByteString::from_static("test_key");
        let content = Content {
            encoding: ValueEncoding::Cbor,
            bytes: Bytes::from_static(b"test_value"),
        };

        repo.create(key.clone(), content.clone()).await.unwrap();

        let result = repo.create(key, content).await;
        assert!(matches!(result, Err(VersionRepositoryError::AlreadyExists)));
    }

    #[tokio::test]
    async fn test_put_and_put_if_tag_matches() {
        let repo = InMemoryVersionRepository::new();
        let key = ByteString::from_static("test_key");
        let content1 = Content {
            encoding: ValueEncoding::Cbor,
            bytes: Bytes::from_static(b"value1"),
        };
        let content2 = Content {
            encoding: ValueEncoding::Cbor,
            bytes: Bytes::from_static(b"value2"),
        };

        let tag1 = repo.put(key.clone(), content1).await.unwrap();

        let tag2 = repo
            .put_if_tag_matches(key.clone(), tag1.clone(), content2.clone())
            .await
            .unwrap();
        assert_ne!(tag1, tag2);

        let tagged_value = repo.get(key.clone()).await.unwrap();
        assert_eq!(tagged_value.tag, tag2);
        assert_eq!(tagged_value.content.bytes, content2.bytes);

        let result = repo.put_if_tag_matches(key, tag1, content2).await;
        assert!(matches!(
            result,
            Err(VersionRepositoryError::PreconditionFailed)
        ));
    }

    #[tokio::test]
    async fn test_delete() {
        let repo = InMemoryVersionRepository::new();
        let key = ByteString::from_static("test_key");
        let content = Content {
            encoding: ValueEncoding::Cbor,
            bytes: Bytes::from_static(b"test_value"),
        };

        repo.create(key.clone(), content).await.unwrap();
        repo.delete(key.clone()).await.unwrap();

        let result = repo.get(key).await;
        assert!(matches!(result, Err(VersionRepositoryError::NotFound)));
    }

    #[tokio::test]
    async fn test_delete_if_tag_matches() {
        let repo = InMemoryVersionRepository::new();
        let key = ByteString::from_static("test_key");
        let content = Content {
            encoding: ValueEncoding::Cbor,
            bytes: Bytes::from_static(b"test_value"),
        };

        let tag = repo.create(key.clone(), content).await.unwrap();

        repo.delete_if_tag_matches(key.clone(), tag.clone())
            .await
            .unwrap();

        let result = repo.get(key.clone()).await;
        assert!(matches!(result, Err(VersionRepositoryError::NotFound)));

        let result = repo.delete_if_tag_matches(key, tag).await;
        assert!(matches!(result, Err(VersionRepositoryError::NotFound)));
    }
}
