// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use anyhow::Context;
use bytes::Bytes;
use bytestring::ByteString;
use object_store::path::{Path, PathPart};
use object_store::{Attribute, Error, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use tracing::info;
use url::Url;
use restate_object_store_util::log_object_store_error;

use restate_object_store_util::create_object_store_client;
use restate_types::config::MetadataClientKind;

use super::version_repository::{Tag, TaggedValue, VersionRepository, VersionRepositoryError};
use crate::objstore::version_repository::{Content, ValueEncoding};

#[derive(Debug)]
pub(crate) struct ObjectStoreVersionRepository {
    object_store: Box<dyn ObjectStore>,
    prefix: Path,
}

impl ObjectStoreVersionRepository {
    pub(crate) async fn from_configuration(
        configuration: MetadataClientKind,
    ) -> anyhow::Result<Self> {
        let MetadataClientKind::ObjectStore {
            path,
            object_store,
            object_store_retry_policy,
        } = configuration
        else {
            anyhow::bail!("unexpected configuration value");
        };

        let mut url = Url::parse(&path).context("Failed parsing metadata repository URL")?;
        // Prevent passing configuration options to object_store via the destination URL.
        url.query()
            .inspect(|params| info!("Metadata path parameters ignored: {params}"));
        url.set_query(None);

        if url.scheme() != "s3" {
            anyhow::bail!("Only the `s3://` protocol is supported for metadata path");
        }
        let prefix = Path::from(url.path());

        let object_store =
            create_object_store_client(url, &object_store, &object_store_retry_policy)
                .await
                .map_err(|e| anyhow::anyhow!("Unable to build an S3 object store: {}", e))?;

        Ok(Self {
            object_store: Box::new(object_store),
            prefix,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_testing() -> Self {
        let store = object_store::memory::InMemory::new();
        Self {
            object_store: Box::new(store),
            prefix: Default::default(),
        }
    }

    /// Convert a metadata store key into an object store path.
    #[inline]
    fn path(&self, key: &ByteString) -> Path {
        self.prefix
            .child(PathPart::from(<ByteString as AsRef<str>>::as_ref(key)))
    }
}

const EXISTS_HEADER: Bytes = Bytes::from_static(b"e");
const DELETED_HEADER: Bytes = Bytes::from_static(b"d");

#[async_trait::async_trait]
impl VersionRepository for ObjectStoreVersionRepository {
    async fn create(
        &self,
        key: ByteString,
        content: Content,
    ) -> Result<Tag, VersionRepositoryError> {
        let path = self.path(&key);

        let mut opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };

        opts.attributes
            .insert(Attribute::ContentEncoding, content.encoding.into());

        let payload = PutPayload::from_iter([EXISTS_HEADER, content.bytes.clone()]);

        match self.object_store.put_opts(&path, payload, opts).await {
            Ok(res) => {
                let etag = res.e_tag.ok_or_else(|| {
                    VersionRepositoryError::UnexpectedCondition(
                        "expecting an ETag to be present".into(),
                    )
                })?;
                Ok(etag.into())
            }
            Err(Error::AlreadyExists { .. }) => {
                // a file with this name already exists.
                // but it can be a deleted marker.
                // so let's find out what's inside
                let get_result = self
                    .object_store
                    .get(&path)
                    .await
                    .map_err(|e| VersionRepositoryError::Network(e.into()))?;
                let etag = get_result.meta.e_tag.as_ref().ok_or_else(|| {
                    VersionRepositoryError::UnexpectedCondition("was expecting an etag".into())
                })?;
                let tag: Tag = etag.to_owned().into();
                let bytes = get_result
                    .bytes()
                    .await
                    .map_err(|e| VersionRepositoryError::Network(e.into()))?;
                if bytes.starts_with(&EXISTS_HEADER) {
                    return Err(VersionRepositoryError::AlreadyExists);
                }
                assert_eq!(bytes, DELETED_HEADER);
                self.put_if_tag_matches(key, tag, content).await
            }
            Err(e) => {
                log_object_store_error("create", &path, &e);
                Err(VersionRepositoryError::Network(e.into()))
            },
        }
    }

    async fn get(&self, key: ByteString) -> Result<TaggedValue, VersionRepositoryError> {
        let path = self.path(&key);
        match self.object_store.get(&path).await {
            Ok(res) => {
                let encoding = res
                    .attributes
                    .get(&Attribute::ContentEncoding)
                    .map(|value| ValueEncoding::from_str(value))
                    .transpose()?
                    .unwrap_or(ValueEncoding::Cbor);

                let etag = res.meta.e_tag.as_ref().ok_or_else(|| {
                    VersionRepositoryError::UnexpectedCondition(
                        "expecting an ETag to be present".into(),
                    )
                })?;
                let tag = etag.to_owned().into();
                let mut buf = res
                    .bytes()
                    .await
                    .map_err(|e| VersionRepositoryError::Network(e.into()))?;
                if buf.starts_with(&DELETED_HEADER) {
                    Err(VersionRepositoryError::NotFound)
                } else {
                    let bytes = buf.split_off(EXISTS_HEADER.len());
                    Ok(TaggedValue {
                        tag,
                        content: Content { encoding, bytes },
                    })
                }
            }
            Err(Error::NotFound { .. }) => Err(VersionRepositoryError::NotFound),
            Err(e) => {
                log_object_store_error("get", &path, &e);
                Err(VersionRepositoryError::Network(e.into()))
            },
        }
    }

    async fn put_if_tag_matches(
        &self,
        key: ByteString,
        expected: Tag,
        new_content: Content,
    ) -> Result<Tag, VersionRepositoryError> {
        let etag = expected.as_string();
        let update_version = UpdateVersion {
            e_tag: Some(etag),
            version: None,
        };

        let path = self.path(&key);
        let mut put_options = PutOptions::from(PutMode::Update(update_version));
        put_options
            .attributes
            .insert(Attribute::ContentEncoding, new_content.encoding.into());

        match self
            .object_store
            .put_opts(
                &path,
                PutPayload::from_iter([EXISTS_HEADER, new_content.bytes]),
                put_options,
            )
            .await
        {
            Ok(res) => {
                let etag = res.e_tag.ok_or_else(|| {
                    VersionRepositoryError::UnexpectedCondition("expecting an etag".into())
                })?;
                let tag: Tag = etag.into();
                Ok(tag)
            }
            Err(Error::Precondition { .. }) => Err(VersionRepositoryError::PreconditionFailed),
            Err(e) => Err(VersionRepositoryError::Network(e.into())),
        }
    }

    async fn put(
        &self,
        key: ByteString,
        new_content: Content,
    ) -> Result<Tag, VersionRepositoryError> {
        let path = self.path(&key);
        let mut put_options = PutOptions::default();
        put_options
            .attributes
            .insert(Attribute::ContentEncoding, new_content.encoding.into());

        match self
            .object_store
            .put_opts(
                &path,
                PutPayload::from_iter([EXISTS_HEADER, new_content.bytes]),
                put_options,
            )
            .await
        {
            Ok(res) => {
                let etag = res.e_tag.ok_or_else(|| {
                    VersionRepositoryError::UnexpectedCondition("expecting an etag".into())
                })?;
                let tag: Tag = etag.into();
                Ok(tag)
            }
            Err(e) => Err(VersionRepositoryError::Network(e.into())),
        }
    }

    async fn delete(&self, key: ByteString) -> Result<(), VersionRepositoryError> {
        let path = self.path(&key);
        match self
            .object_store
            .put(&path, PutPayload::from_bytes(DELETED_HEADER))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(VersionRepositoryError::Network(e.into())),
        }
    }

    async fn delete_if_tag_matches(
        &self,
        key: ByteString,
        expected: Tag,
    ) -> Result<(), VersionRepositoryError> {
        let etag = expected.as_string();
        let update_version = UpdateVersion {
            e_tag: Some(etag),
            version: None,
        };

        let path = self.path(&key);
        match self
            .object_store
            .put_opts(
                &path,
                PutPayload::from_bytes(DELETED_HEADER),
                PutOptions::from(PutMode::Update(update_version)),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(Error::Precondition { .. }) => Err(VersionRepositoryError::PreconditionFailed),
            Err(e) => Err(VersionRepositoryError::Network(e.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::objstore::version_repository::{VersionRepository, VersionRepositoryError};

    use bytes::{Buf, Bytes};
    use bytestring::ByteString;

    use std::sync::Arc;
    use tokio::task::JoinSet;

    const KEY_1: ByteString = ByteString::from_static("1");
    const HELLO_WORLD: Bytes = Bytes::from_static(b"hello world");

    const HELLO: Bytes = Bytes::from_static(b"hello");
    const WORLD: Bytes = Bytes::from_static(b"world");

    #[tokio::test]
    async fn simple_usage() {
        let store = ObjectStoreVersionRepository::new_for_testing();

        let tag = store
            .create(
                KEY_1,
                Content {
                    encoding: ValueEncoding::Bilrost,
                    bytes: HELLO_WORLD,
                },
            )
            .await
            .unwrap();

        let tagged_value = store.get(KEY_1).await.unwrap();

        assert_eq!(tagged_value.tag, tag);
        assert_eq!(tagged_value.content.bytes, HELLO_WORLD);
        assert_eq!(tagged_value.content.encoding, ValueEncoding::Bilrost);
    }

    #[tokio::test]
    async fn get_non_existing_should_fail() {
        let store = ObjectStoreVersionRepository::new_for_testing();

        match store.get(KEY_1).await {
            Err(VersionRepositoryError::NotFound) => {
                // ok!
            }
            _ => {
                panic!("Should be NotFound");
            }
        }
    }

    #[tokio::test]
    async fn create_twice_should_fail() {
        let store = ObjectStoreVersionRepository::new_for_testing();

        store
            .create(
                KEY_1,
                Content {
                    encoding: ValueEncoding::default(),
                    bytes: HELLO_WORLD,
                },
            )
            .await
            .unwrap();

        match store
            .create(
                KEY_1,
                Content {
                    encoding: ValueEncoding::default(),
                    bytes: HELLO_WORLD,
                },
            )
            .await
        {
            Err(VersionRepositoryError::AlreadyExists) => {
                // ok!
            }
            _ => {
                panic!("should have failed");
            }
        }
    }

    #[tokio::test]
    async fn delete_should_work() {
        let store = ObjectStoreVersionRepository::new_for_testing();

        store
            .create(
                KEY_1,
                Content {
                    encoding: ValueEncoding::default(),
                    bytes: HELLO_WORLD,
                },
            )
            .await
            .unwrap();

        store.delete(KEY_1).await.unwrap();

        match store.get(KEY_1).await {
            Err(VersionRepositoryError::NotFound) => {
                // ok!
            }
            _ => {
                panic!("should not be present");
            }
        }
    }

    #[tokio::test]
    async fn create_after_delete_should_work() {
        let store = ObjectStoreVersionRepository::new_for_testing();

        store
            .create(
                KEY_1,
                Content {
                    encoding: ValueEncoding::default(),
                    bytes: HELLO_WORLD,
                },
            )
            .await
            .unwrap();

        store.delete(KEY_1).await.unwrap();

        // also change encoding
        store
            .create(
                KEY_1,
                Content {
                    encoding: ValueEncoding::Bilrost,
                    bytes: WORLD,
                },
            )
            .await
            .unwrap();

        let tv = store.get(KEY_1).await.unwrap();

        assert_eq!(tv.content.bytes, WORLD);
        assert_eq!(tv.content.encoding, ValueEncoding::Bilrost);
    }

    #[tokio::test]
    async fn conditional_put_should_work() {
        let store = ObjectStoreVersionRepository::new_for_testing();

        let tag = store
            .create(
                KEY_1,
                Content {
                    encoding: ValueEncoding::default(),
                    bytes: HELLO_WORLD,
                },
            )
            .await
            .unwrap();

        store
            .put_if_tag_matches(
                KEY_1,
                tag,
                Content {
                    encoding: ValueEncoding::default(),
                    bytes: WORLD,
                },
            )
            .await
            .unwrap();

        let tv = store.get(KEY_1).await.unwrap();

        assert_eq!(tv.content.bytes, WORLD);
    }

    #[tokio::test]
    async fn conditional_put_should_fail() {
        let store = ObjectStoreVersionRepository::new_for_testing();

        let tag = store
            .create(
                KEY_1,
                Content {
                    encoding: ValueEncoding::default(),
                    bytes: HELLO_WORLD,
                },
            )
            .await
            .unwrap();

        store
            .put_if_tag_matches(
                KEY_1,
                tag.clone(),
                Content {
                    encoding: ValueEncoding::default(),
                    bytes: HELLO,
                },
            )
            .await
            .unwrap();

        match store
            .put_if_tag_matches(
                KEY_1,
                tag.clone(),
                Content {
                    encoding: ValueEncoding::default(),
                    bytes: WORLD,
                },
            )
            .await
        {
            Err(VersionRepositoryError::PreconditionFailed) => {
                // ok!
            }
            _ => panic!("should have failed"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrency_test() {
        let store = Arc::new(ObjectStoreVersionRepository::new_for_testing());

        store
            .create(
                KEY_1,
                Content {
                    encoding: ValueEncoding::default(),
                    bytes: Bytes::copy_from_slice(&0u64.to_be_bytes()),
                },
            )
            .await
            .unwrap();

        //
        // first task
        //

        let mut futures = JoinSet::new();

        for _ in 0..2048 {
            let cloned_store = store.clone();
            futures.spawn(async move {
                loop {
                    let (tag, mut content) =
                        cloned_store.get(KEY_1.clone()).await.unwrap().into_inner();

                    let mut n = content.bytes.get_u64();
                    n += 1;

                    match cloned_store
                        .put_if_tag_matches(
                            KEY_1.clone(),
                            tag,
                            Content {
                                encoding: ValueEncoding::default(),
                                bytes: Bytes::copy_from_slice(&n.to_be_bytes()),
                            },
                        )
                        .await
                    {
                        Ok(_) => {
                            break;
                        }
                        Err(VersionRepositoryError::PreconditionFailed) => {
                            continue;
                        }
                        Err(e) => {
                            panic!("should not happened: {}", e);
                        }
                    }
                }
            });
        }

        futures.join_all().await;

        let (_, mut content) = store.get(KEY_1).await.unwrap().into_inner();

        assert_eq!(content.bytes.get_u64(), 2048u64);
    }
}
