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

use anyhow::Context;
use bytes::Bytes;
use bytestring::ByteString;
use object_store::path::{Path, PathPart};
use object_store::{
    Attribute, Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
};
use tracing::{debug, info, instrument};
use url::Url;

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

        // Other schemes may also support conditional writes, but only these are tested.
        if !matches!(url.scheme(), "s3" | "gs") {
            anyhow::bail!(
                "Only the `s3://` and `gs://` protocols are supported for the metadata path, got `{url}`"
            );
        }
        let prefix = Path::from(url.path());

        let object_store =
            create_object_store_client(url, &object_store, &object_store_retry_policy)
                .await
                // Restate reports this error with `{}`, so the cause must be part of the message.
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Unable to build an object store client for the metadata path: {e}"
                    )
                })?;

        Ok(Self {
            object_store: Box::new(object_store),
            prefix,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_testing() -> Self {
        Self {
            object_store: Box::new(tests::VersionMatchingStore::default()),
            prefix: Default::default(),
        }
    }

    /// Convert a metadata store key into an object store path.
    #[inline]
    fn path(&self, key: &ByteString) -> Path {
        self.prefix
            .clone()
            .join(PathPart::from(<ByteString as AsRef<str>>::as_ref(key)))
    }
}

const EXISTS_HEADER: Bytes = Bytes::from_static(b"e");
const DELETED_HEADER: Bytes = Bytes::from_static(b"d");

#[async_trait::async_trait]
impl VersionRepository for ObjectStoreVersionRepository {
    #[instrument(level = "debug", skip(self, content), err(level = "debug"))]
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

        debug!(%key, %path, size = content.bytes.len(), "calling put");

        match self.object_store.put_opts(&path, payload, opts).await {
            Ok(res) => Tag::from_reported(res.e_tag, res.version),
            Err(Error::AlreadyExists { .. }) => {
                // a file with this name already exists.
                // but it can be a deleted marker.
                // so let's find out what's inside
                let get_result = self
                    .object_store
                    .get(&path)
                    .await
                    .map_err(|e| VersionRepositoryError::Network(e.into()))?;
                let tag = Tag::from_reported(
                    get_result.meta.e_tag.clone(),
                    get_result.meta.version.clone(),
                )?;
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
            Err(e) => Err(VersionRepositoryError::Network(e.into())),
        }
    }

    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    async fn get(&self, key: ByteString) -> Result<TaggedValue, VersionRepositoryError> {
        let path = self.path(&key);

        debug!(%key, %path, "calling get");

        match self.object_store.get(&path).await {
            Ok(res) => {
                let encoding = res
                    .attributes
                    .get(&Attribute::ContentEncoding)
                    .map(|value| ValueEncoding::from_str(value))
                    .transpose()?
                    .unwrap_or(ValueEncoding::Cbor);

                let tag = Tag::from_reported(res.meta.e_tag.clone(), res.meta.version.clone())?;
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
            Err(e) => Err(VersionRepositoryError::Network(e.into())),
        }
    }

    #[instrument(level = "debug", skip(self, new_content), err(level = "debug"))]
    async fn put_if_tag_matches(
        &self,
        key: ByteString,
        expected: Tag,
        new_content: Content,
    ) -> Result<Tag, VersionRepositoryError> {
        let path = self.path(&key);

        debug!(
            %key,
            %path,
            ?expected,
            size = new_content.bytes.len(),
            "calling put"
        );

        let mut put_options = PutOptions::from(PutMode::Update(expected.into()));
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
            Ok(res) => Tag::from_reported(res.e_tag, res.version),
            Err(Error::Precondition { .. }) => Err(VersionRepositoryError::PreconditionFailed),
            Err(e) => Err(VersionRepositoryError::Network(e.into())),
        }
    }

    #[instrument(level = "debug", skip(self, new_content), err(level = "debug"))]
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

        debug!(%key, %path, size = new_content.bytes.len(), "calling put");

        match self
            .object_store
            .put_opts(
                &path,
                PutPayload::from_iter([EXISTS_HEADER, new_content.bytes]),
                put_options,
            )
            .await
        {
            Ok(res) => Tag::from_reported(res.e_tag, res.version),
            Err(e) => Err(VersionRepositoryError::Network(e.into())),
        }
    }

    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    async fn delete(&self, key: ByteString) -> Result<(), VersionRepositoryError> {
        let path = self.path(&key);

        debug!(%key, %path, "calling put with deleted tombstone");

        match self
            .object_store
            .put(&path, PutPayload::from_bytes(DELETED_HEADER))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(VersionRepositoryError::Network(e.into())),
        }
    }

    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    async fn delete_if_tag_matches(
        &self,
        key: ByteString,
        expected: Tag,
    ) -> Result<(), VersionRepositoryError> {
        let path = self.path(&key);

        debug!(%key, %path, ?expected, "calling put with deleted tombstone");

        match self
            .object_store
            .put_opts(
                &path,
                PutPayload::from_bytes(DELETED_HEADER),
                PutOptions::from(PutMode::Update(expected.into())),
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
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutResult, UpdateVersion,
    };

    use std::sync::Arc;
    use tokio::task::JoinSet;

    const KEY_1: ByteString = ByteString::from_static("1");
    const HELLO_WORLD: Bytes = Bytes::from_static(b"hello world");

    const HELLO: Bytes = Bytes::from_static(b"hello");
    const WORLD: Bytes = Bytes::from_static(b"world");

    #[test_log::test(tokio::test)]
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

    #[test_log::test(tokio::test)]
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
                            panic!("should not happened: {e}");
                        }
                    }
                }
            });
        }

        futures.join_all().await;

        let (_, mut content) = store.get(KEY_1).await.unwrap().into_inner();

        assert_eq!(content.bytes.get_u64(), 2048u64);
    }

    #[tokio::test]
    async fn rejects_unsupported_metadata_scheme() {
        let err =
            ObjectStoreVersionRepository::from_configuration(MetadataClientKind::ObjectStore {
                path: "az://bucket/prefix".into(),
                object_store: Default::default(),
                object_store_retry_policy: Default::default(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("`s3://` and `gs://`"));
    }

    /// Runs the conditional-write sequence the metadata store relies on against a real object
    /// store, such as `gs://bucket/prefix` with credentials in `GOOGLE_APPLICATION_CREDENTIALS`.
    #[ignore = "requires RESTATE_METADATA_TEST_OBJECT_STORE_PATH and credentials for it"]
    #[test_log::test(tokio::test)]
    async fn conditional_writes_against_real_object_store() {
        let path = std::env::var("RESTATE_METADATA_TEST_OBJECT_STORE_PATH")
            .expect("RESTATE_METADATA_TEST_OBJECT_STORE_PATH must be set");
        let store =
            ObjectStoreVersionRepository::from_configuration(MetadataClientKind::ObjectStore {
                path: format!("{path}/{}", rand::random::<u64>()),
                object_store: Default::default(),
                object_store_retry_policy: Default::default(),
            })
            .await
            .unwrap();
        let content = |bytes| Content {
            encoding: ValueEncoding::Bilrost,
            bytes,
        };

        let created = store.create(KEY_1, content(HELLO)).await.unwrap();
        assert!(matches!(
            store.create(KEY_1, content(HELLO)).await,
            Err(VersionRepositoryError::AlreadyExists)
        ));

        let updated = store
            .put_if_tag_matches(KEY_1, created.clone(), content(WORLD))
            .await
            .unwrap();
        assert!(matches!(
            store
                .put_if_tag_matches(KEY_1, created.clone(), content(HELLO))
                .await,
            Err(VersionRepositoryError::PreconditionFailed)
        ));
        let current = store.get(KEY_1).await.unwrap();
        assert_eq!(current.tag, updated);
        assert_eq!(current.content.bytes, WORLD);

        assert!(matches!(
            store.delete_if_tag_matches(KEY_1, created).await,
            Err(VersionRepositoryError::PreconditionFailed)
        ));
        store.delete_if_tag_matches(KEY_1, updated).await.unwrap();
        assert!(matches!(
            store.get(KEY_1).await,
            Err(VersionRepositoryError::NotFound)
        ));
        store.create(KEY_1, content(HELLO_WORLD)).await.unwrap();

        store
            .object_store
            .delete(&store.path(&KEY_1))
            .await
            .unwrap();
    }

    /// An in-memory store that, like GCS, matches conditional updates on the object version
    /// alone and rejects them without one, so tests catch a tag that loses the version.
    #[derive(Debug, Default)]
    pub(super) struct VersionMatchingStore(InMemory);

    impl std::fmt::Display for VersionMatchingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "VersionMatchingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for VersionMatchingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            mut opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            if let PutMode::Update(UpdateVersion { version, .. }) = &opts.mode {
                let Some(version) = version.clone() else {
                    return Err(Error::Generic {
                        store: "VersionMatchingStore",
                        source: "conditional update without a version".into(),
                    });
                };
                // The in-memory store matches on the ETag, which doubles as the version here.
                opts.mode = PutMode::Update(UpdateVersion {
                    e_tag: Some(version),
                    version: None,
                });
            }
            let result = self.0.put_opts(location, payload, opts).await?;
            Ok(PutResult {
                version: result.e_tag.clone(),
                ..result
            })
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let mut result = self.0.get_opts(location, options).await?;
            result.meta.version = result.meta.e_tag.clone();
            Ok(result)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.0.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.0.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.0.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }
}
