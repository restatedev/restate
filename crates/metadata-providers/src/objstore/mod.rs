// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod codec;

use std::sync::Arc;

use anyhow::Context;
use bytestring::ByteString;
use object_store::path::{Path, PathPart};
use object_store::{Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use tracing::{info, instrument};
use url::Url;

use restate_metadata_store::{ProvisionedMetadataStore, ReadError, WriteError};
use restate_object_store_util::create_object_store_client;
use restate_types::Version;
use restate_types::config::MetadataClientKind;
use restate_types::metadata::{Precondition, VersionedValue};

use self::codec::EncodedObject;

/// A metadata store that keeps each key in its own object and implements preconditions
/// with the object store's conditional writes.
pub struct ObjectStoreMetadataStore {
    object_store: Arc<dyn ObjectStore>,
    prefix: Path,
}

/// An object as read, with the tag that conditional writes match on. S3 matches the ETag,
/// GCS the generation, which object_store reports as the version.
struct StoredObject {
    tag: UpdateVersion,
    /// `None` for a deletion tombstone.
    value: Option<VersionedValue>,
}

impl ObjectStoreMetadataStore {
    pub async fn new(configuration: MetadataClientKind) -> anyhow::Result<Self> {
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

        Ok(Self::with_object_store(object_store, prefix))
    }

    fn with_object_store(object_store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        Self {
            object_store,
            prefix,
        }
    }

    fn path(&self, key: &ByteString) -> Path {
        self.prefix.clone().join(PathPart::from(&**key))
    }

    async fn read(&self, key: &ByteString) -> Result<Option<StoredObject>, ReadError> {
        let mut result = match self.object_store.get(&self.path(key)).await {
            Ok(result) => result,
            Err(Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(ReadError::retryable(err)),
        };
        let tag = UpdateVersion {
            e_tag: result.meta.e_tag.take(),
            version: result.meta.version.take(),
        };
        let attributes = std::mem::take(&mut result.attributes);
        let bytes = result.bytes().await.map_err(ReadError::retryable)?;
        let value = codec::decode(&attributes, bytes).map_err(|e| ReadError::Codec(e.into()))?;
        Ok(Some(StoredObject { tag, value }))
    }

    async fn write(
        &self,
        key: &ByteString,
        mode: PutMode,
        object: &EncodedObject,
    ) -> object_store::Result<()> {
        let opts = PutOptions {
            mode,
            attributes: object.attributes.clone(),
            ..Default::default()
        };
        self.object_store
            .put_opts(&self.path(key), object.payload.clone(), opts)
            .await
            .map(|_| ())
    }

    async fn create(&self, key: &ByteString, object: &EncodedObject) -> Result<(), WriteError> {
        loop {
            match self.write(key, PutMode::Create, object).await {
                Err(Error::AlreadyExists { .. }) => {}
                result => return result.map_err(WriteError::retryable),
            }

            // A deleted key leaves a tombstone, which the create may replace.
            let tag = match self.read(key).await.map_err(into_write_error)? {
                Some(StoredObject { tag, value: None }) => tag,
                Some(StoredObject { value: Some(_), .. }) => {
                    return Err(WriteError::FailedPrecondition("already exists".to_owned()));
                }
                None => continue,
            };
            match self.write(key, PutMode::Update(tag), object).await {
                // Another write replaced the tombstone; check again what the key holds.
                Err(Error::Precondition { .. }) => continue,
                result => return result.map_err(WriteError::retryable),
            }
        }
    }

    /// Writes `object` if the key currently holds a value with version `expected`.
    async fn write_if_version_matches(
        &self,
        key: &ByteString,
        expected: Version,
        object: &EncodedObject,
    ) -> Result<(), WriteError> {
        let tag = match self.read(key).await.map_err(into_write_error)? {
            Some(StoredObject {
                tag,
                value: Some(value),
            }) if value.version == expected => tag,
            Some(StoredObject {
                value: Some(value), ..
            }) => {
                return Err(WriteError::FailedPrecondition(format!(
                    "expected version {expected}, found {}",
                    value.version
                )));
            }
            _ => {
                return Err(WriteError::FailedPrecondition(format!(
                    "expected version {expected}, found no value"
                )));
            }
        };
        self.write(key, PutMode::Update(tag), object)
            .await
            .map_err(|err| match err {
                Error::Precondition { .. } => WriteError::FailedPrecondition(format!(
                    "expected version {expected}, but the value changed concurrently"
                )),
                err => WriteError::retryable(err),
            })
    }
}

#[async_trait::async_trait]
impl ProvisionedMetadataStore for ObjectStoreMetadataStore {
    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        Ok(self.read(&key).await?.and_then(|object| object.value))
    }

    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        Ok(self.get(key).await?.map(|value| value.version))
    }

    #[instrument(level = "debug", skip(self, value), err(level = "debug"))]
    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        let object = codec::encode_value(value);
        match precondition {
            Precondition::None => self
                .write(&key, PutMode::Overwrite, &object)
                .await
                .map_err(WriteError::retryable),
            Precondition::DoesNotExist => self.create(&key, &object).await,
            Precondition::MatchesVersion(expected) => {
                self.write_if_version_matches(&key, expected, &object).await
            }
        }
    }

    #[instrument(level = "debug", skip(self), err(level = "debug"))]
    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let tombstone = codec::tombstone();
        match precondition {
            Precondition::None => self
                .write(&key, PutMode::Overwrite, &tombstone)
                .await
                .map_err(WriteError::retryable),
            Precondition::DoesNotExist => Err(WriteError::terminal(NonsensicalPrecondition)),
            Precondition::MatchesVersion(expected) => {
                self.write_if_version_matches(&key, expected, &tombstone)
                    .await
            }
        }
    }
}

fn into_write_error(err: ReadError) -> WriteError {
    match err {
        ReadError::Codec(err) => WriteError::Codec(err),
        ReadError::Other(err) => WriteError::Other(err),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("deleting a key that must not exist does not make sense")]
struct NonsensicalPrecondition;

#[cfg(test)]
mod tests {
    mod version_matching_store;

    use std::time::Duration;

    use bytes::Bytes;
    use restate_types::errors::MaybeRetryableError;

    use self::version_matching_store::VersionMatchingStore;
    use super::*;

    const KEY: ByteString = ByteString::from_static("key");
    const OTHER_KEY: ByteString = ByteString::from_static("other-key");

    fn store() -> (ObjectStoreMetadataStore, Arc<VersionMatchingStore>) {
        let objects = Arc::new(VersionMatchingStore::default());
        (node(&objects), objects)
    }

    /// A store sharing `objects` with others, as the nodes of a cluster do.
    fn node(objects: &Arc<VersionMatchingStore>) -> ObjectStoreMetadataStore {
        ObjectStoreMetadataStore::with_object_store(objects.clone(), Path::default())
    }

    fn value(version: u32) -> VersionedValue {
        VersionedValue::new(Version::from(version), Bytes::from(version.to_string()))
    }

    fn version(version: u32) -> Precondition {
        Precondition::MatchesVersion(Version::from(version))
    }

    #[track_caller]
    fn assert_failed_precondition(result: Result<(), WriteError>) {
        assert!(
            matches!(result, Err(WriteError::FailedPrecondition(_))),
            "expected a failed precondition, got {result:?}"
        );
    }

    async fn current_version(store: &ObjectStoreMetadataStore) -> Option<u32> {
        store
            .get_version(KEY)
            .await
            .unwrap()
            .map(|version| version.into())
    }

    #[tokio::test]
    async fn missing_key() {
        let (store, _) = store();

        assert!(store.get(KEY).await.unwrap().is_none());
        assert!(store.get_version(KEY).await.unwrap().is_none());
        assert_failed_precondition(store.put(KEY, value(2), version(1)).await);
        assert_failed_precondition(store.delete(KEY, version(1)).await);
    }

    #[tokio::test]
    async fn unconditional_writes() {
        let (store, _) = store();

        store.put(KEY, value(1), Precondition::None).await.unwrap();
        store.put(KEY, value(3), Precondition::None).await.unwrap();
        let stored = store.get(KEY).await.unwrap().unwrap();
        assert_eq!(stored.version, Version::from(3));
        assert_eq!(stored.value, value(3).value);

        store.delete(KEY, Precondition::None).await.unwrap();
        assert!(store.get(KEY).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_only_if_absent() {
        let (store, _) = store();

        store
            .put(KEY, value(1), Precondition::DoesNotExist)
            .await
            .unwrap();
        assert_failed_precondition(store.put(KEY, value(2), Precondition::DoesNotExist).await);
        assert_eq!(current_version(&store).await, Some(1));
    }

    #[tokio::test]
    async fn create_after_delete() {
        let (store, _) = store();

        store
            .put(KEY, value(1), Precondition::DoesNotExist)
            .await
            .unwrap();
        store.delete(KEY, Precondition::None).await.unwrap();
        store
            .put(KEY, value(1), Precondition::DoesNotExist)
            .await
            .unwrap();
        assert_eq!(current_version(&store).await, Some(1));
    }

    #[tokio::test]
    async fn write_if_version_matches() {
        let (store, _) = store();
        store
            .put(KEY, value(1), Precondition::DoesNotExist)
            .await
            .unwrap();

        store.put(KEY, value(2), version(1)).await.unwrap();
        assert_failed_precondition(store.put(KEY, value(3), version(1)).await);
        assert_eq!(current_version(&store).await, Some(2));

        assert_failed_precondition(store.delete(KEY, version(1)).await);
        store.delete(KEY, version(2)).await.unwrap();
        assert!(store.get(KEY).await.unwrap().is_none());
        assert_failed_precondition(store.delete(KEY, version(2)).await);
    }

    #[tokio::test]
    async fn delete_rejects_does_not_exist() {
        let (store, _) = store();

        let Err(err) = store.delete(KEY, Precondition::DoesNotExist).await else {
            panic!("a delete that requires the key to be absent should fail");
        };
        assert!(matches!(err, WriteError::Other(_)) && !err.retryable());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_read_modify_writes() {
        const WRITERS: u32 = 256;
        let objects = Arc::new(VersionMatchingStore::default());
        node(&objects)
            .put(KEY, value(0), Precondition::DoesNotExist)
            .await
            .unwrap();

        let mut writers = tokio::task::JoinSet::new();
        for _ in 0..WRITERS {
            let store = node(&objects);
            writers.spawn(async move {
                loop {
                    let current = store.get(KEY).await.unwrap().unwrap().version;
                    let next = value(u32::from(current) + 1);
                    match store
                        .put(KEY, next, Precondition::MatchesVersion(current))
                        .await
                    {
                        Ok(()) => break,
                        Err(WriteError::FailedPrecondition(_)) => continue,
                        Err(err) => panic!("unexpected error: {err}"),
                    }
                }
            });
        }
        writers.join_all().await;

        assert_eq!(current_version(&node(&objects)).await, Some(WRITERS));
    }

    #[tokio::test]
    async fn pending_write_does_not_delay_other_requests() {
        let (store, objects) = store();
        store
            .put(OTHER_KEY, value(1), Precondition::None)
            .await
            .unwrap();

        let paused = objects.pause_writes().await;
        let put = store.put(KEY, value(1), Precondition::None);
        tokio::pin!(put);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                biased;
                _ = &mut put => panic!("the write should wait until writes resume"),
                other = store.get(OTHER_KEY) => assert!(other.unwrap().is_some()),
            }
        })
        .await
        .expect("a read of another key waited for a pending write");

        drop(paused);
        put.await.unwrap();
        assert_eq!(current_version(&store).await, Some(1));
    }

    #[tokio::test]
    async fn rejects_unsupported_metadata_scheme() {
        let Err(err) = ObjectStoreMetadataStore::new(MetadataClientKind::ObjectStore {
            path: "az://bucket/prefix".into(),
            object_store: Default::default(),
            object_store_retry_policy: Default::default(),
        })
        .await
        else {
            panic!("az:// should be rejected");
        };

        assert!(err.to_string().contains("`s3://` and `gs://`"));
    }

    /// Runs the conditional writes the metadata store relies on against a real object store,
    /// such as `gs://bucket/prefix` with credentials in `GOOGLE_APPLICATION_CREDENTIALS`.
    #[ignore = "requires RESTATE_METADATA_TEST_OBJECT_STORE_PATH and credentials for it"]
    #[test_log::test(tokio::test)]
    async fn conditional_writes_against_real_object_store() {
        let path = std::env::var("RESTATE_METADATA_TEST_OBJECT_STORE_PATH")
            .expect("RESTATE_METADATA_TEST_OBJECT_STORE_PATH must be set");
        let configuration = MetadataClientKind::ObjectStore {
            path: format!("{path}/{}", rand::random::<u64>()),
            object_store: Default::default(),
            object_store_retry_policy: Default::default(),
        };
        let store = ObjectStoreMetadataStore::new(configuration).await.unwrap();

        store
            .put(KEY, value(1), Precondition::DoesNotExist)
            .await
            .unwrap();
        assert_failed_precondition(store.put(KEY, value(1), Precondition::DoesNotExist).await);
        let stale = store.read(&KEY).await.unwrap().unwrap().tag;
        store.put(KEY, value(2), version(1)).await.unwrap();

        // The store must refuse a value or a tombstone written on a tag that no longer matches.
        for object in [codec::encode_value(value(3)), codec::tombstone()] {
            assert!(matches!(
                store
                    .write(&KEY, PutMode::Update(stale.clone()), &object)
                    .await,
                Err(Error::Precondition { .. })
            ));
        }
        assert_eq!(current_version(&store).await, Some(2));

        store.delete(KEY, version(2)).await.unwrap();
        assert!(store.get(KEY).await.unwrap().is_none());
        store
            .put(KEY, value(1), Precondition::DoesNotExist)
            .await
            .unwrap();

        store.object_store.delete(&store.path(&KEY)).await.unwrap();
    }
}
