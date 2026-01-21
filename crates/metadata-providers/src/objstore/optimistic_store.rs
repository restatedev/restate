// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use anyhow::Context;
use bilrost::{Message, OwnedMessage};
use bytes::{BufMut, BytesMut};
use bytestring::ByteString;
use rand::random;

use restate_metadata_store::{ReadError, WriteError};
use restate_types::Version;
use restate_types::config::MetadataClientKind;
use restate_types::metadata::{Precondition, VersionedValue};
use tracing::{instrument, trace};

use super::version_repository::VersionRepositoryError::PreconditionFailed;
use super::version_repository::{TaggedValue, VersionRepository, VersionRepositoryError};
use crate::objstore::version_repository::{Content, Tag, ValueEncoding};

pub(crate) struct OptimisticLockingMetadataStoreBuilder {
    pub(crate) version_repository: Box<dyn VersionRepository>,
    pub(crate) configuration: MetadataClientKind,
}

impl OptimisticLockingMetadataStoreBuilder {
    pub(crate) async fn build(self) -> anyhow::Result<OptimisticLockingMetadataStore> {
        let MetadataClientKind::ObjectStore { .. } = self.configuration else {
            anyhow::bail!("unexpected configuration value");
        };
        Ok(OptimisticLockingMetadataStore::new(self.version_repository))
    }
}

pub struct OptimisticLockingMetadataStore {
    version_repository: Box<dyn VersionRepository>,
    arena: BytesMut,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(tag = "version", content = "value")]
enum OnDiskValue<'a> {
    V1(Cow<'a, VersionedValue>, u64),
}

// similar to OnDiskValue, but without serde support.
// and will be used for bilrost encoding.
#[derive(Debug, Clone, bilrost::Message)]
struct SaltedVersionedValue {
    #[bilrost(1)]
    salt: u64,
    #[bilrost(2)]
    value: VersionedValue,
}

#[instrument(level = "trace", skip(tagged_value), err(level = "trace"))]
fn tagged_value_to_versioned_value(
    tagged_value: TaggedValue,
) -> anyhow::Result<(Tag, VersionedValue)> {
    let tag = tagged_value.tag;

    let versioned_value = match tagged_value.content.encoding {
        ValueEncoding::Cbor => {
            let on_disk: OnDiskValue<'static> =
                ciborium::from_reader(tagged_value.content.bytes.as_ref())?;
            match on_disk {
                OnDiskValue::V1(cow, _) => Ok(cow.into_owned()),
            }
        }
        ValueEncoding::Bilrost => SaltedVersionedValue::decode(tagged_value.content.bytes.as_ref())
            .context("failed to decode bilrost")
            .map(|v| v.value),
    }?;

    trace!(
        tag = %tag.as_string(),
        encoding = ?tagged_value.content.encoding,
        input_value_len = tagged_value.content.bytes.len(),
        output_encoded_len = versioned_value.encoded_len(),
        "successfully decoded tagged value"
    );

    Ok((tag, versioned_value))
}

impl OptimisticLockingMetadataStore {
    fn new(version_repository: Box<dyn VersionRepository>) -> Self {
        Self {
            version_repository,
            arena: BytesMut::with_capacity(8196),
        }
    }

    pub(crate) async fn get(
        &mut self,
        key: ByteString,
    ) -> Result<Option<VersionedValue>, ReadError> {
        match self.version_repository.get(key).await {
            Ok(res) => {
                let (_, d) =
                    tagged_value_to_versioned_value(res).map_err(|e| ReadError::Codec(e.into()))?;
                Ok(Some(d))
            }
            Err(VersionRepositoryError::NotFound) => Ok(None),
            Err(e) => Err(ReadError::retryable(e)),
        }
    }

    pub(crate) async fn get_version(
        &mut self,
        key: ByteString,
    ) -> Result<Option<Version>, ReadError> {
        if let Some(res) = self.get(key).await? {
            Ok(Some(res.version))
        } else {
            Ok(None)
        }
    }

    #[instrument(level = "trace", skip(self, versioned_value), err(level = "trace"))]
    fn serialize_versioned_value(
        &mut self,
        encoding: ValueEncoding,
        versioned_value: VersionedValue,
        salt: u64,
    ) -> Result<Content, WriteError> {
        match encoding {
            ValueEncoding::Cbor => {
                self.arena.clear();
                let writer = (&mut self.arena).writer();
                let on_disk = OnDiskValue::V1(Cow::Owned(versioned_value), salt);
                ciborium::into_writer(&on_disk, writer)
                    .map(|_| self.arena.split().freeze())
                    .map_err(|e| WriteError::Codec(e.into()))
            }
            ValueEncoding::Bilrost => {
                self.arena.clear();
                let salted = SaltedVersionedValue {
                    salt,
                    value: versioned_value,
                };

                self.arena.reserve(salted.encoded_len());
                salted
                    .encode(&mut self.arena)
                    .map_err(|err| WriteError::Codec(err.into()))?;
                Ok(self.arena.split().freeze())
            }
        }
        .map(|bytes| Content { encoding, bytes })
    }

    pub(crate) async fn put(
        &mut self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        // The salt is a random value to break CAS ties. S3 CAS works by comparing ETags (body hashes).
        // We want to avoid two writers thinking they both succeeded if they happen to write identical bodies.
        let salt = random::<u64>();

        let content = self.serialize_versioned_value(ValueEncoding::default(), value, salt)?;
        match precondition {
            Precondition::None => {
                self.version_repository
                    .put(key, content)
                    .await
                    .map_err(WriteError::retryable)?;
                Ok(())
            }
            Precondition::DoesNotExist => {
                match self.version_repository.create(key, content).await {
                    Ok(_) => Ok(()),
                    Err(VersionRepositoryError::AlreadyExists) => {
                        Err(WriteError::FailedPrecondition("already exists".to_string()))
                    }
                    Err(e) => Err(WriteError::retryable(e)),
                }
            }
            Precondition::MatchesVersion(version) => {
                // we need to get the current version here, because the version provided by the API does not
                // match the version provided by the object store (ETag vs logical version)
                //
                // 1. get the current logical version and the object store tag.
                //
                let (current_tag, current_version) =
                    match self.version_repository.get(key.clone()).await {
                        Ok(tagged) => {
                            let (tag, versioned_value) = tagged_value_to_versioned_value(tagged)
                                .map_err(|e| WriteError::Codec(e.into()))?;
                            (tag, versioned_value.version)
                        }
                        Err(VersionRepositoryError::NotFound) => {
                            return Err(WriteError::FailedPrecondition(
                                "no current version exists".to_string(),
                            ));
                        }
                        Err(e) => return Err(WriteError::retryable(e)),
                    };
                //
                // 2. check if logical version is the expected version
                //
                if current_version != version {
                    return Err(WriteError::FailedPrecondition(format!(
                        "expected {version} != got {current_version}"
                    )));
                }
                //
                // 3. try compare and set
                //
                match self
                    .version_repository
                    .put_if_tag_matches(key, current_tag, content)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(PreconditionFailed) => Err(WriteError::FailedPrecondition(
                        "failed precondition".to_string(),
                    )),
                    Err(e) => Err(WriteError::retryable(e)),
                }
            }
        }
    }

    pub(crate) async fn delete(
        &mut self,
        key: ByteString,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        match precondition {
            Precondition::None => match self.version_repository.delete(key).await {
                Ok(_) => Ok(()),
                Err(e) => Err(WriteError::retryable(e)),
            },
            Precondition::DoesNotExist => Err(WriteError::terminal(NonSensicalPrecondition)),
            Precondition::MatchesVersion(version) => {
                // we need to convert a version into a tag, this mean we need to do a read first.
                let (tag, current_version) = match self.version_repository.get(key.clone()).await {
                    Ok(res) => {
                        let (tag, versioned_value) = tagged_value_to_versioned_value(res)
                            .map_err(|e| WriteError::Codec(e.into()))?;
                        (tag, versioned_value.version)
                    }
                    Err(VersionRepositoryError::NotFound) => {
                        return Err(WriteError::FailedPrecondition(
                            "No version found".to_string(),
                        ));
                    }
                    Err(e) => return Err(WriteError::retryable(e)),
                };

                if current_version != version {
                    return Err(WriteError::FailedPrecondition(
                        "version mismatch".to_string(),
                    ));
                }

                match self
                    .version_repository
                    .delete_if_tag_matches(key, tag)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(PreconditionFailed) => Err(WriteError::FailedPrecondition(
                        "failed precondition".to_string(),
                    )),
                    Err(e) => Err(WriteError::retryable(e)),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use bytestring::ByteString;
    use restate_metadata_store::WriteError;
    use restate_types::Version;
    use restate_types::metadata::{Precondition, VersionedValue};

    use crate::objstore::object_store_version_repository::ObjectStoreVersionRepository;
    use crate::objstore::optimistic_store::{
        OptimisticLockingMetadataStore, tagged_value_to_versioned_value,
    };
    use crate::objstore::version_repository::{Content, Tag, TaggedValue, ValueEncoding};

    const KEY_1: ByteString = ByteString::from_static("1");
    const HELLO: Bytes = Bytes::from_static(b"hello");

    #[tokio::test]
    async fn basic_example() {
        let mut store = OptimisticLockingMetadataStore::new(Box::new(
            ObjectStoreVersionRepository::new_for_testing(),
        ));

        store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_if_absent() {
        let mut store = OptimisticLockingMetadataStore::new(Box::new(
            ObjectStoreVersionRepository::new_for_testing(),
        ));

        store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn put_if_absent_should_fail() {
        let mut store = OptimisticLockingMetadataStore::new(Box::new(
            ObjectStoreVersionRepository::new_for_testing(),
        ));

        store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();

        match store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::DoesNotExist,
            )
            .await
        {
            Err(WriteError::FailedPrecondition(_)) => {
                // ok
            }
            _ => {
                panic!("Expected WriteError::FailedPrecondition");
            }
        }
    }

    #[tokio::test]
    async fn put_if_absent_on_deleted_value() {
        let mut store = OptimisticLockingMetadataStore::new(Box::new(
            ObjectStoreVersionRepository::new_for_testing(),
        ));

        store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();
        store.delete(KEY_1, Precondition::None).await.unwrap();

        store
            .put(
                KEY_1,
                VersionedValue::new(Version::MIN.next(), HELLO),
                Precondition::DoesNotExist,
            )
            .await
            .unwrap();
    }

    #[test]
    fn test_encoding() {
        let mut store = OptimisticLockingMetadataStore::new(Box::new(
            ObjectStoreVersionRepository::new_for_testing(),
        ));

        let value = VersionedValue::new(Version::MIN.next(), HELLO);

        let buf = store
            .serialize_versioned_value(ValueEncoding::Cbor, value, 0)
            .unwrap();

        let tagged_value = TaggedValue {
            tag: Tag::from("test".to_string()),
            content: Content {
                encoding: buf.encoding,
                bytes: buf.bytes,
            },
        };

        let (tag, versioned_value) = tagged_value_to_versioned_value(tagged_value).unwrap();
        assert_eq!(tag, Tag::from("test".to_string()));
        assert_eq!(versioned_value.value, HELLO);

        let value = VersionedValue::new(Version::MIN.next(), HELLO);

        let buf = store
            .serialize_versioned_value(ValueEncoding::Bilrost, value, 0)
            .unwrap();

        let tagged_value = TaggedValue {
            tag: Tag::from("test".to_string()),
            content: Content {
                encoding: buf.encoding,
                bytes: buf.bytes,
            },
        };

        let (tag, versioned_value) = tagged_value_to_versioned_value(tagged_value).unwrap();
        assert_eq!(tag, Tag::from("test".to_string()));
        assert_eq!(versioned_value.value, HELLO);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("This combination does not make sense")]
struct NonSensicalPrecondition;
