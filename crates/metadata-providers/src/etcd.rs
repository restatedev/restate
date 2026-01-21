// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, Error as EtcdError, GetOptions, KvClient, Txn,
    TxnOp,
};

use restate_metadata_store::ProvisionedMetadataStore;
use restate_metadata_store::{ReadError, WriteError};
use restate_types::Version;
use restate_types::config::MetadataClientOptions;
use restate_types::errors::GenericError;
use restate_types::metadata::{Precondition, VersionedValue};
use restate_types::net::connect_opts::CommonClientConnectionOptions;

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
struct Error(#[from] EtcdError);

impl From<Error> for ReadError {
    fn from(value: Error) -> Self {
        match value.0 {
            err @ EtcdError::IoError(_) => Self::retryable(err),
            err @ EtcdError::TransportError(_) => Self::retryable(err),
            any => Self::terminal(any),
        }
    }
}

impl From<Error> for WriteError {
    fn from(value: Error) -> Self {
        match value.0 {
            err @ EtcdError::IoError(_) => Self::retryable(err),
            err @ EtcdError::TransportError(_) => Self::retryable(err),
            any => Self::terminal(any),
        }
    }
}

trait FromIntoVecU8: Sized {
    fn to_vec(&self) -> Vec<u8>;
    fn from_slice(v: &[u8]) -> Result<Self, GenericError>;
}

impl FromIntoVecU8 for Version {
    fn to_vec(&self) -> Vec<u8> {
        Vec::from_iter(u32::from(*self).to_le_bytes())
    }

    fn from_slice(v: &[u8]) -> Result<Self, GenericError> {
        if v.len() != size_of::<u32>() {
            return Err("Invalid length".into());
        }

        let mut buf: [u8; size_of::<u32>()] = [0; size_of::<u32>()];
        buf.copy_from_slice(v);
        Ok(u32::from_le_bytes(buf).into())
    }
}

#[derive(Clone)]
pub struct EtcdMetadataStore {
    client: Client,
}

impl EtcdMetadataStore {
    pub async fn new<S: AsRef<[A]>, A: AsRef<str>>(
        addresses: S,
        metadata_options: &MetadataClientOptions,
    ) -> anyhow::Result<Self> {
        let opts = ConnectOptions::new()
            .with_connect_timeout(metadata_options.connect_timeout())
            .with_keep_alive(
                metadata_options.keep_alive_interval(),
                metadata_options.connect_timeout(),
            );
        let client = Client::connect(addresses, Some(opts))
            .await
            .context("failed to connect to etcd cluster")?;

        Ok(Self { client })
    }

    /// deletes a key only if and only if the current value in store
    /// has the exact given version
    async fn delete_if_version_matches(
        &self,
        client: &mut KvClient,
        key: Bytes,
        version: Option<Version>,
    ) -> Result<bool, WriteError> {
        let version_key = Self::version_key(&key);

        let when = match version {
            None => {
                vec![Compare::version(version_key.clone(), CompareOp::Equal, 0)]
            }
            Some(version) => {
                vec![Compare::value(
                    version_key.clone(),
                    CompareOp::Equal,
                    version.to_vec(),
                )]
            }
        };

        let txn = Txn::new().when(when).and_then(vec![
            TxnOp::delete(key, None),
            TxnOp::delete(version_key, None),
        ]);

        let response = client.txn(txn).await.map_err(Error)?;

        Ok(response.succeeded())
    }

    async fn put_force(
        &self,
        client: &mut KvClient,
        key: Bytes,
        value: VersionedValue,
    ) -> Result<(), WriteError> {
        let version_key = Self::version_key(&key);

        let txn = Txn::new().and_then(vec![
            TxnOp::put(key, value.value, None),
            TxnOp::put(version_key, value.version.to_vec(), None),
        ]);

        client.txn(txn).await.map_err(Error)?;

        Ok(())
    }

    async fn delete_force(&self, client: &mut KvClient, key: Bytes) -> Result<(), WriteError> {
        let version_key = Self::version_key(&key);

        let txn = Txn::new().and_then(vec![
            TxnOp::delete(key, None),
            TxnOp::delete(version_key, None),
        ]);

        client.txn(txn).await.map_err(Error)?;

        Ok(())
    }

    /// puts a key/value if and only if the current value in store
    /// has the exact given version
    async fn put_if_version_matches(
        &self,
        client: &mut KvClient,
        key: Bytes,
        value: VersionedValue,
        version: Option<Version>,
    ) -> Result<bool, WriteError> {
        let version_key = Self::version_key(&key);

        let when = match version {
            None => {
                vec![Compare::version(version_key.clone(), CompareOp::Equal, 0)]
            }
            Some(version) => {
                vec![Compare::value(
                    version_key.clone(),
                    CompareOp::Equal,
                    version.to_vec(),
                )]
            }
        };

        let txn = Txn::new().when(when).and_then(vec![
            TxnOp::put(key, value.value, None),
            TxnOp::put(version_key, value.version.to_vec(), None),
        ]);

        let response = client.txn(txn).await.map_err(Error)?;

        Ok(response.succeeded())
    }

    fn version_key(key: &Bytes) -> Bytes {
        const KEY_SUFFIX: &[u8] = b"::version";
        let mut version_key = BytesMut::with_capacity(key.len() + KEY_SUFFIX.len());
        version_key.extend_from_slice(key);
        version_key.extend_from_slice(KEY_SUFFIX);
        version_key.into()
    }
}

#[async_trait::async_trait]
impl ProvisionedMetadataStore for EtcdMetadataStore {
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let mut client = self.client.kv_client();
        let key = key.into_bytes();
        let version_key = Self::version_key(&key);
        let options = GetOptions::new()
            .with_range(version_key.clone())
            .with_prefix();

        let mut response = client
            .get(key.clone(), Some(options))
            .await
            .map_err(Error)?;

        let mut version = None;
        let mut value = None;
        for kv in response.take_kvs().into_iter() {
            let (k, v) = kv.into_key_value();
            if k == key {
                value = Some(v);
            } else if k == version_key {
                version = Some(Version::from_slice(&v).map_err(ReadError::Codec)?);
            }

            if value.is_some() && version.is_some() {
                break;
            }
        }

        if value.is_none() || version.is_none() {
            return Ok(None);
        }

        Ok(Some(VersionedValue::new(
            version.unwrap(),
            value.unwrap().into(),
        )))
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let mut client = self.client.kv_client();
        let key = key.into_bytes();
        let version_key = Self::version_key(&key);

        let mut response = client.get(version_key, None).await.map_err(Error)?;

        // return first value because this suppose to be an exact match
        // not a scan
        let kv = match response.take_kvs().into_iter().next() {
            None => return Ok(None),
            Some(kv) => kv,
        };

        let version = Version::from_slice(kv.value()).map_err(ReadError::Codec)?;

        Ok(Some(version))
    }

    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        let mut client = self.client.kv_client();
        match precondition {
            Precondition::None => self.put_force(&mut client, key.into_bytes(), value).await?,
            Precondition::DoesNotExist => {
                if !self
                    .put_if_version_matches(&mut client, key.into_bytes(), value, None)
                    .await?
                {
                    // pre condition failed.
                    return Err(WriteError::FailedPrecondition(
                        "key-value pair exists".into(),
                    ));
                };
            }
            Precondition::MatchesVersion(version) => {
                if !self
                    .put_if_version_matches(&mut client, key.into_bytes(), value, Some(version))
                    .await?
                {
                    return Err(WriteError::FailedPrecondition(
                        "key version mismatch".into(),
                    ));
                };
            }
        }

        Ok(())
    }

    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let mut client = self.client.kv_client();
        match precondition {
            Precondition::None => {
                self.delete_force(&mut client, key.into_bytes()).await?;
            }
            Precondition::DoesNotExist => {
                if !self
                    .delete_if_version_matches(&mut client, key.into_bytes(), None)
                    .await?
                {
                    // pre condition failed.
                    return Err(WriteError::FailedPrecondition(
                        "key-value pair exists".into(),
                    ));
                };
            }
            Precondition::MatchesVersion(version) => {
                if !self
                    .delete_if_version_matches(&mut client, key.into_bytes(), Some(version))
                    .await?
                {
                    return Err(WriteError::FailedPrecondition(
                        "key version mismatch".into(),
                    ));
                };
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use bytestring::ByteString;
    use restate_metadata_store::{MetadataStore, WriteError};
    use restate_types::{
        Version, config::MetadataClientOptions, metadata::Precondition, metadata::VersionedValue,
    };

    use super::EtcdMetadataStore;

    // todo(azmy): right now the test relies on a running etcd instance in the test environment.
    // which is not available atm.
    // this is why all tests below are [ignored] for now and should be enabled only if
    // etcd is started in the test env, or when testing locally
    const TEST_ADDRESS: [&str; 1] = ["127.0.0.1:32772"];

    #[ignore]
    #[tokio::test]
    async fn test_put_does_not_exist() {
        let opts = MetadataClientOptions::default();
        let client = EtcdMetadataStore::new(&TEST_ADDRESS, &opts).await.unwrap();

        let key: ByteString = "put_does_not_exist".into();
        // make sure key doesn't exist
        client
            .delete(key.clone(), Precondition::None)
            .await
            .unwrap();

        let value = VersionedValue {
            version: Version::MIN,
            value: Bytes::new(),
        };

        client
            .put(key.clone(), value, Precondition::DoesNotExist)
            .await
            .unwrap();

        let result = client.get(key.clone()).await.unwrap();
        assert!(matches!(result, Some(value) if value.version == Version::MIN));
    }

    #[ignore]
    #[tokio::test]
    async fn test_put_with_version() {
        let opts = MetadataClientOptions::default();
        let client = EtcdMetadataStore::new(&TEST_ADDRESS, &opts).await.unwrap();

        let key: ByteString = "put_with_version".into();
        // make sure key doesn't exist
        client
            .delete(key.clone(), Precondition::None)
            .await
            .unwrap();

        let value = VersionedValue {
            version: 5.into(),
            value: Bytes::new(),
        };

        client
            .put(key.clone(), value, Precondition::DoesNotExist)
            .await
            .unwrap();

        let result = client.get(key.clone()).await.unwrap();
        assert!(result.is_some());
        let value = result.unwrap();
        assert_eq!(value.version, 5.into());

        let value = VersionedValue {
            version: 5.into(),
            value: Bytes::new(),
        };

        let result = client
            .put(key.clone(), value, Precondition::MatchesVersion(4.into()))
            .await;

        assert!(matches!(result, Err(WriteError::FailedPrecondition(_))));

        let value = VersionedValue {
            version: 6.into(),
            value: Bytes::new(),
        };

        client
            .put(key.clone(), value, Precondition::MatchesVersion(5.into()))
            .await
            .unwrap();

        let result = client.get(key.clone()).await.unwrap();
        assert!(result.is_some());
        let value = result.unwrap();
        assert_eq!(value.version, 6.into());

        let version = client.get_version(key.clone()).await.unwrap();
        assert!(matches!(version, Some(v) if v == Version::from(6)));
    }

    #[ignore]
    #[tokio::test]
    async fn test_put_force() {
        let opts = MetadataClientOptions::default();
        let client = EtcdMetadataStore::new(&TEST_ADDRESS, &opts).await.unwrap();

        let key: ByteString = "put_force".into();
        // make sure key doesn't exist
        client
            .delete(key.clone(), Precondition::None)
            .await
            .unwrap();

        let value = VersionedValue {
            version: 3.into(),
            value: Bytes::new(),
        };

        client
            .put(key.clone(), value, Precondition::None)
            .await
            .unwrap();

        let result = client.get(key.clone()).await.unwrap();
        assert!(result.is_some());
        let value = result.unwrap();
        assert_eq!(value.version, 3.into());

        let value = VersionedValue {
            version: 5.into(),
            value: Bytes::new(),
        };

        client
            .put(key.clone(), value, Precondition::None)
            .await
            .unwrap();

        let result = client.get(key.clone()).await.unwrap();
        assert!(result.is_some());
        let value = result.unwrap();
        assert_eq!(value.version, 5.into());
    }

    #[ignore]
    #[tokio::test]
    async fn test_delete() {
        let opts = MetadataClientOptions::default();
        let client = EtcdMetadataStore::new(&TEST_ADDRESS, &opts).await.unwrap();

        let key: ByteString = "put_delete_me".into();
        // make sure key doesn't exist
        client
            .delete(key.clone(), Precondition::None)
            .await
            .unwrap();

        let value = VersionedValue {
            version: Version::MIN,
            value: Bytes::new(),
        };

        client
            .put(key.clone(), value, Precondition::DoesNotExist)
            .await
            .unwrap();

        let result = client.get(key.clone()).await.unwrap();
        assert!(matches!(result, Some(value) if value.version == Version::MIN));

        let result = client
            .delete(key.clone(), Precondition::MatchesVersion(2.into()))
            .await;
        assert!(matches!(result, Err(WriteError::FailedPrecondition(_))));

        let result = client
            .delete(key.clone(), Precondition::MatchesVersion(Version::MIN))
            .await;
        assert!(result.is_ok());
    }
}
