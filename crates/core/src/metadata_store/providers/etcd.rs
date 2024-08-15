use crate::metadata_store::{
    MetadataStore, Precondition, ReadError, Version, VersionedValue, WriteError,
};
use anyhow::Context;
use bytestring::ByteString;
use etcd_client::{Client, GetOptions};
use std::sync::Arc;
use tokio::sync::Mutex;

impl From<etcd_client::Error> for ReadError {
    fn from(value: etcd_client::Error) -> Self {
        Self::Network(value.into())
    }
}

pub struct EtcdMetadataStore {
    client: Client,
}

impl EtcdMetadataStore {
    pub async fn new<S: AsRef<[A]>, A: AsRef<str>>(addresses: S) -> anyhow::Result<Self> {
        //todo: maybe expose some of the connection options to the node config
        let client = Client::connect(addresses, None)
            .await
            .context("failed to connect to etcd cluster")?;

        Ok(Self { client })
    }
}

#[async_trait::async_trait]
impl MetadataStore for EtcdMetadataStore {
    /// Gets the value and its current version for the given key. If key-value pair is not present,
    /// then return [`None`].
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let mut client = self.client.kv_client();
        let mut response = client.get(key.into_bytes(), None).await?;

        for kv in response.take_kvs() {
            // return first value because this suppose to be an exact match
            // not a scan
            let version = kv.version();
            let (_, value) = kv.into_key_value();
            return Ok(Some(VersionedValue::new(
                // todo: do we expand the version type to i64/u64?
                Version::from(version as u32),
                value.into(),
            )));
        }

        Ok(None)
    }

    /// Gets the current version for the given key. If key-value pair is not present, then return
    /// [`None`].
    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let mut client = self.client.kv_client();
        let response = client
            .get(
                key.into_bytes(),
                Some(GetOptions::default().with_keys_only().with_limit(1)),
            )
            .await?;

        for kv in response.kvs() {
            return Ok(Some(Version::from(kv.version() as u32)));
        }

        return Ok(None);
    }

    /// Puts the versioned value under the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        unimplemented!()
    }

    /// Deletes the key-value pair for the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        unimplemented!()
    }
}
