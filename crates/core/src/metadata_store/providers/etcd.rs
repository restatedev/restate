use crate::metadata_store::{
    MetadataStore, Precondition, ReadError, Version, VersionedValue, WriteError,
};
use anyhow::Context;
use bytestring::ByteString;
use etcd_client::{Client, GetOptions, KeyValue};

impl From<etcd_client::Error> for ReadError {
    fn from(value: etcd_client::Error) -> Self {
        Self::Network(value.into())
    }
}

//todo: version of the kv is reset to 1 if the key was deleted then recrated.
// this means that a key can change (by means of deletion and recreation) and will
// always have version 1!
// The only way to detect this is to also track the "mod revision" of the store as part
// of the VersionValue.
// The problem is that the restate Version is only u32 while both etcd version and mod version
// are both i64.
//
// What this implementation tries to do is to fit both mod_revision and version in a u32.
// This will work until any of the 2 values is actually bigger than u16::MAX which is not that much
//
// Changing the Version to have a u64 value also delays the problem for later since it will be a while before
// mod_revision or version hit the u32::MAX but that's totally dependent on how frequent the changes are
//
// The correct solution is of course to make Version u128.
trait ToVersion {
    fn to_version(&self) -> Version;
}

impl ToVersion for &KeyValue {
    fn to_version(&self) -> Version {
        Version::from((self.mod_revision() as u32) << 16 | self.version() as u32)
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

            // please read todo! on implementation of .to_version()
            let version = (&kv).to_version();
            let (_, value) = kv.into_key_value();
            return Ok(Some(VersionedValue::new(version, value.into())));
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
            return Ok(Some(kv.to_version()));
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
        // let txt = Txn::new().when(compares)
        unimplemented!()
    }

    /// Deletes the key-value pair for the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        unimplemented!()
    }
}
