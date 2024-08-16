use crate::metadata_store::{
    MetadataStore, Precondition, ReadError, Version, VersionedValue, WriteError,
};
use anyhow::Context;
use bytes::Bytes;
use bytestring::ByteString;
use etcd_client::{
    Client, Compare, CompareOp, Error as EtcdError, GetOptions, KeyValue, KvClient, Txn, TxnOp,
};

impl From<EtcdError> for ReadError {
    fn from(value: EtcdError) -> Self {
        match value {
            err @ EtcdError::IoError(_) => Self::Network(err.into()),
            err @ EtcdError::TransportError(_) => Self::Network(err.into()),
            any => Self::Store(any.into()),
        }
    }
}

impl From<EtcdError> for WriteError {
    fn from(value: EtcdError) -> Self {
        match value {
            err @ EtcdError::IoError(_) => Self::Network(err.into()),
            err @ EtcdError::TransportError(_) => Self::Network(err.into()),
            any => Self::Store(any.into()),
        }
    }
}

trait ToVersion {
    fn to_version(self) -> Result<Version, ReadError>;
}

impl ToVersion for &KeyValue {
    fn to_version(self) -> Result<Version, ReadError> {
        //todo: version of the kv is reset to 1 if the key was deleted then recrated.
        // this means that a key can change (by means of deletion and recreation) and will
        // always have version 1!
        //
        // The only way to detect this is to also track the "mod revision" of the store as part
        // of the VersionValue.
        //
        // The problem is that the restate Version is only u32 while both etcd version and mod version
        // are both i64.
        //
        // Changing the Version to have a u64 value also delays the problem for later since it will be a while before
        // mod_revision or version hit the u32::MAX but that's totally dependent on how frequent the changes are
        //
        // The correct solution is of course to make Version u128.
        //
        // What is implemented instead in current code is to use the lower 32bit of the Etcd version. We return an error
        // if this value exceeds the u32::MAX.
        // Objects will be deleted normally (it means version will reset) so it's up to the user of the store to make sure
        // to use a tombstone instead of relying on actual delete (if needed).
        //
        // This is done instead of implementing the tombstone mechanism directly into the the Etcd store implementation because
        // delete is not used right now, and also because the Etcd implementation is a temporary solution.
        let version = Version::from(u32::try_from(self.version()).map_err(|e| {
            ReadError::Internal(format!("[etcd] key version exceeds max u32: {}", e))
        })?);

        Ok(version)
    }
}

#[derive(Clone)]
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

    /// deletes a key only if and only if the current value in store
    /// has the exact given version
    async fn delete_if_version_matches(
        &self,
        client: &mut KvClient,
        key: Bytes,
        version: i64,
    ) -> Result<bool, WriteError> {
        let txn = Txn::new()
            .when(vec![Compare::version(
                key.clone(),
                CompareOp::Equal,
                version,
            )])
            .and_then(vec![TxnOp::delete(key, None)]);

        let response = client.txn(txn).await?;

        Ok(response.succeeded())
    }

    /// puts a key/value if and only if the current value in store
    /// has the exact given version
    async fn put_if_version_matches(
        &self,
        client: &mut KvClient,
        key: Bytes,
        value: Bytes,
        version: i64,
    ) -> Result<bool, WriteError> {
        let txn = Txn::new()
            .when(vec![Compare::version(
                key.clone(),
                CompareOp::Equal,
                version,
            )])
            .and_then(vec![TxnOp::put(key, value, None)]);

        let response = client.txn(txn).await?;

        Ok(response.succeeded())
    }
}

#[async_trait::async_trait]
impl MetadataStore for EtcdMetadataStore {
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let mut client = self.client.kv_client();
        let mut response = client.get(key.into_bytes(), None).await?;

        // return first value because this is supposed to be an exact match
        // not a scan
        let kv = match response.take_kvs().into_iter().next() {
            None => return Ok(None),
            Some(kv) => kv,
        };

        // please read todo! on implementation of .to_version()
        let version = (&kv).to_version()?;
        let (_, value) = kv.into_key_value();

        Ok(Some(VersionedValue::new(version, value.into())))
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let mut client = self.client.kv_client();
        let mut response = client
            .get(key.into_bytes(), Some(GetOptions::new().with_keys_only()))
            .await?;

        // return first value because this suppose to be an exact match
        // not a scan
        let kv = match response.take_kvs().into_iter().next() {
            None => return Ok(None),
            Some(kv) => kv,
        };

        // please read todo! on implementation of .to_version()
        let version = (&kv).to_version()?;

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
            Precondition::None => {
                client.put(key.into_bytes(), value.value, None).await?;
            }
            Precondition::DoesNotExist => {
                if !self
                    .put_if_version_matches(&mut client, key.into_bytes(), value.value, 0)
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
                    .put_if_version_matches(
                        &mut client,
                        key.into_bytes(),
                        value.value,
                        u32::from(version) as i64,
                    )
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
                client.delete(key.into_bytes(), None).await?;
            }
            Precondition::DoesNotExist => {
                if !self
                    .delete_if_version_matches(&mut client, key.into_bytes(), 0)
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
                    .delete_if_version_matches(
                        &mut client,
                        key.into_bytes(),
                        u32::from(version) as i64,
                    )
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
