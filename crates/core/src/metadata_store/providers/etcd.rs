use crate::metadata_store::{
    MetadataStore, Precondition, ReadError, Version, VersionedValue, WriteError,
};
use anyhow::Context;
use bytes::Bytes;
use bytestring::ByteString;
use etcd_client::{Client, Compare, CompareOp, KeyValue, KvClient, Txn, TxnOp, TxnOpResponse};

impl From<etcd_client::Error> for ReadError {
    fn from(value: etcd_client::Error) -> Self {
        Self::Network(value.into())
    }
}

impl From<etcd_client::Error> for WriteError {
    fn from(value: etcd_client::Error) -> Self {
        Self::Network(value.into())
    }
}

//todo: version of the kv is reset to 1 if the key was deleted then recrated.
// this means that a key can change (by means of deletion and recreation) and will
// always have version 1!
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
// What we do here is ONLY relying on the "version" of the key combined with the fact that we never actually delete the key
// but instead put a "tombstone" in place so version doesn't reset
trait ToVersion {
    fn to_version(self) -> Result<Version, ReadError>;
}

impl ToVersion for &KeyValue {
    fn to_version(self) -> Result<Version, ReadError> {
        if self.version() > u32::MAX as i64 {
            return Err(ReadError::Codec(
                "[etcd] key version exceeds max u32".into(),
            ));
        }

        Ok(Version::from(self.version() as u32))
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

    // put updates the key if and only if the key does not exist
    // and because we never actually delete the key there is a possibility
    // that the key exists, but it's value is nilled (that still considered "does not exist").
    // this is why we run this complex transaction where we
    // - check if the key exists but empty (value is empty bytes)
    // - otherwise, we run an (or_else) branch where itself is a transaction
    //   where we check if the key actually does not exist at all!
    // - Then do a PUT
    pub async fn put_not_exist(
        &self,
        client: &mut KvClient,
        key: Bytes,
        value: Bytes,
    ) -> Result<bool, WriteError> {
        // an or_else branch is executed if the and_then branch of the
        // root transaction was not executed (failed condition)
        // this means that checked if the value exists but empty always
        // done first. Otherwise we check if the value does not exist at all
        let or_else = Txn::new()
            .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then(vec![TxnOp::put(key.clone(), value.clone(), None)]);

        let txn = Txn::new()
            .when(vec![Compare::value(
                key.clone(),
                CompareOp::Equal,
                Vec::default(),
            )])
            .and_then(vec![TxnOp::put(key, value, None)])
            .or_else(vec![TxnOp::txn(or_else)]);
        let response = client.txn(txn).await?;

        // to check if the put operation has actually been "executed"
        // we need to check the success of the individual branches.
        // and we can't really rely on the response.success() output
        // the reason is if the or_else branch was executed this is
        // still considered a failure, while in our case that can still
        // be a success, unless the or_else branch itself failed as well
        if let Some(resp) = response.op_responses().into_iter().next() {
            match resp {
                TxnOpResponse::Txn(txn) => {
                    // this means we had to enter the else branch
                    // and had to run the or_else transaction
                    return Ok(txn.succeeded());
                }
                TxnOpResponse::Put(_) => {
                    return Ok(response.succeeded());
                }
                _ => {
                    unreachable!()
                }
            }
        }
        Ok(response.succeeded())
    }

    // only update the key if has the exact given version
    // otherwise returns false
    pub async fn put_with_version(
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
    /// Gets the value and its current version for the given key. If key-value pair is not present,
    /// then return [`None`].
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let mut client = self.client.kv_client();
        let mut response = client.get(key.into_bytes(), None).await?;

        // return first value because this suppose to be an exact match
        // not a scan
        let kv = match response.take_kvs().into_iter().next() {
            None => return Ok(None),
            Some(kv) => kv,
        };

        // please read todo! on implementation of .to_version()
        let version = (&kv).to_version()?;
        let (_, value) = kv.into_key_value();

        if value.is_empty() {
            // we keep an empty value in place of
            // deleted keys to work around the version limitation
            return Ok(None);
        }

        Ok(Some(VersionedValue::new(version, value.into())))
    }

    /// Gets the current version for the given key. If key-value pair is not present, then return
    /// [`None`].
    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let mut client = self.client.kv_client();
        let mut response = client.get(key.into_bytes(), None).await?;

        // return first value because this suppose to be an exact match
        // not a scan
        let kv = match response.take_kvs().into_iter().next() {
            None => return Ok(None),
            Some(kv) => kv,
        };

        // please read todo! on implementation of .to_version()
        let version = (&kv).to_version()?;
        let (_, value) = kv.into_key_value();

        if value.is_empty() {
            // we keep an empty value in place of deleted
            // keys to work around the version limitation
            return Ok(None);
        }

        Ok(Some(version))
    }

    /// Puts the versioned value under the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    ///
    /// NOTE: this implementation disregard that version attached on the value and depends only on the
    /// auto version provided by etcd
    ///
    /// it's up to the caller of this function to make sure that both versions are in sync.
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
                    .put_not_exist(&mut client, key.into_bytes(), value.value)
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
                    .put_with_version(
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

    /// Deletes the key-value pair for the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let mut client = self.client.kv_client();
        match precondition {
            Precondition::None => {
                client.delete(key.into_bytes(), None).await?;
            }
            Precondition::DoesNotExist => {
                if !self
                    .put_not_exist(&mut client, key.into_bytes(), Vec::default().into())
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
                    .put_with_version(
                        &mut client,
                        key.into_bytes(),
                        Vec::default().into(),
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
