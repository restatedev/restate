// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::{
    config::Credentials,
    error::SdkError,
    operation::{delete_item::DeleteItemError, put_item::PutItemError},
    primitives::Blob,
    types::{AttributeValue, ReturnConsumedCapacity, ReturnValue},
};
use bytestring::ByteString;

use restate_metadata_store::{ProvisionedMetadataStore, ReadError, WriteError};
use restate_types::{
    Version,
    config::DynamoDbOptions,
    metadata::{Precondition, VersionedValue},
};

const KEY_FIELD: &str = "pk";
const VERSION_FIELD: &str = "version";
const DATA_FIELD: &str = "data";

const CONDITION_DOES_NOT_EXIST: &str =
    const_format::concatcp!("attribute_not_exists(", KEY_FIELD, ")");

#[derive(Debug, thiserror::Error)]
pub enum DynamoDbStoreCodecError {
    #[error("Missing field {0}")]
    MissingField(&'static str),
    #[error("Field {0} has wrong type")]
    InvalidType(&'static str),
    #[error("Field {0} has an invalid format")]
    InvalidFormat(&'static str),
}

impl From<DynamoDbStoreCodecError> for ReadError {
    fn from(value: DynamoDbStoreCodecError) -> Self {
        Self::Codec(value.into())
    }
}
pub struct DynamoDbMetadataStore {
    table: String,
    key_prefix: Option<String>,
    client: aws_sdk_dynamodb::Client,
}

impl DynamoDbMetadataStore {
    pub async fn new(
        table: impl Into<String>,
        key_prefix: Option<String>,
        dynamo_db_options: DynamoDbOptions,
    ) -> anyhow::Result<Self> {
        let cfg = aws_config::defaults(BehaviorVersion::latest());

        let cfg = match (
            dynamo_db_options.aws_profile,
            dynamo_db_options.aws_access_key_id,
        ) {
            // loaded from env
            (None, None) => cfg,
            (Some(_), Some(_)) => anyhow::bail!(
                "Either `aws-profile` or `aws-access-key-id` must be provided for dynamo-db metadata provider, but not both"
            ),
            (Some(profile), None) => cfg.profile_name(profile),
            (None, Some(access_key_id)) => {
                let secret_access_key =
                    dynamo_db_options.aws_secret_access_key.ok_or_else(|| {
                        anyhow::anyhow!(
                            "Missing aws-secret-access-key for dynamo-db metadata provider"
                        )
                    })?;

                let cred = Credentials::new(
                    access_key_id,
                    secret_access_key,
                    dynamo_db_options.aws_session_token,
                    None,
                    "config-file",
                );
                cfg.credentials_provider(cred)
            }
        };

        let cfg = if let Some(region) = dynamo_db_options.aws_region {
            cfg.region(Region::new(region))
        } else {
            cfg
        };

        let cfg = if let Some(url) = dynamo_db_options.aws_endpoint_url {
            cfg.endpoint_url(url)
        } else {
            cfg
        };

        let sdk_cfg = cfg.load().await;

        let client = aws_sdk_dynamodb::Client::new(&sdk_cfg);

        Ok(Self {
            table: table.into(),
            key_prefix,
            client,
        })
    }

    #[cfg(test)]
    pub async fn new_test_store(table: impl Into<String>, url: impl Into<String>) -> Self {
        let cfg = aws_config::defaults(BehaviorVersion::latest())
            .test_credentials()
            .endpoint_url(url)
            .region(Region::from_static("fake"))
            .empty_test_environment()
            .load()
            .await;

        Self {
            table: table.into(),
            key_prefix: None,
            client: aws_sdk_dynamodb::Client::new(&cfg),
        }
    }

    fn prefixed(&self, key: ByteString) -> String {
        match &self.key_prefix {
            Some(prefix) => {
                let mut buf = String::with_capacity(prefix.len() + key.len());
                buf.push_str(prefix);
                buf.push_str(&key);
                buf
            }
            None => key.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl ProvisionedMetadataStore for DynamoDbMetadataStore {
    /// Gets the value and its current version for the given key. If key-value pair is not present,
    /// then return [`None`].
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let key = self.prefixed(key);
        let result = self
            .client
            .get_item()
            .table_name(&self.table)
            .return_consumed_capacity(ReturnConsumedCapacity::None)
            .consistent_read(true)
            .key(KEY_FIELD, AttributeValue::S(key))
            .send()
            .await
            .map_err(sdk_err_into_read_err)?;

        let Some(mut item) = result.item else {
            return Ok(None);
        };

        let AttributeValue::N(version_str) = item
            .remove(VERSION_FIELD)
            .ok_or(DynamoDbStoreCodecError::MissingField(VERSION_FIELD))?
        else {
            return Err(DynamoDbStoreCodecError::InvalidType(VERSION_FIELD).into());
        };

        let version: u32 = version_str
            .parse()
            .map_err(|_| DynamoDbStoreCodecError::InvalidFormat(VERSION_FIELD))?;

        let AttributeValue::B(data) = item
            .remove(DATA_FIELD)
            .ok_or(DynamoDbStoreCodecError::MissingField(DATA_FIELD))?
        else {
            return Err(DynamoDbStoreCodecError::InvalidType(DATA_FIELD).into());
        };

        Ok(Some(VersionedValue {
            version: version.into(),
            value: data.into_inner().into(),
        }))
    }

    /// Gets the current version for the given key. If key-value pair is not present, then return
    /// [`None`].
    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let key = self.prefixed(key);
        let result = self
            .client
            .get_item()
            .table_name(&self.table)
            .return_consumed_capacity(ReturnConsumedCapacity::None)
            .attributes_to_get(VERSION_FIELD)
            .consistent_read(true)
            .key(KEY_FIELD, AttributeValue::S(key))
            .send()
            .await
            .map_err(sdk_err_into_read_err)?;

        let Some(mut item) = result.item else {
            return Ok(None);
        };

        let AttributeValue::N(version_str) = item
            .remove(VERSION_FIELD)
            .ok_or(DynamoDbStoreCodecError::MissingField(VERSION_FIELD))?
        else {
            return Err(DynamoDbStoreCodecError::InvalidType(VERSION_FIELD).into());
        };

        let version: u32 = version_str
            .parse()
            .map_err(|_| DynamoDbStoreCodecError::InvalidFormat(VERSION_FIELD))?;

        Ok(Some(version.into()))
    }

    /// Puts the versioned value under the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        let key = self.prefixed(key);
        let put = self
            .client
            .put_item()
            .table_name(&self.table)
            .return_consumed_capacity(ReturnConsumedCapacity::None)
            .return_values(ReturnValue::None)
            .item(KEY_FIELD, AttributeValue::S(key))
            .item(
                VERSION_FIELD,
                AttributeValue::N(u32::from(value.version).to_string()),
            )
            .item(DATA_FIELD, AttributeValue::B(Blob::new(value.value)));

        let put = match precondition {
            Precondition::None => put,
            Precondition::DoesNotExist => put.condition_expression(CONDITION_DOES_NOT_EXIST),
            Precondition::MatchesVersion(version) => put
                .condition_expression("version = :v")
                .expression_attribute_values(
                    ":v",
                    AttributeValue::N(u32::from(version).to_string()),
                ),
        };

        put.send().await.map(|_| ()).map_err(|err| match err {
            SdkError::ServiceError(err) => match err.into_err() {
                PutItemError::ConditionalCheckFailedException(exception) => {
                    WriteError::FailedPrecondition(exception.to_string())
                }
                err => WriteError::terminal(err),
            },
            err => sdk_err_into_write_err(err),
        })
    }

    /// Deletes the key-value pair for the given key following the provided precondition. If the
    /// precondition is not met, then the operation returns a [`WriteError::PreconditionViolation`].
    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let key = self.prefixed(key);
        let delete = self
            .client
            .delete_item()
            .table_name(&self.table)
            .return_consumed_capacity(ReturnConsumedCapacity::None)
            .return_values(ReturnValue::None)
            .key(KEY_FIELD, AttributeValue::S(key));

        let delete = match precondition {
            Precondition::None => delete,
            Precondition::DoesNotExist => delete.condition_expression(CONDITION_DOES_NOT_EXIST),
            Precondition::MatchesVersion(version) => delete
                .condition_expression("version = :v")
                .expression_attribute_values(
                    ":v",
                    AttributeValue::N(u32::from(version).to_string()),
                ),
        };

        delete.send().await.map(|_| ()).map_err(|err| match err {
            SdkError::ServiceError(err) => match err.into_err() {
                DeleteItemError::ConditionalCheckFailedException(exception) => {
                    WriteError::FailedPrecondition(exception.to_string())
                }
                err => WriteError::terminal(err),
            },
            err => sdk_err_into_write_err(err),
        })
    }
}

fn sdk_err_into_read_err<E, R>(err: SdkError<E, R>) -> ReadError
where
    E: std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
{
    match err {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            ReadError::retryable(err)
        }
        err => ReadError::terminal(err),
    }
}

fn sdk_err_into_write_err<E, R>(err: SdkError<E, R>) -> WriteError
where
    E: std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
{
    match err {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => {
            WriteError::retryable(err)
        }
        err => WriteError::terminal(err),
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use bytestring::ByteString;
    use restate_metadata_store::{MetadataStore, WriteError};
    use restate_types::{
        Version,
        metadata::{Precondition, VersionedValue},
    };

    // ## Running the tests
    //
    // The following tests requires that dynamodb to be running locally and listening on port 8000
    //
    // ```ignore
    // # start dynamo db
    // docker run -p 8000:8000 amazon/dynamodb-local
    //
    // # create the metadata table
    //
    // aws dynamodb create-table \
    //     --endpoint-url http://localhost:8000 \
    //     --table-name metadata \
    //     --attribute-definitions \
    //         AttributeName=pk,AttributeType=S
    //     --key-schema \
    //         AttributeName=pk,KeyType=HASH \
    //     --billing-mode PAY_PER_REQUEST
    // ```

    use super::DynamoDbMetadataStore;

    #[ignore = "requires running dynamodb on localhost:8000"]
    #[tokio::test]
    async fn test_put_does_not_exist() {
        let client =
            DynamoDbMetadataStore::new_test_store("metadata", "http://localhost:8000").await;

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

    #[ignore = "requires running dynamodb on localhost:8000"]
    #[tokio::test]
    async fn test_put_with_version() {
        let client =
            DynamoDbMetadataStore::new_test_store("metadata", "http://localhost:8000").await;

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

    #[ignore = "requires running dynamodb on localhost:8000"]
    #[tokio::test]
    async fn test_put_force() {
        let client =
            DynamoDbMetadataStore::new_test_store("metadata", "http://localhost:8000").await;

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

    #[ignore = "requires running dynamodb on localhost:8000"]
    #[tokio::test]
    async fn test_delete() {
        let client =
            DynamoDbMetadataStore::new_test_store("metadata", "http://localhost:8000").await;

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
        assert!(client.get(key.clone()).await.unwrap().is_none());
    }
}
