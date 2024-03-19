// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::grpc_svc::metadata_store_svc_client::MetadataStoreSvcClient;
use crate::grpc_svc::{DeleteRequest, GetRequest, PutRequest};
use crate::local::grpc::pb_conversions::ConversionError;
use crate::{MetadataStore, Precondition, ReadError, VersionedValue, WriteError};
use async_trait::async_trait;
use bytestring::ByteString;
use restate_grpc_util::create_grpc_channel_from_advertised_address;
use restate_types::net::AdvertisedAddress;
use restate_types::Version;
use tonic::transport::Channel;
use tonic::{Code, Status};

/// Client end to interact with the [`LocalMetadataStore`].
#[derive(Debug, Clone)]
pub struct LocalMetadataStoreClient {
    svc_client: MetadataStoreSvcClient<Channel>,
}
impl LocalMetadataStoreClient {
    pub fn new(metadata_store_address: AdvertisedAddress) -> Self {
        let channel = create_grpc_channel_from_advertised_address(metadata_store_address)
            .expect("should not fail");

        Self {
            svc_client: MetadataStoreSvcClient::new(channel),
        }
    }
}

#[async_trait]
impl MetadataStore for LocalMetadataStoreClient {
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let response = self
            .svc_client
            .clone()
            .get(GetRequest { key: key.into() })
            .await?;

        response
            .into_inner()
            .try_into()
            .map_err(|err: ConversionError| ReadError::Internal(err.to_string()))
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let response = self
            .svc_client
            .clone()
            .get_version(GetRequest { key: key.into() })
            .await?;

        Ok(response.into_inner().into())
    }

    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        self.svc_client
            .clone()
            .put(PutRequest {
                key: key.into(),
                value: Some(value.into()),
                precondition: Some(precondition.into()),
            })
            .await?;

        Ok(())
    }

    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        self.svc_client
            .clone()
            .delete(DeleteRequest {
                key: key.into(),
                precondition: Some(precondition.into()),
            })
            .await?;

        Ok(())
    }
}

impl From<Status> for ReadError {
    fn from(status: Status) -> Self {
        match &status.code() {
            Code::Unavailable => ReadError::Network(status.into()),
            _ => ReadError::Internal(status.to_string()),
        }
    }
}

impl From<Status> for WriteError {
    fn from(status: Status) -> Self {
        match &status.code() {
            Code::Unavailable => WriteError::Network(status.into()),
            Code::FailedPrecondition => {
                WriteError::FailedPrecondition(status.message().to_string())
            }
            _ => WriteError::Internal(status.to_string()),
        }
    }
}
