// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytestring::ByteString;
use rand::seq::SliceRandom;
use std::sync::Arc;
use tonic::transport::Channel;
use tonic::{Code, Status};

use restate_core::metadata_store::{
    retry_on_network_error, Precondition, ProvisionedMetadataStore, ReadError, VersionedValue,
    WriteError,
};
use restate_core::network::net_util::create_tonic_channel;
use restate_core::network::net_util::CommonClientConnectionOptions;
use restate_types::config::Configuration;
use restate_types::net::AdvertisedAddress;
use restate_types::retries::RetryPolicy;
use restate_types::Version;

use crate::grpc::pb_conversions::ConversionError;
use crate::grpc_svc::metadata_store_svc_client::MetadataStoreSvcClient;
use crate::grpc_svc::{DeleteRequest, GetRequest, PutRequest};

/// Client end to interact with the metadata store.
#[derive(Debug, Clone)]
pub struct GrpcMetadataStoreClient {
    channels: Arc<Vec<Channel>>,
    svc_client: Arc<ArcSwap<MetadataStoreSvcClient<Channel>>>,
}

impl GrpcMetadataStoreClient {
    pub fn new<T: CommonClientConnectionOptions>(
        metadata_store_addresses: Vec<AdvertisedAddress>,
        options: &T,
    ) -> Self {
        assert!(
            !metadata_store_addresses.is_empty(),
            "At least one metadata store needs to be configured"
        );
        let channels: Vec<_> = metadata_store_addresses
            .into_iter()
            .map(|address| create_tonic_channel(address, options))
            .collect();
        let svc_client = MetadataStoreSvcClient::new(
            channels
                .first()
                .expect("at least one address mus be configured")
                .clone(),
        );

        Self {
            channels: Arc::new(channels),
            svc_client: Arc::new(ArcSwap::from_pointee(svc_client)),
        }
    }

    fn retry_policy() -> RetryPolicy {
        Configuration::pinned()
            .common
            .network_error_retry_policy
            .clone()
    }

    fn choose_different_endpoint(&self) {
        // let's try another endpoint
        let mut rng = rand::thread_rng();
        let new_svc_client = MetadataStoreSvcClient::new(
            self.channels
                .choose(&mut rng)
                .expect("at least one channel be present")
                .clone(),
        );
        self.svc_client.store(Arc::new(new_svc_client))
    }
}

#[async_trait]
impl ProvisionedMetadataStore for GrpcMetadataStoreClient {
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let retry_policy = Self::retry_policy();

        let response = retry_on_network_error(retry_policy, || async {
            let mut client = self.svc_client.load().as_ref().clone();

            let response = client
                .get(GetRequest {
                    key: key.clone().into(),
                })
                .await
                .map_err(map_status_to_read_error);

            if response.is_err() {
                self.choose_different_endpoint();
            }

            response
        })
        .await?;

        response
            .into_inner()
            .try_into()
            .map_err(|err: ConversionError| ReadError::Internal(err.to_string()))
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let retry_policy = Self::retry_policy();

        let response = retry_on_network_error(retry_policy, || async {
            let mut client = self.svc_client.load().as_ref().clone();

            let response = client
                .get_version(GetRequest {
                    key: key.clone().into(),
                })
                .await
                .map_err(map_status_to_read_error);

            if response.is_err() {
                self.choose_different_endpoint();
            }

            response
        })
        .await?;

        Ok(response.into_inner().into())
    }

    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        let retry_policy = Self::retry_policy();

        retry_on_network_error(retry_policy, || async {
            let mut client = self.svc_client.load().as_ref().clone();

            let response = client
                .put(PutRequest {
                    key: key.clone().into(),
                    value: Some(value.clone().into()),
                    precondition: Some(precondition.clone().into()),
                })
                .await
                .map_err(map_status_to_write_error);

            if response.is_err() {
                self.choose_different_endpoint();
            }

            response
        })
        .await?;

        Ok(())
    }

    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let retry_policy = Self::retry_policy();

        retry_on_network_error(retry_policy, || async {
            let mut client = self.svc_client.load().as_ref().clone();

            let response = client
                .delete(DeleteRequest {
                    key: key.clone().into(),
                    precondition: Some(precondition.clone().into()),
                })
                .await
                .map_err(map_status_to_write_error);

            if response.is_err() {
                self.choose_different_endpoint();
            }

            response
        })
        .await?;

        Ok(())
    }
}

fn map_status_to_read_error(status: Status) -> ReadError {
    match &status.code() {
        Code::Unavailable => ReadError::Network(status.into()),
        _ => ReadError::Internal(status.to_string()),
    }
}

fn map_status_to_write_error(status: Status) -> WriteError {
    match &status.code() {
        Code::Unavailable => WriteError::Network(status.into()),
        Code::FailedPrecondition => WriteError::FailedPrecondition(status.message().to_string()),
        _ => WriteError::Internal(status.to_string()),
    }
}
