// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod store;

mod service;

use restate_core::metadata_store::{providers::EtcdMetadataStore, MetadataStoreClient};
use restate_types::{
    config::{MetadataStoreClient as MetadataStoreClientConfig, MetadataStoreClientOptions},
    errors::GenericError,
};
pub use service::LocalMetadataStoreService;

use crate::grpc::client::GrpcMetadataStoreClient;

/// Creates a [`MetadataStoreClient`] for the [`GrpcMetadataStoreClient`].
pub async fn create_client(
    metadata_store_client_options: MetadataStoreClientOptions,
) -> Result<MetadataStoreClient, GenericError> {
    let backoff_policy = Some(metadata_store_client_options.metadata_store_client_backoff_policy);

    let client = match metadata_store_client_options.metadata_store_client {
        MetadataStoreClientConfig::Embedded { address } => {
            let store = GrpcMetadataStoreClient::new(address);
            MetadataStoreClient::new(store, backoff_policy)
        }
        MetadataStoreClientConfig::Etcd { addresses } => {
            let store = EtcdMetadataStore::new(addresses).await?;
            MetadataStoreClient::new(store, backoff_policy)
        }
    };

    Ok(client)
}

#[cfg(test)]
mod tests;
