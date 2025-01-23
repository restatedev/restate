// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::grpc::client::GrpcMetadataStoreClient;
use restate_core::metadata_store::providers::create_object_store_based_meta_store;
use restate_core::metadata_store::{providers::EtcdMetadataStore, MetadataStoreClient};
use restate_core::network::NetworkServerBuilder;
use restate_rocksdb::RocksError;
use restate_types::config::{MetadataStoreOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::{
    config::{MetadataStoreClient as MetadataStoreClientConfig, MetadataStoreClientOptions},
    errors::GenericError,
};

mod store;

use crate::MetadataStoreRunner;
pub use store::LocalMetadataStore;

/// Creates a [`MetadataStoreClient`] for the [`GrpcMetadataStoreClient`].
pub async fn create_client(
    metadata_store_client_options: MetadataStoreClientOptions,
) -> Result<MetadataStoreClient, GenericError> {
    let backoff_policy = Some(
        metadata_store_client_options
            .metadata_store_client_backoff_policy
            .clone(),
    );

    let client = match metadata_store_client_options.metadata_store_client.clone() {
        MetadataStoreClientConfig::Embedded { addresses } => {
            let inner_client =
                GrpcMetadataStoreClient::new(addresses, metadata_store_client_options);
            MetadataStoreClient::new(inner_client, backoff_policy)
        }
        MetadataStoreClientConfig::Etcd { addresses } => {
            let store = EtcdMetadataStore::new(addresses, &metadata_store_client_options).await?;
            MetadataStoreClient::new(store, backoff_policy)
        }
        conf @ MetadataStoreClientConfig::ObjectStore { .. } => {
            let store = create_object_store_based_meta_store(conf).await?;
            MetadataStoreClient::new(store, backoff_policy)
        }
    };

    Ok(client)
}

pub(crate) async fn create_store(
    metadata_store_options: &MetadataStoreOptions,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    health_status: HealthStatus<MetadataServerStatus>,
    server_builder: &mut NetworkServerBuilder,
) -> Result<MetadataStoreRunner<LocalMetadataStore>, RocksError> {
    let store =
        LocalMetadataStore::create(metadata_store_options, rocksdb_options, health_status).await?;
    Ok(MetadataStoreRunner::new(store, server_builder))
}

#[cfg(test)]
mod tests;
