// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;

use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::net_util::create_tonic_channel_from_advertised_address;
use restate_core::{TaskCenter, TaskKind};
use restate_metadata_store::local::LocalMetadataStoreService;
use restate_types::config::{MetadataStoreClientOptions, MetadataStoreOptions, RocksDbOptions};
use restate_types::live::BoxedLiveLoad;
use restate_types::retries::RetryPolicy;

pub async fn start_metadata_store(
    metadata_store_client_options: MetadataStoreClientOptions,
    opts: BoxedLiveLoad<MetadataStoreOptions>,
    updateables_rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    task_center: &TaskCenter,
) -> anyhow::Result<MetadataStoreClient> {
    let service = LocalMetadataStoreService::from_options(opts, updateables_rocksdb_options);
    let grpc_service_name = service.grpc_service_name().to_owned();

    task_center.spawn(
        TaskKind::MetadataStore,
        "local-metadata-store",
        None,
        async move {
            service.run().await?;
            Ok(())
        },
    )?;

    let address = match &metadata_store_client_options.metadata_store_client {
        restate_types::config::MetadataStoreClient::Embedded { address } => address.clone(),
        _ => panic!("unsupported metadata store type"),
    };

    let health_client = HealthClient::new(create_tonic_channel_from_advertised_address(
        address.clone(),
    ));
    let retry_policy = RetryPolicy::exponential(Duration::from_millis(10), 2.0, None, None);

    retry_policy
        .retry(|| async {
            health_client
                .clone()
                .check(HealthCheckRequest {
                    service: grpc_service_name.clone(),
                })
                .await
        })
        .await?;

    let client = restate_metadata_store::local::create_client(metadata_store_client_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create metadata store client: {}", e))?;

    Ok(client)
}
