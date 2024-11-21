// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::info;

use restate_core::metadata_store::MetadataStoreClient;
use restate_core::{TaskCenter, TaskKind};
use restate_metadata_store::local::LocalMetadataStoreService;
use restate_types::config::{MetadataStoreClientOptions, MetadataStoreOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;

pub async fn start_metadata_store(
    metadata_store_client_options: MetadataStoreClientOptions,
    opts: BoxedLiveLoad<MetadataStoreOptions>,
    updateables_rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    task_center: &TaskCenter,
) -> anyhow::Result<MetadataStoreClient> {
    let health_status = HealthStatus::default();
    let service = LocalMetadataStoreService::from_options(
        health_status.clone(),
        opts,
        updateables_rocksdb_options,
    );

    task_center.spawn(
        TaskKind::MetadataStore,
        "local-metadata-store",
        None,
        async move {
            service.run().await?;
            Ok(())
        },
    )?;
    info!("Waiting for local metadata store to startup");
    health_status
        .wait_for_value(MetadataServerStatus::Ready)
        .await;

    let client = restate_metadata_store::local::create_client(metadata_store_client_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create metadata store client: {}", e))?;

    Ok(client)
}
