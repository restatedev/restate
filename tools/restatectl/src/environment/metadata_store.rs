// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use restate_core::network::NetworkServerBuilder;
use restate_core::{TaskCenter, TaskKind};
use restate_metadata_store::local::LocalMetadataStoreService;
use restate_types::config;
use restate_types::config::{MetadataStoreClientOptions, MetadataStoreOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::net::{AdvertisedAddress, BindAddress};
use restate_types::protobuf::common::NodeStatus;

pub async fn start_metadata_store(
    mut metadata_store_client_options: MetadataStoreClientOptions,
    opts: &MetadataStoreOptions,
    updateables_rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    task_center: &TaskCenter,
) -> anyhow::Result<MetadataStoreClient> {
    let mut server_builder = NetworkServerBuilder::default();

    let service = LocalMetadataStoreService::create(
        HealthStatus::default(),
        opts,
        updateables_rocksdb_options,
        &mut server_builder,
    )
    .await?;

    // right now we only support running a local metadata store
    let uds = tempfile::tempdir()?.into_path().join("metadata-rpc-server");
    let bind_address = BindAddress::Uds(uds.clone());
    metadata_store_client_options.metadata_store_client = config::MetadataStoreClient::Embedded {
        address: AdvertisedAddress::Uds(uds),
    };

    let rpc_server_health_status = HealthStatus::default();
    task_center.spawn(TaskKind::RpcServer, "metadata-rpc-server", None, {
        let rpc_server_health_status = rpc_server_health_status.clone();
        async move {
            server_builder
                .run(rpc_server_health_status, None, &bind_address)
                .await
        }
    })?;

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
    rpc_server_health_status
        .wait_for_value(NodeStatus::Alive)
        .await;

    let client = restate_metadata_store::local::create_client(metadata_store_client_options)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create metadata store client: {}", e))?;

    Ok(client)
}
