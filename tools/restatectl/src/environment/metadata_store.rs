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
use restate_core::network::NetworkServerBuilder;
use restate_core::{TaskCenter, TaskKind};
use restate_metadata_server::MetadataServer;
use restate_types::config;
use restate_types::config::Configuration;
use restate_types::health::HealthStatus;
use restate_types::live::Live;
use restate_types::net::{AdvertisedAddress, BindAddress};
use restate_types::protobuf::common::NodeRpcStatus;

pub async fn start_metadata_server(
    mut config: Configuration,
) -> anyhow::Result<MetadataStoreClient> {
    let mut server_builder = NetworkServerBuilder::default();

    // right now we only support running a local metadata store
    let uds = tempfile::tempdir()?.into_path().join("metadata-rpc-server");
    let bind_address = BindAddress::Uds(uds.clone());
    config.common.metadata_client.kind = config::MetadataClientKind::Replicated {
        addresses: vec![AdvertisedAddress::Uds(uds)],
    };

    let (service, client) = restate_metadata_server::create_metadata_server_and_client(
        Live::from_value(config),
        HealthStatus::default(),
        &mut server_builder,
    )
    .await?;

    let rpc_server_health = if !server_builder.is_empty() {
        let rpc_server_health_status = HealthStatus::default();
        TaskCenter::spawn(TaskKind::RpcServer, "metadata-rpc-server", {
            let rpc_server_health_status = rpc_server_health_status.clone();
            async move {
                server_builder
                    .run(rpc_server_health_status, &bind_address)
                    .await
            }
        })?;
        Some(rpc_server_health_status)
    } else {
        None
    };

    TaskCenter::spawn(
        TaskKind::MetadataServer,
        "local-metadata-server",
        async move {
            service.run(None).await?;
            Ok(())
        },
    )?;

    if let Some(rpc_server_health) = rpc_server_health {
        info!("Waiting for local metadata store to startup");
        rpc_server_health.wait_for_value(NodeRpcStatus::Ready).await;
    }

    Ok(client)
}
