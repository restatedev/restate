// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use codederror::CodedError;
use restate_admin::cluster_controller;
use restate_admin::service::AdminService;
use restate_bifrost::Bifrost;
use restate_core::network::MessageRouterBuilder;
use restate_core::network::NetworkServerBuilder;
use restate_core::network::Networking;
use restate_core::network::TransportConnect;
use restate_core::partitions::PartitionRouting;
use restate_core::{Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_service_client::{AssumeRoleCacheMode, ServiceClient};
use restate_service_protocol::discovery::ServiceDiscovery;
use restate_storage_query_datafusion::context::{QueryContext, SelectPartitionsFromMetadata};
use restate_storage_query_datafusion::empty_invoker_status_handle::EmptyInvokerStatusHandle;
use restate_storage_query_datafusion::remote_query_scanner_client::create_remote_scanner_service;
use restate_storage_query_datafusion::remote_query_scanner_manager::{
    RemoteScannerManager, create_partition_locator,
};
use restate_types::config::Configuration;
use restate_types::config::IngressOptions;
use restate_types::health::HealthStatus;
use restate_types::live::Live;
use restate_types::protobuf::common::AdminStatus;
use restate_types::retries::RetryPolicy;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum AdminRoleBuildError {
    #[error("unknown")]
    #[code(unknown)]
    Unknown,
    #[error("failed building the admin service: {0}")]
    #[code(unknown)]
    AdminService(#[from] restate_admin::service::BuildError),
    #[error("failed building the service client: {0}")]
    #[code(unknown)]
    ServiceClient(#[from] restate_service_client::BuildError),
    #[error("failed creating the datafusion query context: {0}")]
    #[code(unknown)]
    QueryDataFusion(#[from] restate_storage_query_datafusion::BuildError),
}

pub struct AdminRole<T> {
    updateable_config: Live<Configuration>,
    controller: Option<cluster_controller::Service<T>>,
    admin: AdminService<IngressOptions>,
}

impl<T: TransportConnect> AdminRole<T> {
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        health_status: HealthStatus<AdminStatus>,
        bifrost: Bifrost,
        updateable_config: Live<Configuration>,
        partition_routing: PartitionRouting,
        networking: Networking<T>,
        metadata: Metadata,
        metadata_writer: MetadataWriter,
        server_builder: &mut NetworkServerBuilder,
        router_builder: &mut MessageRouterBuilder,
        local_query_context: Option<QueryContext>,
    ) -> Result<Self, AdminRoleBuildError> {
        health_status.update(AdminStatus::StartingUp);
        let config = updateable_config.pinned();

        // Total duration roughly 1s
        let retry_policy = RetryPolicy::exponential(Duration::from_millis(100), 2.0, Some(4), None);
        let client =
            ServiceClient::from_options(&config.common.service_client, AssumeRoleCacheMode::None)?;
        let service_discovery = ServiceDiscovery::new(retry_policy, client);

        let query_context = if let Some(query_context) = local_query_context {
            query_context
        } else {
            let remote_scanner_manager = RemoteScannerManager::new(
                create_remote_scanner_service(networking.clone(), router_builder),
                create_partition_locator(partition_routing, metadata.clone()),
            );

            // need to create a remote query context since we are not co-located with a worker role
            QueryContext::with_user_tables(
                &config.admin.query_engine,
                SelectPartitionsFromMetadata,
                None,
                Option::<EmptyInvokerStatusHandle>::None,
                metadata.updateable_schema(),
                remote_scanner_manager,
            )
            .await?
        };

        let admin = AdminService::new(
            metadata_writer.clone(),
            bifrost.clone(),
            config.ingress.clone(),
            service_discovery,
        )
        .with_query_context(query_context);

        let controller = if config.admin.is_cluster_controller_enabled() {
            Some(cluster_controller::Service::new(
                updateable_config.clone(),
                health_status,
                bifrost,
                networking,
                router_builder,
                server_builder,
                metadata_writer,
            ))
        } else {
            None
        };

        Ok(AdminRole {
            updateable_config,
            controller,
            admin,
        })
    }

    pub async fn start(self) -> Result<(), anyhow::Error> {
        if let Some(cluster_controller) = self.controller {
            TaskCenter::spawn_child(
                TaskKind::ClusterController,
                "cluster-controller-service",
                cluster_controller.run(),
            )?;
        }

        TaskCenter::spawn_child(
            TaskKind::RpcServer,
            "admin-rpc-server",
            self.admin.run(self.updateable_config.map(|c| &c.admin)),
        )?;

        Ok(())
    }
}
