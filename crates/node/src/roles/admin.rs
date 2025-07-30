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
use restate_core::network::NetworkServerBuilder;
use restate_core::network::Networking;
use restate_core::network::TransportConnect;
use restate_core::partitions::PartitionRouting;
use restate_core::worker_api::PartitionProcessorInvocationClient;
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
use restate_types::partitions::state::PartitionReplicaSetStates;
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
    controller: Option<cluster_controller::Service<T>>,
    admin: AdminService<IngressOptions, PartitionProcessorInvocationClient<T>>,
}

impl<T: TransportConnect> AdminRole<T> {
    pub async fn create(
        health_status: HealthStatus<AdminStatus>,
        bifrost: Bifrost,
        replica_set_states: PartitionReplicaSetStates,
        networking: Networking<T>,
        metadata_writer: MetadataWriter,
        server_builder: &mut NetworkServerBuilder,
    ) -> Result<Self, AdminRoleBuildError> {
        health_status.update(AdminStatus::StartingUp);
        let config = Configuration::pinned();
        let metadata = Metadata::current();
        let partition_routing =
            PartitionRouting::new(replica_set_states.clone(), TaskCenter::current());

        // Total duration roughly 1s
        let retry_policy = RetryPolicy::exponential(Duration::from_millis(100), 2.0, Some(4), None);
        let client =
            ServiceClient::from_options(&config.common.service_client, AssumeRoleCacheMode::None)?;
        let service_discovery = ServiceDiscovery::new(retry_policy, client);

        let query_context = {
            let remote_scanner_manager = RemoteScannerManager::new(
                create_remote_scanner_service(networking.clone()),
                create_partition_locator(partition_routing.clone()),
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
            PartitionProcessorInvocationClient::new(
                networking.clone(),
                metadata.updateable_partition_table(),
                partition_routing,
            ),
            config.ingress.clone(),
            service_discovery,
        )
        .with_query_context(query_context);

        let controller = if config.admin.is_cluster_controller_enabled() {
            Some(
                cluster_controller::Service::create(
                    health_status,
                    replica_set_states,
                    bifrost,
                    networking,
                    server_builder,
                    metadata_writer,
                )
                .await?,
            )
        } else {
            None
        };

        Ok(AdminRole { controller, admin })
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
            self.admin.run(Configuration::map_live(|c| &c.admin)),
        )?;

        Ok(())
    }
}
