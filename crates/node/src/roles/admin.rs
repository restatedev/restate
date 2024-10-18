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

use restate_types::health::HealthStatus;
use tokio::sync::oneshot;

use codederror::CodedError;
use restate_admin::cluster_controller;
use restate_admin::cluster_controller::ClusterControllerHandle;
use restate_admin::service::AdminService;
use restate_bifrost::Bifrost;
use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::net_util::create_tonic_channel_from_advertised_address;
use restate_core::network::protobuf::node_svc::node_svc_client::NodeSvcClient;
use restate_core::network::MessageRouterBuilder;
use restate_core::network::Networking;
use restate_core::network::TransportConnect;
use restate_core::{task_center, Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_service_client::{AssumeRoleCacheMode, ServiceClient};
use restate_service_protocol::discovery::ServiceDiscovery;
use restate_types::config::Configuration;
use restate_types::config::IngressOptions;
use restate_types::live::Live;
use restate_types::net::AdvertisedAddress;
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
        task_center: TaskCenter,
        updateable_config: Live<Configuration>,
        metadata: Metadata,
        networking: Networking<T>,
        metadata_writer: MetadataWriter,
        router_builder: &mut MessageRouterBuilder,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Self, AdminRoleBuildError> {
        health_status.update(AdminStatus::StartingUp);
        let config = updateable_config.pinned();

        // Total duration roughly 1s
        let retry_policy = RetryPolicy::exponential(Duration::from_millis(100), 2.0, Some(4), None);
        let client =
            ServiceClient::from_options(&config.common.service_client, AssumeRoleCacheMode::None)?;
        let service_discovery = ServiceDiscovery::new(retry_policy, client);

        let admin = AdminService::new(
            metadata_writer.clone(),
            metadata_store_client.clone(),
            config.ingress.clone(),
            service_discovery,
        );

        let controller = if config.admin.is_cluster_controller_enabled() {
            Some(cluster_controller::Service::new(
                updateable_config.clone(),
                health_status,
                task_center,
                metadata,
                networking,
                router_builder,
                metadata_writer,
                metadata_store_client,
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

    pub fn cluster_controller_handle(&self) -> Option<ClusterControllerHandle> {
        self.controller
            .as_ref()
            .map(|controller| controller.handle())
    }

    pub async fn start(
        self,
        bifrost: Bifrost,
        all_partitions_started_tx: oneshot::Sender<()>,
        node_address: AdvertisedAddress,
    ) -> Result<(), anyhow::Error> {
        let tc = task_center();

        if let Some(cluster_controller) = self.controller {
            tc.spawn_child(
                TaskKind::SystemService,
                "cluster-controller-service",
                None,
                cluster_controller.run(bifrost.clone(), Some(all_partitions_started_tx)),
            )?;
        }

        tc.spawn_child(
            TaskKind::RpcServer,
            "admin-rpc-server",
            None,
            self.admin.run(
                self.updateable_config.map(|c| &c.admin),
                NodeSvcClient::new(create_tonic_channel_from_advertised_address(node_address)),
                bifrost,
            ),
        )?;

        Ok(())
    }
}
