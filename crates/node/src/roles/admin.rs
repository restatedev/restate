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

use anyhow::Context;
use codederror::CodedError;
use tonic::transport::Channel;

use restate_admin::cluster_controller::ClusterControllerHandle;
use restate_admin::service::AdminService;
use restate_bifrost::Bifrost;
use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::protobuf::node_svc::node_svc_client::NodeSvcClient;
use restate_core::network::MessageRouterBuilder;
use restate_core::network::Networking;
use restate_core::{task_center, Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_service_client::{AssumeRoleCacheMode, ServiceClient};
use restate_service_protocol::discovery::ServiceDiscovery;
use restate_types::config::Configuration;
use restate_types::config::IngressOptions;
use restate_types::live::Live;
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

pub struct AdminRole {
    updateable_config: Live<Configuration>,
    controller: restate_admin::cluster_controller::Service<Networking>,
    admin: AdminService<IngressOptions>,
}

impl AdminRole {
    pub fn new(
        task_center: TaskCenter,
        updateable_config: Live<Configuration>,
        metadata: Metadata,
        networking: Networking,
        metadata_writer: MetadataWriter,
        router_builder: &mut MessageRouterBuilder,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Self, AdminRoleBuildError> {
        let config = updateable_config.pinned();

        // Total duration roughly 1s
        let retry_policy = RetryPolicy::exponential(Duration::from_millis(100), 2.0, Some(4), None);
        let client =
            ServiceClient::from_options(&config.common.service_client, AssumeRoleCacheMode::None)?;
        let service_discovery = ServiceDiscovery::new(retry_policy, client);

        let admin = AdminService::new(
            metadata_writer,
            metadata_store_client,
            config.ingress.clone(),
            service_discovery,
        );

        let controller = restate_admin::cluster_controller::Service::new(
            updateable_config.clone().map(|c| &c.admin),
            task_center,
            metadata,
            networking,
            router_builder,
        );

        Ok(AdminRole {
            updateable_config,
            controller,
            admin,
        })
    }

    pub fn cluster_controller_handle(&self) -> ClusterControllerHandle {
        self.controller.handle()
    }

    pub async fn start(
        self,
        _bootstrap_cluster: bool,
        bifrost: Bifrost,
    ) -> Result<(), anyhow::Error> {
        let tc = task_center();

        tc.spawn_child(
            TaskKind::SystemService,
            "cluster-controller-service",
            None,
            self.controller.run(bifrost.clone()),
        )?;

        // todo: Make address configurable
        let worker_channel = Channel::builder(
            "http://127.0.0.1:5122/"
                .parse()
                .context("valid worker address uri")?,
        )
        .connect_lazy();
        let node_svc_client = NodeSvcClient::new(worker_channel);

        tc.spawn_child(
            TaskKind::RpcServer,
            "admin-rpc-server",
            None,
            self.admin.run(
                self.updateable_config.map(|c| &c.admin),
                node_svc_client,
                bifrost,
            ),
        )?;

        Ok(())
    }
}
