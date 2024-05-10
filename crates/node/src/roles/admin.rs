// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use codederror::CodedError;
use futures::stream::BoxStream;
use restate_network::Networking;
use std::time::Duration;
use tonic::transport::Channel;

use restate_admin::service::AdminService;
use restate_bifrost::Bifrost;
use restate_cluster_controller::ClusterControllerHandle;
use restate_core::metadata_store::MetadataStoreClient;
use restate_core::{task_center, Metadata, MetadataWriter, TaskKind};
use restate_node_protocol::cluster_controller::ClusterControllerMessage;
use restate_node_protocol::MessageEnvelope;
use restate_node_services::node_svc::node_svc_client::NodeSvcClient;
use restate_service_client::{AssumeRoleCacheMode, ServiceClient};
use restate_service_protocol::discovery::ServiceDiscovery;
use restate_types::arc_util::ArcSwapExt;
use restate_types::config::{IngressOptions, UpdateableConfiguration};
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
    updateable_config: UpdateableConfiguration,
    controller: restate_cluster_controller::Service,
    admin: AdminService<IngressOptions>,
}

impl AdminRole {
    pub fn new(
        updateable_config: UpdateableConfiguration,
        metadata: Metadata,
        networking: Networking,
        metadata_writer: MetadataWriter,
        incoming_controller_messages: BoxStream<'static, MessageEnvelope<ClusterControllerMessage>>,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<Self, AdminRoleBuildError> {
        let config = updateable_config.pinned();

        // Total duration roughly 66 seconds
        let retry_policy = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            Some(10),
            Some(Duration::from_secs(20)),
        );
        let client =
            ServiceClient::from_options(&config.common.service_client, AssumeRoleCacheMode::None)?;
        let service_discovery = ServiceDiscovery::new(retry_policy, client);

        let admin = AdminService::new(
            metadata_writer,
            metadata_store_client,
            config.ingress.clone(),
            service_discovery,
        );

        let controller = restate_cluster_controller::Service::new(
            metadata,
            networking,
            incoming_controller_messages,
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
            self.controller.run(),
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
                self.updateable_config.map_as_updateable_owned(|c| &c.admin),
                node_svc_client,
                bifrost,
            ),
        )?;

        Ok(())
    }
}
