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
use restate_admin::Options as AdminOptions;
use restate_network::Networking;
use restate_node_services::node_svc::node_svc_client::NodeSvcClient;
use tonic::transport::Channel;
use tracing::info;

use restate_admin::service::AdminService;
use restate_bifrost::Bifrost;
use restate_cluster_controller::ClusterControllerHandle;
use restate_core::{task_center, TaskKind};
use restate_meta::{FileMetaReader, FileMetaStorage, MetaService};
use restate_worker::KafkaIngressOptions;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum AdminRoleBuildError {
    #[error("failed creating meta: {0}")]
    Meta(
        #[from]
        #[code]
        restate_meta::BuildError,
    ),
}

#[derive(Debug)]
pub struct AdminRole {
    controller: restate_cluster_controller::Service,
    admin: AdminService,
    meta: MetaService<FileMetaStorage, KafkaIngressOptions>,
}

impl AdminRole {
    pub fn new(
        admin_options: AdminOptions,
        kafka_options: KafkaIngressOptions,
        _networking: Networking,
    ) -> Result<Self, AdminRoleBuildError> {
        let meta = MetaService::from_options(admin_options.meta.clone(), kafka_options)?;
        let admin = AdminService::from_options(
            admin_options,
            meta.schemas(),
            meta.meta_handle(),
            meta.schema_reader(),
        );

        Ok(AdminRole {
            controller: restate_cluster_controller::Service::default(),
            admin,
            meta,
        })
    }

    pub fn cluster_controller_handle(&self) -> ClusterControllerHandle {
        self.controller.handle()
    }

    pub fn schema_reader(&self) -> FileMetaReader {
        self.meta.schema_reader()
    }

    pub async fn start(
        mut self,
        _bootstrap_cluster: bool,
        bifrost: Bifrost,
    ) -> Result<(), anyhow::Error> {
        info!("Running admin role");

        // Init the meta. This will reload the schemas in memory.
        self.meta.init().await?;

        task_center().spawn_child(
            TaskKind::SystemService,
            "meta-service",
            None,
            self.meta.run(),
        )?;

        task_center().spawn_child(
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

        task_center().spawn_child(
            TaskKind::RpcServer,
            "admin-rpc-server",
            None,
            self.admin.run(node_svc_client, bifrost),
        )?;

        Ok(())
    }
}
