// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::grpc::handler::MetadataStoreHandler;
use crate::grpc::server::GrpcServer;
use crate::grpc::service_builder::GrpcServiceBuilder;
use crate::grpc_svc::metadata_store_svc_server::MetadataStoreSvcServer;
use crate::raft::store::RaftMetadataStore;
use crate::{grpc_svc, Error, MetadataStoreService};
use futures::TryFutureExt;
use restate_core::{TaskCenter, TaskKind};
use restate_types::config::MetadataStoreOptions;
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;

pub struct RaftMetadataStoreService {
    health_status: HealthStatus<MetadataServerStatus>,
    options: BoxedLiveLoad<MetadataStoreOptions>,
}

impl RaftMetadataStoreService {
    pub fn new(
        health_status: HealthStatus<MetadataServerStatus>,
        options: BoxedLiveLoad<MetadataStoreOptions>,
    ) -> Self {
        Self {
            options,
            health_status,
        }
    }
}

#[async_trait::async_trait]
impl MetadataStoreService for RaftMetadataStoreService {
    async fn run(mut self) -> Result<(), Error> {
        let store_options = self.options.live_load();
        let store = RaftMetadataStore::create().await.map_err(Error::generic)?;

        let mut builder = GrpcServiceBuilder::default();

        builder.register_file_descriptor_set_for_reflection(grpc_svc::FILE_DESCRIPTOR_SET);
        builder.add_service(MetadataStoreSvcServer::new(MetadataStoreHandler::new(
            store.request_sender(),
        )));

        let grpc_server =
            GrpcServer::new(store_options.bind_address.clone(), builder.build().await?);

        TaskCenter::spawn_child(
            TaskKind::RpcServer,
            "metadata-store-grpc",
            grpc_server.run(self.health_status).map_err(Into::into),
        )?;

        store.run().await.map_err(Error::generic)?;

        Ok(())
    }
}
