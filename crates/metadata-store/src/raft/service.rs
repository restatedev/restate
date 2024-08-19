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
use crate::raft::connection_manager::ConnectionManager;
use crate::raft::grpc_svc::raft_metadata_store_svc_server::RaftMetadataStoreSvcServer;
use crate::raft::handler::RaftMetadataStoreHandler;
use crate::raft::networking::Networking;
use crate::raft::store::RaftMetadataStore;
use crate::{grpc_svc, Error, MetadataStoreService};
use assert2::let_assert;
use futures::TryFutureExt;
use restate_core::{TaskCenter, TaskKind};
use restate_types::config::{Kind, MetadataStoreOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;
use tokio::sync::mpsc;

pub struct RaftMetadataStoreService {
    health_status: HealthStatus<MetadataServerStatus>,
    options: BoxedLiveLoad<MetadataStoreOptions>,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
}

impl RaftMetadataStoreService {
    pub fn new(
        health_status: HealthStatus<MetadataServerStatus>,
        options: BoxedLiveLoad<MetadataStoreOptions>,
        rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    ) -> Self {
        Self {
            options,
            rocksdb_options,
            health_status,
        }
    }
}

#[async_trait::async_trait]
impl MetadataStoreService for RaftMetadataStoreService {
    async fn run(mut self) -> Result<(), Error> {
        let store_options = self.options.live_load();
        let_assert!(Kind::Raft(raft_options) = &store_options.kind);

        let (router_tx, router_rx) = mpsc::channel(128);
        let connection_manager = ConnectionManager::new(raft_options.id, router_tx);
        let store = RaftMetadataStore::create(
            raft_options,
            self.rocksdb_options,
            Networking::new(connection_manager.clone()),
            router_rx,
        )
        .await
        .map_err(Error::generic)?;

        let mut builder = GrpcServiceBuilder::default();

        builder.register_file_descriptor_set_for_reflection(grpc_svc::FILE_DESCRIPTOR_SET);
        builder.add_service(MetadataStoreSvcServer::new(MetadataStoreHandler::new(
            store.request_sender(),
        )));
        builder.add_service(RaftMetadataStoreSvcServer::new(
            RaftMetadataStoreHandler::new(connection_manager),
        ));

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
