// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use crate::grpc_svc;
use crate::grpc_svc::metadata_store_svc_server::MetadataStoreSvcServer;
use crate::local::store::LocalMetadataStore;
use futures::TryFutureExt;
use restate_core::{ShutdownError, TaskCenter, TaskKind};
use restate_rocksdb::RocksError;
use restate_types::config::{MetadataStoreOptions, RocksDbOptions};
use restate_types::health::HealthStatus;
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;

pub struct LocalMetadataStoreService {
    health_status: HealthStatus<MetadataServerStatus>,
    opts: BoxedLiveLoad<MetadataStoreOptions>,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("error while running server grpc reflection service: {0}")]
    GrpcReflection(#[from] tonic_reflection::server::Error),
    #[error("system is shutting down")]
    Shutdown(#[from] ShutdownError),
    #[error("rocksdb error: {0}")]
    RocksDB(#[from] RocksError),
}

impl LocalMetadataStoreService {
    pub fn from_options(
        health_status: HealthStatus<MetadataServerStatus>,
        opts: BoxedLiveLoad<MetadataStoreOptions>,
        rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    ) -> Self {
        health_status.update(MetadataServerStatus::StartingUp);
        Self {
            health_status,
            opts,
            rocksdb_options,
        }
    }

    pub async fn run(self) -> Result<(), Error> {
        let LocalMetadataStoreService {
            health_status,
            mut opts,
            rocksdb_options,
        } = self;
        let options = opts.live_load();
        let bind_address = options.bind_address.clone();
        let store = LocalMetadataStore::create(options, rocksdb_options).await?;
        let mut builder = GrpcServiceBuilder::default();

        builder.register_file_descriptor_set_for_reflection(grpc_svc::FILE_DESCRIPTOR_SET);
        builder.add_service(MetadataStoreSvcServer::new(MetadataStoreHandler::new(
            store.request_sender(),
        )));
        let grpc_server = GrpcServer::new(bind_address, builder.build().await?);

        TaskCenter::spawn_child(
            TaskKind::RpcServer,
            "metadata-store-grpc",
            grpc_server.run(health_status).map_err(Into::into),
        )?;

        store.run().await;

        Ok(())
    }
}
