// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::grpc_util;
use restate_core::{cancellation_watcher, task_center, ShutdownError, TaskKind};
use restate_types::config::{MetadataStoreOptions, RocksDbOptions};
use restate_types::live::LiveLoad;
use restate_types::net::BindAddress;
use tonic::server::NamedService;

use super::BuildError;
use crate::grpc_svc;
use crate::grpc_svc::metadata_store_svc_server::MetadataStoreSvcServer;
use crate::local::grpc::handler::LocalMetadataStoreHandler;
use crate::local::store::LocalMetadataStore;

pub struct LocalMetadataStoreService {
    metadata_store: LocalMetadataStore,
    bind_address: BindAddress,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed running grpc server: {0}")]
    GrpcServer(#[from] grpc_util::Error),
    #[error("error while running server server grpc reflection service: {0}")]
    GrpcReflection(#[from] tonic_reflection::server::Error),
    #[error("system is shutting down")]
    Shutdown(#[from] ShutdownError),
}

impl LocalMetadataStoreService {
    pub fn new(metadata_store: LocalMetadataStore, bind_address: BindAddress) -> Self {
        Self {
            metadata_store,
            bind_address,
        }
    }
    pub fn from_options(
        opts: &MetadataStoreOptions,
        rocksdb_options: impl LiveLoad<RocksDbOptions> + Send + Sync + Clone + 'static,
    ) -> Result<Self, BuildError> {
        let store = LocalMetadataStore::new(opts, rocksdb_options)?;
        Ok(LocalMetadataStoreService::new(
            store,
            opts.bind_address.clone(),
        ))
    }

    pub fn grpc_service_name(&self) -> &str {
        MetadataStoreSvcServer::<LocalMetadataStoreHandler>::NAME
    }

    pub async fn run(self) -> Result<(), Error> {
        // Trace layer
        let span_factory = tower_http::trace::DefaultMakeSpan::new()
            .include_headers(true)
            .level(tracing::Level::ERROR);

        let reflection_service_builder = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(grpc_svc::FILE_DESCRIPTOR_SET);

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<MetadataStoreSvcServer<LocalMetadataStoreHandler>>()
            .await;

        let server_builder = tonic::transport::Server::builder()
            .layer(tower_http::trace::TraceLayer::new_for_grpc().make_span_with(span_factory))
            .add_service(health_service)
            .add_service(MetadataStoreSvcServer::new(LocalMetadataStoreHandler::new(
                self.metadata_store.request_sender(),
            )))
            .add_service(reflection_service_builder.build()?);

        let service = server_builder.into_service();

        task_center().spawn_child(
            TaskKind::RpcServer,
            "metadata-store-grpc",
            None,
            async move {
                grpc_util::run_hyper_server(
                    &self.bind_address,
                    service,
                    cancellation_watcher(),
                    "metadata-store-grpc",
                )
                .await?;
                Ok(())
            },
        )?;

        self.metadata_store.run().await;

        Ok(())
    }
}
