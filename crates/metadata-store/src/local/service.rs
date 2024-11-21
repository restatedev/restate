// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::Request;
use hyper::body::Incoming;
use hyper_util::service::TowerToHyperService;
use restate_types::health::HealthStatus;
use tonic::body::boxed;
use tonic::server::NamedService;
use tower::ServiceExt;
use tower_http::classify::{GrpcCode, GrpcErrorsAsFailures, SharedClassifier};

use restate_core::network::net_util;
use restate_core::{task_center, ShutdownError, TaskKind};
use restate_rocksdb::RocksError;
use restate_types::config::{MetadataStoreOptions, RocksDbOptions};
use restate_types::live::BoxedLiveLoad;
use restate_types::protobuf::common::MetadataServerStatus;

use crate::grpc_svc;
use crate::grpc_svc::metadata_store_svc_server::MetadataStoreSvcServer;
use crate::local::grpc::handler::LocalMetadataStoreHandler;
use crate::local::store::LocalMetadataStore;

pub struct LocalMetadataStoreService {
    health_status: HealthStatus<MetadataServerStatus>,
    opts: BoxedLiveLoad<MetadataStoreOptions>,
    rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed running grpc server: {0}")]
    GrpcServer(#[from] net_util::Error),
    #[error("error while running server server grpc reflection service: {0}")]
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

    pub fn grpc_service_name(&self) -> &str {
        MetadataStoreSvcServer::<LocalMetadataStoreHandler>::NAME
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

        let trace_layer = tower_http::trace::TraceLayer::new(SharedClassifier::new(
            GrpcErrorsAsFailures::new().with_success(GrpcCode::FailedPrecondition),
        ))
        .make_span_with(
            tower_http::trace::DefaultMakeSpan::new()
                .include_headers(true)
                .level(tracing::Level::ERROR),
        );

        let reflection_service_builder = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(grpc_svc::FILE_DESCRIPTOR_SET);

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<MetadataStoreSvcServer<LocalMetadataStoreHandler>>()
            .await;

        let server_builder = tonic::transport::Server::builder()
            .layer(trace_layer)
            .add_service(health_service)
            .add_service(MetadataStoreSvcServer::new(LocalMetadataStoreHandler::new(
                store.request_sender(),
            )))
            .add_service(reflection_service_builder.build_v1()?);

        let service = TowerToHyperService::new(
            server_builder
                .into_service()
                .map_request(|req: Request<Incoming>| req.map(boxed)),
        );

        task_center().spawn_child(
            TaskKind::RpcServer,
            "metadata-store-grpc",
            None,
            async move {
                net_util::run_hyper_server(
                    &bind_address,
                    service,
                    "metadata-store-grpc",
                    || health_status.update(MetadataServerStatus::Ready),
                    || health_status.update(MetadataServerStatus::Unknown),
                )
                .await?;
                Ok(())
            },
        )?;

        store.run().await;

        Ok(())
    }
}
