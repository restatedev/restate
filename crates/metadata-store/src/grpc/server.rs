// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
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
use restate_core::network::net_util;
use restate_core::ShutdownError;
use restate_types::health::HealthStatus;
use restate_types::net::BindAddress;
use restate_types::protobuf::common::MetadataServerStatus;
use tonic::body::boxed;
use tonic::service::Routes;
use tower::ServiceExt;
use tower_http::classify::{GrpcCode, GrpcErrorsAsFailures, SharedClassifier};

pub struct GrpcServer {
    bind_address: BindAddress,
    routes: Routes,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed running grpc server: {0}")]
    GrpcServer(#[from] net_util::Error),
    #[error("system is shutting down")]
    Shutdown(#[from] ShutdownError),
}

impl GrpcServer {
    pub fn new(bind_address: BindAddress, routes: Routes) -> Self {
        Self {
            bind_address,
            routes,
        }
    }

    pub async fn run(self, health_status: HealthStatus<MetadataServerStatus>) -> Result<(), Error> {
        let span_factory = tower_http::trace::DefaultMakeSpan::new()
            .include_headers(true)
            .level(tracing::Level::ERROR);

        let trace_layer = tower_http::trace::TraceLayer::new(SharedClassifier::new(
            GrpcErrorsAsFailures::new().with_success(GrpcCode::FailedPrecondition),
        ))
        .make_span_with(span_factory);

        let server_builder = tonic::transport::Server::builder()
            .layer(trace_layer)
            .add_routes(self.routes);

        let service = TowerToHyperService::new(
            server_builder
                .into_service()
                .map_request(|req: Request<Incoming>| req.map(boxed)),
        );

        net_util::run_hyper_server(
            &self.bind_address,
            service,
            "metadata-store-grpc",
            || health_status.update(MetadataServerStatus::Ready),
            || health_status.update(MetadataServerStatus::Unknown),
        )
        .await?;

        Ok(())
    }
}
