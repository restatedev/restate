// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::Json;
use axum::http::StatusCode;
use axum::routing::{MethodFilter, get, on};

use restate_core::TaskCenter;
use restate_core::network::grpc::CoreNodeSvcHandler;
use restate_core::network::{ConnectionManager, NetworkServerBuilder};
use restate_core::{Identification, MetadataWriter};
use restate_tracing_instrumentation::prometheus_metrics::Prometheus;
use restate_types::config::Configuration;

use super::grpc_svc_handler::{MetadataProxySvcHandler, NodeCtlSvcHandler};
use super::pprof;
use crate::network_server::metrics::render_metrics;
use crate::network_server::state::NodeCtrlHandlerStateBuilder;

pub struct NetworkServer {}

impl NetworkServer {
    pub async fn run(
        connection_manager: ConnectionManager,
        mut server_builder: NetworkServerBuilder,
        metadata_writer: MetadataWriter,
        prometheus: Prometheus,
    ) -> Result<(), anyhow::Error> {
        // Configure Metric Exporter
        let mut state_builder = NodeCtrlHandlerStateBuilder::default();
        state_builder.task_center(TaskCenter::current());

        state_builder.prometheus_handle(prometheus.into());

        let shared_state = state_builder.build().expect("should be infallible");

        let post_or_put = MethodFilter::POST.or(MethodFilter::PUT);

        // -- HTTP service (for prometheus et al.)
        let axum_router = axum::Router::new()
            .route("/health", get(report_health))
            .route("/health/live", get(report_health))
            .route("/health/ready", get(report_readiness))
            .route("/metrics", get(render_metrics))
            .route("/debug/pprof/heap", get(pprof::heap))
            .route(
                "/debug/pprof/heap/activate",
                on(post_or_put, pprof::activate_heap),
            )
            .route(
                "/debug/pprof/heap/deactivate",
                on(post_or_put, pprof::deactivate_heap),
            )
            // Forward compatibility to remove "pprof" prefix
            // Introduced in v1.6.0
            .route("/debug/heap/dump", get(pprof::heap))
            .route(
                "/debug/heap/activate",
                on(post_or_put, pprof::activate_heap),
            )
            .route(
                "/debug/heap/deactivate",
                on(post_or_put, pprof::deactivate_heap),
            )
            .route("/debug/heap/purge", on(post_or_put, pprof::jemalloc_purge))
            .route("/debug/heap/flamegraph", get(pprof::heap_flamegraph))
            .with_state(shared_state);

        server_builder.register_axum_routes(axum_router);

        let node_rpc_health = TaskCenter::with_current(|tc| tc.health().node_rpc_status());

        server_builder.register_grpc_service(
            MetadataProxySvcHandler::new(metadata_writer.raw_metadata_store_client().clone())
                .into_server(&Configuration::pinned().networking),
            restate_metadata_store::protobuf::metadata_proxy_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder.register_grpc_service(
            NodeCtlSvcHandler::new(metadata_writer)
                .into_server(&Configuration::pinned().networking),
            restate_core::protobuf::node_ctl_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder.register_grpc_service(
            CoreNodeSvcHandler::new(connection_manager)
                .into_server(&Configuration::pinned().networking),
            restate_core::network::protobuf::core_node_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder.run(node_rpc_health).await?;

        Ok(())
    }
}

/// Always responds with `200 OK`. Also serves as liveness check (`/health/live`): a response means
/// the process is up and serving requests, regardless of the node's status.
pub async fn report_health() -> Json<Identification> {
    Json(Identification::get())
}

/// Readiness check (`/health/ready`): responds with `200 OK` if the node is alive, i.e. all of its
/// roles have started, and with `503 Service Unavailable` otherwise (e.g. starting up or shutting
/// down).
pub async fn report_readiness() -> (StatusCode, Json<Identification>) {
    let identification = Identification::get();
    let status_code = if identification.status.is_alive() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status_code, Json(identification))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use restate_core::{TaskCenter, TestCoreEnvBuilder};
    use restate_types::config::{Configuration, set_current_config};
    use restate_types::health::NodeStatus;
    use restate_types::net::listener::AddressBook;

    use super::report_readiness;

    #[restate_core::test]
    async fn health_endpoints_status_codes() {
        set_current_config(Configuration::new_unix_sockets());
        assert!(TaskCenter::try_set_address_book(AddressBook::new(
            restate_types::config::node_filepath("")
        )));
        let _env = TestCoreEnvBuilder::with_incoming_only_connector()
            .build()
            .await;

        for (node_status, expected_readiness) in [
            (NodeStatus::Unknown, StatusCode::SERVICE_UNAVAILABLE),
            (NodeStatus::StartingUp, StatusCode::SERVICE_UNAVAILABLE),
            (NodeStatus::Alive, StatusCode::OK),
            (NodeStatus::ShuttingDown, StatusCode::SERVICE_UNAVAILABLE),
        ] {
            TaskCenter::with_current(|tc| tc.health().node_status().update(node_status));

            let (readiness, body) = report_readiness().await;
            assert_eq!(readiness, expected_readiness, "{node_status:?}");
            assert_eq!(body.status, node_status);
        }
    }
}
