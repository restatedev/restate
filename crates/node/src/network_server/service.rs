// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::pin;

use axum::routing::{get, on, MethodFilter};
use tokio::time::MissedTickBehavior;
use tonic::codec::CompressionEncoding;
use tracing::{debug, trace};

use restate_core::metadata_store::MetadataStoreClient;
use restate_core::network::protobuf::core_node_svc::core_node_svc_server::CoreNodeSvcServer;
use restate_core::network::tonic_service_filter::{TonicServiceFilter, WaitForReady};
use restate_core::network::{ConnectionManager, NetworkServerBuilder, TransportConnect};
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_server::NodeCtlSvcServer;
use restate_core::{cancellation_watcher, TaskCenter, TaskKind};
use restate_types::config::CommonOptions;
use restate_types::health::Health;
use restate_types::protobuf::common::NodeStatus;

use super::grpc_svc_handler::{CoreNodeSvcHandler, NodeCtlSvcHandler};
use super::pprof;
use crate::network_server::metrics::{install_global_prometheus_recorder, render_metrics};
use crate::network_server::state::NodeCtrlHandlerStateBuilder;

pub struct NetworkServer {}

impl NetworkServer {
    pub async fn run<T: TransportConnect>(
        health: Health,
        connection_manager: ConnectionManager<T>,
        mut server_builder: NetworkServerBuilder,
        options: CommonOptions,
        metadata_store_client: MetadataStoreClient,
    ) -> Result<(), anyhow::Error> {
        // Configure Metric Exporter
        let mut state_builder = NodeCtrlHandlerStateBuilder::default();
        state_builder.task_center(TaskCenter::current());

        if !options.disable_prometheus {
            let prometheus_handle = install_global_prometheus_recorder(&options);

            TaskCenter::spawn_child(TaskKind::SystemService, "prometheus-metrics-upkeep", {
                let prometheus_handle = prometheus_handle.clone();
                async move {
                    debug!("Prometheus metrics upkeep loop started");

                    let mut update_interval =
                        tokio::time::interval(std::time::Duration::from_secs(5));
                    update_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    let mut cancel = pin!(cancellation_watcher());

                    loop {
                        tokio::select! {
                            _ = &mut cancel => {
                                debug!("Prometheus metrics upkeep loop stopped");
                                break;
                            }
                            _ = update_interval.tick() => {
                                trace!("Performing Prometheus metrics upkeep...");
                                prometheus_handle.run_upkeep();
                            }
                        }
                    }
                    Ok(())
                }
            })?;

            state_builder.prometheus_handle(Some(prometheus_handle));
        }

        let shared_state = state_builder.build().expect("should be infallible");

        let post_or_put = MethodFilter::POST.or(MethodFilter::PUT);

        // -- HTTP service (for prometheus et al.)
        let axum_router = axum::Router::new()
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
            .with_state(shared_state);

        server_builder.register_axum_routes(axum_router);

        let node_health = health.node_status();
        let node_rpc_health = health.node_rpc_status();

        server_builder.register_grpc_service(
            NodeCtlSvcServer::new(NodeCtlSvcHandler::new(
                TaskCenter::current(),
                options.cluster_name().to_owned(),
                options.roles,
                health,
                metadata_store_client,
            ))
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip),
            restate_core::protobuf::node_ctl_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder.register_grpc_service(
            TonicServiceFilter::new(
                CoreNodeSvcServer::new(CoreNodeSvcHandler::new(connection_manager))
                    .max_decoding_message_size(32 * 1024 * 1024)
                    .max_encoding_message_size(32 * 1024 * 1024)
                    .accept_compressed(CompressionEncoding::Gzip)
                    .send_compressed(CompressionEncoding::Gzip),
                WaitForReady::new(node_health, NodeStatus::Alive),
            ),
            restate_core::network::protobuf::core_node_svc::FILE_DESCRIPTOR_SET,
        );

        server_builder
            .run(node_rpc_health, &options.bind_address.unwrap())
            .await?;

        Ok(())
    }
}
