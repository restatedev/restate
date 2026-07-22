// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use cling::prelude::*;

use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_client::NodeCtlSvcClient;
use restate_core::protobuf::node_ctl_svc::{
    DrainHttpIngressRequest, HttpIngressDrainStatus, IdentResponse, new_node_ctl_client,
};
use restate_types::net::address::{AdvertisedAddress, FabricPort};
use restate_types::{GenerationalNodeId, NodeId};

use crate::connection::ConnectionInfo;
use crate::util::grpc_channel;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "drain_http_ingress")]
/// Starts or polls the lifetime-scoped HTTP ingress drain on one node.
///
/// Use `--single-address http://127.0.0.1:5122` to address the local node directly without
/// reading the nodes configuration. This is the recommended mode for a preStop hook.
pub struct DrainHttpIngressOpts {
    /// The exact node generation to drain (for example N1:9)
    #[arg(long, required_unless_present = "current", conflicts_with = "current")]
    node: Option<GenerationalNodeId>,

    /// Drain the node at --single-address using its current generation
    #[arg(long, conflicts_with = "node")]
    current: bool,

    /// Print the response as JSON
    #[arg(long)]
    json: bool,
}

async fn drain_http_ingress(
    connection: &ConnectionInfo,
    opts: &DrainHttpIngressOpts,
) -> anyhow::Result<()> {
    let address = target_address(connection, opts).await?;
    let channel = grpc_channel(address);
    let mut client = new_node_ctl_client(channel, &CliContext::get().network);
    let node_id = match opts.node {
        Some(node_id) => node_id,
        None => current_node_id(&mut client).await?,
    };
    let request = drain_request(node_id);

    let timeout = CliContext::get().request_timeout();
    let response = tokio::time::timeout(timeout, client.drain_http_ingress(request))
        .await
        .map_err(|_| anyhow::anyhow!("HTTP ingress drain request timed out after {timeout:?}"))??
        .into_inner();
    let status = HttpIngressDrainStatus::try_from(response.status)
        .unwrap_or(HttpIngressDrainStatus::Unspecified);
    let status = status_name(status);

    if opts.json {
        c_println!(
            "{}",
            serde_json::json!({
                "node_id": response.node_id.map(|node_id| node_id.to_string()),
                "status": status,
                "in_flight_requests": response.in_flight_requests,
                "in_flight_connections": response.in_flight_connections,
            })
        );
    } else {
        c_println!(
            "{} HTTP ingress: {}; in-flight requests: {}; in-flight connections: {}",
            node_id,
            status,
            response.in_flight_requests,
            response.in_flight_connections,
        );
    }

    Ok(())
}

async fn target_address(
    connection: &ConnectionInfo,
    opts: &DrainHttpIngressOpts,
) -> anyhow::Result<AdvertisedAddress<FabricPort>> {
    if opts.current {
        return current_address(connection.single_address.as_ref());
    }

    if let Some(address) = &connection.single_address {
        return Ok(address.clone());
    }

    let node_id = opts.node.expect("--node is required without --current");

    let nodes_config = connection
        .get_nodes_configuration()
        .await
        .context("failed to discover the target node; use --single-address to connect directly")?;
    Ok(nodes_config.find_node_by_id(node_id)?.address.clone())
}

fn current_address(
    single_address: Option<&AdvertisedAddress<FabricPort>>,
) -> anyhow::Result<AdvertisedAddress<FabricPort>> {
    single_address
        .cloned()
        .context("--current requires --single-address")
}

async fn current_node_id(
    client: &mut NodeCtlSvcClient<tonic::transport::Channel>,
) -> anyhow::Result<GenerationalNodeId> {
    let timeout = CliContext::get().request_timeout();
    let response = tokio::time::timeout(timeout, client.get_ident(()))
        .await
        .map_err(|_| anyhow::anyhow!("GetIdent request timed out after {timeout:?}"))??
        .into_inner();

    current_node_id_from_ident(response)
}

fn current_node_id_from_ident(response: IdentResponse) -> anyhow::Result<GenerationalNodeId> {
    response
        .node_id
        .map(NodeId::from)
        .and_then(NodeId::as_generational)
        .context("the node at --single-address has not initialized a generational node ID")
}

fn drain_request(node_id: GenerationalNodeId) -> DrainHttpIngressRequest {
    DrainHttpIngressRequest {
        expected_node_id: Some(node_id.into()),
    }
}

fn status_name(status: HttpIngressDrainStatus) -> &'static str {
    match status {
        HttpIngressDrainStatus::NotPresent => "not-present",
        HttpIngressDrainStatus::Active => "active",
        HttpIngressDrainStatus::Draining => "draining",
        HttpIngressDrainStatus::Drained => "drained",
        HttpIngressDrainStatus::Unspecified => "unspecified",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::stream;
    use tokio::sync::{Mutex, oneshot};
    use tonic::{Request, Response, Status, transport::Server};

    use restate_core::protobuf::node_ctl_svc::node_ctl_svc_server::{NodeCtlSvc, NodeCtlSvcServer};
    use restate_core::protobuf::node_ctl_svc::{
        ClusterHealthResponse, DrainHttpIngressResponse, GetMetadataRequest, GetMetadataResponse,
        ProvisionClusterRequest, ProvisionClusterResponse, TriggerCompactionRequest,
        TriggerCompactionResponse,
    };

    use super::*;

    #[derive(Debug, PartialEq)]
    enum Call {
        GetIdent,
        Drain(Option<restate_types::protobuf::common::GenerationalNodeId>),
    }

    struct TestNodeCtlSvc {
        node_id: GenerationalNodeId,
        calls: Arc<Mutex<Vec<Call>>>,
    }

    #[tonic::async_trait]
    impl NodeCtlSvc for TestNodeCtlSvc {
        async fn get_ident(
            &self,
            _request: Request<()>,
        ) -> Result<Response<IdentResponse>, Status> {
            self.calls.lock().await.push(Call::GetIdent);
            Ok(Response::new(IdentResponse {
                node_id: Some(self.node_id.into()),
                ..Default::default()
            }))
        }

        async fn get_metadata(
            &self,
            _request: Request<GetMetadataRequest>,
        ) -> Result<Response<GetMetadataResponse>, Status> {
            Err(Status::unimplemented("not needed by this test"))
        }

        async fn provision_cluster(
            &self,
            _request: Request<ProvisionClusterRequest>,
        ) -> Result<Response<ProvisionClusterResponse>, Status> {
            Err(Status::unimplemented("not needed by this test"))
        }

        async fn cluster_health(
            &self,
            _request: Request<()>,
        ) -> Result<Response<ClusterHealthResponse>, Status> {
            Err(Status::unimplemented("not needed by this test"))
        }

        async fn trigger_compaction(
            &self,
            _request: Request<TriggerCompactionRequest>,
        ) -> Result<Response<TriggerCompactionResponse>, Status> {
            Err(Status::unimplemented("not needed by this test"))
        }

        async fn drain_http_ingress(
            &self,
            request: Request<DrainHttpIngressRequest>,
        ) -> Result<Response<DrainHttpIngressResponse>, Status> {
            self.calls
                .lock()
                .await
                .push(Call::Drain(request.into_inner().expected_node_id));
            Ok(Response::new(DrainHttpIngressResponse {
                node_id: Some(self.node_id.into()),
                status: HttpIngressDrainStatus::Drained.into(),
                in_flight_requests: 0,
                in_flight_connections: 0,
            }))
        }
    }

    #[test]
    fn current_and_node_are_exclusive() {
        assert!(
            DrainHttpIngressOpts::try_parse_from([
                "drain-http-ingress",
                "--current",
                "--node",
                "N1:9",
            ])
            .is_err()
        );
    }

    #[test]
    fn either_current_or_node_is_required() {
        assert!(DrainHttpIngressOpts::try_parse_from(["drain-http-ingress"]).is_err());
        assert_eq!(
            DrainHttpIngressOpts::try_parse_from(["drain-http-ingress", "--current"])
                .expect("--current should parse")
                .node,
            None
        );
    }

    #[test]
    fn current_requires_single_address() {
        assert!(current_address(None).is_err());
    }

    #[test]
    fn current_uses_the_exact_ident_generation_for_the_drain_request() {
        let expected_node_id = GenerationalNodeId::new(1, 9);
        let response = IdentResponse {
            node_id: Some(expected_node_id.into()),
            ..Default::default()
        };

        let request = drain_request(
            current_node_id_from_ident(response).expect("ident should contain a generation"),
        );

        assert_eq!(request.expected_node_id, Some(expected_node_id.into()));
    }

    #[tokio::test]
    async fn current_drains_the_generation_resolved_from_the_single_address() {
        let expected_node_id = GenerationalNodeId::new(1, 9);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let service = TestNodeCtlSvc {
            node_id: expected_node_id,
            calls: Arc::clone(&calls),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have an address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(
                    NodeCtlSvcServer::new(service)
                        .accept_compressed(tonic::codec::CompressionEncoding::Zstd),
                )
                .serve_with_incoming_shutdown(
                    stream::unfold(listener, |listener| async move {
                        listener
                            .accept()
                            .await
                            .ok()
                            .map(|(stream, _)| (Ok::<_, std::io::Error>(stream), listener))
                    }),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .expect("server should run");
        });

        let connection = ConnectionInfo::try_parse_from([
            "restatectl",
            "--single-address",
            &format!("http://{address}"),
        ])
        .expect("single address should parse");
        let opts = DrainHttpIngressOpts::try_parse_from(["drain-http-ingress", "--current"])
            .expect("current should parse");

        drain_http_ingress(&connection, &opts)
            .await
            .expect("drain should succeed");

        shutdown_tx
            .send(())
            .expect("server should still be running");
        server.await.expect("server task should not panic");
        assert_eq!(
            *calls.lock().await,
            vec![Call::GetIdent, Call::Drain(Some(expected_node_id.into()))]
        );
    }
}
