// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use clap::Parser;
use cling::Cling;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tonic::{Request, Response, Status};

use restate_cli_util::CliContext;
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_server::{NodeCtlSvc, NodeCtlSvcServer};
use restate_core::protobuf::node_ctl_svc::{
    GetMetadataRequest, GetMetadataResponse, IdentResponse, ProvisionClusterRequest,
    ProvisionClusterResponse, TriggerCompactionRequest, TriggerCompactionResponse,
    new_node_ctl_client,
};
use restate_types::logs::metadata::Logs;
use restate_types::net::address::ControlPort;
use restate_types::nodes_config::{
    ClusterFingerprint, NodeConfig, NodesConfiguration, QualifiedAddress, Role,
};
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::common::{MetadataKind, NodeStatus};
use restate_types::storage::StorageCodec;
use restate_types::{GenerationalNodeId, RestateVersion, Version};

use super::{ConnectionInfo, ConnectionInfoError};
use crate::CliApp;
use crate::commands::node::list_nodes::{ListNodesOpts, list_nodes};

#[derive(Clone)]
struct ControlService {
    route: &'static str,
    nodes: Arc<NodesConfiguration>,
    calls: Arc<Mutex<Vec<(&'static str, &'static str)>>>,
}

impl ControlService {
    fn record(&self, method: &'static str) {
        self.calls.lock().unwrap().push((self.route, method));
    }
}

#[tonic::async_trait]
impl NodeCtlSvc for ControlService {
    async fn get_ident(&self, _: Request<()>) -> Result<Response<IdentResponse>, Status> {
        self.record("ident");
        Ok(Response::new(IdentResponse {
            status: NodeStatus::Alive.into(),
            cluster_name: self.route.to_owned(),
            roles: vec![Role::Admin.to_string()],
            nodes_config_version: 1,
            logs_version: 1,
            partition_table_version: 1,
            ..Default::default()
        }))
    }

    async fn get_metadata(
        &self,
        request: Request<GetMetadataRequest>,
    ) -> Result<Response<GetMetadataResponse>, Status> {
        let mut encoded = BytesMut::new();
        match request.into_inner().kind() {
            MetadataKind::NodesConfiguration => {
                self.record("nodes");
                StorageCodec::encode(self.nodes.as_ref(), &mut encoded).unwrap();
            }
            MetadataKind::Logs => {
                self.record("logs");
                StorageCodec::encode(
                    &Logs::with_logs_configuration(Default::default()),
                    &mut encoded,
                )
                .unwrap();
            }
            MetadataKind::PartitionTable => {
                self.record("partitions");
                StorageCodec::encode(
                    &PartitionTable::with_equally_sized_partitions(Version::MIN, 0),
                    &mut encoded,
                )
                .unwrap();
            }
            _ => return Err(Status::unimplemented("metadata kind")),
        }
        Ok(Response::new(GetMetadataResponse {
            encoded: encoded.freeze(),
        }))
    }

    async fn provision_cluster(
        &self,
        _: Request<ProvisionClusterRequest>,
    ) -> Result<Response<ProvisionClusterResponse>, Status> {
        Err(Status::unimplemented("provision"))
    }

    async fn trigger_compaction(
        &self,
        _: Request<TriggerCompactionRequest>,
    ) -> Result<Response<TriggerCompactionResponse>, Status> {
        self.record("compact");
        Ok(Response::new(TriggerCompactionResponse::default()))
    }
}

/// Serves the control service for `nodes` on each listener, recording calls by route.
fn serve(
    nodes: &Arc<NodesConfiguration>,
    calls: &Arc<Mutex<Vec<(&'static str, &'static str)>>>,
    listeners: impl IntoIterator<Item = (&'static str, TcpListener)>,
) -> JoinSet<Result<(), tonic::transport::Error>> {
    let mut servers = JoinSet::new();
    for (route, listener) in listeners {
        let service = NodeCtlSvcServer::new(ControlService {
            route,
            nodes: nodes.clone(),
            calls: calls.clone(),
        })
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        servers.spawn(async move {
            let incoming = futures::stream::unfold(listener, |listener| async {
                Some((listener.accept().await.map(|(stream, _)| stream), listener))
            });
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
        });
    }
    servers
}

#[tokio::test]
async fn discovered_calls_use_control_routes_and_legacy_fallback() -> anyhow::Result<()> {
    let fabric = TcpListener::bind("127.0.0.1:0").await?;
    let control = TcpListener::bind("127.0.0.1:0").await?;
    let legacy = TcpListener::bind("127.0.0.1:0").await?;
    let fabric_address = format!("http://{}", fabric.local_addr()?);
    let control_address = format!("http://{}", control.local_addr()?);
    let legacy_address = format!("http://{}", legacy.local_addr()?);

    let mut nodes = NodesConfiguration::new(
        Version::MIN,
        "control-routing".to_owned(),
        ClusterFingerprint::generate(),
    );
    nodes.upsert_node(
        NodeConfig::builder()
            .name("control".to_owned())
            .current_generation(GenerationalNodeId::new(1, 1))
            .address(fabric_address.parse()?)
            .ctrl_address(Some(control_address.parse()?))
            .roles(Role::Admin.into())
            .binary_version(RestateVersion::current())
            .build(),
    );
    nodes.upsert_node(
        NodeConfig::builder()
            .name("legacy".to_owned())
            .current_generation(GenerationalNodeId::new(2, 1))
            .address(legacy_address.parse()?)
            .roles(Role::Worker.into())
            .binary_version(RestateVersion::current())
            .build(),
    );
    let nodes = Arc::new(nodes);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let _servers = serve(
        &nodes,
        &calls,
        [("fabric", fabric), ("control", control), ("legacy", legacy)],
    );

    let connection = ConnectionInfo::try_parse_from(["ctl", "--address", &fabric_address])?;
    connection.get_nodes_configuration().await?;
    assert_eq!(
        std::mem::take(&mut *calls.lock().unwrap()),
        [("fabric", "ident"), ("fabric", "nodes")]
    );

    // A cached bootstrap fabric connection must not outrank a node's control route.
    connection.get_logs().await?;
    connection.get_partition_table().await?;
    let ident = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_node_ctl_client(channel, &CliContext::get().network)
                .get_ident(())
                .await
        })
        .await?
        .into_inner();
    assert_eq!(ident.cluster_name, "control");
    list_nodes(&connection, &ListNodesOpts { extra: true }).await?;
    let discovered_calls = std::mem::take(&mut *calls.lock().unwrap());
    assert!(discovered_calls.iter().all(|(route, _)| *route != "fabric"));
    for route in ["control", "legacy"] {
        assert!(discovered_calls.contains(&(route, "ident")));
    }
    assert!(discovered_calls.iter().any(|(_, method)| *method == "logs"));
    assert!(
        discovered_calls
            .iter()
            .any(|(_, method)| *method == "partitions")
    );

    // Control/fabric aliases have one cache entry, and failures are scoped to the route.
    let cache_size = connection.open_connections.lock().await.len();
    connection.connect(&fabric_address.parse()?).await?;
    assert_eq!(connection.open_connections.lock().await.len(), cache_size);
    connection
        .dead_nodes
        .write()
        .unwrap()
        .insert(fabric_address.parse()?);
    connection.connect(&control_address.parse()?).await?;
    let mut attempts = 0;
    let terminal = connection
        .try_each(None, |_| {
            attempts += 1;
            std::future::ready(Err::<(), _>(Status::failed_precondition("stop")))
        })
        .await;
    assert!(matches!(terminal, Err(ConnectionInfoError::NodesErrors(_))));
    assert_eq!(attempts, 1);

    // Exercise the direct compaction path through the command parser as well.
    let result = Cling::<CliApp>::try_parse_from([
        "restatectl",
        "--address",
        &fabric_address,
        "storage",
        "compact",
        "--node",
        "N1,N2",
    ])?
    .run()
    .await;
    assert!(result.is_success(), "{:?}", result.result_ref());
    let direct_calls = std::mem::take(&mut *calls.lock().unwrap());
    for route in ["control", "legacy"] {
        assert_eq!(
            direct_calls
                .iter()
                .filter(|call| **call == (route, "compact"))
                .count(),
            1
        );
    }
    assert!(!direct_calls.contains(&("fabric", "compact")));

    // Explicit single-address metadata/operations keep using the supplied seed.
    let single = ConnectionInfo::try_parse_from(["ctl", "--single-address", &fabric_address])?;
    single.get_logs().await?;
    single.get_partition_table().await?;
    let ident = single
        .try_each(Some(Role::Admin), |channel| async {
            new_node_ctl_client(channel, &CliContext::get().network)
                .get_ident(())
                .await
        })
        .await?
        .into_inner();
    assert_eq!(ident.cluster_name, "fabric");
    assert!(
        calls
            .lock()
            .unwrap()
            .iter()
            .all(|(route, _)| *route == "fabric")
    );

    // Explicit UDS paths and HTTPS URIs are preserved when parsing seeds.
    for (input, expected) in [
        (
            "unix:/cluster/node/fabric.sock",
            "unix:/cluster/node/fabric.sock",
        ),
        ("https://[::1]:9443/rpc", "https://[::1]:9443/rpc"),
    ] {
        let parsed = ConnectionInfo::try_parse_from(["ctl", "--address", input])?;
        assert_eq!(parsed.get_effective_addresses()[0].to_string(), expected);
    }
    Ok(())
}

#[tokio::test]
async fn location_entries_are_selected_by_seed_inference_and_flag() -> anyhow::Result<()> {
    // Node 1 publishes a plain control address and entries for callers in `external` and in
    // zone `z1`; node 2 publishes a plain control address and an `external` entry behind an
    // `https` one. Selection needs no network, so only node 1's external route is served, for
    // the end-to-end call at the end; the other addresses just have to differ.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let n1_external = format!("http://{}", listener.local_addr()?);
    let address = |route: &str| match route {
        "n1-external" => n1_external.clone(),
        route => format!("http://{route}:5122"),
    };
    let entry = |route: &str, location: &str| QualifiedAddress::<ControlPort> {
        address: address(route).parse().unwrap(),
        location: Some(location.parse().unwrap()),
    };
    let mut nodes = NodesConfiguration::new(
        Version::MIN,
        "location-routing".to_owned(),
        ClusterFingerprint::generate(),
    );
    nodes.upsert_node(
        NodeConfig::builder()
            .name("n1".to_owned())
            .current_generation(GenerationalNodeId::new(1, 1))
            .address(address("n1-plain").parse()?)
            .ctrl_address(Some(address("n1-plain").parse()?))
            .ctrl_addresses(vec![
                entry("n1-external", "external"),
                entry("n1-zone", "z1"),
            ])
            .roles(Role::Admin.into())
            .binary_version(RestateVersion::current())
            .build(),
    );
    nodes.upsert_node(
        NodeConfig::builder()
            .name("n2".to_owned())
            .current_generation(GenerationalNodeId::new(2, 1))
            .address(address("n2-plain").parse()?)
            .ctrl_address(Some(address("n2-plain").parse()?))
            .ctrl_addresses(vec![
                QualifiedAddress {
                    address: "https://n2.example.com".parse()?,
                    location: Some("external".parse()?),
                },
                entry("n2-external", "external"),
            ])
            .roles(Role::Worker.into())
            .binary_version(RestateVersion::current())
            .build(),
    );
    let nodes = Arc::new(nodes);
    let dials = |args: &[&str]| -> anyhow::Result<[String; 2]> {
        let connection = ConnectionInfo::try_parse_from([&["ctl"][..], args].concat())?;
        Ok(["n1", "n2"].map(|name| {
            connection
                .ctrl_address_for(&nodes, nodes.find_node_by_name(name).unwrap())
                .to_string()
        }))
    };
    let dialed = |route: &str| format!("{}/", address(route));
    let n1_plain = address("n1-plain");
    let two_locations = format!("{},{}", n1_external, address("n1-zone"));
    for (args, expected, case) in [
        (
            vec!["--address", &n1_external],
            ["n1-external", "n2-external"],
            "a seed matching node 1's external entry adopts external for every node; node 2's \
             https entry is skipped without a CA",
        ),
        (
            vec!["--address", &n1_plain],
            ["n1-plain", "n2-plain"],
            "a seed matching no entry: the plain addresses",
        ),
        (
            vec!["--address", &n1_external, "--location", "z1"],
            ["n1-zone", "n2-plain"],
            "--location overrides inference; a node without an entry there keeps its plain address",
        ),
        (
            vec!["--address", &two_locations],
            ["n1-plain", "n2-plain"],
            "seeds matching entries in two locations are ambiguous: the plain addresses",
        ),
    ] {
        assert_eq!(dials(&args)?, expected.map(dialed), "{case}");
    }

    // End to end: a tool seeded through the external route makes its discovered calls there.
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut servers = serve(&nodes, &calls, [("n1-external", listener)]);
    let connection = ConnectionInfo::try_parse_from(["ctl", "--address", &n1_external])?;
    connection.get_nodes_configuration().await?;
    let ident = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_node_ctl_client(channel, &CliContext::get().network)
                .get_ident(())
                .await
        })
        .await?
        .into_inner();
    assert_eq!(ident.cluster_name, "n1-external");
    servers.abort_all();
    Ok(())
}
