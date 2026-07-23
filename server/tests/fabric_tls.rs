// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use enumset::EnumSet;
use googletest::IntoTestResult;
use rcgen::{CertificateParams, KeyPair};
use tempfile::TempDir;
use tracing::info;

use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::network::tls::{ClientIdentityFiles, TlsClientConfig};
use restate_core::protobuf::node_ctl_svc::new_node_ctl_client;
use restate_local_cluster_runner::{
    cluster::{Cluster, StartedCluster},
    node::{BinarySource, NodeSpec},
};
use restate_types::NodeId;
use restate_types::config::{Configuration, FabricTlsOptions, NetworkingOptions, TlsMode};
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::address::{AdvertisedAddress, FabricPort};
use restate_types::nodes_config::NodesConfiguration;
use restate_types::replication::ReplicationProperty;

mod common;

fn generate_ca() -> (rcgen::Certificate, KeyPair) {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "test-ca");
    let key_pair = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    (cert, key_pair)
}

fn generate_node_cert(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    node_name: &str,
) -> (rcgen::Certificate, KeyPair) {
    // Test nodes bind and advertise loopback addresses, so the cert must carry
    // matching SANs for server-certificate hostname verification to succeed.
    let mut params = CertificateParams::new(vec![
        node_name.to_owned(),
        "localhost".to_owned(),
        "127.0.0.1".to_owned(),
        "::1".to_owned(),
    ])
    .unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, node_name);
    let node_key = KeyPair::generate().unwrap();
    let node_cert = params.signed_by(&node_key, ca_cert, ca_key).unwrap();
    (node_cert, node_key)
}

fn write_certs_to_dir(
    dir: &Path,
    ca_cert: &rcgen::Certificate,
    node_cert: &rcgen::Certificate,
    node_key: &KeyPair,
) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let ca_path = dir.join("ca.pem");
    let cert_path = dir.join("node.pem");
    let key_path = dir.join("node-key.pem");

    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    std::fs::write(&cert_path, node_cert.pem()).unwrap();
    std::fs::write(&key_path, node_key.serialize_pem()).unwrap();

    (ca_path, cert_path, key_path)
}

fn configure_tls_nodes(
    base_config: Configuration,
    tls_dir: &Path,
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    num_nodes: u32,
    mode: TlsMode,
) -> Vec<NodeSpec> {
    let mut nodes = NodeSpec::new_test_nodes(
        base_config,
        BinarySource::CargoTest,
        EnumSet::all(),
        num_nodes,
        false,
    );

    for (i, node) in nodes.iter_mut().enumerate() {
        let node_name = format!("node-{}", i + 1);
        let node_dir = tls_dir.join(&node_name);
        std::fs::create_dir_all(&node_dir).unwrap();

        let (node_cert, node_key) = generate_node_cert(ca_cert, ca_key, &node_name);
        let (ca_path, cert_path, key_path) =
            write_certs_to_dir(&node_dir, ca_cert, &node_cert, &node_key);

        node.config_mut().networking.tls = Some(FabricTlsOptions {
            mode,
            cert_file: cert_path,
            key_file: key_path,
            ca_files: vec![ca_path],
            require_client_auth: true,
            refresh_interval: restate_util_time::NonZeroFriendlyDuration::from_secs_unchecked(3600),
            allowed_subject_names: vec!["*".into()],
            client: None,
        });
    }

    nodes
}

/// Assert that every node registered in the cluster's nodes configuration
/// advertises an `https://` fabric address.
async fn assert_all_nodes_advertise_tls(
    cluster: &StartedCluster,
    expected_nodes: usize,
) -> googletest::Result<Vec<AdvertisedAddress<FabricPort>>> {
    let nodes_config = cluster.nodes[0]
        .metadata_client()
        .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone())
        .await
        .expect("can read nodes configuration")
        .expect("nodes configuration must exist after provisioning");

    let addresses: Vec<_> = nodes_config
        .iter()
        .map(|(node_id, node_config)| (node_id, node_config.address.clone()))
        .collect();
    assert_eq!(addresses.len(), expected_nodes);
    let mut advertised_addresses = Vec::with_capacity(addresses.len());
    for (node_id, address) in addresses {
        assert!(
            address.to_string().starts_with("https://"),
            "node {node_id} must advertise an https:// fabric address, got '{address}'"
        );
        advertised_addresses.push(address);
    }
    Ok(advertised_addresses)
}

async fn grpc_node_identity_and_cluster_health(
    address: AdvertisedAddress<FabricPort>,
    networking: &NetworkingOptions,
) -> Result<(NodeId, String), tonic::Status> {
    let channel = create_tonic_channel(address, networking, DNSResolution::Gai);
    let mut client = new_node_ctl_client(channel, networking);
    let ident = client.get_ident(()).await?.into_inner();
    let node_id = ident
        .node_id
        .ok_or_else(|| tonic::Status::failed_precondition("node has not joined the cluster"))?
        .into();
    let cluster_name = client
        .cluster_health(())
        .await
        .map(|response| response.into_inner().cluster_name)?;
    Ok((node_id, cluster_name))
}

fn with_http_scheme(address: &AdvertisedAddress<FabricPort>) -> AdvertisedAddress<FabricPort> {
    address
        .to_string()
        .replacen("https://", "http://", 1)
        .parse()
        .expect("valid plaintext fabric address")
}

fn install_test_tls_client(tls_dir: &Path, ca_cert: &rcgen::Certificate, ca_key: &KeyPair) {
    let client_dir = tls_dir.join("client");
    std::fs::create_dir_all(&client_dir).unwrap();
    let (client_cert, client_key) = generate_node_cert(ca_cert, ca_key, "test-client");
    let (ca_path, cert_path, key_path) =
        write_certs_to_dir(&client_dir, ca_cert, &client_cert, &client_key);
    let client_config = TlsClientConfig::new(
        Some(ClientIdentityFiles {
            cert_file: &cert_path,
            key_file: &key_path,
        }),
        &[ca_path],
        &["*"],
    )
    .expect("valid test TLS client");
    assert!(
        client_config.set_global(),
        "test TLS client configuration already installed"
    );
}

async fn verify_tls_mode(
    tls_dir: &Path,
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    mode: TlsMode,
    cluster_name: &str,
    accepts_plaintext: bool,
) -> googletest::Result<()> {
    let mut base_config = Configuration::new_random_ports();
    base_config.common.auto_provision = false;
    base_config.common.default_num_partitions = 1;

    let mode_tls_dir = tls_dir.join(cluster_name);
    let nodes = configure_tls_nodes(base_config, &mode_tls_dir, ca_cert, ca_key, 3, mode);

    info!(?mode, "Starting 3-node cluster with fabric TLS");
    let cluster = Cluster::builder()
        .cluster_name(cluster_name)
        .nodes(nodes)
        .temp_base_dir(cluster_name)
        .build()
        .start()
        .await?;

    cluster.nodes[0]
        .provision_cluster(
            None,
            ReplicationProperty::new_unchecked(3),
            None,
            EnumSet::empty(),
        )
        .await
        .into_test_result()?;

    info!(?mode, "Waiting for cluster to become healthy");
    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let authorities = assert_all_nodes_advertise_tls(&cluster, 3).await?;
    let mut contacted_nodes = HashSet::with_capacity(authorities.len());

    for address in authorities {
        let networking = &cluster.nodes[0].config().networking;
        let (node_id, actual_cluster_name) =
            grpc_node_identity_and_cluster_health(address.clone(), networking)
                .await
                .map_err(anyhow::Error::from)
                .into_test_result()?;
        assert_eq!(actual_cluster_name, cluster_name);
        assert!(
            contacted_nodes.insert(node_id),
            "{mode:?} contacted node {node_id} through more than one advertised address"
        );

        let plaintext_result =
            grpc_node_identity_and_cluster_health(with_http_scheme(&address), networking).await;
        assert_eq!(
            plaintext_result.is_ok(),
            accepts_plaintext,
            "{mode:?} plaintext policy mismatch on {address}: {plaintext_result:?}"
        );
    }
    assert_eq!(contacted_nodes.len(), 3);

    info!(?mode, "Fabric TLS mode verified");
    Ok(())
}

#[test_log::test(restate_core::test)]
async fn fabric_tls_modes() -> googletest::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let tls_dir = TempDir::new().unwrap();
    let (ca_cert, ca_key) = generate_ca();
    install_test_tls_client(tls_dir.path(), &ca_cert, &ca_key);

    verify_tls_mode(
        tls_dir.path(),
        &ca_cert,
        &ca_key,
        TlsMode::Require,
        "fabric_tls_require",
        false,
    )
    .await?;
    verify_tls_mode(
        tls_dir.path(),
        &ca_cert,
        &ca_key,
        TlsMode::Prefer,
        "fabric_tls_prefer",
        true,
    )
    .await?;
    Ok(())
}
