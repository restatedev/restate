// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::time::Duration;

use enumset::EnumSet;
use googletest::IntoTestResult;
use rcgen::{CertificateParams, KeyPair};
use tempfile::TempDir;
use tracing::info;

use restate_local_cluster_runner::{
    cluster::{Cluster, StartedCluster},
    node::{BinarySource, NodeSpec},
};
use restate_types::config::{Configuration, FabricTlsOptions, TlsMode};
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
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
            mode: mode.clone(),
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
/// advertises an `https://` fabric address. This is what makes peers dial each
/// other with TLS — a node registered with `http://` would be dialed in
/// plaintext regardless of its own TLS config. Returns the `host:port`
/// authorities so callers can probe the fabric ports directly.
async fn assert_all_nodes_advertise_tls(
    cluster: &StartedCluster,
    expected_nodes: usize,
) -> googletest::Result<Vec<String>> {
    let nodes_config = cluster.nodes[0]
        .metadata_client()
        .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone())
        .await
        .expect("can read nodes configuration")
        .expect("nodes configuration must exist after provisioning");

    let addresses: Vec<_> = nodes_config
        .iter()
        .map(|(node_id, node_config)| (node_id, node_config.address.to_string()))
        .collect();
    assert_eq!(addresses.len(), expected_nodes);
    let mut authorities = Vec::with_capacity(addresses.len());
    for (node_id, address) in addresses {
        assert!(
            address.starts_with("https://"),
            "node {node_id} must advertise an https:// fabric address, got '{address}'"
        );
        authorities.push(
            address
                .trim_start_matches("https://")
                .trim_end_matches('/')
                .to_owned(),
        );
    }
    Ok(authorities)
}

/// Probes a fabric TCP port with a plaintext HTTP/2 connection and returns
/// whether the server answered in plaintext. In strict mode the first bytes
/// reach the TLS acceptor, the handshake fails, and the server closes the
/// connection without responding; in optional mode the protocol sniff routes
/// the connection to the plaintext HTTP/2 server, which answers the preface
/// with a SETTINGS frame.
async fn plaintext_http2_accepted(authority: &str) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(authority)
        .await
        .expect("fabric TCP port is reachable");
    stream
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .expect("can write HTTP/2 preface");

    let mut buf = [0u8; 16];
    match tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf)).await {
        // read of 0 bytes = server closed the connection (TLS handshake failed)
        Ok(Ok(n)) => n > 0,
        Ok(Err(_)) => false,
        Err(_) => panic!("timed out waiting for a response on {authority}"),
    }
}

#[test_log::test(restate_core::test)]
async fn fabric_tls_strict_cluster() -> googletest::Result<()> {
    let tls_dir = TempDir::new().unwrap();
    let (ca_cert, ca_key) = generate_ca();

    let mut base_config = Configuration::new_random_ports();
    base_config.common.auto_provision = false;
    base_config.common.default_num_partitions = 1;

    let nodes = configure_tls_nodes(
        base_config,
        tls_dir.path(),
        &ca_cert,
        &ca_key,
        3,
        TlsMode::Strict,
    );

    info!("Starting 3-node cluster with strict mTLS");
    let cluster = Cluster::builder()
        .cluster_name("tls-strict-cluster")
        .nodes(nodes)
        .temp_base_dir("fabric_tls_strict")
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

    info!("Waiting for cluster to become healthy over mTLS");
    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let authorities = assert_all_nodes_advertise_tls(&cluster, 3).await?;

    // strict mode must reject plaintext connections on the fabric port
    for authority in &authorities {
        assert!(
            !plaintext_http2_accepted(authority).await,
            "strict mode must reject plaintext connections on {authority}"
        );
    }

    info!("Cluster is healthy with strict mTLS — test passed");
    Ok(())
}

#[test_log::test(restate_core::test)]
async fn fabric_tls_optional_mode() -> googletest::Result<()> {
    let tls_dir = TempDir::new().unwrap();
    let (ca_cert, ca_key) = generate_ca();

    let mut base_config = Configuration::new_random_ports();
    base_config.common.auto_provision = false;
    base_config.common.default_num_partitions = 1;

    let nodes = configure_tls_nodes(
        base_config,
        tls_dir.path(),
        &ca_cert,
        &ca_key,
        3,
        TlsMode::Optional,
    );

    info!("Starting 3-node cluster with optional TLS mode");
    let cluster = Cluster::builder()
        .cluster_name("tls-optional-cluster")
        .nodes(nodes)
        .temp_base_dir("fabric_tls_optional")
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

    info!("Waiting for cluster to become healthy (optional mode)");
    cluster.wait_healthy(Duration::from_secs(30)).await?;

    let authorities = assert_all_nodes_advertise_tls(&cluster, 3).await?;

    // optional mode must still accept plaintext connections (rolling upgrades)
    for authority in &authorities {
        assert!(
            plaintext_http2_accepted(authority).await,
            "optional mode must accept plaintext connections on {authority}"
        );
    }

    info!("Cluster is healthy with optional TLS mode — test passed");
    Ok(())
}
