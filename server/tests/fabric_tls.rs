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
    cluster::Cluster,
    node::{BinarySource, NodeSpec},
};
use restate_types::config::{Configuration, FabricTlsOptions, TlsMode};
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
    let mut params = CertificateParams::new(vec![node_name.to_owned()]).unwrap();
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
            refresh_interval: restate_time_util::NonZeroFriendlyDuration::from_secs_unchecked(3600),
            client: None,
        });
    }

    nodes
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
        .provision_cluster(None, ReplicationProperty::new_unchecked(3), None)
        .await
        .into_test_result()?;

    info!("Waiting for cluster to become healthy over mTLS");
    cluster.wait_healthy(Duration::from_secs(30)).await?;

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
        .provision_cluster(None, ReplicationProperty::new_unchecked(3), None)
        .await
        .into_test_result()?;

    info!("Waiting for cluster to become healthy (optional mode)");
    cluster.wait_healthy(Duration::from_secs(30)).await?;

    info!("Cluster is healthy with optional TLS mode — test passed");
    Ok(())
}
