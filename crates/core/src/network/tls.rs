// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

use restate_types::config::FabricTlsOptions;

/// Holds hot-swappable TLS configurations for both server and client roles.
#[derive(Clone)]
pub struct TlsCertResolver {
    server_config: Arc<ArcSwap<ServerConfig>>,
    client_config: Arc<ArcSwap<ClientConfig>>,
}

impl TlsCertResolver {
    pub fn new(opts: &FabricTlsOptions) -> anyhow::Result<Self> {
        let server = build_server_config(opts)?;
        let client = build_client_config(opts)?;
        Ok(Self {
            server_config: Arc::new(ArcSwap::from_pointee(server)),
            client_config: Arc::new(ArcSwap::from_pointee(client)),
        })
    }

    pub fn server_config(&self) -> Arc<ServerConfig> {
        self.server_config.load_full()
    }

    pub fn client_config(&self) -> Arc<ClientConfig> {
        self.client_config.load_full()
    }

    pub fn tls_acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.server_config())
    }

    /// Spawns a background task that periodically reloads certificates from disk.
    pub fn spawn_reloader(
        &self,
        opts: FabricTlsOptions,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let server_config = Arc::clone(&self.server_config);
        let client_config = Arc::clone(&self.client_config);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip first immediate tick
            loop {
                ticker.tick().await;
                match build_server_config(&opts) {
                    Ok(new_server) => {
                        server_config.store(Arc::new(new_server));
                        info!("Fabric TLS server certificates reloaded");
                    }
                    Err(e) => {
                        warn!("Failed to reload fabric TLS server certificates: {e}");
                    }
                }
                match build_client_config(&opts) {
                    Ok(new_client) => {
                        client_config.store(Arc::new(new_client));
                        info!("Fabric TLS client certificates reloaded");
                    }
                    Err(e) => {
                        warn!("Failed to reload fabric TLS client certificates: {e}");
                    }
                }
            }
        })
    }
}

fn build_server_config(opts: &FabricTlsOptions) -> anyhow::Result<ServerConfig> {
    let certs = load_certs(&opts.cert_file)?;
    let key = load_private_key(&opts.key_file)?;

    let builder = ServerConfig::builder();

    let builder = if opts.require_client_auth {
        let mut root_store = RootCertStore::empty();
        for ca_path in &opts.ca_files {
            for cert in load_certs(ca_path)? {
                root_store.add(cert)?;
            }
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(root_store)).build()?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };

    let config = builder.with_single_cert(certs, key)?;
    Ok(config)
}

fn build_client_config(opts: &FabricTlsOptions) -> anyhow::Result<ClientConfig> {
    let mut root_store = RootCertStore::empty();
    for ca_path in opts.client_ca_files() {
        for cert in load_certs(ca_path)? {
            root_store.add(cert)?;
        }
    }

    let builder = ClientConfig::builder().with_root_certificates(root_store);

    let cert_file = opts.client_cert_file();
    let key_file = opts.client_key_file();

    let certs = load_certs(cert_file)?;
    let key = load_private_key(key_file)?;
    let config = builder.with_client_auth_cert(certs, key)?;

    Ok(config)
}

fn load_certs(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("Failed to open cert file '{}': {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse certs from '{}': {e}", path.display()))?;
    if certs.is_empty() {
        anyhow::bail!("No certificates found in '{}'", path.display());
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("Failed to open key file '{}': {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| anyhow::anyhow!("No private key found in '{}'", path.display()))
}
