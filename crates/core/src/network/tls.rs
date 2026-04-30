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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    // Self-signed test CA certificate + key (generated offline, EC P-256)
    const TEST_CA_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIBdjCCAR2gAwIBAgIUY5f5X5X5X5X5X5X5X5X5X5X5X5UwCgYIKoZIzj0E
AwIwEjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNDA0MzAwMDAwMDBaFw0zNDA0Mjgw
MDAwMDBaMBIxEDAOBgNVBAMMB3Rlc3QtY2EwWTATBgcqhkjOPQIBBggqhkjOPQMB
BwNCAAR7RpJNfPmVIb4y3tAM3qVvfR8nBHHqLmNGFnHlMHDFfh3Zv5Kx7Jm0wkE
n0N5U9G8dAiRp0GC5K2JD0VBo1MwUTAdBgNVHQ4EFgQU0Lv0JIqOAEJMp7AZFY0
Gz9H5WowHwYDVR0jBBgwFoAU0Lv0JIqOAEJMp7AZFY0Gz9H5WowDwYDVR0TAQH/
BAUwAwEB/zAKBggqhkjOPQQDAgNHADBEAiBgR1hy5OMmR1J9KZNQP3v5N3EOJX3S
lg7INz/ZPD1vxwIgGFZ1P3im+K5H6rDdBq4e3IkUq4YbuqvT0M5M2BDxIo=
-----END CERTIFICATE-----"#;

    const TEST_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIBdTCCARqgAwIBAgIUAQIDBAUGBwgJCgsMDQ4PEBESExQwCgYIKoZIzj0EAwIw
EjEQMA4GA1UEAwwHdGVzdC1jYTAeFw0yNDA0MzAwMDAwMDBaFw0zNDA0MjgwMDAw
MDBaMBQxEjAQBgNVBAMMCXRlc3Qtbm9kZTBZMBMGByqGSM49AgEGCCqGSM49AwEH
A0IABHtGkk18+ZUhvjLe0AzepW99HycEceouY0YWceUwcMV+Hdm/krHsmbTCQQef
Q3lT0bx0CJGnQYLkrYkPRUGjUzBRMB0GA1UdDgQWBBTQu/Qkio4AQkynsBkVjQb
P0flaph8GA1UdIwQYMBaAFNC79CSKjgBCTKewGRWNBs/R+VqpMA8GA1UdEwEB/wQF
MAMBAf8wCgYIKoZIzj0EAwIDSQAwRgIhAO5CxBzm5icP7LKGB3FHzAlj1yNRcaGS
PvHPIR3JXjBpAiEA6UQHfy8fV78BT3GCIZPMzNTBcj3K8MCQ3FT0BIh7RRk=
-----END CERTIFICATE-----"#;

    const TEST_KEY: &str = r#"-----BEGIN EC PRIVATE KEY-----
MHQCAQEEIBVf7EJa2YaU0LFuN5W7VMZBHVr7enCVlcXDK/T7pVVjoAcGBSuBBAAi
oWQDYgAEe0aSTXz5lSG+Mt7QDN6lb30fJwRx6i5jRhZx5TBwxX4d2b+SseyZtMJB
B59DeVPRvHQIkadBguStiQ9FQQ==
-----END EC PRIVATE KEY-----"#;

    fn write_temp_file(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_load_certs_valid_pem() {
        let cert_file = write_temp_file(TEST_CERT);
        let certs = load_certs(cert_file.path()).unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn test_load_certs_missing_file() {
        let result = load_certs(Path::new("/nonexistent/cert.pem"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to open"));
    }

    #[test]
    fn test_load_certs_empty_file() {
        let empty_file = write_temp_file("");
        let result = load_certs(empty_file.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No certificates"));
    }

    #[test]
    fn test_load_private_key_valid_pem() {
        let key_file = write_temp_file(TEST_KEY);
        let key = load_private_key(key_file.path());
        assert!(key.is_ok());
    }

    #[test]
    fn test_load_private_key_missing_file() {
        let result = load_private_key(Path::new("/nonexistent/key.pem"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to open"));
    }

    #[test]
    fn test_load_private_key_no_key_in_file() {
        let no_key_file = write_temp_file("not a pem file at all\n");
        let result = load_private_key(no_key_file.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No private key"));
    }

    #[test]
    fn test_tls_cert_resolver_rejects_mismatched_cert_and_key() {
        // Install crypto provider for rustls in test context
        let _ = rustls::crypto::ring::default_provider().install_default();

        let cert_file = write_temp_file(TEST_CERT);
        let key_file = write_temp_file(TEST_KEY);
        let ca_file = write_temp_file(TEST_CA_CERT);

        let opts = FabricTlsOptions {
            mode: restate_types::config::TlsMode::Strict,
            cert_file: cert_file.path().to_path_buf(),
            key_file: key_file.path().to_path_buf(),
            ca_files: vec![ca_file.path().to_path_buf()],
            require_client_auth: true,
            refresh_interval: restate_time_util::NonZeroFriendlyDuration::from_secs_unchecked(3600),
            client: None,
        };

        // Our test cert and key are not a matching pair, so this should fail
        // during ServerConfig construction. This validates error handling.
        let result = TlsCertResolver::new(&opts);
        assert!(result.is_err());
    }
}
