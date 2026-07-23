// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DistinguishedName, RootCertStore, ServerConfig, SignatureScheme};
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};
use wildmatch::WildMatchPattern;
use x509_parser::prelude::*;

use std::sync::OnceLock;

use restate_types::config::FabricTlsOptions;

use crate::{ShutdownError, TaskCenter, TaskId, TaskKind, cancellation_watcher};

/// The process-wide fabric TLS resolver, set once at node startup when
/// `[networking.tls]` is configured. Fabric TLS configuration is process-global
/// (like [`restate_types::config::Configuration`]), and channels to fabric
/// peers are created from many places that dial advertised addresses directly
/// (metadata-store client, raft networking, control channels) — they all pick
/// up the client identity and server verifier through this handle.
static GLOBAL_RESOLVER: OnceLock<TlsCertResolver> = OnceLock::new();

/// Client-side TLS materials for outbound fabric connections: the client
/// identity as PEM (the form tonic consumes) and the server-certificate
/// verifier enforcing chain validation plus subject-name authorization.
pub struct ClientTlsMaterials {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    pub verifier: Arc<dyn ServerCertVerifier>,
}

/// Holds hot-swappable TLS configurations for both server and client roles.
#[derive(Clone)]
pub struct TlsCertResolver {
    server_config: Arc<ArcSwap<ServerConfig>>,
    client_materials: Arc<ArcSwap<ClientTlsMaterials>>,
}

impl TlsCertResolver {
    pub fn new(opts: &FabricTlsOptions) -> anyhow::Result<Self> {
        let server = build_server_config(opts)?;
        let client = build_client_materials(opts)?;
        Ok(Self {
            server_config: Arc::new(ArcSwap::from_pointee(server)),
            client_materials: Arc::new(ArcSwap::from_pointee(client)),
        })
    }

    pub fn server_config(&self) -> Arc<ServerConfig> {
        self.server_config.load_full()
    }

    pub fn client_materials(&self) -> Arc<ClientTlsMaterials> {
        self.client_materials.load_full()
    }

    pub fn tls_acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.server_config())
    }

    /// Registers this resolver as the process-wide fabric TLS resolver.
    /// Returns false if one was already set.
    pub fn set_global(&self) -> bool {
        GLOBAL_RESOLVER.set(self.clone()).is_ok()
    }

    /// The process-wide fabric TLS resolver, if fabric TLS is configured.
    pub fn global() -> Option<&'static TlsCertResolver> {
        GLOBAL_RESOLVER.get()
    }

    /// Spawns a background task that periodically reloads certificates from disk.
    /// The task is managed by the `TaskCenter` and is cancelled on system shutdown.
    pub fn spawn_reloader(
        &self,
        opts: FabricTlsOptions,
        interval: Duration,
    ) -> Result<TaskId, ShutdownError> {
        let server_config = Arc::clone(&self.server_config);
        let client_materials = Arc::clone(&self.client_materials);

        TaskCenter::spawn(
            TaskKind::Background,
            "fabric-tls-cert-reloader",
            async move {
                let mut cancelled = std::pin::pin!(cancellation_watcher());
                let mut ticker = tokio::time::interval(interval);
                ticker.tick().await; // skip first immediate tick
                loop {
                    tokio::select! {
                        _ = &mut cancelled => return Ok(()),
                        _ = ticker.tick() => {}
                    }
                    match build_server_config(&opts) {
                        Ok(new_server) => {
                            server_config.store(Arc::new(new_server));
                            info!("Fabric TLS server certificates reloaded");
                        }
                        Err(e) => {
                            warn!("Failed to reload fabric TLS server certificates: {e}");
                        }
                    }
                    match build_client_materials(&opts) {
                        Ok(new_client) => {
                            client_materials.store(Arc::new(new_client));
                            info!("Fabric TLS client certificates reloaded");
                        }
                        Err(e) => {
                            warn!("Failed to reload fabric TLS client certificates: {e}");
                        }
                    }
                }
            },
        )
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
        let webpki_verifier = WebPkiClientVerifier::builder(Arc::new(root_store)).build()?;

        // Any list containing "*" matches every subject, so the verifier would
        // be pure overhead. An empty list is rejected by validate() when client
        // auth is on; tolerated here for direct construction in tests.
        let ca_only_trust = opts.allowed_subject_names.is_empty()
            || opts.allowed_subject_names.iter().any(|s| s == "*");
        if ca_only_trust {
            builder.with_client_cert_verifier(webpki_verifier)
        } else {
            let san_verifier = SubjectNameVerifier {
                inner: webpki_verifier,
                allowed_patterns: opts.allowed_subject_names.clone(),
            };
            builder.with_client_cert_verifier(Arc::new(san_verifier))
        }
    } else {
        builder.with_no_client_auth()
    };

    let config = builder.with_single_cert(certs, key)?;
    Ok(config)
}

/// Returns true if the certificate's Subject Common Name (CN) or any Subject
/// Alternative Name (DNS/URI) matches at least one allowed glob pattern.
fn cert_subject_matches(cert_der: &CertificateDer<'_>, allowed_patterns: &[String]) -> bool {
    let Ok((_, cert)) = X509Certificate::from_der(cert_der.as_ref()) else {
        return false;
    };

    // Check Subject CN
    if let Some(cn) = cert.subject().iter_common_name().next()
        && let Ok(cn_str) = cn.as_str()
    {
        for pattern in allowed_patterns {
            if glob_match(pattern, cn_str) {
                return true;
            }
        }
    }

    // Check SANs (DNS names and URIs)
    let Some(san_ext) = cert
        .extensions()
        .iter()
        .find(|e| e.oid == oid_registry::OID_X509_EXT_SUBJECT_ALT_NAME)
    else {
        return false;
    };

    let ParsedExtension::SubjectAlternativeName(san) = san_ext.parsed_extension() else {
        return false;
    };

    for name in &san.general_names {
        let value = match name {
            GeneralName::DNSName(dns) => *dns,
            GeneralName::URI(uri) => *uri,
            _ => continue,
        };
        for pattern in allowed_patterns {
            if glob_match(pattern, value) {
                return true;
            }
        }
    }

    false
}

/// Wraps a standard certificate verifier and additionally checks that the peer
/// certificate's Subject Common Name (CN) or Subject Alternative Names (DNS/URI)
/// match at least one allowed pattern. This provides authorization on top of mTLS.
#[derive(Debug)]
struct SubjectNameVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    allowed_patterns: Vec<String>,
}

impl SubjectNameVerifier {
    fn cert_subject_matches(&self, cert_der: &CertificateDer<'_>) -> bool {
        cert_subject_matches(cert_der, &self.allowed_patterns)
    }
}

impl ClientCertVerifier for SubjectNameVerifier {
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let result = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;

        if !self.cert_subject_matches(end_entity) {
            // maps to an AccessDenied alert: the chain is valid but the
            // peer's identity is not authorized
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }

        Ok(result)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn glob_match(pattern: &str, value: &str) -> bool {
    // `?` is disabled as a wildcard so only `*` has special meaning in patterns.
    WildMatchPattern::<'*', '\0'>::new(pattern).matches(value)
}

/// Verifies server certificates for outbound fabric connections. Chain
/// validation is delegated to [`WebPkiServerVerifier`]; endpoint identity is
/// established by matching the server certificate's CN/SANs against
/// `allowed-subject-names` instead of strict hostname verification. This is
/// required for SPIFFE-style certificates (URI SANs only), where the dialed
/// host never matches a SAN, and gives the client the same authorization
/// guarantee the server side has: a peer holding a certificate from a shared
/// CA but with a foreign identity is rejected.
#[derive(Debug)]
struct SubjectNameServerVerifier {
    inner: Arc<WebPkiServerVerifier>,
    allowed_patterns: Vec<String>,
}

impl ServerCertVerifier for SubjectNameServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let result = match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(result) => result,
            // Hostname mismatch is expected when certificates carry identity
            // in URI SANs (e.g. SPIFFE); the subject-name check below is the
            // endpoint-identity check in that case. All other errors
            // (untrusted chain, expiry, revocation) remain fatal.
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => ServerCertVerified::assertion(),
            Err(e) => return Err(e),
        };

        // "*" is the documented CA-only trust opt-in: chain validation only,
        // no identity check. Skipping cert_subject_matches also covers certs
        // that carry neither a CN nor SANs.
        let ca_only = self.allowed_patterns.iter().any(|s| s == "*");
        if !ca_only && !cert_subject_matches(end_entity, &self.allowed_patterns) {
            // maps to an AccessDenied alert: the chain is valid but the
            // peer's identity is not authorized
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ));
        }

        Ok(result)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn build_client_materials(opts: &FabricTlsOptions) -> anyhow::Result<ClientTlsMaterials> {
    let mut root_store = RootCertStore::empty();
    for ca_path in opts.client_ca_files() {
        for cert in load_certs(ca_path)? {
            root_store.add(cert)?;
        }
    }
    let webpki_verifier = WebPkiServerVerifier::builder(Arc::new(root_store)).build()?;

    // Unlike the server side, the wildcard cannot short-circuit to the raw
    // webpki verifier here: webpki enforces hostname matching, which fails for
    // SPIFFE URI-only or CN-only node certificates. CA-only trust ("*") means
    // chain validation without endpoint-identity checking, which is exactly
    // what SubjectNameServerVerifier does when the wildcard is present.
    let verifier: Arc<dyn ServerCertVerifier> = if opts.allowed_subject_names.is_empty() {
        webpki_verifier
    } else {
        Arc::new(SubjectNameServerVerifier {
            inner: webpki_verifier,
            allowed_patterns: opts.allowed_subject_names.clone(),
        })
    };

    let cert_file = opts.client_cert_file();
    let key_file = opts.client_key_file();

    // Validate the client certificate and key as a *pair* (not just that each
    // PEM parses): tonic consumes them much later, when the first outbound TLS
    // channel is built, and a mismatch there would panic the connection path
    // instead of failing startup / hot-reload.
    let certs = load_certs(cert_file)?;
    let key = load_private_key(key_file)?;
    rustls::sign::CertifiedKey::from_der(certs, key, &rustls::crypto::ring::default_provider())
        .map_err(|e| {
            anyhow::anyhow!(
                "Client certificate '{}' and key '{}' are not a valid pair: {e}",
                cert_file.display(),
                key_file.display()
            )
        })?;

    let cert_pem = std::fs::read(cert_file)
        .map_err(|e| anyhow::anyhow!("Failed to read cert file '{}': {e}", cert_file.display()))?;
    let key_pem = std::fs::read(key_file)
        .map_err(|e| anyhow::anyhow!("Failed to read key file '{}': {e}", key_file.display()))?;

    Ok(ClientTlsMaterials {
        cert_pem,
        key_pem,
        verifier,
    })
}

fn load_certs(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let certs: Vec<_> = CertificateDer::pem_file_iter(path)
        .map_err(|e| anyhow::anyhow!("Failed to open cert file '{}': {e}", path.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse certs from '{}': {e}", path.display()))?;
    if certs.is_empty() {
        anyhow::bail!("No certificates found in '{}'", path.display());
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path)
        .map_err(|e| anyhow::anyhow!("No private key found in '{}': {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    // Self-signed test certificate + key (generated offline, EC P-256)
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
    fn load_certs_valid_pem() {
        let cert_file = write_temp_file(TEST_CERT);
        let certs = load_certs(cert_file.path()).unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn load_certs_missing_file() {
        let result = load_certs(Path::new("/nonexistent/cert.pem"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to open"));
    }

    #[test]
    fn load_certs_empty_file() {
        let empty_file = write_temp_file("");
        let result = load_certs(empty_file.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No certificates"));
    }

    #[test]
    fn load_private_key_valid_pem() {
        let key_file = write_temp_file(TEST_KEY);
        let key = load_private_key(key_file.path());
        assert!(key.is_ok());
    }

    #[test]
    fn load_private_key_missing_file() {
        let result = load_private_key(Path::new("/nonexistent/key.pem"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No private key"));
    }

    #[test]
    fn load_private_key_no_key_in_file() {
        let no_key_file = write_temp_file("not a pem file at all\n");
        let result = load_private_key(no_key_file.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No private key"));
    }

    #[test]
    fn tls_cert_resolver_rejects_mismatched_cert_and_key() {
        // Install crypto provider for rustls in test context
        let _ = rustls::crypto::ring::default_provider().install_default();

        let ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let node_params = rcgen::CertificateParams::new(vec!["node".to_owned()]).unwrap();
        let node_key = rcgen::KeyPair::generate().unwrap();
        let node_cert = node_params.self_signed(&node_key).unwrap();
        // a different keypair than the one the certificate was issued for
        let wrong_key = rcgen::KeyPair::generate().unwrap();

        let cert_file = write_temp_file(&node_cert.pem());
        let key_file = write_temp_file(&wrong_key.serialize_pem());
        let ca_file = write_temp_file(&ca_cert.pem());

        let opts = FabricTlsOptions {
            mode: restate_types::config::TlsMode::Require,
            cert_file: cert_file.path().to_path_buf(),
            key_file: key_file.path().to_path_buf(),
            ca_files: vec![ca_file.path().to_path_buf()],
            require_client_auth: true,
            refresh_interval: restate_util_time::NonZeroFriendlyDuration::from_secs_unchecked(3600),
            allowed_subject_names: vec![],
            client: None,
        };

        // The cert and key are not a matching pair, so ServerConfig
        // construction must fail — and for that reason, not e.g. file I/O.
        let Err(err) = TlsCertResolver::new(&opts) else {
            panic!("mismatched cert/key pair must be rejected");
        };
        let err = err.to_string().to_lowercase();
        assert!(
            err.contains("key"),
            "expected a private-key/certificate mismatch error, got: {err}"
        );
    }

    /// A mismatched pair in the separate `[networking.tls.client]` override
    /// must fail at startup, not later when the first outbound channel is
    /// built (where tonic would panic in `apply_fabric_tls`).
    #[test]
    fn tls_cert_resolver_rejects_mismatched_client_override() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let params = rcgen::CertificateParams::new(vec!["node".to_owned()]).unwrap();
        let node_key = rcgen::KeyPair::generate().unwrap();
        let node_cert = params.self_signed(&node_key).unwrap();
        let wrong_key = rcgen::KeyPair::generate().unwrap();

        let cert_file = write_temp_file(&node_cert.pem());
        let key_file = write_temp_file(&node_key.serialize_pem());
        let wrong_key_file = write_temp_file(&wrong_key.serialize_pem());

        let opts = FabricTlsOptions {
            mode: restate_types::config::TlsMode::Require,
            cert_file: cert_file.path().to_path_buf(),
            key_file: key_file.path().to_path_buf(),
            ca_files: vec![cert_file.path().to_path_buf()],
            require_client_auth: false,
            refresh_interval: restate_util_time::NonZeroFriendlyDuration::from_secs_unchecked(3600),
            allowed_subject_names: vec![],
            // server pair is valid; the client override pairs the same cert
            // with the wrong key
            client: Some(restate_types::config::FabricTlsClientOptions {
                cert_file: None,
                key_file: Some(wrong_key_file.path().to_path_buf()),
                root_ca_files: None,
            }),
        };

        let Err(err) = TlsCertResolver::new(&opts) else {
            panic!("mismatched client cert/key override must be rejected at startup");
        };
        assert!(
            err.to_string().contains("not a valid pair"),
            "expected a client pair validation error, got: {err}"
        );
    }

    #[test]
    fn glob_match_exact() {
        assert!(glob_match("restate-node", "restate-node"));
        assert!(!glob_match("restate-node", "other-node"));
    }

    #[test]
    fn glob_match_trailing_wildcard() {
        assert!(glob_match("spiffe://domain/*", "spiffe://domain/admin"));
        assert!(glob_match(
            "spiffe://domain/*",
            "spiffe://domain/worker/staging"
        ));
        assert!(!glob_match("spiffe://domain/*", "spiffe://other/admin"));
    }

    #[test]
    fn glob_match_middle_wildcard() {
        assert!(glob_match("spiffe://*/admin", "spiffe://domain/admin"));
        assert!(!glob_match("spiffe://*/admin", "spiffe://domain/worker"));
    }

    #[test]
    fn glob_match_prefix() {
        assert!(glob_match("restate-*", "restate-admin"));
        assert!(glob_match("restate-*", "restate-worker"));
        assert!(!glob_match("restate-*", "other-admin"));
    }

    #[test]
    fn glob_match_multiple_wildcards() {
        assert!(glob_match(
            "spiffe://*.pin220.com/restate-agents/*",
            "spiffe://svc.pin220.com/restate-agents/staging/admin"
        ));
    }

    #[test]
    fn glob_match_requires_backtracking() {
        // The wildcard must be able to "give back" characters: `*` matches "bc"
        // so that the literal "bc" tail still matches. A greedy matcher without
        // backtracking rejects these.
        assert!(glob_match("a*bc", "abcbc"));
        assert!(glob_match(
            "spiffe://domain/*/admin",
            "spiffe://domain/admin/team/admin"
        ));
        assert!(!glob_match("a*bc", "abcb"));
        // `?` must have no special meaning in subject-name patterns
        assert!(glob_match("node?1", "node?1"));
        assert!(!glob_match("node?1", "node-1"));
    }

    fn generate_cert(cn: &str, san_uris: &[&str], san_dns: &[&str]) -> CertificateDer<'static> {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);

        let mut alt_names = Vec::new();
        for uri in san_uris {
            alt_names.push(rcgen::SanType::URI((*uri).try_into().unwrap()));
        }
        for dns in san_dns {
            alt_names.push(rcgen::SanType::DnsName((*dns).try_into().unwrap()));
        }
        params.subject_alt_names = alt_names;

        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        cert.der().clone()
    }

    fn make_verifier(patterns: &[&str]) -> SubjectNameVerifier {
        SubjectNameVerifier {
            inner: Arc::new(rustls::server::NoClientAuth),
            allowed_patterns: patterns.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    #[test]
    fn subject_verifier_accepts_matching_san_uri() {
        let verifier = make_verifier(&["spiffe://svc.pin220.com/restate-agents/*"]);
        let cert = generate_cert(
            "irrelevant-cn",
            &["spiffe://svc.pin220.com/restate-agents/staging/admin"],
            &[],
        );
        assert!(verifier.cert_subject_matches(&cert));
    }

    #[test]
    fn subject_verifier_accepts_matching_san_dns() {
        let verifier = make_verifier(&["restate-*.internal"]);
        let cert = generate_cert("irrelevant-cn", &[], &["restate-node1.internal"]);
        assert!(verifier.cert_subject_matches(&cert));
    }

    #[test]
    fn subject_verifier_accepts_matching_cn() {
        let verifier = make_verifier(&["restate-*"]);
        // CN-only cert (no SANs)
        let cert = generate_cert("restate-admin", &[], &[]);
        assert!(verifier.cert_subject_matches(&cert));
        // CN still matches when non-matching SANs are present
        let cert = generate_cert("restate-admin", &["spiffe://other/id"], &[]);
        assert!(verifier.cert_subject_matches(&cert));
        // neither CN nor SANs match
        let cert = generate_cert("kafka-broker-1", &[], &[]);
        assert!(!verifier.cert_subject_matches(&cert));
    }

    #[test]
    fn subject_verifier_rejects_non_matching() {
        let verifier = make_verifier(&["spiffe://svc.pin220.com/restate-agents/*"]);
        let cert = generate_cert(
            "other-service",
            &["spiffe://svc.pin220.com/other-service/staging/worker"],
            &[],
        );
        assert!(!verifier.cert_subject_matches(&cert));
    }

    #[test]
    fn subject_verifier_rejects_no_match_anywhere() {
        let verifier = make_verifier(&["spiffe://svc.pin220.com/restate-agents/*"]);
        let cert = generate_cert("unrelated-cn", &[], &[]);
        assert!(!verifier.cert_subject_matches(&cert));
    }

    #[test]
    fn subject_verifier_multiple_patterns() {
        let verifier = make_verifier(&[
            "spiffe://svc.pin220.com/restate-agents/*/admin",
            "spiffe://svc.pin220.com/restate-agents/*/worker",
        ]);

        let admin_cert = generate_cert(
            "node",
            &["spiffe://svc.pin220.com/restate-agents/staging/admin"],
            &[],
        );
        let worker_cert = generate_cert(
            "node",
            &["spiffe://svc.pin220.com/restate-agents/staging/worker"],
            &[],
        );
        let other_cert = generate_cert(
            "node",
            &["spiffe://svc.pin220.com/restate-agents/staging/ingress"],
            &[],
        );

        assert!(verifier.cert_subject_matches(&admin_cert));
        assert!(verifier.cert_subject_matches(&worker_cert));
        assert!(!verifier.cert_subject_matches(&other_cert));
    }

    /// End-to-end test of the client-side server-cert verifier with a real
    /// CA-signed chain: a SPIFFE-style cert (URI SAN only, no hostname match)
    /// must be accepted when its subject matches an allowed pattern, and
    /// rejected when it does not — even though it chains to a trusted CA.
    #[test]
    fn server_verifier_enforces_subject_patterns_with_spiffe_certs() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test-ca");
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let sign_leaf = |spiffe_id: &str| -> CertificateDer<'static> {
            let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
            params.subject_alt_names = vec![rcgen::SanType::URI(spiffe_id.try_into().unwrap())];
            let key = rcgen::KeyPair::generate().unwrap();
            params
                .signed_by(&key, &ca_cert, &ca_key)
                .unwrap()
                .der()
                .clone()
        };

        let mut root_store = RootCertStore::empty();
        root_store.add(ca_cert.der().clone()).unwrap();
        let verifier = SubjectNameServerVerifier {
            inner: WebPkiServerVerifier::builder(Arc::new(root_store))
                .build()
                .unwrap(),
            allowed_patterns: vec!["spiffe://domain/restate/*".to_owned()],
        };

        let server_name = ServerName::try_from("192.168.1.1").unwrap();
        let now = UnixTime::now();

        // Trusted chain + matching subject → accepted despite hostname mismatch
        let good = sign_leaf("spiffe://domain/restate/node-1");
        assert!(
            verifier
                .verify_server_cert(&good, &[], &server_name, &[], now)
                .is_ok()
        );

        // Trusted chain + non-matching subject → rejected (shared-CA scenario)
        let bad = sign_leaf("spiffe://domain/other-service/node-1");
        assert!(
            verifier
                .verify_server_cert(&bad, &[], &server_name, &[], now)
                .is_err()
        );

        // Untrusted chain (self-signed) → rejected regardless of subject
        let self_signed = generate_cert("x", &["spiffe://domain/restate/node-1"], &[]);
        assert!(
            verifier
                .verify_server_cert(&self_signed, &[], &server_name, &[], now)
                .is_err()
        );

        // Wildcard = CA-only trust: any identity from the trusted CA is
        // accepted despite the hostname mismatch, but chain validation still
        // applies (self-signed remains rejected).
        let ca_only = SubjectNameServerVerifier {
            inner: verifier.inner,
            allowed_patterns: vec!["*".to_owned()],
        };
        let foreign = sign_leaf("spiffe://domain/other-service/node-1");
        assert!(
            ca_only
                .verify_server_cert(&foreign, &[], &server_name, &[], now)
                .is_ok()
        );
        assert!(
            ca_only
                .verify_server_cert(&self_signed, &[], &server_name, &[], now)
                .is_err()
        );
    }
}
