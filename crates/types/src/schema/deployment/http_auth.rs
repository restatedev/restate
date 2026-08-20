// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persisted shape of the per-deployment HTTP authentication block.
//!
//! The REST surface (`crates/admin-rest-model`) defines its own
//! independent copy of these types. The two schemas evolve under
//! different compatibility rules (wire under REST versioning,
//! persisted under storage-format evolution) and sharing a struct
//! couples that evolution.
//!
//! The persisted shape encodes the type-level invariant that every
//! `GoogleIdTokenAuth` record carries a concrete `audience`. The wire
//! type leaves it optional so an operator may omit it on register; the
//! REST handler derives the value from the deployment URI before the
//! record is persisted. See `crates/admin-rest-model/src/deployments.rs`
//! for the URI-aware `into_persisted` conversion.

use bytestring::ByteString;
use http::Uri;

/// Per-deployment authentication configuration for HTTP deployments.
///
/// Externally-tagged enum so future providers (e.g. non-Google OIDC
/// sources) can be added without altering the encoding of the existing
/// `GoogleIdToken` variant.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum HttpAuth {
    GoogleIdToken(GoogleIdTokenAuth),
}

/// Persisted Google OIDC ID-token authentication. `audience` is always present in the persisted
/// shape: callers building this value must supply a concrete audience, derived from the deployment
/// URI when the operator did not provide one explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct GoogleIdTokenAuth {
    /// Service account email to impersonate via `iamcredentials:generateIdToken`. None means use
    /// the ambient ADC identity directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    impersonate_service_account: Option<ByteString>,
    /// Explicit OIDC `aud` claim. Required at the type level on the persisted record.
    audience: ByteString,
    /// Full resource name of a GCP workload identity federation provider, e.g.
    /// `//iam.googleapis.com/projects/N/locations/global/workloadIdentityPools/P/providers/R`.
    /// When set, the ID token is minted via the AWS -> GCP federation chain (broker role
    /// assumption, SigV4 subject token, Google STS exchange, then impersonation) instead of the
    /// server's ambient Application Default Credentials. Requires `impersonate_service_account`:
    /// the external-account credential this chain produces cannot mint an ID token ambiently.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workload_identity_provider: Option<ByteString>,
}

/// Invariant violation constructing a [`GoogleIdTokenAuth`]. Checked once, in [`GoogleIdTokenAuth::new`],
/// so every persisted record satisfies it by construction.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GoogleIdTokenAuthError {
    #[error(
        "workload_identity_provider requires impersonate_service_account to be set; the \
         external-account credential the federation chain produces cannot mint an ID token \
         ambiently"
    )]
    ProviderRequiresImpersonation,
    #[error(
        "workload_identity_provider '{provider}' is not a canonical GCP workload identity \
         provider resource name (expected \
         //iam.googleapis.com/projects/<project-number>/locations/<location>/workloadIdentityPools/<pool>/providers/<provider>): \
         {reason}"
    )]
    InvalidProviderResourceName {
        provider: ByteString,
        reason: &'static str,
    },
}

impl GoogleIdTokenAuthError {
    /// The `auth.*` field path this error applies to, for surfacing as a REST `InvalidField`.
    pub fn field(&self) -> &'static str {
        "auth.workload_identity_provider"
    }
}

impl GoogleIdTokenAuth {
    /// Builds a persisted `GoogleIdTokenAuth`, rejecting a `workload_identity_provider` that lacks
    /// `impersonate_service_account` or is not a canonical GCP resource name -- the only two ways
    /// this record can be invalid, so checking them here means every `GoogleIdTokenAuth` in
    /// existence already satisfies them.
    pub fn new(
        audience: ByteString,
        impersonate_service_account: Option<ByteString>,
        workload_identity_provider: Option<ByteString>,
    ) -> Result<Self, GoogleIdTokenAuthError> {
        if let Some(provider) = &workload_identity_provider {
            if impersonate_service_account.is_none() {
                return Err(GoogleIdTokenAuthError::ProviderRequiresImpersonation);
            }
            if let Err(reason) = validate_provider_resource_name(provider) {
                return Err(GoogleIdTokenAuthError::InvalidProviderResourceName {
                    provider: provider.clone(),
                    reason,
                });
            }
        }
        Ok(Self {
            impersonate_service_account,
            audience,
            workload_identity_provider,
        })
    }

    pub fn audience(&self) -> &ByteString {
        &self.audience
    }

    pub fn impersonate_service_account(&self) -> Option<&ByteString> {
        self.impersonate_service_account.as_ref()
    }

    pub fn workload_identity_provider(&self) -> Option<&ByteString> {
        self.workload_identity_provider.as_ref()
    }
}

/// Validates that `resource` has the shape
/// `//iam.googleapis.com/projects/<project-number>/locations/<location>/workloadIdentityPools/<pool>/providers/<provider>`:
/// a numeric project number and three identifier segments (letters, digits, `-`, `_`). Persisted
/// verbatim into cache keys, SigV4 `SignedHeaders`, and the Google STS `audience` parameter, so an
/// arbitrary string here is a functional bug waiting to happen, not just a cosmetic one.
fn validate_provider_resource_name(resource: &str) -> Result<(), &'static str> {
    let is_identifier = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    };

    let Some(rest) = resource.strip_prefix("//iam.googleapis.com/projects/") else {
        return Err("must start with //iam.googleapis.com/projects/<project-number>");
    };
    let segments: Vec<&str> = rest.split('/').collect();
    let [
        project,
        "locations",
        location,
        "workloadIdentityPools",
        pool,
        "providers",
        provider,
    ] = segments.as_slice()
    else {
        return Err(
            "expected .../projects/<number>/locations/<location>/workloadIdentityPools/<pool>/providers/<provider>",
        );
    };
    if project.is_empty() || !project.bytes().all(|b| b.is_ascii_digit()) {
        return Err("project must be a numeric project number");
    }
    if !is_identifier(location) || !is_identifier(pool) || !is_identifier(provider) {
        return Err(
            "location, workload identity pool, and provider must be non-empty identifiers (letters, digits, '-', '_')",
        );
    }
    Ok(())
}

/// Derive the OIDC audience from a deployment URI:
///
/// - lowercase scheme
/// - host reproduced verbatim (IPv6 literals keep their brackets)
/// - explicit port only when present in the URI (default ports omitted)
/// - userinfo, path, query, fragment discarded
///
/// Returns `None` if the URI is missing a scheme or a host. Used by the REST boundary to fill in
/// the persisted audience when the operator did not supply one explicitly.
pub fn derive_audience(uri: &Uri) -> Option<String> {
    let scheme = uri.scheme()?;
    let authority = uri.authority()?;
    let host = authority.host();
    let port = authority.port_u16();

    let mut out = String::with_capacity(scheme.as_str().len() + host.len() + 16);
    for c in scheme.as_str().chars() {
        out.extend(c.to_lowercase());
    }
    out.push_str("://");
    out.push_str(host);
    if let Some(p) = port {
        out.push(':');
        out.push_str(&p.to_string());
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(uri: &str) -> Uri {
        uri.parse().unwrap()
    }

    #[test]
    fn audience_origin_no_port() {
        assert_eq!(
            derive_audience(&parse("https://svc-abc-uc.a.run.app/discover")).unwrap(),
            "https://svc-abc-uc.a.run.app"
        );
    }

    #[test]
    fn audience_omits_default_port_when_implicit() {
        // No explicit port in URL = no port in audience.
        assert_eq!(
            derive_audience(&parse("https://svc.example.com/")).unwrap(),
            "https://svc.example.com"
        );
    }

    #[test]
    fn audience_preserves_explicit_non_default_port() {
        assert_eq!(
            derive_audience(&parse("https://svc.example.com:8443/path")).unwrap(),
            "https://svc.example.com:8443"
        );
    }

    #[test]
    fn audience_lowercases_scheme() {
        assert_eq!(
            derive_audience(&parse("HTTPS://Example.COM/")).unwrap(),
            "https://Example.COM"
        );
    }

    #[test]
    fn audience_ipv6_literal_keeps_brackets() {
        assert_eq!(
            derive_audience(&parse("https://[2001:db8::1]:8443/foo")).unwrap(),
            "https://[2001:db8::1]:8443"
        );
    }

    #[test]
    fn audience_discards_path_query_fragment() {
        assert_eq!(
            derive_audience(&parse("https://svc.example.com/discover?token=abc#frag")).unwrap(),
            "https://svc.example.com"
        );
    }

    #[test]
    fn audience_returns_none_for_uri_without_host() {
        // A path-only URI has no scheme or authority; derivation must fail so the REST boundary
        // can refuse to persist an incomplete record.
        assert!(derive_audience(&parse("/discover")).is_none());
    }

    const PROVIDER: &str = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/aws";

    #[test]
    fn new_enforces_provider_requires_impersonation() {
        let cases: [(Option<&str>, Option<&str>, bool); 3] = [
            (
                Some("sa@proj.iam.gserviceaccount.com"),
                Some(PROVIDER),
                true,
            ),
            (None, Some(PROVIDER), false),
            (Some("sa@proj.iam.gserviceaccount.com"), None, true),
        ];
        for (impersonate, provider, should_succeed) in cases {
            let result = GoogleIdTokenAuth::new(
                ByteString::from_static("https://svc.example.com"),
                impersonate.map(ByteString::from),
                provider.map(ByteString::from),
            );
            assert_eq!(
                result.is_ok(),
                should_succeed,
                "impersonate={impersonate:?} provider={provider:?}: {result:?}"
            );
            if !should_succeed {
                assert_eq!(
                    result.unwrap_err(),
                    GoogleIdTokenAuthError::ProviderRequiresImpersonation
                );
            }
        }
    }

    fn provider_auth(provider: &str) -> Result<GoogleIdTokenAuth, GoogleIdTokenAuthError> {
        GoogleIdTokenAuth::new(
            ByteString::from_static("https://svc.example.com"),
            Some(ByteString::from_static("sa@proj.iam.gserviceaccount.com")),
            Some(ByteString::from(provider.to_owned())),
        )
    }

    #[test]
    fn accepts_canonical_provider_resource_names() {
        for provider in [
            PROVIDER,
            "//iam.googleapis.com/projects/999999999999/locations/global/workloadIdentityPools/my-pool/providers/my-provider",
            "//iam.googleapis.com/projects/1/locations/eu/workloadIdentityPools/p/providers/r",
        ] {
            provider_auth(provider)
                .unwrap_or_else(|e| panic!("expected '{provider}' to be accepted, got {e}"));
        }
    }

    #[test]
    fn rejects_non_canonical_provider_resource_names() {
        for provider in [
            "",
            "not-a-resource-name",
            "iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/aws",
            "//iam.googleapis.com/projects/abc/locations/global/workloadIdentityPools/pool/providers/aws",
            "//iam.googleapis.com/projects//locations/global/workloadIdentityPools/pool/providers/aws",
            "//iam.googleapis.com/projects/123/locations//workloadIdentityPools/pool/providers/aws",
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools//providers/aws",
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/",
            "//iam.googleapis.com/projects/123/regions/global/workloadIdentityPools/pool/providers/aws",
            "//iam.googleapis.com/projects/123/locations/global/pools/pool/providers/aws",
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/endpoints/aws",
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/aws/extra",
            // Rejected only by the tightened per-segment identifier check: no `/` in these, so
            // the old segment-count-only parser accepted them.
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/has space/providers/aws",
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/100%",
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/caf\u{e9}",
            "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool?/providers/aws",
        ] {
            let err = provider_auth(provider)
                .expect_err(&format!("expected '{provider}' to be rejected"));
            assert_eq!(err.field(), "auth.workload_identity_provider");
        }
    }
}
