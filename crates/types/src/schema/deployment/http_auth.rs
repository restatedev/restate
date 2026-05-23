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
}

impl GoogleIdTokenAuth {
    pub fn new(audience: ByteString, impersonate_service_account: Option<ByteString>) -> Self {
        Self {
            impersonate_service_account,
            audience,
        }
    }

    pub fn audience(&self) -> &ByteString {
        &self.audience
    }

    pub fn impersonate_service_account(&self) -> Option<&ByteString> {
        self.impersonate_service_account.as_ref()
    }
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
}
