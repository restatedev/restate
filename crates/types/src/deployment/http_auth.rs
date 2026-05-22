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

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct GoogleIdTokenAuth {
    /// Service account email to impersonate via
    /// `iamcredentials:generateIdToken`. None means use the ambient ADC
    /// identity directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impersonate_service_account: Option<bytestring::ByteString>,
    /// Explicit OIDC `aud` claim. None means derive from the deployment
    /// URL origin per the audience-derivation algorithm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<bytestring::ByteString>,
}

/// Derive the OIDC audience from a deployment URI per the audience
/// derivation algorithm (REQ-DEP-07):
///
/// - lowercase scheme
/// - host reproduced verbatim (IPv6 literals keep their brackets)
/// - explicit port only when present in the URI (default ports omitted)
/// - userinfo, path, query, fragment discarded
///
/// Returns `None` if the URI is missing a scheme or a host. Single
/// source of truth for both the service-client (which uses the value
/// when minting tokens) and the CLI (which displays it in `dp
/// describe`).
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
