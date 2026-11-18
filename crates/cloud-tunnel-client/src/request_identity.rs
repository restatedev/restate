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

use http::HeaderMap;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Invalid request identity public key")]
    InvalidPublicKey,
    #[error("Missing signature scheme header")]
    MissingSignatureScheme,
    #[error("Unexpected signature scheme; expected 'v1'")]
    UnexpectedSignatureScheme,
    #[error("Missing JWT header")]
    MissingJWT,
    #[error("Invalid JWT header")]
    InvalidJWT,
    #[error("Could not validate JWT: {0}")]
    JWTValidation(#[from] jsonwebtoken::errors::Error),
    #[error("JWT target '{method} {path_and_query}' does not match the request")]
    TargetMismatch {
        method: String,
        path_and_query: String,
    },
}

pub(crate) fn parse_public_key(public_key: &str) -> Result<jsonwebtoken::DecodingKey, Error> {
    let unprefixed_public_key = match public_key.strip_prefix("publickeyv1_") {
        Some(unprefixed_public_key) => unprefixed_public_key,
        None => return Err(Error::InvalidPublicKey),
    };
    let public_key_bytes = bs58::decode(unprefixed_public_key)
        .into_vec()
        .map_err(|_| Error::InvalidPublicKey)?;
    Ok(jsonwebtoken::DecodingKey::from_ed_der(&public_key_bytes))
}

#[derive(Deserialize)]
struct Claims {
    target: Target,
}

/// The request exactly as restate-server sent it: the deployment's base path joined with `aud`,
/// plus the query of the deployment URL. Every current server signs this claim.
#[derive(Deserialize)]
struct Target {
    method: String,
    path_and_query: String,
}

/// The tunnel this client serves. restate-server dials `/{unprefixed_environment_id}/{tunnel_name}`
/// followed by the path this client receives, and the tunnel proxy strips that prefix.
pub(crate) struct Tunnel<'a> {
    pub(crate) environment_id: &'a str,
    /// Known once the start handshake has completed, which happens before any request is proxied.
    pub(crate) tunnel_name: Option<&'a str>,
}

pub(crate) fn validate_request_identity(
    decoding_key: &jsonwebtoken::DecodingKey,
    tunnel: &Tunnel<'_>,
    headers: &HeaderMap,
    method: &http::Method,
    path_and_query: &str,
) -> Result<(), Error> {
    match headers
        .get("x-restate-signature-scheme")
        .ok_or(Error::MissingSignatureScheme)?
        .to_str()
    {
        Ok("v1") => {}
        _ => return Err(Error::UnexpectedSignatureScheme),
    };

    let jwt = match headers
        .get("x-restate-jwt-v1")
        .ok_or(Error::MissingJWT)?
        .to_str()
    {
        Ok(jwt) => jwt,
        Err(_) => return Err(Error::InvalidJWT),
    };

    let mut validate = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
    validate.required_spec_claims = HashSet::from(["exp".into(), "iat".into(), "nbf".into()]);
    validate.leeway = 0;
    validate.reject_tokens_expiring_in_less_than = 0;
    validate.validate_exp = true;
    validate.validate_nbf = true;
    // The target claim fully binds the request, so jsonwebtoken's audience matching is disabled.
    validate.validate_aud = false;

    let claims = jsonwebtoken::decode::<Claims>(jwt, decoding_key, &validate)?.claims;

    if !target_matches(&claims.target, tunnel, method, path_and_query) {
        return Err(Error::TargetMismatch {
            method: claims.target.method,
            path_and_query: claims.target.path_and_query,
        });
    }

    Ok(())
}

/// `target_matches` checks that the signed `target` claim binds the exact request method, path,
/// and query. The tunnel prefix (`/{environment_id}/{tunnel_name}`) is stripped before comparing.
fn target_matches(
    target: &Target,
    tunnel: &Tunnel<'_>,
    method: &http::Method,
    path_and_query: &str,
) -> bool {
    let (Some(environment_id), Some(tunnel_name)) = (
        tunnel.environment_id.strip_prefix("env_"),
        tunnel.tunnel_name,
    ) else {
        return false;
    };
    let received = target
        .path_and_query
        .strip_prefix('/')
        .and_then(|rest| rest.strip_prefix(environment_id))
        .and_then(|rest| rest.strip_prefix('/'))
        .and_then(|rest| rest.strip_prefix(tunnel_name));

    target.method == method.as_str() && received == Some(path_and_query)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::assert_matches;
    use std::time::{SystemTime, UNIX_EPOCH};

    use http::{HeaderValue, Method};
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use serde::Serialize;

    use super::*;

    // The key pair restate-service-client's request identity tests use
    const PRIVATE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIPe++4ZTPQDF81otpoU/mOGHC2vOAVp9WbiCblvn3nXO
-----END PRIVATE KEY-----"#;
    pub(crate) const PUBLIC_KEY: &str = "publickeyv1_AfQwmwfgEZhrWpvv8N52SHpRtZqGGaFr4AZN6qtYWSiY";

    pub(crate) const TUNNEL: Tunnel<'static> = Tunnel {
        environment_id: "env_abc",
        tunnel_name: Some("laptop"),
    };

    #[derive(Serialize)]
    pub(crate) struct TestTarget<'a> {
        pub(crate) method: &'a str,
        pub(crate) path_and_query: &'a str,
    }

    #[derive(Serialize)]
    pub(crate) struct TestClaims<'a> {
        pub(crate) target: TestTarget<'a>,
        pub(crate) exp: u64,
        pub(crate) iat: u64,
        pub(crate) nbf: u64,
    }

    impl<'a> TestClaims<'a> {
        /// Valid for a minute either side of now, like restate-server's default leeway.
        pub(crate) fn new(target: (&'a str, &'a str)) -> Self {
            let now = now();
            let (method, path_and_query) = target;
            Self {
                target: TestTarget {
                    method,
                    path_and_query,
                },
                exp: now + 60,
                iat: now,
                nbf: now - 60,
            }
        }

        pub(crate) fn headers(&self) -> HeaderMap {
            let jwt = jsonwebtoken::encode(
                &Header::new(Algorithm::EdDSA),
                self,
                &EncodingKey::from_ed_pem(PRIVATE_KEY).unwrap(),
            )
            .unwrap();

            let mut headers = HeaderMap::new();
            headers.insert("x-restate-signature-scheme", HeaderValue::from_static("v1"));
            headers.insert("x-restate-jwt-v1", jwt.try_into().unwrap());
            headers
        }
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn validate(
        claims: &TestClaims<'_>,
        method: Method,
        path_and_query: &str,
    ) -> Result<(), Error> {
        validate_request_identity(
            &parse_public_key(PUBLIC_KEY).unwrap(),
            &TUNNEL,
            &claims.headers(),
            &method,
            path_and_query,
        )
    }

    #[test]
    fn target_claim_binds_the_whole_request() {
        let received = "/https/greeter.svc/9080/base/invoke/Greeter/greet?tenant=a";
        let signed = "/abc/laptop/https/greeter.svc/9080/base/invoke/Greeter/greet?tenant=a";
        let claims = |path_and_query| TestClaims::new(("POST", path_and_query));

        validate(&claims(signed), Method::POST, received).unwrap();
        validate(
            &TestClaims::new(("GET", "/abc/laptop/http/h/80/discover")),
            Method::GET,
            "/http/h/80/discover",
        )
        .unwrap();

        // each differs from `signed` in one place, so only the target can reject it
        for other in [
            "/xyz/laptop/https/greeter.svc/9080/base/invoke/Greeter/greet?tenant=a",
            "/abc/desktop/https/greeter.svc/9080/base/invoke/Greeter/greet?tenant=a",
            "/abc/laptop/https/admin.svc/9080/base/invoke/Greeter/greet?tenant=a",
            "/abc/laptop/https/greeter.svc/8443/base/invoke/Greeter/greet?tenant=a",
            "/abc/laptop/https/greeter.svc/9080/other/invoke/Greeter/greet?tenant=a",
            "/abc/laptop/https/greeter.svc/9080/base/invoke/Greeter/greet?tenant=b",
            "/abc/laptop/https/greeter.svc/9080/base/invoke/Greeter/greet",
            "/abclaptop/https/greeter.svc/9080/base/invoke/Greeter/greet?tenant=a",
            "/https/greeter.svc/9080/base/invoke/Greeter/greet?tenant=a",
            // what a Lambda deployment's token carries
            "/invoke/Greeter/greet",
        ] {
            assert_matches!(
                validate(&claims(other), Method::POST, received),
                Err(Error::TargetMismatch { .. }),
                "{other} should be rejected"
            );
        }
        assert_matches!(
            validate(&claims(signed), Method::GET, received),
            Err(Error::TargetMismatch { .. })
        );

        // an unknown tunnel name or an environment id without its prefix matches nothing
        let key = parse_public_key(PUBLIC_KEY).unwrap();
        for tunnel in [
            Tunnel {
                environment_id: "env_abc",
                tunnel_name: None,
            },
            Tunnel {
                environment_id: "abc",
                tunnel_name: Some("laptop"),
            },
        ] {
            assert_matches!(
                validate_request_identity(
                    &key,
                    &tunnel,
                    &claims(signed).headers(),
                    &Method::POST,
                    received
                ),
                Err(Error::TargetMismatch { .. })
            );
        }
    }

    #[test]
    fn malformed_and_stale_tokens_are_rejected() {
        let path = "/https/greeter.svc/9080/discover";
        let now = now();
        let target = ("POST", "/abc/laptop/https/greeter.svc/9080/discover");
        let mut expired = TestClaims::new(target);
        (expired.nbf, expired.iat, expired.exp) = (now - 120, now - 60, now - 1);
        let mut not_yet_valid = TestClaims::new(target);
        (not_yet_valid.nbf, not_yet_valid.iat, not_yet_valid.exp) = (now + 60, now + 60, now + 120);

        for (name, claims) in [("expired", expired), ("not yet valid", not_yet_valid)] {
            assert_matches!(
                validate(&claims, Method::POST, path),
                Err(Error::JWTValidation(_)),
                "{name} should be rejected"
            );
        }
    }
}
