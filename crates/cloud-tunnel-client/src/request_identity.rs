// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::HeaderMap;
use serde::Deserialize;
use std::collections::HashSet;

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
struct Claims {}

pub(crate) fn validate_request_identity(
    decoding_key: &jsonwebtoken::DecodingKey,
    headers: &HeaderMap,
    path: &str,
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
    validate.required_spec_claims =
        HashSet::from(["aud".into(), "exp".into(), "iat".into(), "nbf".into()]);
    validate.leeway = 0;
    validate.reject_tokens_expiring_in_less_than = 0;
    validate.validate_exp = true;
    validate.validate_nbf = true;
    validate.validate_aud = false;
    validate.set_audience(&[path]);

    jsonwebtoken::decode::<Claims>(jwt, decoding_key, &validate)?;

    Ok(())
}
