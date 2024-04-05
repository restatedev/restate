use std::fmt;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::time::SystemTime;

use hyper::header::{HeaderName, HeaderValue};
use hyper::{Body, Request};

use ring::signature::{Ed25519KeyPair, KeyPair};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::http::{HttpError, SigningPrivateKeyReadError};

pub(crate) struct SigningKey {
    header: jsonwebtoken::Header,
    key: jsonwebtoken::EncodingKey,
}

impl Debug for SigningKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("SigningKey(")?;
        f.write_str(self.header.kid.as_ref().unwrap().as_str())?;
        f.write_str(")")
    }
}

impl SigningKey {
    pub(crate) fn from_pem_file(
        request_identity_private_key_pem_file: PathBuf,
    ) -> Result<Self, SigningPrivateKeyReadError> {
        let pem_bytes = std::fs::read(request_identity_private_key_pem_file.as_path())?;
        let mut pems = pem::parse_many(pem_bytes)?;
        if pems.len() != 1 {
            return Err(SigningPrivateKeyReadError::OneKeyExpected(pems.len()));
        };
        let pem_bytes = pems.pop().unwrap().into_contents();

        let keypair = Ed25519KeyPair::from_pkcs8_maybe_unchecked(pem_bytes.as_slice())
            .map_err(SigningPrivateKeyReadError::KeyRejected)?;
        let kid = format!(
            "publickeyv1_{}",
            bs58::encode(keypair.public_key()).into_string()
        );
        let key = jsonwebtoken::EncodingKey::from_ed_der(pem_bytes.as_slice());

        info!(kid, path = ?request_identity_private_key_pem_file, "Loaded request identity key");

        Ok(Self {
            header: jsonwebtoken::Header {
                typ: Some("JWT".into()),
                kid: Some(kid),
                alg: jsonwebtoken::Algorithm::EdDSA,
                ..Default::default()
            },
            key,
        })
    }
}

pub struct Signer<'key> {
    claims: Claims,
    signing_key: &'key SigningKey,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Claims {
    aud: String,
    exp: u64,
    iat: u64,
    nbf: u64,
}

const JWT_HEADER: HeaderName = HeaderName::from_static("x-restate-jwt-v1");

/// The time to add and subtract from the current time to determine expiry and not-before times
const LEEWAY_SECONDS: u64 = 60;

impl<'key> Signer<'key> {
    const SCHEME: HeaderValue = HeaderValue::from_static("v1");

    pub(crate) fn new(path: String, signing_key: &'key SigningKey) -> Result<Self, HttpError> {
        let unix_seconds = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("duration since Unix epoch should be well-defined")
            .as_secs();

        Ok(Self {
            claims: Claims {
                aud: path,
                nbf: unix_seconds.saturating_sub(LEEWAY_SECONDS),
                iat: unix_seconds,
                exp: unix_seconds.saturating_add(LEEWAY_SECONDS),
            },
            signing_key,
        })
    }
}

impl<'key> super::SignRequest for Signer<'key> {
    type Error = jsonwebtoken::errors::Error;
    fn sign_request(self, mut request: Request<Body>) -> Result<Request<Body>, Self::Error> {
        let jwt = jsonwebtoken::encode(
            &self.signing_key.header,
            &self.claims,
            &self.signing_key.key,
        )?;

        request.headers_mut().extend([
            (super::SCHEME_HEADER, Self::SCHEME),
            (
                JWT_HEADER,
                jwt.try_into()
                    .expect("jsonwebtokens must never have non-ascii characters"),
            ),
        ]);

        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request_identity::SignRequest;

    use std::collections::HashSet;
    use std::io::Write;

    static PRIVATE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIPe++4ZTPQDF81otpoU/mOGHC2vOAVp9WbiCblvn3nXO
-----END PRIVATE KEY-----"#;
    static PUBLIC_KEY: &[u8] = br#"-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAj5BTvH+WJo0QGHm2hdLOuk6P7szKgTQxmpnnmZe/DcU=
-----END PUBLIC KEY-----"#;

    #[test]
    fn test_read_key() {
        let mut pemfile = tempfile::NamedTempFile::new().unwrap();
        pemfile.write_all(PRIVATE_KEY).unwrap();

        let key = SigningKey::from_pem_file(pemfile.path().to_path_buf()).unwrap();

        assert_eq!(
            key.header.kid.unwrap(),
            "publickeyv1_AfQwmwfgEZhrWpvv8N52SHpRtZqGGaFr4AZN6qtYWSiY"
        )
    }

    #[test]
    fn test_sign() {
        let mut pemfile = tempfile::NamedTempFile::new().unwrap();
        pemfile.write_all(PRIVATE_KEY).unwrap();

        let key = SigningKey::from_pem_file(pemfile.path().to_path_buf()).unwrap();
        let signer = Signer::new("/invoke/foo".into(), &key).unwrap();

        let request = Request::new(Body::empty());

        let request = signer.sign_request(request).unwrap();

        assert_eq!(
            request
                .headers()
                .get("x-restate-signature-scheme")
                .expect("signature scheme header must be present"),
            &Signer::SCHEME
        );

        let jwt = request
            .headers()
            .get("x-restate-jwt-v1")
            .expect("jwt must be present");

        let decoding_key = jsonwebtoken::DecodingKey::from_ed_pem(PUBLIC_KEY).unwrap();

        let mut validate = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
        validate.required_spec_claims =
            HashSet::from(["aud".into(), "exp".into(), "iat".into(), "nbf".into()]);
        validate.leeway = 0;
        validate.reject_tokens_expiring_in_less_than = 0;
        validate.validate_exp = true;
        validate.validate_nbf = true;
        validate.set_audience(&["/invoke/foo"]);

        let decoded =
            jsonwebtoken::decode::<Claims>(jwt.to_str().unwrap(), &decoding_key, &validate)
                .expect("jwt must decode successfully");

        assert_eq!(
            decoded.header.kid.unwrap(),
            "publickeyv1_AfQwmwfgEZhrWpvv8N52SHpRtZqGGaFr4AZN6qtYWSiY"
        );
        assert_eq!(decoded.header.typ.unwrap(), "JWT");
        assert_eq!(decoded.header.alg, jsonwebtoken::Algorithm::EdDSA);
    }
}
