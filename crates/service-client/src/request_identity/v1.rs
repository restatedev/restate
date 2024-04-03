use std::borrow::Cow;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::time::SystemTime;

use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Body, Request};
use itertools::Itertools;
use ring::signature::{Ed25519KeyPair, KeyPair};
use tracing::info;

use crate::http::{HttpError, SigningPrivateKeyReadError};

pub(crate) fn read_pem_file(
    request_signing_private_key_pem_file: PathBuf,
) -> Result<Vec<SigningKey>, SigningPrivateKeyReadError> {
    let pem_bytes = std::fs::read(request_signing_private_key_pem_file)?;
    let pems = pem::parse_many(pem_bytes)?;
    let pem_length = pems.len();

    let keys = pems
        .into_iter()
        .map(|pem| {
            Ed25519KeyPair::from_pkcs8_maybe_unchecked(pem.into_contents().as_slice())
                .map_err(SigningPrivateKeyReadError::KeyRejected)
        })
        .try_fold(
            Vec::with_capacity(pem_length),
            |mut vec, key: Result<_, SigningPrivateKeyReadError>| {
                vec.push(SigningKey(key?));
                Result::<_, SigningPrivateKeyReadError>::Ok(vec)
            },
        )?;

    info!(
        public_keys = %CommaSep(&keys),
        "Loaded request signing keys"
    );

    Ok(keys)
}

#[derive(Debug)]
pub(crate) struct SigningKey(Ed25519KeyPair);

impl Display for SigningKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("signingkeyv1_")?;
        let string = bs58::encode(self.0.public_key()).into_string();
        f.write_str(string.as_str())
    }
}

struct CommaSep<'a, T>(&'a [T]);

impl<'a, T: Display> Display for CommaSep<'a, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (i, v) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", v)?;
        }
        Ok(())
    }
}

pub(crate) struct Signer<'keys> {
    to_sign: String,
    unix_seconds: HeaderValue,
    signing_keys: &'keys [SigningKey],
}

const SECONDS_HEADER: HeaderName = HeaderName::from_static("x-restate-unix-seconds");
const SIGNATURES_HEADER: HeaderName = HeaderName::from_static("x-restate-signatures");
const PUBLIC_KEYS_HEADER: HeaderName = HeaderName::from_static("x-restate-public-keys");

impl<'keys> Signer<'keys> {
    const SCHEME: HeaderValue = HeaderValue::from_static("v1");

    pub(crate) fn new<'a>(
        method: &'a str,
        path: &'a str,
        signing_keys: &'keys [SigningKey],
    ) -> Result<Self, HttpError> {
        let unix_seconds = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("duration since Unix epoch should be well-defined")
            .as_secs()
            .to_string();

        let to_sign = format!("{}\n{}\n{}", method, path, unix_seconds);

        Ok(Self {
            to_sign,
            unix_seconds: unix_seconds
                .try_into()
                .expect("u64 string must be a valid header value"),
            signing_keys,
        })
    }
}

impl<'keys> super::SignRequest for Signer<'keys> {
    fn sign_request(self, mut request: Request<Body>) -> Request<Body> {
        let (public_keys, signatures): (String, String) = {
            let iter = self.signing_keys.iter().map(|k| {
                (
                    Cow::<str>::Owned(k.to_string()), // we use display format (bs58 with prefix) here for direct string comparison in sdks
                    Cow::<str>::Owned(
                        BASE64_URL_SAFE_NO_PAD.encode(k.0.sign(self.to_sign.as_bytes())),
                    ),
                )
            });
            Itertools::intersperse(iter, (Cow::Borrowed(","), Cow::Borrowed(",")))
        }
        .unzip();

        request.headers_mut().extend([
            (super::SCHEME_HEADER, Self::SCHEME),
            (SECONDS_HEADER, self.unix_seconds),
            (
                PUBLIC_KEYS_HEADER,
                public_keys
                    .try_into()
                    .expect("base58 must be a valid header value"),
            ),
            (
                SIGNATURES_HEADER,
                signatures
                    .try_into()
                    .expect("base64 must be a valid header value"),
            ),
        ]);

        request
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request_identity::SignRequest;
    use ring::signature::VerificationAlgorithm;
    use std::io::Write;

    static ONE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIPe++4ZTPQDF81otpoU/mOGHC2vOAVp9WbiCblvn3nXO
-----END PRIVATE KEY-----"#;

    static TWO_KEYS: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIPe++4ZTPQDF81otpoU/mOGHC2vOAVp9WbiCblvn3nXO
-----END PRIVATE KEY-----
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIF9luJgnEYi48JkZU6ZegFj5njQ2nWVEomzH4mhd6fQa
-----END PRIVATE KEY-----"#;

    #[test]
    fn test_read_key() {
        let mut pemfile = tempfile::NamedTempFile::new().unwrap();
        pemfile.write_all(ONE_KEY).unwrap();

        let keys = read_pem_file(pemfile.path().to_path_buf()).unwrap();

        assert_eq!(keys.len(), 1);
        assert_eq!(
            keys[0].to_string(),
            "signingkeyv1_AfQwmwfgEZhrWpvv8N52SHpRtZqGGaFr4AZN6qtYWSiY"
        )
    }

    #[test]
    fn test_read_multiple() {
        let mut pemfile = tempfile::NamedTempFile::new().unwrap();
        pemfile.write_all(TWO_KEYS).unwrap();

        let keys = read_pem_file(pemfile.path().to_path_buf()).unwrap();

        assert_eq!(keys.len(), 2);
        assert_eq!(
            keys[0].to_string(),
            "signingkeyv1_AfQwmwfgEZhrWpvv8N52SHpRtZqGGaFr4AZN6qtYWSiY"
        );
        assert_eq!(
            keys[1].to_string(),
            "signingkeyv1_CVmG1AvSyedeZpwwd3MRGbRu5yFt3QXXEpQJKyigB9A5"
        )
    }

    #[test]
    fn test_sign() {
        let mut pemfile = tempfile::NamedTempFile::new().unwrap();
        pemfile.write_all(TWO_KEYS).unwrap();

        let keys = read_pem_file(pemfile.path().to_path_buf()).unwrap();
        let signer = Signer::new("POST", "/invoke/foo", &keys).unwrap();

        let request = Request::new(Body::empty());

        let request = signer.sign_request(request);

        let seconds = request
            .headers()
            .get("x-restate-unix-seconds")
            .expect("seconds header must be present")
            .to_str()
            .unwrap();

        assert_eq!(
            request
                .headers()
                .get("x-restate-signature-scheme")
                .expect("signature scheme header must be present"),
            &Signer::SCHEME
        );
        assert_eq!(seconds.len(), 10);
        assert_eq!(
            request.headers().get("x-restate-public-keys").expect("public keys header must be present"),
            "signingkeyv1_AfQwmwfgEZhrWpvv8N52SHpRtZqGGaFr4AZN6qtYWSiY,signingkeyv1_CVmG1AvSyedeZpwwd3MRGbRu5yFt3QXXEpQJKyigB9A5"
        );

        let signed = format!("POST\n/invoke/foo\n{}", seconds);

        for (i, signature) in request
            .headers()
            .get("x-restate-signatures")
            .expect("signature header must be present")
            .to_str()
            .unwrap()
            .split(',')
            .map(|sig| {
                BASE64_URL_SAFE_NO_PAD
                    .decode(sig)
                    .expect("signatures must be valid base64")
            })
            .enumerate()
        {
            ring::signature::ED25519
                .verify(
                    keys[i].0.public_key().as_ref().into(),
                    signed.as_bytes().into(),
                    signature.as_slice().into(),
                )
                .expect("signature must validate")
        }
    }
}
