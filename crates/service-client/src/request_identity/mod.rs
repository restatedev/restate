use hyper::header::HeaderName;

use hyper::HeaderMap;

pub(crate) mod v1;

const SCHEME_HEADER: HeaderName = HeaderName::from_static("x-restate-signature-scheme");

pub trait SignRequest {
    type Error;
    fn insert_identity(self, headers: HeaderMap) -> Result<HeaderMap, Self::Error>;
}

impl<T: SignRequest> SignRequest for Option<T> {
    type Error = T::Error;
    fn insert_identity(self, headers: HeaderMap) -> Result<HeaderMap, Self::Error> {
        match self {
            Some(signer) => signer.insert_identity(headers),
            None => Ok(headers),
        }
    }
}
