use hyper::header::HeaderName;

use hyper::{Body, Request};

pub(crate) mod v1;

const SCHEME_HEADER: HeaderName = HeaderName::from_static("x-restate-signature-scheme");

pub trait SignRequest {
    type Error;
    fn sign_request(self, request: Request<Body>) -> Result<Request<Body>, Self::Error>;
}

impl<T: SignRequest> SignRequest for Option<T> {
    type Error = T::Error;
    fn sign_request(self, request: Request<Body>) -> Result<Request<Body>, Self::Error> {
        match self {
            Some(signer) => signer.sign_request(request),
            None => Ok(request),
        }
    }
}
