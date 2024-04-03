use hyper::header::HeaderName;
use hyper::http::HeaderValue;
use hyper::{Body, Request};

pub(crate) mod v1;

const SCHEME_HEADER: HeaderName = HeaderName::from_static("x-restate-signature-scheme");

pub(crate) trait SignRequest {
    fn sign_request(self, request: Request<Body>) -> Request<Body>;
}

impl<T: SignRequest> SignRequest for Option<T> {
    fn sign_request(self, mut request: Request<Body>) -> Request<Body> {
        match self {
            Some(signer) => signer.sign_request(request),
            None => {
                const SCHEME: HeaderValue = HeaderValue::from_static("unsigned");
                request.headers_mut().insert(SCHEME_HEADER, SCHEME);
                request
            }
        }
    }
}
