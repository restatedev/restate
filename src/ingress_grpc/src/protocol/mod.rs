use super::BoxBody;

use http::{HeaderMap, HeaderValue, Response};
use tonic::Status;

pub(crate) enum Protocol {
    // Use tonic (gRPC or gRPC-Web)
    Tonic,
    // Use Connect
    Connect,
}

impl Protocol {
    pub(crate) fn pick_protocol(headers: &HeaderMap<HeaderValue>) -> Self {
        let content_type = headers.get(http::header::CONTENT_TYPE);
        if matches!(
            content_type,
            Some(ct) if ct.as_bytes().starts_with(b"application/json") || ct.as_bytes().starts_with(b"application/proto") || ct.as_bytes().starts_with(b"application/protobuf")
        ) {
            Protocol::Connect
        } else {
            Protocol::Tonic
        }
    }

    pub(crate) fn encode_status(&self, status: Status) -> Response<BoxBody> {
        unimplemented!()
    }
}
