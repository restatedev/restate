mod connect_utils;

use bytes::{Buf, Bytes};
use http::{HeaderMap, HeaderValue, Response};
use http_body::combinators::UnsyncBoxBody;
use http_body::Body;
use tonic::Status;
use tower::BoxError;

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
        match self {
            Protocol::Tonic => status.to_http().map(to_box_body),
            Protocol::Connect => connect_utils::status_response(status).map(to_box_body),
        }
    }
}

pub type BoxBody = UnsyncBoxBody<Bytes, BoxError>;

fn to_box_body<B, BE>(body: B) -> UnsyncBoxBody<Bytes, BoxError>
where
    BE: Into<BoxError> + 'static,
    B: Body<Data = Bytes, Error = BE> + Sized + Send + 'static,
{
    body.map_err(Into::into).boxed_unsync()
}
