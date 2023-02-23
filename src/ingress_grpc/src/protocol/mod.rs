mod connect_utils;

use super::*;

use std::future::Future;

use bytes::{Buf, Bytes};
use http::request::Parts;
use http::{HeaderMap, HeaderValue, Request, Response};
use http_body::combinators::UnsyncBoxBody;
use http_body::Body;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use tonic::Status;
use tower::BoxError;
use tracing::debug;

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

    pub(crate) async fn handle_request<H, F>(
        self,
        req: Request<hyper::Body>,
        handler_fn: H,
    ) -> Result<Response<BoxBody>, BoxError>
    where
        H: FnOnce(IngressRequest) -> F,
        F: Future<Output = IngressResult>,
    {
        // Parse service_name and method_name
        let mut path_parts: Vec<&str> = req.uri().path().split('/').collect();
        if path_parts.len() != 3 {
            // Let's immediately reply with a status code not found
            debug!(
                "Cannot parse the request path '{}' into a valid GRPC/Connect request path. \
                Allowed format is '/Service-Name/Method-Name'",
                req.uri().path()
            );
            return Ok(self.encode_status(Status::not_found(format!(
                "Request path {} invalid",
                req.uri().path()
            ))));
        }
        let method_name = path_parts.remove(2).to_string();
        let service_name = path_parts.remove(1).to_string();

        // Extract tracing context if any
        let tracing_context = TraceContextPropagator::new()
            .extract(&opentelemetry_http::HeaderExtractor(req.headers()));

        unimplemented!()
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
