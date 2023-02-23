mod connect_utils;
mod tonic_adapter;
mod tower_utils;

use super::*;

use std::future::Future;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Request, Response};
use http_body::combinators::UnsyncBoxBody;
use http_body::Body;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use tonic::server::Grpc;
use tonic::Status;
use tower::{BoxError, Layer, Service};
use tower_utils::service_fn_once;
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
        H: FnOnce(IngressRequest) -> F + Clone + Send + 'static,
        F: Future<Output = IngressResult> + Send,
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

        // Create Ingress request headers
        let ingress_request_headers =
            IngressRequestHeaders::new(service_name, method_name, tracing_context);

        match self {
            Protocol::Tonic => {
                Self::handle_tonic_request(ingress_request_headers, req, handler_fn).await
            }
            Protocol::Connect => {
                unimplemented!()
            }
        }
    }

    async fn handle_tonic_request<H, F>(
        ingress_request_headers: IngressRequestHeaders,
        req: Request<hyper::Body>,
        handler_fn: H,
    ) -> Result<Response<BoxBody>, BoxError>
    where
        // TODO Clone bound is not needed,
        //  remove it once https://github.com/hyperium/tonic/issues/1290 is released
        H: FnOnce(IngressRequest) -> F + Clone + Send + 'static,
        F: Future<Output = IngressResult> + Send,
    {
        // Why FnOnce and service_fn_once are safe here?
        //
        // The reason is that the interface of Grpc::unary() is probably incorrect,
        // because it gets the ownership of the service, rather than a &self mut borrow.
        //
        // There is no reason to get the ownership, as the service could be reused.
        // There is also no reason for which Grpc::unary() should invoke twice Service::call() within
        // its code (you can verify this point by looking inside the Grpc::unary() implementation).
        //
        // Hence we can safely provide a service which after the first Service::call()
        // is consumed and it cannot be reused anymore.

        let mut s = tonic_web::GrpcWebLayer::new().layer(service_fn_once(move |hyper_req| async {
            Ok::<_, Status>(
                Grpc::new(tonic_adapter::NoopCodec)
                    .unary(
                        tonic_adapter::TonicUnaryServiceAdapter::new(
                            ingress_request_headers,
                            handler_fn,
                        ),
                        hyper_req,
                    )
                    .await,
            )
        }));

        s.call(req)
            .await
            .map_err(BoxError::from)
            .map(|res| res.map(to_box_body))
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
