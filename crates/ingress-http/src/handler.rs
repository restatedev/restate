// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use std::num::ParseIntError;
use std::string;

use crate::metric_definitions::{
    INGRESS_REQUESTS, INGRESS_REQUEST_DURATION, REQUEST_ADMITTED, REQUEST_COMPLETED,
    REQUEST_DENIED_THROTTLE,
};
use http_body_util::{BodyExt, Either, Empty, Full};
use hyper::http::{HeaderName, HeaderValue};
use hyper::{header, HeaderMap, Method, Request, Response, StatusCode};
use metrics::{counter, histogram};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::trace::{SpanContext, TraceContextExt};
use restate_ingress_dispatcher::{IdempotencyMode, IngressRequest, IngressRequestSender};
use restate_schema_api::component::{ComponentMetadataResolver, ComponentType};
use restate_types::errors::UserErrorCode;
use restate_types::identifiers::{InvocationId, ServiceId};
use restate_types::invocation::SpanRelation;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, info, info_span, trace, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");
const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
const IDEMPOTENCY_RETENTION_PERIOD: HeaderName =
    HeaderName::from_static("idempotency-retention-period");
const IDEMPOTENCY_EXPIRES: HeaderName = HeaderName::from_static("idempotency-expires");
const WILDCARD: HeaderValue = HeaderValue::from_static("*");
const TRUE: HeaderValue = HeaderValue::from_static("true");

#[derive(Debug, thiserror::Error)]
pub(crate) enum HandlerError {
    #[error("not found")]
    NotFound,
    #[error("too many requests")]
    TooManyRequests,
    #[error(
        "bad path, expected either /:service-name/:handler or /:object-name/:object-key/:handler"
    )]
    BadPath,
    #[error("bad path, cannot decode key: {0:?}")]
    UrlDecodingError(string::FromUtf8Error),
    #[error("the invoked component is not public")]
    PrivateComponent,
    #[error("bad idempotency header: {0:?}")]
    BadIdempotency(anyhow::Error),
    #[error("cannot read body: {0:?}")]
    Body(anyhow::Error),
    #[error("unavailable")]
    Unavailable,
    #[error("method not allowed")]
    MethodNotAllowed,
    #[error("using the idempotency key and send together is not yet supported")]
    SendAndIdempotencyKey,
}

impl HandlerError {
    pub(crate) fn into_response(self) -> Response<Empty<Bytes>> {
        let status_code = match &self {
            HandlerError::NotFound => StatusCode::NOT_FOUND,
            HandlerError::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
            HandlerError::BadPath => StatusCode::BAD_REQUEST,
            HandlerError::PrivateComponent => StatusCode::BAD_REQUEST,
            HandlerError::BadIdempotency(_) => StatusCode::BAD_REQUEST,
            HandlerError::Body(_) => StatusCode::INTERNAL_SERVER_ERROR,
            HandlerError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            HandlerError::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            HandlerError::UrlDecodingError(_) => StatusCode::BAD_REQUEST,
            HandlerError::SendAndIdempotencyKey => StatusCode::NOT_IMPLEMENTED,
        };

        Response::builder()
            .status(status_code)
            .body(Empty::default())
            .unwrap()
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
#[serde(rename_all = "camelCase")]
pub(crate) struct HealthResponse {
    components: Vec<String>,
}

#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
#[serde(rename_all = "camelCase")]
pub(crate) struct SendResponse {
    invocation_id: InvocationId,
}

#[derive(Clone)]
pub(crate) struct Handler<Schemas> {
    schemas: Schemas,
    request_tx: IngressRequestSender,
    global_concurrency_semaphore: Arc<Semaphore>,
}

impl<Schemas> Handler<Schemas> {
    pub(crate) fn new(
        schemas: Schemas,
        request_tx: IngressRequestSender,
        global_concurrency_semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            schemas,
            request_tx,
            global_concurrency_semaphore,
        }
    }
}

impl<Schemas> Handler<Schemas>
where
    Schemas: ComponentMetadataResolver + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle<B: http_body::Body>(
        self,
        connect_info: ConnectInfo,
        req: Request<B>,
    ) -> Response<Either<Empty<Bytes>, Full<Bytes>>>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        // This chunk of code here contains CORS handling.
        // We used to do this with tower, using https://docs.rs/tower-web/0.3.7/tower_web/middleware/cors/index.html
        // but this is unfortunately not easy to integrate now b/c this ingress is built on hyper/http 1.0
        // We should re-evaluate it once tower-http will provide some support for hyper/http 1.0

        // Handle CORS requests
        if req.method() == Method::OPTIONS {
            return self.handle_preflight(req);
        }

        let result = if req.uri().path().eq_ignore_ascii_case("/restate/health") {
            self.handle_health(req)
        } else {
            self.handle_inner(connect_info, req).await
        };
        let mut response = result.unwrap_or_else(|e| e.into_response().map(Either::Left));

        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, WILDCARD);
        response
            .headers_mut()
            .insert(header::ACCESS_CONTROL_ALLOW_CREDENTIALS, TRUE);

        response
    }

    async fn handle_inner<B: http_body::Body>(
        self,
        connect_info: ConnectInfo,
        req: Request<B>,
    ) -> Result<Response<Either<Empty<Bytes>, Full<Bytes>>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        let start_time = Instant::now();

        // Acquire the semaphore permit to check if we have available quota
        let permit = if let Ok(p) = self.global_concurrency_semaphore.try_acquire_owned() {
            p
        } else {
            warn!("No available quota to process the request");
            counter!(INGRESS_REQUESTS, "status" => REQUEST_DENIED_THROTTLE).increment(1);
            return Err(HandlerError::TooManyRequests);
        };

        // Register request admitted
        counter!(INGRESS_REQUESTS, "status" => REQUEST_ADMITTED).increment(1);

        // Parse component_name
        let path_parts: Vec<&str> = req.uri().path().split('/').skip(1).collect();
        if path_parts.is_empty() {
            return Err(HandlerError::NotFound);
        }
        let component_name = path_parts[0].to_string();

        // Parse the rest of the path chunks
        let (key, request_type) =
            if let Some(ct) = self.schemas.resolve_latest_component_type(&component_name) {
                parse_component_request_path_chunks(ct, &path_parts[1..])?
            } else {
                return Err(HandlerError::NotFound);
            };

        // Check if handler exists and whether it's public
        let handler_name = match &request_type {
            RequestType::Call { handler } => handler.clone(),
            RequestType::Send { handler } => handler.clone(),
        }
        .to_string();

        if let Some(basic_component_metadata) = self
            .schemas
            .resolve_latest_component_handler(&component_name, &handler_name)
        {
            if !basic_component_metadata.public {
                return Err(HandlerError::PrivateComponent);
            }
        } else {
            return Err(HandlerError::NotFound);
        }

        // Craft FullInvocationId
        let fid = if let Some(key) = key {
            FullInvocationId::generate(ServiceId::new(component_name.clone(), key.to_owned()))
        } else {
            FullInvocationId::generate(ServiceId::new(
                component_name.clone(),
                Uuid::now_v7().to_string(),
            ))
        };

        let (client_addr, client_port) = (connect_info.address(), connect_info.port());

        // Create the ingress span and attach it to the next async block.
        // This span is committed once the async block terminates, recording the execution time of the invocation.
        // Another span is created later by the ServiceInvocationFactory, for the ServiceInvocation itself,
        // which is used by the Restate components to correctly link to a single parent span
        // to commit intermediate results of the processing.
        let ingress_span = info_span!(
            "ingress_invoke",
            otel.name = format!("ingress_invoke {}/{}", component_name, handler_name),
            rpc.system = "restate",
            rpc.service = %component_name,
            rpc.method = %handler_name,
            client.socket.address = %client_addr,
            client.socket.port = %client_port,
        );

        // Extract tracing context if any
        let tracing_context =
            TraceContextPropagator::new().extract(&HeaderExtractor(req.headers()));

        // Attach this ingress_span to the parent parsed from the headers, if any.
        span_relation(tracing_context.span().span_context()).attach_to_span(&ingress_span);

        // We need the context to link it to the service invocation span
        let ingress_span_context = ingress_span.context().span().span_context().clone();

        let cloned_handler_name = handler_name.clone();
        let handle_fut = async move {
            info!("Processing ingress request");

            // Check HTTP Method
            if req.method() != Method::GET && req.method() != Method::POST {
                return Err(HandlerError::MethodNotAllowed);
            }

            // TODO validate content-type
            //  https://github.com/restatedev/restate/issues/1230

            // Check if Idempotency-Key is available
            let idempotency_mode = parse_idempotency_key_and_retention_period(req.headers())?;

            // Collect body
            let collected_request_bytes = req
                .into_body()
                .collect()
                .await
                .map_err(|e| HandlerError::Body(e.into()))?
                .to_bytes();
            trace!(rpc.request = ?collected_request_bytes);

            let span_relation = SpanRelation::Parent(ingress_span_context);

            match request_type {
                RequestType::Call { .. } => {
                    Self::handle_component_call(
                        fid,
                        cloned_handler_name,
                        idempotency_mode,
                        collected_request_bytes,
                        span_relation,
                        self.request_tx,
                    )
                    .await
                }
                RequestType::Send { .. } => {
                    Self::handle_component_send(
                        fid,
                        cloned_handler_name,
                        idempotency_mode,
                        collected_request_bytes,
                        span_relation,
                        self.request_tx,
                    )
                    .await
                }
            }
        }
        .instrument(ingress_span);

        async move {
            let result = handle_fut.await;
            // We hold the semaphore permit up to the end of the request processing
            drop(permit);
            // Note that we only record (mostly) successful requests here. We might want to
            // change this in the _near_ future.
            histogram!(
                INGRESS_REQUEST_DURATION,
                "rpc.service" => component_name.clone(),
                "rpc.method" => handler_name.clone(),
            )
            .record(start_time.elapsed());

            counter!(
                INGRESS_REQUESTS,
                "status" => REQUEST_COMPLETED,
                "rpc.service" => component_name,
                "rpc.method" => handler_name,
            )
            .increment(1);
            result
        }
        .await
    }

    async fn handle_component_call(
        fid: FullInvocationId,
        cloned_handler_name: String,
        idempotency_mode: IdempotencyMode,
        collected_request_bytes: Bytes,
        span_relation: SpanRelation,
        request_tx: IngressRequestSender,
    ) -> Result<Response<Either<Empty<Bytes>, Full<Bytes>>>, HandlerError> {
        let (invocation, response_rx) = IngressRequest::invocation(
            fid,
            cloned_handler_name,
            collected_request_bytes,
            span_relation,
            idempotency_mode,
        );
        if request_tx.send(invocation).is_err() {
            debug!("Ingress dispatcher is closed while there is still an invocation in flight.");
            return Err(HandlerError::Unavailable);
        }

        // Wait on response
        let response = if let Ok(response) = response_rx.await {
            response
        } else {
            warn!("Response channel was closed");
            return Err(HandlerError::Unavailable);
        };

        // Prepare response metadata
        let mut response_builder = hyper::Response::builder();

        // Add idempotency expiry time if available
        if let Some(expire_time) = response.idempotency_expire_time() {
            response_builder = response_builder.header(IDEMPOTENCY_EXPIRES, expire_time);
        }

        match response.into() {
            Ok(response_payload) => {
                trace!(rpc.response = ?response_payload, "Complete external HTTP request successfully");
                // TODO this is a temporary solution until we have some format awareness.
                //  https://github.com/restatedev/restate/issues/1230
                response_builder = response_builder.header(header::CONTENT_TYPE, APPLICATION_JSON);

                Ok(response_builder
                    .body(Either::Right(Full::new(response_payload)))
                    .unwrap())
            }
            Err(error) => {
                info!(rpc.response = ?error, "Complete external HTTP request with a failure");
                response_builder = response_builder
                    .status(invocation_status_code_to_http_status_code(
                        error.code().into(),
                    ))
                    .header(header::CONTENT_TYPE, APPLICATION_JSON);
                Ok(response_builder
                    .body(Either::Right(Full::new(
                        serde_json::to_vec(&error).unwrap().into(),
                    )))
                    .unwrap())
            }
        }
    }

    async fn handle_component_send(
        fid: FullInvocationId,
        handler_name: String,
        idempotency_mode: IdempotencyMode,
        collected_request_bytes: Bytes,
        span_relation: SpanRelation,
        request_tx: IngressRequestSender,
    ) -> Result<Response<Either<Empty<Bytes>, Full<Bytes>>>, HandlerError> {
        if !matches!(idempotency_mode, IdempotencyMode::None) {
            // TODO https://github.com/restatedev/restate/issues/1233
            return Err(HandlerError::SendAndIdempotencyKey);
        }

        let invocation_id = InvocationId::from(&fid);

        // Send the service invocation and wait on ack
        let (invocation, ack_rx) = IngressRequest::background_invocation(
            fid,
            handler_name,
            collected_request_bytes,
            span_relation,
            None,
        );
        if request_tx.send(invocation).is_err() {
            debug!("Ingress dispatcher is closed while there is still an invocation in flight.");
            return Err(HandlerError::Unavailable);
        }
        if ack_rx.await.is_err() {
            warn!("Response channel was closed");
            return Err(HandlerError::Unavailable);
        };

        trace!("Complete external HTTP send request successfully");
        Ok(Response::builder()
            .status(StatusCode::ACCEPTED)
            .header(header::CONTENT_TYPE, APPLICATION_JSON)
            .body(Either::Right(Full::new(
                serde_json::to_vec(&SendResponse { invocation_id })
                    .unwrap()
                    .into(),
            )))
            .unwrap())
    }

    fn handle_preflight<B: http_body::Body>(
        &self,
        req: Request<B>,
    ) -> Response<Either<Empty<Bytes>, Full<Bytes>>> {
        let mut response_builder =
            hyper::Response::builder().header(header::ACCESS_CONTROL_ALLOW_ORIGIN, WILDCARD);
        if let Some(value) = req.headers().get(header::ACCESS_CONTROL_REQUEST_HEADERS) {
            response_builder = response_builder.header(header::ACCESS_CONTROL_ALLOW_HEADERS, value)
        }
        if let Some(value) = req.headers().get(header::ACCESS_CONTROL_REQUEST_METHOD) {
            response_builder = response_builder.header(header::ACCESS_CONTROL_ALLOW_METHODS, value)
        }
        response_builder
            .body(Either::Left(Empty::default()))
            .unwrap()
    }

    #[allow(clippy::type_complexity)]
    fn handle_health<B: http_body::Body>(
        &self,
        req: Request<B>,
    ) -> Result<Response<Either<Empty<Bytes>, Full<Bytes>>>, HandlerError> {
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }
        let response = HealthResponse {
            components: self
                .schemas
                .list_components()
                .into_iter()
                .map(|c| c.name)
                .collect(),
        };
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, APPLICATION_JSON)
            .body(Either::Right(Full::new(
                serde_json::to_vec(&response)
                    .expect("Serializing the HealthResponse must not fail")
                    .into(),
            )))
            .unwrap())
    }
}

enum RequestType {
    Call { handler: String },
    Send { handler: String },
}

fn parse_component_request_path_chunks<'a>(
    component_type: ComponentType,
    mut path_parts: &'a [&'a str],
) -> Result<(Option<String>, RequestType), HandlerError> {
    // Parse key only for VirtualObject
    let mut key: Option<String> = None;
    if matches!(component_type, ComponentType::VirtualObject) {
        if path_parts.is_empty() {
            return Err(HandlerError::BadPath);
        }
        let encoded_key = path_parts[0];
        key = Some(
            urlencoding::decode(encoded_key)
                .map_err(HandlerError::UrlDecodingError)?
                .into_owned(),
        );
        path_parts = &path_parts[1..];
    }

    if path_parts.is_empty() {
        return Err(HandlerError::BadPath);
    }
    let handler = path_parts[0].to_owned();
    path_parts = &path_parts[1..];

    if path_parts.is_empty() {
        return Ok((key, RequestType::Call { handler }));
    }
    if path_parts.len() == 1 && path_parts[0].eq_ignore_ascii_case("send") {
        // TODO add support for parsing delay
        return Ok((key, RequestType::Send { handler }));
    }

    Err(HandlerError::BadPath)
}

fn span_relation(request_span: &SpanContext) -> SpanRelation {
    if request_span.is_valid() {
        SpanRelation::Parent(request_span.clone())
    } else {
        SpanRelation::None
    }
}

fn parse_idempotency_key_and_retention_period(
    headers: &HeaderMap,
) -> Result<IdempotencyMode, HandlerError> {
    let idempotency_key = if let Some(idempotency_key) = headers.get(IDEMPOTENCY_KEY) {
        Bytes::copy_from_slice(idempotency_key.as_bytes())
    } else {
        return Ok(IdempotencyMode::None);
    };

    if let Some(retention_period_sec) = headers.get(IDEMPOTENCY_RETENTION_PERIOD) {
        let retention_period = Duration::from_secs(
            retention_period_sec
                .to_str()
                .map_err(|e| HandlerError::BadIdempotency(e.into()))?
                .parse()
                .map_err(|e: ParseIntError| HandlerError::BadIdempotency(e.into()))?,
        );
        Ok(IdempotencyMode::key(
            idempotency_key,
            Some(retention_period),
        ))
    } else {
        Ok(IdempotencyMode::key(idempotency_key, None))
    }
}

fn invocation_status_code_to_http_status_code(code: UserErrorCode) -> StatusCode {
    match code {
        UserErrorCode::Cancelled => StatusCode::REQUEST_TIMEOUT,
        UserErrorCode::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
        UserErrorCode::InvalidArgument => StatusCode::BAD_REQUEST,
        UserErrorCode::DeadlineExceeded => StatusCode::REQUEST_TIMEOUT,
        UserErrorCode::NotFound => StatusCode::NOT_FOUND,
        UserErrorCode::AlreadyExists => StatusCode::CONFLICT,
        UserErrorCode::PermissionDenied => StatusCode::FORBIDDEN,
        UserErrorCode::Unauthenticated => StatusCode::UNAUTHORIZED,
        UserErrorCode::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        UserErrorCode::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        UserErrorCode::Aborted => StatusCode::CONFLICT,
        UserErrorCode::OutOfRange => StatusCode::BAD_REQUEST,
        UserErrorCode::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        UserErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        UserErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        UserErrorCode::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// This is taken from opentelemetry-http,
//  we need this because that module is still on the old http crate version
// https://github.com/open-telemetry/opentelemetry-rust/blob/ef4701055cc39d3448d5e5392812ded00cdd4476/opentelemetry-http/src/lib.rs#L14 License APL 2.0
pub struct HeaderExtractor<'a>(pub &'a HeaderMap);

impl<'a> Extractor for HeaderExtractor<'a> {
    /// Get a value for a key from the HeaderMap.  If the value is not valid ASCII, returns None.
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    /// Collect all the keys from the HeaderMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|value| value.as_str())
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use super::mocks::*;
    use super::*;

    use restate_core::TestCoreEnv;
    use tokio::sync::mpsc;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn call_service() {
        let greeting_req = GreetingRequest {
            person: "Francesco".to_string(),
        };

        let req = hyper::Request::builder()
            .uri("http://localhost/greeter.Greeter/greet")
            .method(Method::POST)
            .body(Full::new(Bytes::from(
                serde_json::to_vec(&greeting_req).unwrap(),
            )))
            .unwrap();

        let response = handle(req, |ingress_req| {
            // Get the function invocation and assert on it
            let (fid, method_name, argument, _, _, response_tx) = ingress_req.expect_invocation();
            restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.Greeter");
            restate_test_util::assert_eq!(method_name, "greet");

            let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
            restate_test_util::assert_eq!(&greeting_req.person, "Francesco");

            response_tx
                .send(
                    Ok(serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into())
                    .into(),
                )
                .unwrap();
        })
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let (_, response_body) = response.into_parts();
        let response_bytes = response_body.collect().await.unwrap().to_bytes();
        let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
        restate_test_util::assert_eq!(response_value.greeting, "Igal");
    }

    #[tokio::test]
    #[traced_test]
    async fn call_service_with_get() {
        let req = hyper::Request::builder()
            .uri("http://localhost/greeter.Greeter/greet")
            .method(Method::GET)
            .body(Empty::<Bytes>::default())
            .unwrap();

        let response = handle(req, |ingress_req| {
            // Get the function invocation and assert on it
            let (fid, method_name, argument, _, _, response_tx) = ingress_req.expect_invocation();
            restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.Greeter");
            restate_test_util::assert_eq!(method_name, "greet");

            assert!(argument.is_empty());

            response_tx
                .send(
                    Ok(serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into())
                    .into(),
                )
                .unwrap();
        })
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let (_, response_body) = response.into_parts();
        let response_bytes = response_body.collect().await.unwrap().to_bytes();
        let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
        restate_test_util::assert_eq!(response_value.greeting, "Igal");
    }

    #[tokio::test]
    #[traced_test]
    async fn call_virtual_object() {
        let greeting_req = GreetingRequest {
            person: "Francesco".to_string(),
        };

        let req = hyper::Request::builder()
            .uri("http://localhost/greeter.GreeterObject/my-key/greet")
            .method(Method::POST)
            .body(Full::new(Bytes::from(
                serde_json::to_vec(&greeting_req).unwrap(),
            )))
            .unwrap();

        let response = handle(req, |ingress_req| {
            // Get the function invocation and assert on it
            let (fid, method_name, argument, _, _, response_tx) = ingress_req.expect_invocation();
            restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.GreeterObject");
            restate_test_util::assert_eq!(fid.service_id.key, &"my-key");
            restate_test_util::assert_eq!(method_name, "greet");

            let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
            restate_test_util::assert_eq!(&greeting_req.person, "Francesco");

            response_tx
                .send(
                    Ok(serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into())
                    .into(),
                )
                .unwrap();
        })
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let (_, response_body) = response.into_parts();
        let response_bytes = response_body.collect().await.unwrap().to_bytes();
        let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
        restate_test_util::assert_eq!(response_value.greeting, "Igal");
    }

    #[tokio::test]
    #[traced_test]
    async fn send_service() {
        let greeting_req = GreetingRequest {
            person: "Francesco".to_string(),
        };

        let req = hyper::Request::builder()
            .uri("http://localhost/greeter.Greeter/greet/send")
            .method(Method::POST)
            .body(Full::new(Bytes::from(
                serde_json::to_vec(&greeting_req).unwrap(),
            )))
            .unwrap();

        let response = handle(req, |ingress_req| {
            // Get the function invocation and assert on it
            let (fid, method_name, argument, _, ack_tx) =
                ingress_req.expect_background_invocation();
            restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.Greeter");
            restate_test_util::assert_eq!(method_name, "greet");

            let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
            restate_test_util::assert_eq!(&greeting_req.person, "Francesco");

            ack_tx.send(()).unwrap();
        })
        .await;

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let (_, response_body) = response.into_parts();
        let response_bytes = response_body.collect().await.unwrap().to_bytes();
        let _: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn send_virtual_object() {
        let greeting_req = GreetingRequest {
            person: "Francesco".to_string(),
        };

        let req = hyper::Request::builder()
            .uri("http://localhost/greeter.GreeterObject/my-key/greet/send")
            .method(Method::POST)
            .body(Full::new(Bytes::from(
                serde_json::to_vec(&greeting_req).unwrap(),
            )))
            .unwrap();

        let response = handle(req, |ingress_req| {
            // Get the function invocation and assert on it
            let (fid, method_name, argument, _, ack_tx) =
                ingress_req.expect_background_invocation();
            restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.GreeterObject");
            restate_test_util::assert_eq!(fid.service_id.key, &"my-key");
            restate_test_util::assert_eq!(method_name, "greet");

            let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
            restate_test_util::assert_eq!(&greeting_req.person, "Francesco");

            ack_tx.send(()).unwrap();
        })
        .await;

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let (_, response_body) = response.into_parts();
        let response_bytes = response_body.collect().await.unwrap().to_bytes();
        let _: SendResponse = serde_json::from_slice(&response_bytes).unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn idempotency_key_parsing() {
        let greeting_req = GreetingRequest {
            person: "Francesco".to_string(),
        };

        let req = hyper::Request::builder()
            .uri("http://localhost/greeter.Greeter/greet")
            .method(Method::POST)
            .header(IDEMPOTENCY_KEY, "123456")
            .body(Full::new(Bytes::from(
                serde_json::to_vec(&greeting_req).unwrap(),
            )))
            .unwrap();

        let response = handle(req, |ingress_req| {
            // Get the function invocation and assert on it
            let (fid, method_name, argument, _, idempotency_mode, response_tx) =
                ingress_req.expect_invocation();
            restate_test_util::assert_eq!(fid.service_id.service_name, "greeter.Greeter");
            restate_test_util::assert_eq!(method_name, "greet");

            let greeting_req: GreetingRequest = serde_json::from_slice(&argument).unwrap();
            restate_test_util::assert_eq!(&greeting_req.person, "Francesco");

            restate_test_util::assert_eq!(
                idempotency_mode,
                IdempotencyMode::key(Bytes::from_static(b"123456"), None)
            );

            response_tx
                .send(
                    Ok(serde_json::to_vec(&GreetingResponse {
                        greeting: "Igal".to_string(),
                    })
                    .unwrap()
                    .into())
                    .into(),
                )
                .unwrap();
        })
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let (_, response_body) = response.into_parts();
        let response_bytes = response_body.collect().await.unwrap().to_bytes();
        let response_value: GreetingResponse = serde_json::from_slice(&response_bytes).unwrap();
        restate_test_util::assert_eq!(response_value.greeting, "Igal");
    }

    #[tokio::test]
    #[traced_test]
    async fn bad_path_service() {
        test_handle_with_status_code_error_response(
            hyper::Request::get("http://localhost/greeter.Greeter")
                .body(Empty::<Bytes>::default())
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;

        test_handle_with_status_code_error_response(
            hyper::Request::get("http://localhost/greeter.Greeter/greet/sendbla")
                .body(Empty::<Bytes>::default())
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;

        test_handle_with_status_code_error_response(
            hyper::Request::get("http://localhost/greeter.Greeter/greet/send/bla")
                .body(Empty::<Bytes>::default())
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    #[traced_test]
    async fn bad_path_virtual_object() {
        test_handle_with_status_code_error_response(
            hyper::Request::get("http://localhost/greeter.GreeterObject/my-key")
                .body(Empty::<Bytes>::default())
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;

        test_handle_with_status_code_error_response(
            hyper::Request::get("http://localhost/greeter.GreeterObject/my-key/greet/sendbla")
                .body(Empty::<Bytes>::default())
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;

        test_handle_with_status_code_error_response(
            hyper::Request::get("http://localhost/greeter.GreeterObject/my-key/greet/send/bla")
                .body(Empty::<Bytes>::default())
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    #[traced_test]
    async fn unknown_component() {
        test_handle_with_status_code_error_response(
            hyper::Request::get(
                "http://localhost/whatevernotexistingservice/whatevernotexistinghandler",
            )
            .body(Empty::<Bytes>::default())
            .unwrap(),
            StatusCode::NOT_FOUND,
        )
        .await;
    }

    #[tokio::test]
    #[traced_test]
    async fn unknown_handler() {
        test_handle_with_status_code_error_response(
            hyper::Request::get("http://localhost/greeter.Greeter/whatevernotexistinghandler")
                .body(Empty::<Bytes>::default())
                .unwrap(),
            StatusCode::NOT_FOUND,
        )
        .await;
    }

    #[tokio::test]
    #[traced_test]
    async fn private_service() {
        test_handle_with_status_code_error_response(
            hyper::Request::get("http://localhost/greeter.GreeterPrivate/greet")
                .body(Empty::<Bytes>::default())
                .unwrap(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    #[traced_test]
    async fn health() {
        let req = hyper::Request::builder()
            .uri("http://localhost/restate/health")
            .method(Method::GET)
            .body(Empty::<Bytes>::default())
            .unwrap();

        let response = handle(req, |_| {
            panic!("This code should not be reached in this test");
        })
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let (_, response_body) = response.into_parts();
        let response_bytes = response_body.collect().await.unwrap().to_bytes();
        let _: HealthResponse = serde_json::from_slice(&response_bytes).unwrap();
    }

    async fn test_handle_with_status_code_error_response<B: http_body::Body + Send + 'static>(
        req: Request<B>,
        expected_status_code: StatusCode,
    ) -> Response<Either<Empty<Bytes>, Full<Bytes>>>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
        <B as http_body::Body>::Data: Send + Sync + 'static,
    {
        let response = handle(req, |_| {
            panic!("This code should not be reached in this test");
        })
        .await;

        assert_eq!(response.status(), expected_status_code);

        response
    }

    async fn handle<B: http_body::Body + Send + 'static>(
        req: Request<B>,
        f: impl FnOnce(IngressRequest) + Send + 'static,
    ) -> Response<Either<Empty<Bytes>, Full<Bytes>>>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
        <B as http_body::Body>::Data: Send + Sync + 'static,
    {
        let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        let (ingress_request_tx, mut ingress_request_rx) = mpsc::unbounded_channel();

        let handler_fut = node_env.tc.run_in_scope(
            "ingress",
            None,
            Handler::new(
                mock_component_resolver(),
                ingress_request_tx,
                Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            )
            .handle(ConnectInfo::new("0.0.0.0:0".parse().unwrap()), req),
        );

        // Mock the service invocation receiver
        tokio::spawn(async move {
            f(ingress_request_rx.recv().await.unwrap());
        });

        handler_fut.await
    }
}
