// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::IngressRequestHeaders;
use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use hyper::Body;
use prost_reflect::{DeserializeOptions, SerializeOptions};
use restate_schema_api::json::{JsonMapperResolver, JsonToProtobufMapper, ProtobufToJsonMapper};
use tonic::{Code, Status};
use tracing::warn;

const APPLICATION_JSON: &str = "application/json";
const APPLICATION_PROTO: &str = "application/proto";

// Clippy false positive, might be caused by Bytes contained within HeaderValue.
// https://github.com/rust-lang/rust/issues/40543#issuecomment-1212981256
#[allow(clippy::declare_interior_mutable_const)]
const APPLICATION_JSON_HEADER: HeaderValue = HeaderValue::from_static(APPLICATION_JSON);
#[allow(clippy::declare_interior_mutable_const)]
const APPLICATION_PROTO_HEADER: HeaderValue = HeaderValue::from_static(APPLICATION_PROTO);

pub(super) fn verify_headers_and_infer_body_type<B: http_body::Body>(
    request: &mut Request<B>,
) -> Result<ConnectBodyType, Response<Body>> {
    // Check content encoding
    if !is_content_encoding_identity(request.headers()) {
        return Err(status::not_implemented());
    }

    return match *request.method() {
        Method::GET => Ok(ConnectBodyType::OnlyJsonResponse),
        Method::POST => {
            // Read content type
            match request
                .headers_mut()
                .remove(CONTENT_TYPE)
                .and_then(|hv| ConnectBodyType::parse_from_header(&hv))
            {
                Some(ct) => Ok(ct),
                None => Err(status::unsupported_media_type()),
            }
        }
        _ => Err(status::method_not_allowed()),
    };
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ConnectBodyType {
    Protobuf,
    Json,
    OnlyJsonResponse,
}

impl ConnectBodyType {
    fn parse_from_header(content_type: &HeaderValue) -> Option<Self> {
        if let Ok(ct) = content_type.to_str() {
            return if ct.starts_with(APPLICATION_JSON) {
                Some(ConnectBodyType::Json)
            } else if ct.starts_with(APPLICATION_PROTO) {
                Some(ConnectBodyType::Protobuf)
            } else {
                None
            };
        }
        None
    }

    pub(super) fn infer_encoder_and_decoder<MapperResolver, JsonDecoder, JsonEncoder>(
        &self,
        ingress_request_header: &IngressRequestHeaders,
        mapper_resolver: MapperResolver,
    ) -> Result<(Decoder<JsonDecoder>, Encoder<JsonEncoder>), Response<Body>>
    where
        MapperResolver: JsonMapperResolver<
            JsonToProtobufMapper = JsonDecoder,
            ProtobufToJsonMapper = JsonEncoder,
        >,
    {
        match self {
            ConnectBodyType::Protobuf => Ok((Decoder::Protobuf, Encoder::Protobuf)),
            ConnectBodyType::Json => {
                let (json_decoder, json_encoder) = mapper_resolver
                    .resolve_json_mapper_for_service(
                        &ingress_request_header.service_name,
                        &ingress_request_header.method_name,
                    )
                    .ok_or_else(|| status::code_response(Code::NotFound))?;

                Ok((Decoder::Json(json_decoder), Encoder::Json(json_encoder)))
            }
            ConnectBodyType::OnlyJsonResponse => {
                let (_, json_encoder) = mapper_resolver
                    .resolve_json_mapper_for_service(
                        &ingress_request_header.service_name,
                        &ingress_request_header.method_name,
                    )
                    .ok_or_else(|| status::code_response(Code::NotFound))?;

                Ok((Decoder::Empty, Encoder::Json(json_encoder)))
            }
        }
    }
}

pub(super) enum Decoder<JsonDecoder> {
    Protobuf,
    Empty,
    Json(JsonDecoder),
}

impl<JsonDecoder: JsonToProtobufMapper> Decoder<JsonDecoder> {
    pub(super) async fn decode(
        self,
        req: Request<Body>,
        json_deserialize_options: &DeserializeOptions,
    ) -> Result<Bytes, Response<Body>> {
        if matches!(&self, Decoder::Empty) {
            return Ok(Bytes::default());
        }

        let collected_body = hyper::body::to_bytes(req.into_body()).await.map_err(|e| {
            warn!("Error when reading the body: {}", e);
            status::internal_server_error()
        })?;

        match self {
            Decoder::Empty => {
                unreachable!()
            }
            Decoder::Protobuf => Ok(collected_body),
            Decoder::Json(mapper) => mapper
                .json_to_protobuf(collected_body, json_deserialize_options)
                .map_err(|e| {
                    warn!("Error when parsing request: {}", e);
                    status::status_response(Status::invalid_argument(format!(
                        "Error when parsing request: {}",
                        e
                    )))
                }),
        }
    }
}

pub(super) enum Encoder<JsonEncoder> {
    Protobuf,
    Json(JsonEncoder),
}

impl<JsonEncoder: ProtobufToJsonMapper> Encoder<JsonEncoder> {
    pub(super) fn encode_response(
        self,
        response_body: Bytes,
        json_serialize_options: &SerializeOptions,
    ) -> Response<Body> {
        match self {
            Encoder::Protobuf => Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, APPLICATION_PROTO_HEADER)
                .body(response_body.into())
                .unwrap(),
            Encoder::Json(encoder) => {
                match encoder.protobuf_to_json(response_body, json_serialize_options) {
                    Ok(b) => Response::builder()
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, APPLICATION_JSON_HEADER)
                        .body(b.into())
                        .unwrap(),
                    Err(err) => {
                        warn!("The response payload cannot be serialized: {}", err);
                        status::status_response(Status::internal(format!(
                            "The response payload cannot be serialized: {err}",
                        )))
                    }
                }
            }
        }
    }
}

#[inline]
fn is_content_encoding_identity(headers: &HeaderMap<HeaderValue>) -> bool {
    return headers
        .get(CONTENT_ENCODING)
        .and_then(|hv| hv.to_str().ok().map(|s| s.contains("identity")))
        .unwrap_or(true);
}

pub(super) mod status {
    use super::*;

    use serde::Serialize;

    pub(super) fn method_not_allowed() -> Response<Body> {
        http_code_response(StatusCode::METHOD_NOT_ALLOWED)
    }

    pub(super) fn unsupported_media_type() -> Response<Body> {
        http_code_response(StatusCode::UNSUPPORTED_MEDIA_TYPE)
    }

    pub(super) fn internal_server_error() -> Response<Body> {
        code_response(Code::Internal)
    }

    pub(super) fn not_implemented() -> Response<Body> {
        code_response(Code::Unimplemented)
    }

    #[derive(Serialize)]
    struct StatusJsonResponse<'a> {
        code: &'a str,
        message: &'a str,
    }

    impl<'a> From<&'a Code> for StatusJsonResponse<'a> {
        fn from(code: &'a Code) -> Self {
            Self {
                code: code_str(code),
                message: code.description(),
            }
        }
    }

    impl<'a> From<&'a Status> for StatusJsonResponse<'a> {
        fn from(status: &'a Status) -> Self {
            Self {
                code: code_str(&status.code()),
                message: status.message(),
            }
        }
    }

    pub(super) fn code_response(code: Code) -> Response<Body> {
        let status_response: StatusJsonResponse<'_> = (&code).into();

        Response::builder()
            .status(grpc_status_code_to_http_status_code(code))
            .header(CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&status_response).unwrap().into())
            .unwrap()
    }

    pub(crate) fn status_response(status: Status) -> Response<Body> {
        let status_response: StatusJsonResponse<'_> = (&status).into();

        Response::builder()
            .status(grpc_status_code_to_http_status_code(status.code()))
            .header(CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(&status_response).unwrap().into())
            .unwrap()
    }

    pub(super) fn http_code_response(status_code: StatusCode) -> Response<Body> {
        Response::builder()
            .status(status_code)
            .body(Body::empty())
            .unwrap()
    }

    fn grpc_status_code_to_http_status_code(code: Code) -> StatusCode {
        // This mapping comes from https://connect.build/docs/protocol/#error-codes
        match code {
            Code::Ok => StatusCode::OK,
            Code::Cancelled => StatusCode::REQUEST_TIMEOUT,
            Code::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
            Code::InvalidArgument => StatusCode::BAD_REQUEST,
            Code::DeadlineExceeded => StatusCode::REQUEST_TIMEOUT,
            Code::NotFound => StatusCode::NOT_FOUND,
            Code::AlreadyExists => StatusCode::CONFLICT,
            Code::PermissionDenied => StatusCode::FORBIDDEN,
            Code::Unauthenticated => StatusCode::UNAUTHORIZED,
            Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
            Code::Aborted => StatusCode::CONFLICT,
            Code::OutOfRange => StatusCode::BAD_REQUEST,
            Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            Code::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Code::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn code_str(code: &Code) -> &'static str {
        match code {
            Code::Ok => "ok",
            Code::Cancelled => "cancelled",
            Code::Unknown => "unknown",
            Code::InvalidArgument => "invalid_argument",
            Code::DeadlineExceeded => "deadline_exceeded",
            Code::NotFound => "not_found",
            Code::AlreadyExists => "already_exists",
            Code::PermissionDenied => "permission_denied",
            Code::Unauthenticated => "unauthenticated",
            Code::ResourceExhausted => "resource_exhausted",
            Code::FailedPrecondition => "failed_precondition",
            Code::Aborted => "aborted",
            Code::OutOfRange => "out_of_range",
            Code::Unimplemented => "unimplemented",
            Code::Internal => "internal",
            Code::Unavailable => "unavailable",
            Code::DataLoss => "data_loss",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use http::StatusCode;
    use restate_test_util::{assert_eq, test};
    use serde_json::json;

    #[test]
    fn resolve_content_type() {
        assert_eq!(
            ConnectBodyType::parse_from_header(&HeaderValue::from_static("application/json"))
                .unwrap(),
            ConnectBodyType::Json
        );
        assert_eq!(
            ConnectBodyType::parse_from_header(&HeaderValue::from_static(
                "application/json; encoding=utf8"
            ))
            .unwrap(),
            ConnectBodyType::Json
        );
    }

    #[test]
    fn get_requests_are_valid() {
        let content_type = verify_headers_and_infer_body_type(
            &mut Request::get("http://localhost/greeter.Greeter/Greet")
                .body(hyper::Body::empty())
                .unwrap(),
        )
        .unwrap();

        assert_eq!(ConnectBodyType::OnlyJsonResponse, content_type);
    }

    #[test]
    fn wrong_http_method() {
        let err_response = verify_headers_and_infer_body_type(
            &mut Request::builder()
                .uri("http://localhost/greeter.Greeter/Greet")
                .method(Method::PUT)
                .header(CONTENT_TYPE, "application/json")
                .body::<Body>(json!({"person": "Francesco"}).to_string().into())
                .unwrap(),
        )
        .unwrap_err();

        assert_eq!(err_response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[test(tokio::test)]
    async fn unknown_content_type() {
        let err_response = verify_headers_and_infer_body_type(
            &mut Request::builder()
                .uri("http://localhost/greeter.Greeter/Greet")
                .method(Method::POST)
                .header(CONTENT_TYPE, "application/yaml")
                .body::<Body>("person: Francesco".to_string().into())
                .unwrap(),
        )
        .unwrap_err();

        assert_eq!(err_response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
}
