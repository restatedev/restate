use bytes::Buf;
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use http::request::Parts;
use http::{Method, Request, Response, StatusCode};
use hyper::Body;
use prost_reflect::{DynamicMessage, MethodDescriptor};
use serde::Serialize;
use tonic::{Code, Status};
use tracing::warn;

use content_type::ConnectContentType;

pub(super) async fn decode_request(
    request: Request<Body>,
    method_descriptor: &MethodDescriptor,
) -> Result<(ConnectContentType, DynamicMessage), Response<Body>> {
    let (mut parts, body) = request.into_parts();

    // Check content encoding
    if !is_content_encoding_identity(&parts) {
        return Err(status::not_implemented());
    }

    return if parts.method == Method::GET && is_get_allowed(method_descriptor) {
        Ok((
            ConnectContentType::Json,
            DynamicMessage::new(method_descriptor.input()),
        ))
    } else if parts.method == Method::POST {
        // Read content type
        let content_type = match parts
            .headers
            .remove(CONTENT_TYPE)
            .and_then(|hv| content_type::resolve_content_type(&hv))
        {
            Some(ct) => ct,
            None => return Err(status::unsupported_media_type()),
        };

        // Read the body
        let body = match hyper::body::to_bytes(body).await {
            Ok(b) => b,
            Err(e) => {
                warn!("Error when reading the body: {}", e);
                return Err(status::internal_server_error());
            }
        };

        // Transcode the message
        let msg = match content_type::read_message(content_type, method_descriptor.input(), body) {
            Ok(msg) => msg,
            Err(e) => {
                warn!(
                    "Error when parsing request {}: {}",
                    method_descriptor.full_name(),
                    e
                );
                return Err(status::status_response(Status::invalid_argument(format!(
                    "Error when parsing request {}: {}",
                    method_descriptor.full_name(),
                    e
                ))));
            }
        };

        Ok((content_type, msg))
    } else {
        Err(status::method_not_allowed())
    };
}

pub(super) fn encode_response<B: Buf>(
    response_body: B,
    method_descriptor: &MethodDescriptor,
    request_content_type: ConnectContentType,
) -> Response<Body> {
    let dynamic_message = match DynamicMessage::decode(method_descriptor.output(), response_body) {
        Ok(msg) => msg,
        Err(err) => {
            warn!("The response payload cannot be decoded: {}", err);
            return status::status_response(Status::internal(format!(
                "The response payload cannot be decoded: {err}",
            )));
        }
    };

    let (content_type_header, body) =
        match content_type::write_message(request_content_type, dynamic_message) {
            Ok(r) => r,
            Err(err) => {
                warn!("The response payload cannot be serialized: {}", err);
                return status::status_response(Status::internal(format!(
                    "The response payload cannot be serialized: {err}",
                )));
            }
        };

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type_header)
        .body(body)
        .unwrap()
}

#[inline]
fn is_content_encoding_identity(parts: &Parts) -> bool {
    return parts
        .headers
        .get(CONTENT_ENCODING)
        .and_then(|hv| hv.to_str().ok().map(|s| s.contains("identity")))
        .unwrap_or(true);
}

#[inline]
fn is_get_allowed(desc: &MethodDescriptor) -> bool {
    return desc.input().full_name() == "google.protobuf.Empty";
}

pub(super) mod content_type {
    use super::*;

    use bytes::{Buf, BufMut, BytesMut};
    use http::HeaderValue;
    use prost::Message;
    use prost_reflect::MessageDescriptor;
    use tower::BoxError;

    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub enum ConnectContentType {
        Protobuf,
        Json,
    }

    pub(super) fn resolve_content_type(content_type: &HeaderValue) -> Option<ConnectContentType> {
        if let Ok(ct) = content_type.to_str() {
            return if ct.starts_with("application/json") {
                Some(ConnectContentType::Json)
            } else if ct.starts_with("application/proto") || ct.starts_with("application/protobuf")
            {
                Some(ConnectContentType::Protobuf)
            } else {
                None
            };
        }
        None
    }

    pub(super) fn read_message(
        content_type: ConnectContentType,
        msg_desc: MessageDescriptor,
        payload_buf: impl Buf + Sized,
    ) -> Result<DynamicMessage, BoxError> {
        match content_type {
            ConnectContentType::Json => {
                let mut deser = serde_json::Deserializer::from_reader(payload_buf.reader());
                let dynamic_message = DynamicMessage::deserialize(msg_desc, &mut deser)?;
                deser.end()?;
                Ok(dynamic_message)
            }
            ConnectContentType::Protobuf => Ok(DynamicMessage::decode(msg_desc, payload_buf)?),
        }
    }

    pub(super) fn write_message(
        content_type: ConnectContentType,
        msg: DynamicMessage,
    ) -> Result<(HeaderValue, Body), BoxError> {
        match content_type {
            ConnectContentType::Json => {
                let mut ser = serde_json::Serializer::new(BytesMut::new().writer());

                msg.serialize(&mut ser)?;

                Ok((
                    HeaderValue::from_static("application/json"),
                    ser.into_inner().into_inner().freeze().into(),
                ))
            }
            ConnectContentType::Protobuf => Ok((
                HeaderValue::from_static("application/proto"),
                msg.encode_to_vec().into(),
            )),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::mocks::greeter_get_count_method_descriptor;

        use bytes::Bytes;
        use http::HeaderValue;
        use test_utils::assert_eq;

        #[test]
        fn resolve_json() {
            assert_eq!(
                resolve_content_type(&HeaderValue::from_static("application/json")).unwrap(),
                ConnectContentType::Json
            );
            assert_eq!(
                resolve_content_type(&HeaderValue::from_static("application/json; encoding=utf8"))
                    .unwrap(),
                ConnectContentType::Json
            );
        }

        #[test]
        fn read_google_protobuf_empty() {
            read_message(
                ConnectContentType::Json,
                greeter_get_count_method_descriptor().input(),
                Bytes::from("{}"),
            )
            .unwrap()
            .transcode_to::<()>()
            .unwrap();
        }

        #[tokio::test]
        async fn write_google_protobuf_empty() {
            let (_, b) = write_message(
                ConnectContentType::Json,
                DynamicMessage::new(greeter_get_count_method_descriptor().input()),
            )
            .unwrap();

            assert_eq!(
                hyper::body::to_bytes(b).await.unwrap(),
                Bytes::from_static(b"{}")
            )
        }
    }
}

pub(super) mod status {
    use super::*;

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

    use crate::mocks::{greeter_greet_method_descriptor, pb};
    use bytes::Bytes;
    use http::StatusCode;
    use http_body::Body;
    use prost::Message;
    use serde_json::json;
    use test_utils::{assert_eq, test};

    #[test(tokio::test)]
    async fn decode_greet_json() {
        let (ct, request_payload) = decode_request(
            Request::builder()
                .uri("http://localhost/greeter.Greeter/Greet")
                .method(Method::POST)
                .header(CONTENT_TYPE, "application/json")
                .body(json!({"person": "Francesco"}).to_string().into())
                .unwrap(),
            &greeter_greet_method_descriptor(),
        )
        .await
        .unwrap();

        assert_eq!(
            request_payload
                .transcode_to::<pb::GreetingRequest>()
                .unwrap(),
            pb::GreetingRequest {
                person: "Francesco".to_string()
            }
        );
        assert_eq!(ct, ConnectContentType::Json);
    }

    #[test(tokio::test)]
    async fn decode_greet_protobuf() {
        let (ct, request_payload) = decode_request(
            Request::builder()
                .uri("http://localhost/greeter.Greeter/Greet")
                .method(Method::POST)
                .header(CONTENT_TYPE, "application/protobuf")
                .body(
                    pb::GreetingRequest {
                        person: "Francesco".to_string(),
                    }
                    .encode_to_vec()
                    .into(),
                )
                .unwrap(),
            &greeter_greet_method_descriptor(),
        )
        .await
        .unwrap();

        assert_eq!(
            request_payload
                .transcode_to::<pb::GreetingRequest>()
                .unwrap(),
            pb::GreetingRequest {
                person: "Francesco".to_string()
            }
        );
        assert_eq!(ct, ConnectContentType::Protobuf);
    }

    #[test(tokio::test)]
    async fn decode_wrong_http_method() {
        let err_response = decode_request(
            Request::builder()
                .uri("http://localhost/greeter.Greeter/Greet")
                .method(Method::PUT)
                .header(CONTENT_TYPE, "application/json")
                .body(json!({"person": "Francesco"}).to_string().into())
                .unwrap(),
            &greeter_greet_method_descriptor(),
        )
        .await
        .unwrap_err();

        assert_eq!(err_response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[test(tokio::test)]
    async fn decode_unknown_content_type() {
        let err_response = decode_request(
            Request::builder()
                .uri("http://localhost/greeter.Greeter/Greet")
                .method(Method::POST)
                .header(CONTENT_TYPE, "application/yaml")
                .body("person: Francesco".to_string().into())
                .unwrap(),
            &greeter_greet_method_descriptor(),
        )
        .await
        .unwrap_err();

        assert_eq!(err_response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[test(tokio::test)]
    async fn encode_greet_json() {
        let pb_response = Bytes::from(
            pb::GreetingResponse {
                greeting: "Hello Francesco".to_string(),
            }
            .encode_to_vec(),
        );

        let mut res = encode_response(
            pb_response,
            &greeter_greet_method_descriptor(),
            ConnectContentType::Json,
        );

        let body = res.data().await.unwrap().unwrap();
        let json_body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            json_body.get("greeting").unwrap().as_str().unwrap(),
            "Hello Francesco"
        );
    }
}
