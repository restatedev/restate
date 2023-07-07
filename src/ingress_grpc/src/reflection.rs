//! This module is very similar to tonic-reflection and in some parts copied,
//! but it differs in the fact that we have interior mutability
//! https://github.com/hyperium/tonic/issues/1328
//!
//! Tonic License MIT: https://github.com/hyperium/tonic

use futures::Stream;
use pin_project::pin_project;
use restate_pb::grpc::reflection::server_reflection_request::MessageRequest;
use restate_pb::grpc::reflection::server_reflection_response::MessageResponse;
use restate_pb::grpc::reflection::server_reflection_server::*;
use restate_pb::grpc::reflection::*;
use restate_schema_api::proto_symbol::ProtoSymbolResolver;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

#[pin_project]
pub struct ReflectionServiceStream<ProtoSymbols> {
    #[pin]
    request_stream: Streaming<ServerReflectionRequest>,
    proto_symbols: ProtoSymbols,
}

fn handle_request<ProtoSymbols: ProtoSymbolResolver>(
    proto_symbols: &ProtoSymbols,
    request: &MessageRequest,
) -> Result<MessageResponse, Status> {
    use restate_pb::grpc::reflection::*;

    Ok(match request {
        MessageRequest::FileByFilename(f) => proto_symbols
            .get_file_descriptor(f)
            .map(|file| {
                MessageResponse::FileDescriptorResponse(FileDescriptorResponse {
                    file_descriptor_proto: vec![file],
                })
            })
            .ok_or_else(|| Status::not_found(f.to_string()))?,
        MessageRequest::FileContainingSymbol(symbol_name) => proto_symbols
            .get_file_descriptors_by_symbol_name(symbol_name)
            .map(|files| {
                MessageResponse::FileDescriptorResponse(FileDescriptorResponse {
                    file_descriptor_proto: files,
                })
            })
            .ok_or_else(|| Status::not_found(symbol_name.to_string()))?,
        MessageRequest::FileContainingExtension(_) => {
            // We don't index extensions, empty response is fine
            MessageResponse::FileDescriptorResponse(FileDescriptorResponse::default())
        }
        MessageRequest::AllExtensionNumbersOfType(_) => {
            // We don't index extensions, empty response is fine
            MessageResponse::AllExtensionNumbersResponse(ExtensionNumberResponse::default())
        }
        MessageRequest::ListServices(_) => {
            debug!("List services");
            MessageResponse::ListServicesResponse(ListServiceResponse {
                service: proto_symbols
                    .list_services()
                    .into_iter()
                    .map(|svc| restate_pb::grpc::reflection::ServiceResponse { name: svc })
                    .collect(),
            })
        }
    })
}

impl<ProtoSymbols: ProtoSymbolResolver> Stream for ReflectionServiceStream<ProtoSymbols> {
    type Item = Result<ServerReflectionResponse, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let request = match ready!(this.request_stream.poll_next(cx)) {
            Some(Ok(req)) => req,
            _ => return Poll::Ready(None),
        };

        let message_request = if let Some(req) = request.message_request.as_ref() {
            req
        } else {
            return Poll::Ready(Some(Err(Status::invalid_argument(
                "Unexpected empty MessageRequest",
            ))));
        };

        let message_response = match handle_request(this.proto_symbols, message_request) {
            Ok(resp_msg) => resp_msg,
            Err(status) => MessageResponse::ErrorResponse(ErrorResponse {
                error_code: status.code() as i32,
                error_message: status.message().to_string(),
            }),
        };

        Poll::Ready(Some(Ok(ServerReflectionResponse {
            valid_host: request.host.clone(),
            original_request: Some(request),
            message_response: Some(message_response),
        })))
    }
}

#[derive(Debug, Clone)]
pub(super) struct ServerReflectionService<ProtoSymbols>(pub(super) ProtoSymbols);

#[tonic::async_trait]
impl<ProtoSymbols: ProtoSymbolResolver + Clone + Send + Sync + 'static> ServerReflection
    for ServerReflectionService<ProtoSymbols>
{
    type ServerReflectionInfoStream = ReflectionServiceStream<ProtoSymbols>;

    async fn server_reflection_info(
        &self,
        request: Request<Streaming<ServerReflectionRequest>>,
    ) -> Result<Response<Self::ServerReflectionInfoStream>, Status> {
        Ok(Response::new(ReflectionServiceStream {
            request_stream: request.into_inner(),
            proto_symbols: self.0.clone(),
        }))
    }
}
