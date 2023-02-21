use super::*;

use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use common::types::{
    IngressId, ServiceInvocation, ServiceInvocationFactory, ServiceInvocationResponseSink,
    SpanRelation,
};
use opentelemetry_api::trace::{SpanContext, TraceContextExt};
use pin_project::pin_project;
use tokio::sync::mpsc;
use tonic::Status;
use tower::Service;
use tracing::instrument::Instrumented;
use tracing::{info, info_span, trace, warn, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Handler for a single request/response.
pub struct RequestResponseHandler<InvocationFactory> {
    ingress_id: Option<IngressId>,

    invocation_factory: InvocationFactory,

    response_requester: IngressResponseRequester,
}

impl<InvocationFactory> RequestResponseHandler<InvocationFactory>
where
    InvocationFactory: ServiceInvocationFactory,
{
    pub fn new(
        ingress_id: IngressId,
        invocation_factory: InvocationFactory,
        response_requester: IngressResponseRequester,
    ) -> Self {
        Self {
            ingress_id: Some(ingress_id),
            invocation_factory,
            response_requester,
        }
    }
}

impl<InvocationFactory> Service<(IngressRequest, mpsc::OwnedPermit<ServiceInvocation>)>
    for RequestResponseHandler<InvocationFactory>
where
    InvocationFactory: ServiceInvocationFactory,
{
    type Response = IngressResponse;
    type Error = IngressError;
    type Future = Instrumented<HandlerResponseFut>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(
        &mut self,
        req: (IngressRequest, mpsc::OwnedPermit<ServiceInvocation>),
    ) -> Self::Future {
        let (req_headers, req_payload) = req.0;
        let permit = req.1;

        let span = info_span!(
            "Ingress invocation",
            rpc.system = "grpc",
            rpc.service = %req_headers.service_name,
            rpc.method = %req_headers.method_name
        );

        trace!(parent: &span, rpc.request = ?req_payload);

        let service_invocation = match self.invocation_factory.create(
            &req_headers.service_name,
            &req_headers.method_name,
            req_payload,
            ServiceInvocationResponseSink::Ingress(self.ingress_id.take().unwrap()),
            SpanRelation::from_parent(create_parent_span_context(
                req_headers.tracing_context.span().span_context(),
            )),
        ) {
            Ok(i) => i,
            Err(e) => {
                warn!(parent: &span, "Cannot create service invocation: {:?}", e);
                return HandlerResponseFut::Error(Some(e)).instrument(span);
            }
        };

        info!(parent: &span, restate.invocation.id = %service_invocation.id);
        trace!(parent: &span, restate.invocation.request_headers = ?req_headers);

        let (response_registration_cmd, response_rx) =
            Command::prepare(service_invocation.id.clone());
        if self
            .response_requester
            .send(response_registration_cmd)
            .is_err()
        {
            warn!(
                parent: &span,
                "Cannot register invocation to response dispatcher loop"
            );
            return HandlerResponseFut::Error(Some(Status::unavailable("Unavailable")))
                .instrument(span);
        }

        // Send the ServiceInvocation
        permit.send(service_invocation);

        // We need to wait for response
        HandlerResponseFut::WaitingResponse(response_rx).instrument(span)
    }
}

#[pin_project(project = HandlerResponseFutProj)]
pub enum HandlerResponseFut {
    WaitingResponse(#[pin] CommandResponseReceiver<IngressResult>),
    Error(Option<Status>),
}

impl Future for HandlerResponseFut {
    type Output = IngressResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        return match self.as_mut().project() {
            HandlerResponseFutProj::WaitingResponse(response_rx) => {
                Poll::Ready(match ready!(response_rx.poll(cx)) {
                    Ok(Ok(response_payload)) => {
                        trace!(rpc.response = ?response_payload, "Complete external gRPC request successfully");

                        Ok(response_payload)
                    }
                    Ok(Err(status)) => {
                        info!(
                                rpc.grpc.status_code = ?status.code(),
                                rpc.grpc.status_message = ?status.message(),
                                "Complete external gRPC request with a failure");

                        Err(status)
                    }
                    Err(_) => {
                        warn!("Response channel was closed");
                        return Poll::Ready(Err(Status::unavailable("Unavailable")));
                    }
                })
            }
            HandlerResponseFutProj::Error(err) => {
                Poll::Ready(Err(err.take().expect("Future should not be polled twice")))
            }
        };
    }
}

fn create_parent_span_context(request_span: &SpanContext) -> SpanContext {
    if request_span.is_valid() {
        request_span.clone()
    } else {
        tracing::Span::current()
            .context()
            .span()
            .span_context()
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::wait_sender_quota_svc::WaitSenderPermitService;
    use bytes::Bytes;
    use mockall::mock;
    use test_utils::{assert_eq, let_assert, test};
    use tokio::sync::mpsc;
    use tonic::Code;
    use tower::ServiceExt;

    #[test(tokio::test)]
    async fn cannot_create_service_invocation() {
        let req = (
            IngressRequestHeaders {
                service_name: "Unknown".to_string(),
                method_name: "Unknown".to_string(),
                tracing_context: Default::default(),
            },
            Bytes::from_static(&[0, 0, 0, 0, 0]),
        );

        let mut failing_service_invocation_factory = MockMockServiceInvocationFactory::new();
        failing_service_invocation_factory
            .expect_create()
            .once()
            .return_once(|_, _, _, _, _| Err(Status::not_found("Not found")));

        let (function_invocation, handle_result) =
            test_request(req, failing_service_invocation_factory, None).await;

        assert!(function_invocation.is_none());

        let status = handle_result.unwrap_err();
        assert_eq!(status.code(), Code::NotFound);
    }

    #[test(tokio::test)]
    async fn successful_invocation() {
        let req_payload = Bytes::from_static(&[1, 2, 3, 4]);
        let res_payload = Bytes::from_static(&[5, 6, 7, 8]);
        let req = (
            IngressRequestHeaders {
                service_name: "MySvc".to_string(),
                method_name: "MyMethod".to_string(),
                tracing_context: Default::default(),
            },
            req_payload.clone(),
        );

        let mut service_invocation_factory = MockMockServiceInvocationFactory::new();
        service_invocation_factory.expect_create().once().returning(
            |service_name: &str,
             method_name: &str,
             request_payload: Bytes,
             response_sink: ServiceInvocationResponseSink,
             span_relation: SpanRelation| {
                Ok(ServiceInvocation {
                    id: ServiceInvocationId::new(service_name, Bytes::new(), uuid::Uuid::now_v7()),
                    method_name: method_name.to_string().into(),
                    argument: request_payload,
                    response_sink,
                    span_relation,
                })
            },
        );

        let (function_invocation, handle_result) = test_request(
            req,
            service_invocation_factory,
            Some(Ok(res_payload.clone())),
        )
        .await;
        let_assert!(Some(ServiceInvocation { argument, .. }) = function_invocation);
        assert_eq!(argument, req_payload);
        assert_eq!(handle_result.unwrap(), res_payload);
    }

    // --- Mocking code

    mock! {
        MockServiceInvocationFactory {}
        impl ServiceInvocationFactory for MockServiceInvocationFactory {
            fn create(
                &self,
                service_name: &str,
                method_name: &str,
                request_payload: Bytes,
                response_sink: ServiceInvocationResponseSink,
                span_relation: SpanRelation,
            ) -> Result<ServiceInvocation, tonic::Status>;
        }
    }

    async fn test_request(
        req: IngressRequest,
        service_invocation_factory: MockMockServiceInvocationFactory,
        response_to_inject: Option<IngressResult>,
    ) -> (Option<ServiceInvocation>, IngressResult) {
        let (service_invocation_tx, mut service_invocation_rx) = mpsc::channel(1);
        let (registration_command_tx, registration_command_rx) = mpsc::unbounded_channel();

        let handler = WaitSenderPermitService::new(
            service_invocation_tx,
            || Status::unavailable("Unavailable"),
            RequestResponseHandler::new(
                IngressId("127.0.0.1:8080".parse().unwrap()),
                service_invocation_factory,
                registration_command_tx,
            ),
        );
        let handler_handle = tokio::spawn(async move { handler.oneshot(req).await });

        let mut service_invocation = None;
        if let Some(res) = response_to_inject {
            Command::inject_response(registration_command_rx, |_| res).await;
            service_invocation = service_invocation_rx.recv().await
        }

        (service_invocation, handler_handle.await.unwrap())
    }
}
