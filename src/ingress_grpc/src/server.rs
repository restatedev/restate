use super::response_dispatcher::IngressResponseRequester;
use super::*;

use std::net::SocketAddr;
use std::sync::Arc;

use common::types::{IngressId, ServiceInvocation, ServiceInvocationFactory};
use futures::FutureExt;
use tokio::sync::oneshot;
use tokio::sync::{mpsc, Semaphore};
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

pub type StartSignal = oneshot::Receiver<SocketAddr>;

pub struct HyperServerIngress<DescriptorRegistry, InvocationFactory> {
    listening_addr: SocketAddr,
    concurrency_limit: usize,

    // Parameters to build the layers
    ingress_id: IngressId,
    method_descriptor_registry: DescriptorRegistry,
    invocation_factory: InvocationFactory,
    response_requester: IngressResponseRequester,
    service_invocation_sender: mpsc::Sender<ServiceInvocation>,

    // Signals
    start_signal_tx: oneshot::Sender<SocketAddr>,
}

impl<DescriptorRegistry, InvocationFactory>
    HyperServerIngress<DescriptorRegistry, InvocationFactory>
where
    DescriptorRegistry: MethodDescriptorRegistry + Clone + Send + 'static,
    InvocationFactory: ServiceInvocationFactory + Clone + Send + 'static,
{
    pub fn new(
        listening_addr: SocketAddr,
        concurrency_limit: usize,
        ingress_id: IngressId,
        method_descriptor_registry: DescriptorRegistry,
        invocation_factory: InvocationFactory,
        response_requester: IngressResponseRequester,
        service_invocation_sender: mpsc::Sender<ServiceInvocation>,
    ) -> (Self, StartSignal) {
        let (start_signal_tx, start_signal_rx) = oneshot::channel();

        let ingress = Self {
            listening_addr,
            concurrency_limit,
            ingress_id,
            method_descriptor_registry,
            invocation_factory,
            response_requester,
            service_invocation_sender,
            start_signal_tx,
        };

        (ingress, start_signal_rx)
    }

    pub async fn run(self, drain: drain::Watch) {
        let HyperServerIngress {
            listening_addr,
            concurrency_limit,
            ingress_id,
            method_descriptor_registry,
            invocation_factory,
            response_requester,
            service_invocation_sender,
            start_signal_tx,
        } = self;
        info!(address = %listening_addr, "Starting hyper endpoint");

        let server_builder = hyper::Server::bind(&listening_addr);

        let global_concurrency_limit_semaphore = Arc::new(Semaphore::new(concurrency_limit));

        let make_svc = Shared::new(
            ServiceBuilder::new()
                .layer(CorsLayer::very_permissive())
                .service(handler::Handler::new(
                    ingress_id,
                    invocation_factory,
                    method_descriptor_registry,
                    response_requester,
                    service_invocation_sender,
                    global_concurrency_limit_semaphore,
                )),
        );

        let server = server_builder.serve(make_svc);

        // future completion does not affect endpoint
        let _ = start_signal_tx.send(server.local_addr());

        if let Err(e) = server
            .with_graceful_shutdown(drain.signaled().map(|_| ()))
            .await
        {
            warn!("Server is shutting down with error: {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    use bytes::Bytes;
    use common::types::{ServiceInvocationId, ServiceInvocationResponseSink, SpanRelation};
    use drain::Signal;
    use http::header::CONTENT_TYPE;
    use http::StatusCode;
    use hyper::Body;
    use prost::Message;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    use test_utils::{assert_eq, test};

    use crate::mocks::*;

    // Could be shipped by the common crate with feature "mocks" enabled
    #[derive(Clone)]
    struct MockServiceInvocationFactory;

    impl ServiceInvocationFactory for MockServiceInvocationFactory {
        fn create(
            &self,
            service_name: &str,
            method_name: &str,
            request_payload: Bytes,
            response_sink: ServiceInvocationResponseSink,
            span_relation: SpanRelation,
        ) -> Result<ServiceInvocation, Status> {
            Ok(ServiceInvocation {
                id: ServiceInvocationId::new(service_name, Bytes::new(), uuid::Uuid::now_v7()),
                method_name: method_name.to_string().into(),
                argument: request_payload,
                response_sink,
                span_relation,
            })
        }
    }

    #[test(tokio::test)]
    async fn test_http_connect_call() {
        let (drain, address, service_invocation_future, ingress_handle) =
            bootstrap_test(pb::GreetingResponse {
                greeting: "Igal".to_string(),
            })
            .await;

        // Send the request
        let json_payload = json!({"person": "Francesco"});
        let http_response = hyper::Client::new()
            .request(
                hyper::Request::post(format!("http://{address}/greeter.Greeter/Greet"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&json_payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(http_response.status(), StatusCode::OK);

        // Get the function invocation and assert on it
        let mut service_invocation = service_invocation_future.await.unwrap().unwrap();
        assert_eq!(
            service_invocation.id.service_id.service_name,
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.method_name, "Greet");
        let greeting_req = pb::GreetingRequest::decode(&mut service_invocation.argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        // Read the http_response_future
        let (_, response_body) = http_response.into_parts();
        let response_bytes = hyper::body::to_bytes(response_body).await.unwrap();
        let response_json_value: serde_json::Value =
            serde_json::from_slice(&response_bytes).unwrap();
        assert_eq!(
            response_json_value
                .get("greeting")
                .unwrap()
                .as_str()
                .unwrap(),
            "Igal"
        );

        drain.drain().await;
        ingress_handle.await.unwrap();
    }

    #[test(tokio::test)]
    async fn test_grpc_call() {
        let expected_greeting_response = pb::GreetingResponse {
            greeting: "Igal".to_string(),
        };

        let (drain, address, service_invocation_future, ingress_handle) =
            bootstrap_test(expected_greeting_response.clone()).await;

        let mut client = pb::greeter_client::GreeterClient::connect(format!("http://{address}"))
            .await
            .unwrap();

        let response = client
            .greet(pb::GreetingRequest {
                person: "Francesco".to_string(),
            })
            .await
            .unwrap();

        let mut service_invocation = service_invocation_future.await.unwrap().unwrap();
        assert_eq!(
            service_invocation.id.service_id.service_name,
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.method_name, "Greet");
        let greeting_req = pb::GreetingRequest::decode(&mut service_invocation.argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        // Read the http_response_future
        let greeting_res = response.into_inner();
        assert_eq!(greeting_res, expected_greeting_response);

        drain.drain().await;
        ingress_handle.await.unwrap();
    }

    async fn bootstrap_test(
        res: pb::GreetingResponse,
    ) -> (
        Signal,
        SocketAddr,
        JoinHandle<Option<ServiceInvocation>>,
        JoinHandle<()>,
    ) {
        let (drain, watch) = drain::channel();
        let (service_invocation_tx, mut service_invocation_rx) = mpsc::channel(2);
        let (registration_command_tx, mut registration_command_rx) = mpsc::unbounded_channel();

        // Create the ingress and start it
        let (ingress, start_signal) = HyperServerIngress::new(
            "0.0.0.0:0".parse().unwrap(),
            Semaphore::MAX_PERMITS,
            IngressId("0.0.0.0:0".parse().unwrap()),
            test_descriptor_registry(),
            MockServiceInvocationFactory,
            registration_command_tx,
            service_invocation_tx,
        );
        let ingress_handle = tokio::spawn(ingress.run(watch));

        // Mock the response
        let serialized_response = Bytes::from(res.encode_to_vec());
        tokio::spawn(async move {
            registration_command_rx
                .recv()
                .await
                .unwrap()
                .reply(Ok(serialized_response))
        });

        // Mock the service invocation receiver
        let service_invocation_future =
            tokio::spawn(async move { service_invocation_rx.recv().await });

        // Wait server to start
        let address = start_signal.await.unwrap();

        (drain, address, service_invocation_future, ingress_handle)
    }
}
