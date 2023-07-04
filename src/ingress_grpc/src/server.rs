use super::options::JsonOptions;
use super::*;

use codederror::CodedError;
use futures::FutureExt;
use restate_schema_api::json::JsonMapperResolver;
use restate_schema_api::proto_symbol::ProtoSymbolResolver;
use restate_types::identifiers::IngressId;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::Semaphore;
use tower::make::Shared;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::info;

pub type StartSignal = oneshot::Receiver<SocketAddr>;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum IngressServerError {
    #[error(
        "failed binding to address '{address}' specified in 'worker.ingress_grpc.bind_address'"
    )]
    #[code(restate_errors::RT0004)]
    Binding {
        address: SocketAddr,
        #[source]
        source: hyper::Error,
    },
    #[error("error while running ingress grpc server: {0}")]
    #[code(unknown)]
    Running(#[from] hyper::Error),
}

pub struct HyperServerIngress<Schemas, InvocationFactory> {
    listening_addr: SocketAddr,
    concurrency_limit: usize,

    // Parameters to build the layers
    ingress_id: IngressId,
    json: JsonOptions,
    schemas: Schemas,
    invocation_factory: InvocationFactory,
    dispatcher_command_sender: DispatcherCommandSender,

    // Signals
    start_signal_tx: oneshot::Sender<SocketAddr>,
}

impl<Schemas, JsonDecoder, JsonEncoder, InvocationFactory>
    HyperServerIngress<Schemas, InvocationFactory>
where
    Schemas: JsonMapperResolver<JsonToProtobufMapper = JsonDecoder, ProtobufToJsonMapper = JsonEncoder>
        + ProtoSymbolResolver
        + Clone
        + Send
        + Sync
        + 'static,
    JsonDecoder: Send,
    JsonEncoder: Send,
    InvocationFactory: ServiceInvocationFactory + Clone + Send + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        listening_addr: SocketAddr,
        concurrency_limit: usize,
        json: JsonOptions,
        ingress_id: IngressId,
        schemas: Schemas,
        invocation_factory: InvocationFactory,
        dispatcher_command_sender: DispatcherCommandSender,
    ) -> (Self, StartSignal) {
        let (start_signal_tx, start_signal_rx) = oneshot::channel();

        let ingress = Self {
            listening_addr,
            concurrency_limit,
            json,
            ingress_id,
            schemas,
            invocation_factory,
            dispatcher_command_sender,
            start_signal_tx,
        };

        (ingress, start_signal_rx)
    }

    pub async fn run(self, drain: drain::Watch) -> Result<(), IngressServerError> {
        let HyperServerIngress {
            listening_addr,
            concurrency_limit,
            ingress_id,
            json,
            schemas,
            invocation_factory,
            dispatcher_command_sender,
            start_signal_tx,
        } = self;

        let server_builder = hyper::Server::try_bind(&listening_addr).map_err(|err| {
            IngressServerError::Binding {
                address: listening_addr,
                source: err,
            }
        })?;

        let global_concurrency_limit_semaphore = Arc::new(Semaphore::new(concurrency_limit));

        let make_svc = Shared::new(
            ServiceBuilder::new()
                .layer(CorsLayer::very_permissive())
                .service(handler::Handler::new(
                    ingress_id,
                    json,
                    invocation_factory,
                    schemas.clone(),
                    schemas,
                    dispatcher_command_sender,
                    global_concurrency_limit_semaphore,
                )),
        );

        let server = server_builder.serve(make_svc);

        info!(
            net.host.addr = %server.local_addr().ip(),
            net.host.port = %server.local_addr().port(),
            "Ingress gRPC/gRPC-web/Connect listening"
        );

        // future completion does not affect endpoint
        let _ = start_signal_tx.send(server.local_addr());

        server
            .with_graceful_shutdown(drain.signaled().map(|_| ()))
            .await
            .map_err(IngressServerError::Running)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    use bytes::Bytes;
    use drain::Signal;
    use http::header::CONTENT_TYPE;
    use http::StatusCode;
    use hyper::Body;
    use prost::Message;
    use restate_types::identifiers::ServiceInvocationId;
    use restate_types::invocation::ServiceInvocationResponseSink;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    use restate_test_util::{assert_eq, test};

    use crate::mocks::*;

    // Could be shipped by the restate_types crate with feature "mocks" enabled
    #[derive(Clone)]
    struct MockServiceInvocationFactory;

    impl ServiceInvocationFactory for MockServiceInvocationFactory {
        fn create(
            &self,
            service_name: &str,
            method_name: &str,
            request_payload: Bytes,
            response_sink: Option<ServiceInvocationResponseSink>,
            related_span: SpanRelation,
        ) -> Result<(ServiceInvocation, Span), ServiceInvocationFactoryError> {
            Ok(ServiceInvocation::new(
                ServiceInvocationId::new(service_name, Bytes::new(), uuid::Uuid::now_v7()),
                method_name.to_string().into(),
                request_payload,
                response_sink,
                related_span,
            ))
        }
    }

    #[test(tokio::test)]
    async fn test_http_connect_call() {
        let (drain, address, cmd_fut, ingress_handle) = bootstrap_test().await;
        let cmd_fut = tokio::spawn(async move {
            let (service_invocation, response_tx) = cmd_fut.await.unwrap().unwrap().into_inner();
            response_tx
                .send(Ok(mocks::pb::GreetingResponse {
                    greeting: "Igal".to_string(),
                }
                .encode_to_vec()
                .into()))
                .unwrap();
            service_invocation
        });

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
        let mut service_invocation = cmd_fut.await.unwrap();
        assert_eq!(
            service_invocation.id.service_id.service_name,
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.method_name, "Greet");
        let greeting_req =
            mocks::pb::GreetingRequest::decode(&mut service_invocation.argument).unwrap();
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
        ingress_handle.await.unwrap().unwrap();
    }

    #[test(tokio::test)]
    async fn test_ingress_service_http_connect_call() {
        let (drain, address, cmd_fut, ingress_handle) = bootstrap_test().await;
        let cmd_fut = tokio::spawn(async move {
            let (service_invocation, response_tx) = cmd_fut.await.unwrap().unwrap().into_inner();
            assert!(response_tx.is_closed());
            service_invocation
        });

        // Send the request
        let json_payload = json!({
            "service": "greeter.Greeter",
            "method": "Greet",
            "argument": {
                "person": "Francesco"
            }
        });
        let http_response = hyper::Client::new()
            .request(
                hyper::Request::post(format!("http://{address}/dev.restate.Ingress/Invoke"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&json_payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(http_response.status(), StatusCode::OK);

        // Get the function invocation and assert on it
        let mut service_invocation = cmd_fut.await.unwrap();
        assert_eq!(
            service_invocation.id.service_id.service_name,
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.method_name, "Greet");
        let greeting_req =
            mocks::pb::GreetingRequest::decode(&mut service_invocation.argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");
        assert!(service_invocation.response_sink.is_none());

        // Read the http_response_future
        let (_, response_body) = http_response.into_parts();
        let response_bytes = hyper::body::to_bytes(response_body).await.unwrap();
        let response_json_value: serde_json::Value =
            serde_json::from_slice(&response_bytes).unwrap();
        let sid: ServiceInvocationId = response_json_value
            .get("sid")
            .unwrap()
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(sid.service_id.service_name, "greeter.Greeter");

        drain.drain().await;
        ingress_handle.await.unwrap().unwrap();
    }

    #[test(tokio::test)]
    async fn test_grpc_call() {
        let expected_greeting_response = mocks::pb::GreetingResponse {
            greeting: "Igal".to_string(),
        };
        let encoded_greeting_response = Bytes::from(expected_greeting_response.encode_to_vec());

        let (drain, address, cmd_fut, ingress_handle) = bootstrap_test().await;
        let cmd_fut = tokio::spawn(async move {
            let (service_invocation, response_tx) = cmd_fut.await.unwrap().unwrap().into_inner();
            response_tx.send(Ok(encoded_greeting_response)).unwrap();
            service_invocation
        });

        let mut client =
            mocks::pb::greeter_client::GreeterClient::connect(format!("http://{address}"))
                .await
                .unwrap();

        let response = client
            .greet(mocks::pb::GreetingRequest {
                person: "Francesco".to_string(),
            })
            .await
            .unwrap();

        let mut service_invocation = cmd_fut.await.unwrap();
        assert_eq!(
            service_invocation.id.service_id.service_name,
            "greeter.Greeter"
        );
        assert_eq!(service_invocation.method_name, "Greet");
        let greeting_req =
            mocks::pb::GreetingRequest::decode(&mut service_invocation.argument).unwrap();
        assert_eq!(&greeting_req.person, "Francesco");

        // Read the http_response_future
        let greeting_res = response.into_inner();
        assert_eq!(greeting_res, expected_greeting_response);

        drain.drain().await;
        ingress_handle.await.unwrap().unwrap();
    }

    async fn bootstrap_test() -> (
        Signal,
        SocketAddr,
        JoinHandle<Option<Command<ServiceInvocation, IngressResult>>>,
        JoinHandle<Result<(), IngressServerError>>,
    ) {
        let (drain, watch) = drain::channel();
        let (dispatcher_command_tx, mut dispatcher_command_rx) = mpsc::unbounded_channel();

        // Create the ingress and start it
        let (ingress, start_signal) = HyperServerIngress::new(
            "0.0.0.0:0".parse().unwrap(),
            Semaphore::MAX_PERMITS,
            JsonOptions::default(),
            IngressId("0.0.0.0:0".parse().unwrap()),
            test_schemas(),
            MockServiceInvocationFactory,
            dispatcher_command_tx,
        );
        let ingress_handle = tokio::spawn(ingress.run(watch));

        // Mock the service invocation receiver
        let cmd_fut = tokio::spawn(async move { dispatcher_command_rx.recv().await });

        // Wait server to start
        let address = start_signal.await.unwrap();

        (drain, address, cmd_fut, ingress_handle)
    }
}
