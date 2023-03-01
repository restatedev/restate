use bytes::Bytes;
use common::types::{
    ServiceInvocation, ServiceInvocationFactory, ServiceInvocationFactoryError,
    ServiceInvocationResponseSink, SpanRelation,
};
use ingress_grpc::{HyperServerIngress, InMemoryMethodDescriptorRegistry, ResponseDispatcherLoop};
use tokio::select;

type ExternalClientIngress =
    HyperServerIngress<InMemoryMethodDescriptorRegistry, DefaultServiceInvocationFactory>;

pub(super) struct ExternalClientIngressRunner {
    response_dispatcher_loop: ResponseDispatcherLoop,
    external_client_ingress: ExternalClientIngress,
}

impl ExternalClientIngressRunner {
    pub(super) fn new(
        external_client_ingress: ExternalClientIngress,
        response_dispatcher_loop: ResponseDispatcherLoop,
    ) -> Self {
        Self {
            external_client_ingress,
            response_dispatcher_loop,
        }
    }

    pub(super) async fn run(self, shutdown_watch: drain::Watch) {
        let ExternalClientIngressRunner {
            response_dispatcher_loop,
            external_client_ingress,
        } = self;

        select! {
            _ = response_dispatcher_loop.run(shutdown_watch.clone()) => {},
            _ = external_client_ingress.run(shutdown_watch) => {},
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct DefaultServiceInvocationFactory;

impl ServiceInvocationFactory for DefaultServiceInvocationFactory {
    fn create(
        &self,
        _service_name: &str,
        _method_name: &str,
        _request_payload: Bytes,
        _response_sink: ServiceInvocationResponseSink,
        _span_relation: SpanRelation,
    ) -> Result<ServiceInvocation, ServiceInvocationFactoryError> {
        todo!("https://github.com/restatedev/restate/issues/133")
    }
}
