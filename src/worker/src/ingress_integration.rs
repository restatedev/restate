use bytes::Bytes;
use common::types::{
    InvocationId, ServiceInvocation, ServiceInvocationFactory, ServiceInvocationFactoryError,
    ServiceInvocationId, ServiceInvocationResponseSink, SpanRelation,
};
use ingress_grpc::{
    HyperServerIngress, InMemoryMethodDescriptorRegistry, IngressDispatcherLoop, IngressOutput,
};
use service_key_extractor::{KeyExtractor, KeyExtractorsRegistry};
use tokio::select;
use tokio::sync::mpsc;

type ExternalClientIngress = HyperServerIngress<
    InMemoryMethodDescriptorRegistry,
    DefaultServiceInvocationFactory<KeyExtractorsRegistry>,
>;

pub(super) struct ExternalClientIngressRunner {
    ingress_dispatcher_loop: IngressDispatcherLoop,
    external_client_ingress: ExternalClientIngress,
    sender: mpsc::Sender<IngressOutput>,
}

impl ExternalClientIngressRunner {
    pub(super) fn new(
        external_client_ingress: ExternalClientIngress,
        ingress_dispatcher_loop: IngressDispatcherLoop,
        sender: mpsc::Sender<IngressOutput>,
    ) -> Self {
        Self {
            external_client_ingress,
            ingress_dispatcher_loop,
            sender,
        }
    }

    pub(super) async fn run(self, shutdown_watch: drain::Watch) {
        let ExternalClientIngressRunner {
            ingress_dispatcher_loop,
            external_client_ingress,
            sender,
        } = self;

        select! {
            _ = ingress_dispatcher_loop.run(sender, shutdown_watch.clone()) => {},
            _ = external_client_ingress.run(shutdown_watch) => {},
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct DefaultServiceInvocationFactory<K> {
    key_extractor: K,
}

impl<K> DefaultServiceInvocationFactory<K>
where
    K: KeyExtractor + Clone,
{
    pub(super) fn new(key_extractor: K) -> Self {
        Self { key_extractor }
    }

    fn extract_key(
        &self,
        service_name: impl AsRef<str>,
        method_name: impl AsRef<str>,
        request_payload: Bytes,
    ) -> Result<Bytes, ServiceInvocationFactoryError> {
        self.key_extractor
            .extract(service_name.as_ref(), method_name.as_ref(), request_payload)
            .map_err(|err| match err {
                service_key_extractor::Error::NotFound => {
                    ServiceInvocationFactoryError::unknown_service_method(
                        service_name.as_ref(),
                        method_name.as_ref(),
                    )
                }
                err => ServiceInvocationFactoryError::key_extraction_error(err),
            })
    }
}

impl<K> ServiceInvocationFactory for DefaultServiceInvocationFactory<K>
where
    K: KeyExtractor + Clone,
{
    fn create(
        &self,
        service_name: &str,
        method_name: &str,
        request_payload: Bytes,
        response_sink: ServiceInvocationResponseSink,
        span_relation: SpanRelation,
    ) -> Result<ServiceInvocation, ServiceInvocationFactoryError> {
        let key = self.extract_key(service_name, method_name, request_payload.clone())?;

        let invocation_id = InvocationId::now_v7();
        let id = ServiceInvocationId::new(service_name, key, invocation_id);

        let service_invocation = ServiceInvocation {
            id,
            method_name: method_name.into(),
            response_sink,
            argument: request_payload,
            span_relation,
        };

        Ok(service_invocation)
    }
}
