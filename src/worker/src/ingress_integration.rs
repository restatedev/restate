use crate::service_invocation_factory::DefaultServiceInvocationFactory;
use ingress_grpc::{
    HyperServerIngress, InMemoryMethodDescriptorRegistry, IngressDispatcherLoop, IngressOutput,
};
use service_key_extractor::KeyExtractorsRegistry;
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
