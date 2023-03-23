use crate::service_invocation_factory::DefaultServiceInvocationFactory;
use ingress_grpc::{
    HyperServerIngress, IngressDispatcherLoop, IngressDispatcherLoopError, IngressOutput,
    ReflectionRegistry,
};
use service_metadata::InMemoryMethodDescriptorRegistry;
use tokio::select;
use tokio::sync::mpsc;

type ExternalClientIngress = HyperServerIngress<
    InMemoryMethodDescriptorRegistry,
    DefaultServiceInvocationFactory,
    ReflectionRegistry,
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

    pub(super) async fn run(
        self,
        shutdown_watch: drain::Watch,
    ) -> Result<(), IngressDispatcherLoopError> {
        let ExternalClientIngressRunner {
            ingress_dispatcher_loop,
            external_client_ingress,
            sender,
        } = self;

        select! {
            result = ingress_dispatcher_loop.run(sender, shutdown_watch.clone()) => result?,
            _ = external_client_ingress.run(shutdown_watch) => {},
        }

        Ok(())
    }
}
