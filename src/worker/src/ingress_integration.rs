use codederror::CodedError;
use restate_ingress_grpc::{
    HyperServerIngress, IngressDispatcherLoop, IngressDispatcherLoopError, IngressOutput,
};
use restate_schema_impl::Schemas;
use tokio::select;
use tokio::sync::mpsc;

type ExternalClientIngress = HyperServerIngress<Schemas>;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum IngressIntegrationError {
    #[error(transparent)]
    #[code(unknown)]
    DispatcherLoop(#[from] IngressDispatcherLoopError),
    #[error(transparent)]
    Ingress(
        #[from]
        #[code]
        restate_ingress_grpc::IngressServerError,
    ),
}

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
    ) -> Result<(), IngressIntegrationError> {
        let ExternalClientIngressRunner {
            ingress_dispatcher_loop,
            external_client_ingress,
            sender,
        } = self;

        select! {
            result = ingress_dispatcher_loop.run(sender, shutdown_watch.clone()) => result?,
            result = external_client_ingress.run(shutdown_watch) => result?,
        }

        Ok(())
    }
}
