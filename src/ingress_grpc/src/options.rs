use crate::response_dispatcher::IngressResponseRequester;
use crate::{HyperServerIngress, IngressOutput, MethodDescriptorRegistry};
use common::types::{IngressId, ServiceInvocationFactory};
use std::net::SocketAddr;
use tokio::sync::mpsc;

#[derive(Debug, clap::Parser)]
#[group(skip)]
pub struct Options {
    #[arg(
        long = "external-client-ingress-bind-address",
        env = "EXTERNAL_CLIENT_INGRESS_BIND_ADDRESS",
        default_value = "0.0.0.0:9090"
    )]
    bind_address: SocketAddr,

    #[arg(
        long = "external-client-ingress-concurrency-limit",
        env = "EXTERNAL_CLIENT_INGRESS_CONCURRENCY_LIMIT",
        default_value_t = 1000
    )]
    concurrency_limit: usize,
}

impl Options {
    pub fn build<DescriptorRegistry, InvocationFactory>(
        self,
        ingress_id: IngressId,
        descriptor_registry: DescriptorRegistry,
        invocation_factory: InvocationFactory,
        response_requester: IngressResponseRequester,
        ingress_output_sender: mpsc::Sender<IngressOutput>,
    ) -> HyperServerIngress<DescriptorRegistry, InvocationFactory>
    where
        DescriptorRegistry: MethodDescriptorRegistry + Clone + Send + 'static,
        InvocationFactory: ServiceInvocationFactory + Clone + Send + 'static,
    {
        let Options {
            bind_address,
            concurrency_limit,
        } = self;

        let (hyper_ingress_server, _) = HyperServerIngress::new(
            bind_address,
            concurrency_limit,
            ingress_id,
            descriptor_registry,
            invocation_factory,
            response_requester,
            ingress_output_sender,
        );

        hyper_ingress_server
    }
}
