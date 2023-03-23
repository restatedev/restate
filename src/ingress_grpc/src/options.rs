use super::reflection::ServerReflection;
use super::HyperServerIngress;
use super::*;

use common::types::IngressId;
use serde::{Deserialize, Serialize};
use service_metadata::MethodDescriptorRegistry;
use std::net::SocketAddr;

#[derive(Debug, Serialize, Deserialize)]
pub struct Options {
    bind_address: SocketAddr,
    concurrency_limit: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9090".parse().unwrap(),
            concurrency_limit: 1000,
        }
    }
}

impl Options {
    pub fn build<DescriptorRegistry, InvocationFactory, ReflectionService>(
        self,
        ingress_id: IngressId,
        descriptor_registry: DescriptorRegistry,
        invocation_factory: InvocationFactory,
        reflection_service: ReflectionService,
    ) -> (
        IngressDispatcherLoop,
        HyperServerIngress<DescriptorRegistry, InvocationFactory, ReflectionService>,
    )
    where
        DescriptorRegistry: MethodDescriptorRegistry + Clone + Send + 'static,
        InvocationFactory: ServiceInvocationFactory + Clone + Send + 'static,
        ReflectionService: ServerReflection,
    {
        let Options {
            bind_address,
            concurrency_limit,
        } = self;

        let ingress_dispatcher_loop = IngressDispatcherLoop::new(ingress_id);

        let (hyper_ingress_server, _) = HyperServerIngress::new(
            bind_address,
            concurrency_limit,
            ingress_id,
            descriptor_registry,
            invocation_factory,
            reflection_service,
            ingress_dispatcher_loop.create_command_sender(),
        );

        (ingress_dispatcher_loop, hyper_ingress_server)
    }
}
