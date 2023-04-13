use super::reflection::ServerReflection;
use super::HyperServerIngress;
use super::*;

use restate_common::types::IngressId;
use restate_service_metadata::MethodDescriptorRegistry;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// # Ingress options
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "IngressOptions"))]
pub struct Options {
    /// # Bind address
    ///
    /// The address to bind for the ingress.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_bind_address")
    )]
    bind_address: SocketAddr,
    /// # Concurrency limit
    ///
    /// Local concurrency limit to use to limit the amount of concurrent requests. If exceeded, the ingress will reply immediately with an appropriate status code.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_concurrency_limit")
    )]
    concurrency_limit: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: Options::default_bind_address(),
            concurrency_limit: Options::default_concurrency_limit(),
        }
    }
}

impl Options {
    fn default_bind_address() -> SocketAddr {
        "0.0.0.0:9090".parse().unwrap()
    }

    fn default_concurrency_limit() -> usize {
        1000
    }

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
