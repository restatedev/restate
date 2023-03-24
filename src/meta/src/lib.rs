mod rest_api;
mod service;
mod storage;

use crate::storage::InMemoryMetaStorage;
use rest_api::MetaRestEndpoint;
use service::MetaService;
use service_key_extractor::KeyExtractorsRegistry;
use service_metadata::{InMemoryMethodDescriptorRegistry, InMemoryServiceEndpointRegistry};
use std::net::SocketAddr;
use tokio::join;
use tracing::debug;

#[derive(Debug, clap::Parser)]
#[group(skip)]
pub struct Options {
    /// Address of the REST endpoint
    #[arg(
        long = "meta-rest-addr",
        env = "META_REST_ADDRESS",
        default_value = "0.0.0.0:8081"
    )]
    rest_addr: SocketAddr,
    /// Concurrency limit for the Meta Operational REST APIs
    #[arg(
        long = "meta-rest-api-concurrency-limit",
        env = "META_REST_API_CONCURRENCY_LIMIT",
        default_value_t = 1000
    )]
    // We cannot name it concurrency_limit because clap won't like it,
    // because otherwise there will be two different concurrency_limit fields in the parent options struct.
    meta_concurrency_limit: usize,
}

impl Options {
    pub fn build(self) -> Meta {
        let key_extractors_registry = KeyExtractorsRegistry::default();
        let method_descriptors_registry = InMemoryMethodDescriptorRegistry::default();
        let service_endpoint_registry = InMemoryServiceEndpointRegistry::default();
        let service = MetaService::new(
            key_extractors_registry.clone(),
            method_descriptors_registry.clone(),
            service_endpoint_registry.clone(),
            InMemoryMetaStorage::default(),
            Default::default(),
        );

        Meta {
            key_extractors_registry,
            method_descriptors_registry,
            service_endpoint_registry,
            rest_endpoint: MetaRestEndpoint::new(self.rest_addr, self.meta_concurrency_limit),
            service,
        }
    }
}

pub struct Meta {
    key_extractors_registry: KeyExtractorsRegistry,
    method_descriptors_registry: InMemoryMethodDescriptorRegistry,
    service_endpoint_registry: InMemoryServiceEndpointRegistry,

    rest_endpoint: MetaRestEndpoint,
    service: MetaService<InMemoryMetaStorage>,
}

impl Meta {
    pub fn key_extractors_registry(&self) -> KeyExtractorsRegistry {
        self.key_extractors_registry.clone()
    }

    pub fn method_descriptor_registry(&self) -> InMemoryMethodDescriptorRegistry {
        self.method_descriptors_registry.clone()
    }

    pub fn service_endpoint_registry(&self) -> InMemoryServiceEndpointRegistry {
        self.service_endpoint_registry.clone()
    }

    pub async fn run(self, drain: drain::Watch) {
        let (shutdown_signal, shutdown_watch) = drain::channel();

        let meta_handle = self.service.meta_handle();
        let service_endpoint_registry = self.service_endpoint_registry();
        let method_descriptor_registry = self.method_descriptor_registry();

        let service_fut = self.service.run(shutdown_watch.clone());
        let rest_endpoint_fut = self.rest_endpoint.run(
            meta_handle,
            service_endpoint_registry,
            method_descriptor_registry,
            shutdown_watch,
        );
        tokio::pin!(service_fut, rest_endpoint_fut);

        let shutdown = drain.signaled();

        tokio::select! {
            _ = shutdown => {
                debug!("Initiating shutdown of meta");

                // ignored because we are shutting down
                let _ = join!(shutdown_signal.drain(), service_fut, rest_endpoint_fut);

                debug!("Completed shutdown of meta");
            },
            _ = &mut rest_endpoint_fut => {
                panic!("Rest endpoint stopped running");
            },
            _ = &mut service_fut => {
                panic!("Service stopped running");
            },
        }
    }
}
