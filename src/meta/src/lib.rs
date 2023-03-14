mod rest_api;
mod service;
mod storage;

use crate::storage::InMemoryMetaStorage;
use ingress_grpc::InMemoryMethodDescriptorRegistry;
use rest_api::MetaRestEndpoint;
use service::MetaService;
use service_key_extractor::KeyExtractorsRegistry;
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
}

impl Options {
    pub fn build(self) -> Meta {
        let key_extractors_registry = KeyExtractorsRegistry::default();
        let method_descriptors_registry = InMemoryMethodDescriptorRegistry::default();
        let service = MetaService::new(
            key_extractors_registry.clone(),
            method_descriptors_registry.clone(),
            InMemoryMetaStorage::default(),
            Default::default(),
        );

        Meta {
            key_extractors_registry,
            method_descriptors_registry,
            rest_endpoint: MetaRestEndpoint::new(self.rest_addr),
            service,
        }
    }
}

pub struct Meta {
    key_extractors_registry: KeyExtractorsRegistry,
    method_descriptors_registry: InMemoryMethodDescriptorRegistry,

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

    pub async fn run(self, drain: drain::Watch) {
        let (shutdown_signal, shutdown_watch) = drain::channel();

        let meta_handle = self.service.meta_handle();

        let service_fut = self.service.run(shutdown_watch.clone());
        let rest_endpoint_fut = self.rest_endpoint.run(meta_handle, shutdown_watch);
        tokio::pin!(service_fut, rest_endpoint_fut);

        let shutdown = drain.signaled();

        tokio::select! {
            _ = shutdown => {
                debug!("Initiating shutdown of meta");

                shutdown_signal.drain().await;

                // ignored because we are shutting down
                let _ = join!(service_fut, rest_endpoint_fut);

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
