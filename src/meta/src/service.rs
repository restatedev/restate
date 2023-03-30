use std::collections::HashMap;

use common::retry_policy::RetryPolicy;
use futures_util::command::{Command, UnboundedCommandReceiver, UnboundedCommandSender};
use hyper::http::{HeaderName, HeaderValue};
use hyper::Uri;
use service_key_extractor::KeyExtractorsRegistry;
use service_metadata::{
    DeliveryOptions, EndpointMetadata, InMemoryMethodDescriptorRegistry,
    InMemoryServiceEndpointRegistry,
};
use service_protocol::discovery::{ServiceDiscovery, ServiceDiscoveryError};
use tokio::sync::mpsc;
use tracing::{debug, info};

use super::storage::{MetaStorage, MetaStorageError};

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error(transparent)]
    Discovery(#[from] ServiceDiscoveryError),
    #[error(transparent)]
    Storage(#[from] MetaStorageError),
    #[error("meta closed")]
    MetaClosed,
}

#[derive(Clone)]
pub struct MetaHandle(UnboundedCommandSender<MetaHandleRequest, MetaHandleResponse>);

enum MetaHandleRequest {
    DiscoverEndpoint {
        uri: Uri,
        additional_headers: HashMap<HeaderName, HeaderValue>,
    },
}

enum MetaHandleResponse {
    DiscoverEndpoint(Result<Vec<String>, MetaError>),
}

impl MetaHandle {
    pub async fn register(
        &self,
        uri: Uri,
        additional_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<Vec<String>, MetaError> {
        let (cmd, response_tx) = Command::prepare(MetaHandleRequest::DiscoverEndpoint {
            uri,
            additional_headers,
        });
        self.0.send(cmd).map_err(|_e| MetaError::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::DiscoverEndpoint(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| MetaError::MetaClosed)?
    }
}

// -- Service implementation

pub struct MetaService<Storage> {
    key_extractors_registry: KeyExtractorsRegistry,
    method_descriptors_registry: InMemoryMethodDescriptorRegistry,
    service_endpoint_registry: InMemoryServiceEndpointRegistry,
    service_discovery: ServiceDiscovery,

    storage: Storage,

    handle: MetaHandle,
    api_cmd_rx: UnboundedCommandReceiver<MetaHandleRequest, MetaHandleResponse>,
}

impl<Storage> MetaService<Storage>
where
    Storage: MetaStorage,
{
    pub fn new(
        key_extractors_registry: KeyExtractorsRegistry,
        method_descriptors_registry: InMemoryMethodDescriptorRegistry,
        service_endpoint_registry: InMemoryServiceEndpointRegistry,
        storage: Storage,
        service_discovery_retry_policy: RetryPolicy,
    ) -> Self {
        let (api_cmd_tx, api_cmd_rx) = mpsc::unbounded_channel();

        Self {
            key_extractors_registry,
            method_descriptors_registry,
            service_endpoint_registry,
            service_discovery: ServiceDiscovery::new(service_discovery_retry_policy),
            storage,
            handle: MetaHandle(api_cmd_tx),
            api_cmd_rx,
        }
    }

    pub fn meta_handle(&self) -> MetaHandle {
        self.handle.clone()
    }

    pub async fn run(mut self, drain: drain::Watch) {
        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                cmd = self.api_cmd_rx.recv() => {
                    let (req, replier) = cmd.expect("This channel should never be closed").into_inner();

                    // If error, the client went away, so it's fine to ignore it
                    let _ = replier.send(match req {
                        MetaHandleRequest::DiscoverEndpoint { uri, additional_headers } => MetaHandleResponse::DiscoverEndpoint(
                            self.discover_endpoint(uri, additional_headers).await
                        )
                    });
                },
                _ = shutdown.as_mut() => {
                    debug!("Shutdown meta");
                    return;
                },
            }
        }
    }

    async fn discover_endpoint(
        &mut self,
        uri: Uri,
        additional_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<Vec<String>, MetaError> {
        debug!("Starting discovery of Restate services at service endpoint '{uri}'.");

        let discovered_metadata = self
            .service_discovery
            .discover(&uri, &additional_headers)
            .await?;

        let mut registered_services = Vec::with_capacity(discovered_metadata.services.len());
        for (service, service_instance_type) in discovered_metadata.services {
            let service_descriptor = discovered_metadata
                .descriptor_pool
                .get_service_by_name(&service)
                .expect("service discovery returns a service available in the descriptor pool");
            registered_services.push(service.clone());
            self.storage
                .register_service(
                    service.clone(),
                    service_instance_type.clone(),
                    service_descriptor.clone(),
                )
                .await?;

            info!("Discovered service '{service}' running at '{uri}'.");

            if tracing::enabled!(tracing::Level::DEBUG) {
                service_descriptor
                    .methods()
                    .for_each(|method| debug!("Discovered '{service}/{}'", method.name()));
            }

            self.method_descriptors_registry
                .register(service_descriptor);
            self.key_extractors_registry
                .register(service.clone(), service_instance_type);
            self.service_endpoint_registry.register_service_endpoint(
                service,
                EndpointMetadata::new(
                    uri.clone(),
                    discovered_metadata.protocol_type,
                    DeliveryOptions::new(additional_headers.clone(), None), // TODO needs to support retry policies as well: https://github.com/restatedev/restate/issues/184
                ),
            );
        }

        Ok(registered_services)
    }
}
