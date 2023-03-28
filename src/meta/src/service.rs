use super::storage::{MetaStorage, MetaStorageError};

use futures::StreamExt;
use std::collections::HashMap;

use common::retry_policy::RetryPolicy;
use futures_util::command::{Command, UnboundedCommandReceiver, UnboundedCommandSender};
use hyper::http::{HeaderName, HeaderValue};
use hyper::Uri;
use prost_reflect::DescriptorPool;
use service_key_extractor::KeyExtractorsRegistry;
use service_metadata::{
    DeliveryOptions, EndpointMetadata, InMemoryMethodDescriptorRegistry,
    InMemoryServiceEndpointRegistry, ServiceMetadata,
};
use service_protocol::discovery::{ServiceDiscovery, ServiceDiscoveryError};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

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

        if let Err(e) = self.reload().await {
            error!("Cannot start meta: {e}");
            return;
        }

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

    async fn reload(&mut self) -> Result<(), MetaError> {
        let mut endpoints_stream = self.storage.reload().await?;

        while let Some(res) = endpoints_stream.next().await {
            let (endpoint_metadata, services, descriptor_pool) = res?;
            for service_meta in services {
                info!(
                    "Reloading service '{}' running at '{}'.",
                    service_meta.name(),
                    endpoint_metadata.address()
                );
                self.propagate_service_registration(
                    service_meta,
                    endpoint_metadata.clone(),
                    &descriptor_pool,
                );
            }
        }

        Ok(())
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

        // Store the new endpoint in storage
        let endpoint_metadata = EndpointMetadata::new(
            uri.clone(),
            discovered_metadata.protocol_type,
            DeliveryOptions::new(additional_headers, None), // TODO needs to support retry policies as well: https://github.com/restatedev/restate/issues/184
        );
        let services = discovered_metadata
            .services
            .into_iter()
            .map(|(svc_name, instance_type)| ServiceMetadata::new(svc_name, instance_type))
            .collect::<Vec<_>>();
        self.storage
            .register_endpoint(
                &endpoint_metadata,
                &services,
                discovered_metadata.descriptor_pool.clone(),
            )
            .await?;

        // Propagate changes
        let mut registered_services = Vec::with_capacity(services.len());
        for service_meta in services {
            registered_services.push(service_meta.name().to_string());
            info!(
                "Discovered service '{}' running at '{uri}'.",
                service_meta.name()
            );

            self.propagate_service_registration(
                service_meta,
                endpoint_metadata.clone(),
                &discovered_metadata.descriptor_pool,
            );
        }

        Ok(registered_services)
    }

    fn propagate_service_registration(
        &mut self,
        service_meta: ServiceMetadata,
        endpoint_metadata: EndpointMetadata,
        descriptor_pool: &DescriptorPool,
    ) {
        let service_descriptor = descriptor_pool
            .get_service_by_name(service_meta.name())
            .expect("Service mut be available in the descriptor pool. This is a runtime bug.");

        if tracing::enabled!(tracing::Level::DEBUG) {
            service_descriptor.methods().for_each(|method| {
                debug!(
                    "Registering method '{}/{}'",
                    service_meta.name(),
                    method.name()
                )
            });
        }

        self.method_descriptors_registry
            .register(service_descriptor);
        self.key_extractors_registry.register(
            service_meta.name().to_string(),
            service_meta.instance_type().clone(),
        );
        self.service_endpoint_registry
            .register_service_endpoint(service_meta.name().to_string(), endpoint_metadata);
    }
}
