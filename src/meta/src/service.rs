use super::storage::{MetaStorage, MetaStorageError};

use futures::StreamExt;
use std::collections::HashMap;
use std::future::Future;

use hyper::http::{HeaderName, HeaderValue};
use hyper::Uri;
use prost_reflect::DescriptorPool;
use restate_common::retry_policy::RetryPolicy;
use restate_errors::{error_it, warn_it};
use restate_futures_util::command::{Command, UnboundedCommandReceiver, UnboundedCommandSender};
use restate_ingress_grpc::{ReflectionRegistry, RegistrationError};
use restate_service_key_extractor::KeyExtractorsRegistry;
use restate_service_metadata::{
    DeliveryOptions, EndpointMetadata, InMemoryMethodDescriptorRegistry,
    InMemoryServiceEndpointRegistry, ServiceMetadata,
};
use restate_service_protocol::discovery::{ServiceDiscovery, ServiceDiscoveryError};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum MetaError {
    #[error(transparent)]
    Discovery(
        #[from]
        #[code]
        ServiceDiscoveryError,
    ),
    #[error(transparent)]
    #[code(unknown)]
    Storage(#[from] MetaStorageError),
    #[error(transparent)]
    #[code(unknown)]
    SchemaRegistry(#[from] RegistrationError),
    #[error("meta closed")]
    #[code(unknown)]
    MetaClosed,
    #[error("request aborted because the client went away")]
    #[code(unknown)]
    RequestAborted,
}

#[derive(Clone)]
pub struct MetaHandle(UnboundedCommandSender<MetaHandleRequest, MetaHandleResponse>);

enum MetaHandleRequest {
    DiscoverEndpoint {
        uri: Uri,
        additional_headers: HashMap<HeaderName, HeaderValue>,
        retry_policy: Option<RetryPolicy>,
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
        retry_policy: Option<RetryPolicy>,
    ) -> Result<Vec<String>, MetaError> {
        let (cmd, response_tx) = Command::prepare(MetaHandleRequest::DiscoverEndpoint {
            uri,
            additional_headers,
            retry_policy,
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
    reflections_registry: ReflectionRegistry,
    service_discovery: ServiceDiscovery,

    storage: Storage,

    handle: MetaHandle,
    api_cmd_rx: UnboundedCommandReceiver<MetaHandleRequest, MetaHandleResponse>,

    reloaded: bool,
}

impl<Storage> MetaService<Storage>
where
    Storage: MetaStorage,
{
    pub fn new(
        key_extractors_registry: KeyExtractorsRegistry,
        method_descriptors_registry: InMemoryMethodDescriptorRegistry,
        service_endpoint_registry: InMemoryServiceEndpointRegistry,
        reflections_registry: ReflectionRegistry,
        storage: Storage,
        service_discovery_retry_policy: RetryPolicy,
    ) -> Self {
        let (api_cmd_tx, api_cmd_rx) = mpsc::unbounded_channel();

        Self {
            key_extractors_registry,
            method_descriptors_registry,
            service_endpoint_registry,
            reflections_registry,
            service_discovery: ServiceDiscovery::new(service_discovery_retry_policy),
            storage,
            handle: MetaHandle(api_cmd_tx),
            api_cmd_rx,
            reloaded: false,
        }
    }

    pub fn meta_handle(&self) -> MetaHandle {
        self.handle.clone()
    }

    pub async fn init(&mut self) -> Result<(), MetaError> {
        self.reload().await.map_err(|e| {
            error_it!(e);
            e
        })
    }

    pub async fn run(mut self, drain: drain::Watch) -> Result<(), MetaError> {
        debug_assert!(
            self.reloaded,
            "The Meta service was not init-ed before running it"
        );

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                cmd = self.api_cmd_rx.recv() => {
                    let (req, mut replier) = cmd.expect("This channel should never be closed").into_inner();

                    let res = match req {
                        MetaHandleRequest::DiscoverEndpoint { uri, additional_headers, retry_policy } => MetaHandleResponse::DiscoverEndpoint(
                            self.discover_endpoint(uri, additional_headers, retry_policy, replier.aborted()).await
                                .map_err(|e| {
                                    warn_it!(e); e
                                })
                        )
                    };

                    // If error, the client went away, so it's fine to ignore it
                    let _ = replier.send(res);
                },
                _ = shutdown.as_mut() => {
                    debug!("Shutdown meta");
                    return Ok(());
                },
            }
        }
    }

    async fn reload(&mut self) -> Result<(), MetaError> {
        let mut endpoints_stream = self.storage.reload().await?;

        while let Some(res) = endpoints_stream.next().await {
            let (endpoint_metadata, services, descriptor_pool) = res?;
            self.reflections_registry.register_new_services(
                endpoint_metadata.id(),
                services.iter().map(|s| s.name().to_string()).collect(),
                descriptor_pool.clone(),
            )?;
            for service_meta in services {
                info!(
                    rpc.service = service_meta.name(),
                    restate.service_endpoint.url = %endpoint_metadata.address(),
                    "Reloading service"
                );
                self.propagate_service_registration(
                    service_meta,
                    endpoint_metadata.clone(),
                    &descriptor_pool,
                );
            }
        }

        self.reloaded = true;
        Ok(())
    }

    async fn discover_endpoint(
        &mut self,
        uri: Uri,
        additional_headers: HashMap<HeaderName, HeaderValue>,
        retry_policy: Option<RetryPolicy>,
        abort_signal: impl Future<Output = ()>,
    ) -> Result<Vec<String>, MetaError> {
        debug!(http.url = %uri, "Discovering Service endpoint");

        let discovered_metadata = tokio::select! {
            res = self.service_discovery.discover(&uri, &additional_headers) => res,
            _ = abort_signal => return Err(MetaError::RequestAborted),
        }?;

        // Store the new endpoint in storage
        let endpoint_metadata = EndpointMetadata::new(
            uri.clone(),
            discovered_metadata.protocol_type,
            DeliveryOptions::new(additional_headers, retry_policy),
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
                rpc.service = service_meta.name(),
                restate.service_endpoint.url = %uri,
                "Discovered service"
            );

            self.propagate_service_registration(
                service_meta,
                endpoint_metadata.clone(),
                &discovered_metadata.descriptor_pool,
            );
        }
        self.reflections_registry.register_new_services(
            endpoint_metadata.id(),
            registered_services.clone(),
            discovered_metadata.descriptor_pool,
        )?;

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
                    rpc.service = service_meta.name(),
                    rpc.method = method.name(),
                    "Registering method"
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
