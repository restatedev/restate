use super::storage::{MetaStorage, MetaStorageError};

use futures::StreamExt;
use std::collections::HashMap;
use std::future::Future;

use hyper::http::{HeaderName, HeaderValue};
use hyper::Uri;
use restate_errors::{error_it, warn_it};
use restate_futures_util::command::{Command, UnboundedCommandReceiver, UnboundedCommandSender};
use restate_hyper_util::proxy_connector::Proxy;
use restate_schema_api::endpoint::{DeliveryOptions, EndpointMetadata};
use restate_schema_impl::{RegistrationError, Schemas, ServiceRegistrationRequest};
use restate_service_protocol::discovery::{ServiceDiscovery, ServiceDiscoveryError};
use restate_types::retries::RetryPolicy;
use tokio::sync::mpsc;
use tracing::{debug, error};

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
    schemas: Schemas,

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
        schemas: Schemas,
        storage: Storage,
        service_discovery_retry_policy: RetryPolicy,
        proxy: Option<Proxy>,
    ) -> Self {
        let (api_cmd_tx, api_cmd_rx) = mpsc::unbounded_channel();

        Self {
            schemas,
            service_discovery: ServiceDiscovery::new(service_discovery_retry_policy, proxy),
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
            self.schemas
                .register_new_endpoint(endpoint_metadata, services, descriptor_pool)?;
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
            .map(|(svc_name, instance_type)| {
                ServiceRegistrationRequest::new(svc_name, instance_type)
            })
            .collect::<Vec<_>>();
        self.storage
            .register_endpoint(
                &endpoint_metadata,
                &services,
                discovered_metadata.descriptor_pool.clone(),
            )
            .await?;

        let registered_services = services.iter().map(|sm| sm.name().to_string()).collect();

        // Propagate changes
        self.schemas.register_new_endpoint(
            endpoint_metadata,
            services,
            discovered_metadata.descriptor_pool,
        )?;

        Ok(registered_services)
    }
}
