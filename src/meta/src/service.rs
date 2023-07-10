use super::storage::{MetaStorage, MetaStorageError};

use std::collections::HashMap;
use std::future::Future;

use hyper::http::{HeaderName, HeaderValue};
use hyper::Uri;
use restate_errors::warn_it;
use restate_futures_util::command::{Command, UnboundedCommandReceiver, UnboundedCommandSender};
use restate_hyper_util::proxy_connector::Proxy;
use restate_schema_api::endpoint::{DeliveryOptions, EndpointMetadata};
use restate_schema_impl::{
    RegistrationError, Schemas, SchemasUpdateCommand, ServiceRegistrationRequest,
};
use restate_service_protocol::discovery::{ServiceDiscovery, ServiceDiscoveryError};
use restate_types::identifiers::{EndpointId, ServiceRevision};
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
        force: bool,
    },
}

pub(crate) struct DiscoverEndpointResponse {
    pub(crate) endpoint: EndpointId,
    pub(crate) services: Vec<(String, ServiceRevision)>,
}

enum MetaHandleResponse {
    DiscoverEndpoint(Result<DiscoverEndpointResponse, MetaError>),
}

impl MetaHandle {
    pub(crate) async fn register(
        &self,
        uri: Uri,
        additional_headers: HashMap<HeaderName, HeaderValue>,
        force: bool,
    ) -> Result<DiscoverEndpointResponse, MetaError> {
        let (cmd, response_tx) = Command::prepare(MetaHandleRequest::DiscoverEndpoint {
            uri,
            additional_headers,
            force,
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
        self.reload().await
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
                        MetaHandleRequest::DiscoverEndpoint { uri, additional_headers, force } => MetaHandleResponse::DiscoverEndpoint(
                            self.discover_endpoint(uri, additional_headers, force, replier.aborted()).await
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
        let update_commands = self.storage.reload().await?;
        self.schemas.apply_updates(update_commands)?;
        self.reloaded = true;
        Ok(())
    }

    async fn discover_endpoint(
        &mut self,
        uri: Uri,
        additional_headers: HashMap<HeaderName, HeaderValue>,
        force: bool,
        abort_signal: impl Future<Output = ()>,
    ) -> Result<DiscoverEndpointResponse, MetaError> {
        debug!(http.url = %uri, "Discovering Service endpoint");

        let discovered_metadata = tokio::select! {
            res = self.service_discovery.discover(&uri, &additional_headers) => res,
            _ = abort_signal => return Err(MetaError::RequestAborted),
        }?;

        // Compute the diff with the current state of Schemas
        let schemas_update_commands = self.schemas.compute_new_endpoint_updates(
            EndpointMetadata::new(
                uri.clone(),
                discovered_metadata.protocol_type,
                DeliveryOptions::new(additional_headers),
            ),
            discovered_metadata
                .services
                .into_iter()
                .map(|(svc_name, instance_type)| {
                    ServiceRegistrationRequest::new(svc_name, instance_type)
                })
                .collect::<Vec<_>>(),
            discovered_metadata.descriptor_pool,
            force,
        )?;

        // Compute the response
        let discovery_response =
            Self::infer_discovery_response_from_update_commands(&schemas_update_commands);

        // Store update commands to disk
        self.storage.store(schemas_update_commands.clone()).await?;

        // Propagate updates in memory
        self.schemas.apply_updates(schemas_update_commands)?;

        Ok(discovery_response)
    }

    fn infer_discovery_response_from_update_commands(
        commands: &[SchemasUpdateCommand],
    ) -> DiscoverEndpointResponse {
        for schema_update_command in commands {
            if let SchemasUpdateCommand::InsertEndpoint {
                metadata, services, ..
            } = schema_update_command
            {
                return DiscoverEndpointResponse {
                    endpoint: metadata.id(),
                    services: services.clone(),
                };
            }
        }

        panic!("Expecting a SchemasUpdateCommand::InsertEndpoint command. This looks like a bug");
    }
}
