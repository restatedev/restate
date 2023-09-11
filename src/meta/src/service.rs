// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::storage::{MetaStorage, MetaStorageError};

use std::collections::HashMap;
use std::future::Future;

use hyper::http::{HeaderName, HeaderValue};
use hyper::Uri;
use restate_errors::warn_it;
use restate_futures_util::command::{Command, UnboundedCommandReceiver, UnboundedCommandSender};
use restate_hyper_util::proxy_connector::Proxy;
use restate_schema_api::endpoint::{DeliveryOptions, EndpointMetadata};
use restate_schema_api::subscription::{Subscription, SubscriptionResolver};
use restate_schema_impl::{
    InsertServiceUpdateCommand, RegistrationError, Schemas, SchemasUpdateCommand,
    ServiceRegistrationRequest,
};
use restate_service_protocol::discovery::{ServiceDiscovery, ServiceDiscoveryError};
use restate_types::identifiers::{EndpointId, ServiceRevision};
use restate_types::retries::RetryPolicy;
use restate_worker_api::SubscriptionController;
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
        force: bool,
    },
    ModifyService {
        service_name: String,
        public: bool,
    },
    RemoveEndpoint {
        endpoint_id: EndpointId,
    },
    CreateSubscription {
        id: Option<String>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
    },
    DeleteSubscription {
        subscription_id: String,
    },
}

pub(crate) struct DiscoverEndpointResponse {
    pub(crate) endpoint: EndpointId,
    pub(crate) services: Vec<(String, ServiceRevision)>,
}

enum MetaHandleResponse {
    DiscoverEndpoint(Result<DiscoverEndpointResponse, MetaError>),
    ModifyService(Result<(), MetaError>),
    RemoveEndpoint(Result<(), MetaError>),
    CreateSubscription(Result<Subscription, MetaError>),
    DeleteSubscription(Result<(), MetaError>),
}

impl MetaHandle {
    pub(crate) async fn register_endpoint(
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

    pub(crate) async fn modify_service(
        &self,
        service_name: String,
        public: bool,
    ) -> Result<(), MetaError> {
        let (cmd, response_tx) = Command::prepare(MetaHandleRequest::ModifyService {
            service_name,
            public,
        });
        self.0.send(cmd).map_err(|_e| MetaError::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::ModifyService(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| MetaError::MetaClosed)?
    }

    pub(crate) async fn remove_endpoint(&self, endpoint_id: EndpointId) -> Result<(), MetaError> {
        let (cmd, response_tx) =
            Command::prepare(MetaHandleRequest::RemoveEndpoint { endpoint_id });
        self.0.send(cmd).map_err(|_e| MetaError::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::RemoveEndpoint(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| MetaError::MetaClosed)?
    }

    pub(crate) async fn create_subscription(
        &self,
        id: Option<String>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<Subscription, MetaError> {
        let (cmd, response_tx) = Command::prepare(MetaHandleRequest::CreateSubscription {
            id,
            source,
            sink,
            metadata,
        });
        self.0.send(cmd).map_err(|_e| MetaError::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::CreateSubscription(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| MetaError::MetaClosed)?
    }

    pub(crate) async fn delete_subscription(
        &self,
        subscription_id: String,
    ) -> Result<(), MetaError> {
        let (cmd, response_tx) =
            Command::prepare(MetaHandleRequest::DeleteSubscription { subscription_id });
        self.0.send(cmd).map_err(|_e| MetaError::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::DeleteSubscription(res) => res,
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
        self.reload_schemas().await
    }

    pub async fn run(
        mut self,
        worker_handle: impl restate_worker_api::Handle + Clone + Send + Sync + 'static,
        drain: drain::Watch,
    ) -> Result<(), MetaError> {
        debug_assert!(
            self.reloaded,
            "The Meta service was not init-ed before running it"
        );

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        // The reason we reload subscriptions here and not in init() is because
        // reload_subscriptions writes to a bounded channel read by the worker.
        // If the worker is not running, this could deadlock when reaching the channel capacity.
        // While here, we're safe to assume the worker is running and will read from that channel.
        self.reload_subscriptions(&worker_handle).await;

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
                        ),
                        MetaHandleRequest::ModifyService { service_name, public } => MetaHandleResponse::ModifyService(
                            self.modify_service(service_name, public).await
                                .map_err(|e| {
                                    warn_it!(e); e
                                })
                        ),
                        MetaHandleRequest::RemoveEndpoint { endpoint_id } => MetaHandleResponse::RemoveEndpoint(
                            self.remove_endpoint(endpoint_id).await
                                .map_err(|e| {
                                    warn_it!(e); e
                                })
                        ),
                        MetaHandleRequest::CreateSubscription { id, source, sink, metadata } => MetaHandleResponse::CreateSubscription(
                            self.create_subscription(id, source, sink, metadata, worker_handle.clone()).await
                                .map_err(|e| {
                                    warn_it!(e); e
                                })
                        ),
                        MetaHandleRequest::DeleteSubscription { subscription_id } => MetaHandleResponse::DeleteSubscription(
                            self.delete_subscription(subscription_id, worker_handle.clone()).await
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

    async fn reload_schemas(&mut self) -> Result<(), MetaError> {
        let update_commands = self.storage.reload().await?;
        self.schemas.apply_updates(update_commands)?;
        self.reloaded = true;
        Ok(())
    }

    async fn reload_subscriptions(
        &mut self,
        worker_handle: &(impl restate_worker_api::Handle + Send + Sync + 'static),
    ) {
        for subscription in self.schemas.list_subscriptions() {
            // If the worker is closing, we can ignore this
            let _ = worker_handle
                .subscription_controller_handle()
                .start_subscription(subscription)
                .await;
        }
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

        // Propagate updates
        self.store_and_apply_updates(schemas_update_commands)
            .await?;

        Ok(discovery_response)
    }

    async fn modify_service(
        &mut self,
        service_name: String,
        public: bool,
    ) -> Result<(), MetaError> {
        debug!(rpc.service = service_name, "Modify service");

        // Compute the diff and propagate updates
        let update_commands = vec![self
            .schemas
            .compute_modify_service_updates(service_name, public)?];
        self.store_and_apply_updates(update_commands).await?;

        Ok(())
    }

    async fn remove_endpoint(&mut self, endpoint_id: EndpointId) -> Result<(), MetaError> {
        debug!(restate.service_endpoint.id = %endpoint_id, "Remove endpoint");

        // Compute the diff and propagate updates
        let update_commands = self.schemas.compute_remove_endpoint(endpoint_id)?;
        self.store_and_apply_updates(update_commands).await?;

        Ok(())
    }

    async fn create_subscription(
        &mut self,
        id: Option<String>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
        worker_handle: impl restate_worker_api::Handle + Clone + Send + Sync + 'static,
    ) -> Result<Subscription, MetaError> {
        info!(restate.subscription.source = %source, restate.subscription.sink = %sink, "Create subscription");

        // Compute the diff and propagate updates
        let (sub, update_command) = self.schemas.compute_add_subscription(
            id,
            source,
            sink,
            metadata,
            worker_handle.subscription_controller_handle(),
        )?;
        self.store_and_apply_updates(vec![update_command]).await?;
        let _ = worker_handle
            .subscription_controller_handle()
            .start_subscription(sub.clone())
            .await;

        Ok(sub)
    }

    async fn delete_subscription(
        &mut self,
        sub_id: String,
        worker_handle: impl restate_worker_api::Handle + Clone + Send + Sync + 'static,
    ) -> Result<(), MetaError> {
        info!(restate.subscription.id = %sub_id, "Delete subscription");

        // Compute the diff and propagate updates
        let update_command = self.schemas.compute_remove_subscription(sub_id.clone())?;
        self.store_and_apply_updates(vec![update_command]).await?;
        let _ = worker_handle
            .subscription_controller_handle()
            .stop_subscription(sub_id)
            .await;

        Ok(())
    }

    async fn store_and_apply_updates(
        &mut self,
        commands: Vec<SchemasUpdateCommand>,
    ) -> Result<(), MetaError> {
        // Store update commands to disk
        self.storage.store(commands.clone()).await?;

        // Propagate updates in memory
        self.schemas.apply_updates(commands)?;

        Ok(())
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
                    services: services
                        .iter()
                        .map(|InsertServiceUpdateCommand { name, revision, .. }| {
                            (name.clone(), *revision)
                        })
                        .collect(),
                };
            }
        }

        panic!("Expecting a SchemasUpdateCommand::InsertEndpoint command. This looks like a bug");
    }
}
