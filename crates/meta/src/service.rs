// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::error::Error;
use super::storage::MetaStorage;

use std::collections::HashMap;
use std::future::Future;

use http::Uri;
use tokio::sync::mpsc;
use tracing::{debug, info};

use restate_core::cancellation_watcher;
use restate_errors::warn_it;
use restate_futures_util::command::{Command, UnboundedCommandReceiver, UnboundedCommandSender};
use restate_schema_api::component::ComponentMetadata;
use restate_schema_api::deployment::{DeliveryOptions, DeploymentMetadata};
use restate_schema_api::service::ServiceMetadata;
use restate_schema_api::subscription::{Subscription, SubscriptionValidator};
use restate_schema_impl::{Schemas, SchemasUpdateCommand};
use restate_service_protocol::old_discovery::{DiscoverEndpoint, ServiceDiscovery};
use restate_types::identifiers::{DeploymentId, SubscriptionId};
use restate_types::retries::RetryPolicy;

use restate_service_client::{Endpoint, ServiceClient};
use restate_service_protocol::discovery;
use restate_service_protocol::discovery::ComponentDiscovery;

#[derive(Debug, Clone)]
pub struct MetaHandle(UnboundedCommandSender<MetaHandleRequest, MetaHandleResponse>);

/// Whether to force the registration of an existing endpoint or not
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Force {
    Yes,
    No,
}

impl Force {
    pub fn force_enabled(&self) -> bool {
        *self == Self::Yes
    }
}

/// Whether to apply the changes or not
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub enum ApplyMode {
    DryRun,
    #[default]
    Apply,
}

impl ApplyMode {
    pub fn should_apply(&self) -> bool {
        *self == Self::Apply
    }
}

enum MetaHandleRequest {
    OldDiscoverDeployment {
        deployment_endpoint: DiscoverEndpoint,
        force: Force,
        apply_changes: ApplyMode,
    },
    DiscoverDeployment {
        deployment_endpoint: discovery::DiscoverEndpoint,
        force: Force,
        apply_changes: ApplyMode,
    },
    ModifyService {
        service_name: String,
        public: bool,
    },
    RemoveDeployment {
        deployment_id: DeploymentId,
    },
    CreateSubscription {
        id: Option<SubscriptionId>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
    },
    DeleteSubscription {
        subscription_id: SubscriptionId,
    },
}

pub struct OldDiscoverDeploymentResponse {
    pub deployment: DeploymentId,
    pub services: Vec<ServiceMetadata>,
}

pub struct DiscoverDeploymentResponse {
    pub deployment: DeploymentId,
    pub services: Vec<ComponentMetadata>,
}

enum MetaHandleResponse {
    OldDiscoverDeployment(Result<OldDiscoverDeploymentResponse, Error>),
    DiscoverDeployment(Result<DiscoverDeploymentResponse, Error>),
    ModifyService(Result<(), Error>),
    RemoveDeployment(Result<(), Error>),
    CreateSubscription(Result<Subscription, Error>),
    DeleteSubscription(Result<(), Error>),
}

impl MetaHandle {
    pub async fn old_register_deployment(
        &self,
        deployment_endpoint: DiscoverEndpoint,
        force: Force,
        apply_changes: ApplyMode,
    ) -> Result<OldDiscoverDeploymentResponse, Error> {
        let (cmd, response_tx) = Command::prepare(MetaHandleRequest::OldDiscoverDeployment {
            deployment_endpoint,
            force,
            apply_changes,
        });
        self.0.send(cmd).map_err(|_e| Error::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::OldDiscoverDeployment(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| Error::MetaClosed)?
    }

    pub async fn register_deployment(
        &self,
        deployment_endpoint: discovery::DiscoverEndpoint,
        force: Force,
        apply_changes: ApplyMode,
    ) -> Result<DiscoverDeploymentResponse, Error> {
        let (cmd, response_tx) = Command::prepare(MetaHandleRequest::DiscoverDeployment {
            deployment_endpoint,
            force,
            apply_changes,
        });
        self.0.send(cmd).map_err(|_e| Error::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::DiscoverDeployment(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| Error::MetaClosed)?
    }

    pub async fn modify_service(&self, service_name: String, public: bool) -> Result<(), Error> {
        let (cmd, response_tx) = Command::prepare(MetaHandleRequest::ModifyService {
            service_name,
            public,
        });
        self.0.send(cmd).map_err(|_e| Error::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::ModifyService(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| Error::MetaClosed)?
    }

    pub async fn remove_deployment(&self, deployment_id: DeploymentId) -> Result<(), Error> {
        let (cmd, response_tx) =
            Command::prepare(MetaHandleRequest::RemoveDeployment { deployment_id });
        self.0.send(cmd).map_err(|_e| Error::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::RemoveDeployment(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| Error::MetaClosed)?
    }

    pub async fn create_subscription(
        &self,
        id: Option<SubscriptionId>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<Subscription, Error> {
        let (cmd, response_tx) = Command::prepare(MetaHandleRequest::CreateSubscription {
            id,
            source,
            sink,
            metadata,
        });
        self.0.send(cmd).map_err(|_e| Error::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::CreateSubscription(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| Error::MetaClosed)?
    }

    pub async fn delete_subscription(&self, subscription_id: SubscriptionId) -> Result<(), Error> {
        let (cmd, response_tx) =
            Command::prepare(MetaHandleRequest::DeleteSubscription { subscription_id });
        self.0.send(cmd).map_err(|_e| Error::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::DeleteSubscription(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| Error::MetaClosed)?
    }
}

// -- Service implementation

#[derive(Debug)]
pub struct MetaService<Storage, SV> {
    schemas: Schemas,

    old_service_discovery: ServiceDiscovery,
    component_discovery: ComponentDiscovery,

    storage: Storage,
    subscription_validator: SV,

    handle: MetaHandle,
    api_cmd_rx: UnboundedCommandReceiver<MetaHandleRequest, MetaHandleResponse>,

    reloaded: bool,
}

impl<Storage, SV> MetaService<Storage, SV>
where
    Storage: MetaStorage,
    SV: SubscriptionValidator,
{
    pub fn new(
        schemas: Schemas,
        storage: Storage,
        subscription_validator: SV,
        service_discovery_retry_policy: RetryPolicy,
        client: ServiceClient,
    ) -> Self {
        let (api_cmd_tx, api_cmd_rx) = mpsc::unbounded_channel();

        Self {
            schemas,
            old_service_discovery: ServiceDiscovery::new(
                service_discovery_retry_policy.clone(),
                client.clone(),
            ),
            component_discovery: ComponentDiscovery::new(service_discovery_retry_policy, client),
            storage,
            subscription_validator,
            handle: MetaHandle(api_cmd_tx),
            api_cmd_rx,
            reloaded: false,
        }
    }

    pub fn schemas(&self) -> Schemas {
        self.schemas.clone()
    }

    pub fn meta_handle(&self) -> MetaHandle {
        self.handle.clone()
    }

    pub fn schema_reader(&self) -> Storage::Reader {
        self.storage.create_reader()
    }

    pub async fn init(&mut self) -> Result<(), Error> {
        self.reload_schemas().await
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        debug_assert!(
            self.reloaded,
            "The Meta service was not init-ed before running it"
        );

        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                cmd = self.api_cmd_rx.recv() => {
                    let (req, mut replier) = cmd.expect("This channel should never be closed").into_inner();

                    let res = match req {
                        MetaHandleRequest::OldDiscoverDeployment { deployment_endpoint, force, apply_changes } => MetaHandleResponse::OldDiscoverDeployment(
                            self.old_discover_deployment(deployment_endpoint, force, apply_changes, replier.aborted()).await
                                .map_err(|e| {
                                    warn_it!(e); e
                                })
                        ),
                        MetaHandleRequest::DiscoverDeployment { deployment_endpoint, force, apply_changes } => MetaHandleResponse::DiscoverDeployment(
                            self.discover_deployment(deployment_endpoint, force, apply_changes, replier.aborted()).await
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
                        MetaHandleRequest::RemoveDeployment { deployment_id } => MetaHandleResponse::RemoveDeployment(
                            self.remove_deployment(deployment_id).await
                                .map_err(|e| {
                                    warn_it!(e); e
                                })
                        ),
                        MetaHandleRequest::CreateSubscription { id, source, sink, metadata } => MetaHandleResponse::CreateSubscription(
                            self.create_subscription(id, source, sink, metadata).await
                                .map_err(|e| {
                                    warn_it!(e); e
                                })
                        ),
                        MetaHandleRequest::DeleteSubscription { subscription_id } => MetaHandleResponse::DeleteSubscription(
                            self.delete_subscription(subscription_id).await
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

    async fn reload_schemas(&mut self) -> Result<(), Error> {
        let update_commands = self.storage.reload().await?;
        self.schemas.apply_updates(update_commands)?;
        self.reloaded = true;
        Ok(())
    }

    async fn old_discover_deployment(
        &mut self,
        endpoint: DiscoverEndpoint,
        force: Force,
        apply_changes: ApplyMode,
        abort_signal: impl Future<Output = ()>,
    ) -> Result<OldDiscoverDeploymentResponse, Error> {
        debug!(restate.deployment.address = %endpoint.address(), "Discovering deployment");

        let discovered_metadata = tokio::select! {
            res = self.old_service_discovery.discover(&endpoint) => res,
            _ = abort_signal => return Err(Error::RequestAborted),
        }?;

        let deployment_metadata = match endpoint.into_inner() {
            (Endpoint::Http(uri, _), headers) => DeploymentMetadata::new_http(
                uri.clone(),
                discovered_metadata.protocol_type,
                DeliveryOptions::new(headers),
            ),
            (Endpoint::Lambda(arn, assume_role_arn), headers) => {
                DeploymentMetadata::new_lambda(arn, assume_role_arn, DeliveryOptions::new(headers))
            }
        };

        // Compute the diff with the current state of Schemas
        let schemas_update_commands = self.schemas.compute_new_deployment(
            None, /* requested_deployment_id */
            deployment_metadata,
            discovered_metadata.services,
            discovered_metadata.descriptor_pool,
            force.force_enabled(),
        )?;

        // Compute the response
        let discovery_response =
            Self::old_infer_discovery_response_from_update_commands(&schemas_update_commands);

        if apply_changes.should_apply() {
            // Propagate updates
            self.store_and_apply_updates(schemas_update_commands)
                .await?;
        } else {
            debug!("Not applying schemas update commands because of dry-run mode");
        }

        Ok(discovery_response)
    }

    async fn discover_deployment(
        &mut self,
        endpoint: discovery::DiscoverEndpoint,
        force: Force,
        apply_changes: ApplyMode,
        abort_signal: impl Future<Output = ()>,
    ) -> Result<DiscoverDeploymentResponse, Error> {
        debug!(restate.deployment.address = %endpoint.address(), "Discovering deployment");

        let discovered_metadata = tokio::select! {
            res = self.component_discovery.discover(&endpoint) => res,
            _ = abort_signal => return Err(Error::RequestAborted),
        }?;

        let deployment_metadata = match endpoint.into_inner() {
            (Endpoint::Http(uri, _), headers) => DeploymentMetadata::new_http(
                uri.clone(),
                discovered_metadata.protocol_type,
                DeliveryOptions::new(headers),
            ),
            (Endpoint::Lambda(arn, assume_role_arn), headers) => {
                DeploymentMetadata::new_lambda(arn, assume_role_arn, DeliveryOptions::new(headers))
            }
        };

        // TODO
        let schemas_update_commands = vec![];
        // Compute the diff with the current state of Schemas
        // let schemas_update_commands = self.schemas.compute_new_deployment(
        //     None, /* requested_deployment_id */
        //     deployment_metadata,
        //     discovered_metadata.services,
        //     discovered_metadata.descriptor_pool,
        //     force.force_enabled(),
        // )?;

        // Compute the response
        let discovery_response =
            Self::infer_discovery_response_from_update_commands(&schemas_update_commands);

        if apply_changes.should_apply() {
            // Propagate updates
            self.store_and_apply_updates(schemas_update_commands)
                .await?;
        } else {
            debug!("Not applying schemas update commands because of dry-run mode");
        }

        Ok(discovery_response)
    }

    async fn modify_service(&mut self, service_name: String, public: bool) -> Result<(), Error> {
        debug!(rpc.service = service_name, "Modify service");

        // Compute the diff and propagate updates
        let update_commands = vec![self.schemas.compute_modify_service(service_name, public)?];
        self.store_and_apply_updates(update_commands).await?;

        Ok(())
    }

    async fn remove_deployment(&mut self, deployment_id: DeploymentId) -> Result<(), Error> {
        debug!(restate.deployment.id = %deployment_id, "Remove deployment");

        // Compute the diff and propagate updates
        let update_commands = self.schemas.compute_remove_deployment(deployment_id)?;
        self.store_and_apply_updates(update_commands).await?;

        Ok(())
    }

    async fn create_subscription(
        &mut self,
        id: Option<SubscriptionId>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<Subscription, Error> {
        info!(restate.subscription.source = %source, restate.subscription.sink = %sink, "Create subscription");

        // Compute the diff and propagate updates
        let (sub, update_command) = self.schemas.compute_add_subscription(
            id,
            source,
            sink,
            metadata,
            &self.subscription_validator,
        )?;
        self.store_and_apply_updates(vec![update_command]).await?;

        Ok(sub)
    }

    async fn delete_subscription(&mut self, id: SubscriptionId) -> Result<(), Error> {
        info!(restate.subscription.id = %id, "Delete subscription");

        // Compute the diff and propagate updates
        let update_command = self.schemas.compute_remove_subscription(id)?;
        self.store_and_apply_updates(vec![update_command]).await?;

        Ok(())
    }

    async fn store_and_apply_updates(
        &mut self,
        commands: Vec<SchemasUpdateCommand>,
    ) -> Result<(), Error> {
        // Store update commands to disk
        self.storage.store(commands.clone()).await?;

        // Propagate updates in memory
        self.schemas.apply_updates(commands)?;

        Ok(())
    }

    fn infer_discovery_response_from_update_commands(
        commands: &[SchemasUpdateCommand],
    ) -> DiscoverDeploymentResponse {
        // for schema_update_command in commands {
        //     if let SchemasUpdateCommand::InsertDeployment {
        //         deployment_id,
        //         metadata: _,
        //         services,
        //         descriptor_pool,
        //     } = schema_update_command
        //     {
        //         return DiscoverDeploymentResponse {
        //             deployment: *deployment_id,
        //             services: services
        //                 .iter()
        //                 .map(|update_command| {
        //                     let service_descriptor = descriptor_pool
        //                         .get_service_by_name(&update_command.name)
        //                         .expect(
        //                             "A service descriptor must be present in the descriptor pool",
        //                         );
        //                     update_command
        //                         .as_service_metadata(*deployment_id, &service_descriptor)
        //                         .expect("Discovered services cannot be built-in services")
        //                 })
        //                 .collect(),
        //         };
        //     }
        // }

        panic!("Expecting a SchemasUpdateCommand::InsertDeployment command. This looks like a bug");
    }

    fn old_infer_discovery_response_from_update_commands(
        commands: &[SchemasUpdateCommand],
    ) -> OldDiscoverDeploymentResponse {
        for schema_update_command in commands {
            if let SchemasUpdateCommand::InsertDeployment {
                deployment_id,
                metadata: _,
                services,
                descriptor_pool,
            } = schema_update_command
            {
                return OldDiscoverDeploymentResponse {
                    deployment: *deployment_id,
                    services: services
                        .iter()
                        .map(|update_command| {
                            let service_descriptor = descriptor_pool
                                .get_service_by_name(&update_command.name)
                                .expect(
                                    "A service descriptor must be present in the descriptor pool",
                                );
                            update_command
                                .as_service_metadata(*deployment_id, &service_descriptor)
                                .expect("Discovered services cannot be built-in services")
                        })
                        .collect(),
                };
            }
        }

        panic!("Expecting a SchemasUpdateCommand::InsertDeployment command. This looks like a bug");
    }
}
