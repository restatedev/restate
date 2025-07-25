// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use http::Uri;
use tracing::subscriber::NoSubscriber;

use restate_core::{Metadata, MetadataWriter, ShutdownError};
use restate_metadata_store::ReadModifyWriteError;
use restate_service_protocol::discovery::{DiscoverEndpoint, DiscoveredEndpoint, ServiceDiscovery};
use restate_types::identifiers::{DeploymentId, ServiceRevision, SubscriptionId};
use restate_types::schema::deployment::{
    DeliveryOptions, Deployment, DeploymentMetadata, DeploymentResolver,
};
use restate_types::schema::service::{HandlerMetadata, ServiceMetadata, ServiceMetadataResolver};
use restate_types::schema::subscriptions::{
    ListSubscriptionFilter, Subscription, SubscriptionResolver, SubscriptionValidator,
};
use restate_types::schema::{Schema, updater};

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum SchemaRegistryError {
    #[error(transparent)]
    Schema(
        #[from]
        #[code]
        updater::SchemaError,
    ),
    #[error(transparent)]
    Discovery(
        #[from]
        #[code]
        restate_service_protocol::discovery::DiscoveryError,
    ),
    #[error("internal error: {0}")]
    #[code(unknown)]
    Internal(String),
    #[error(transparent)]
    #[code(unknown)]
    Shutdown(#[from] ShutdownError),
}

impl From<ReadModifyWriteError<updater::SchemaError>> for SchemaRegistryError {
    fn from(value: ReadModifyWriteError<updater::SchemaError>) -> Self {
        match value {
            ReadModifyWriteError::FailedOperation(err) => SchemaRegistryError::Schema(err),
            err => SchemaRegistryError::Internal(err.to_string()),
        }
    }
}

/// Responsible for updating the registered schema information. This includes the discovery of
/// new deployments.
#[derive(Clone)]
pub struct SchemaRegistry<V> {
    metadata_writer: MetadataWriter,
    service_discovery: ServiceDiscovery,
    subscription_validator: V,
}

impl<V> SchemaRegistry<V> {
    pub fn new(
        metadata_writer: MetadataWriter,
        service_discovery: ServiceDiscovery,
        subscription_validator: V,
    ) -> Self {
        Self {
            metadata_writer,
            service_discovery,
            subscription_validator,
        }
    }

    pub async fn register_deployment(
        &self,
        discover_endpoint: DiscoverEndpoint,
        force: updater::Force,
        apply_mode: updater::ApplyMode,
    ) -> Result<(Deployment, Vec<ServiceMetadata>), SchemaRegistryError> {
        // The number of concurrent discovery calls is bound by the number of concurrent
        // {register,update}_deployment calls. If it should become a problem that a user tries to register
        // the same endpoint too often, then we need to add a synchronization mechanism which
        // ensures that only a limited number of discover calls per endpoint are running.
        let discovered_metadata = self.service_discovery.discover(discover_endpoint).await?;

        let deployment_metadata = match discovered_metadata.endpoint {
            DiscoveredEndpoint::Http(uri, http_version) => DeploymentMetadata::new_http(
                uri.clone(),
                discovered_metadata.protocol_type,
                http_version,
                DeliveryOptions::new(discovered_metadata.headers),
                discovered_metadata.supported_protocol_versions,
                discovered_metadata.sdk_version,
            ),
            DiscoveredEndpoint::Lambda(arn, assume_role_arn) => DeploymentMetadata::new_lambda(
                arn,
                assume_role_arn,
                DeliveryOptions::new(discovered_metadata.headers),
                discovered_metadata.supported_protocol_versions,
                discovered_metadata.sdk_version,
            ),
        };

        let (id, services) = if !apply_mode.should_apply() {
            let mut updater =
                updater::SchemaUpdater::new(Metadata::with_current(|m| m.schema()).deref().clone());

            // suppress logging output in case of a dry run
            let id = tracing::subscriber::with_default(NoSubscriber::new(), || {
                updater.add_deployment(
                    deployment_metadata,
                    discovered_metadata.services,
                    force.force_enabled(),
                )
            })?;

            let schema_information = updater.into_inner();
            let (deployment, services) = schema_information
                .get_deployment_and_services(&id)
                .expect("deployment was just added");

            (deployment, services)
        } else {
            let mut new_deployment_id = None;
            let schema_information = self
                .metadata_writer
                .global_metadata()
                .read_modify_write(|schema_information: Option<Arc<Schema>>| {
                    let mut updater = updater::SchemaUpdater::new(
                        schema_information
                            .map(|s| s.as_ref().clone())
                            .unwrap_or_default(),
                    );

                    new_deployment_id = Some(updater.add_deployment(
                        deployment_metadata.clone(),
                        discovered_metadata.services.clone(),
                        force.force_enabled(),
                    )?);
                    Ok(updater.into_inner())
                })
                .await?;

            let new_deployment_id = new_deployment_id.expect("deployment was just added");
            let (deployment, services) = schema_information
                .get_deployment_and_services(&new_deployment_id)
                .expect("deployment was just added");

            (deployment, services)
        };

        Ok((id, services))
    }

    pub async fn update_deployment(
        &self,
        deployment_id: DeploymentId,
        discover_endpoint: DiscoverEndpoint,
        apply_mode: updater::ApplyMode,
    ) -> Result<(Deployment, Vec<ServiceMetadata>), SchemaRegistryError> {
        // The number of concurrent discovery calls is bound by the number of concurrent
        // {register,update}_deployment calls. If it should become a problem that a user tries to register
        // the same endpoint too often, then we need to add a synchronization mechanism which
        // ensures that only a limited number of discover calls per endpoint are running.
        let discovered_metadata = self.service_discovery.discover(discover_endpoint).await?;

        let deployment_metadata = match discovered_metadata.endpoint {
            DiscoveredEndpoint::Http(uri, http_version) => DeploymentMetadata::new_http(
                uri.clone(),
                discovered_metadata.protocol_type,
                http_version,
                DeliveryOptions::new(discovered_metadata.headers),
                discovered_metadata.supported_protocol_versions,
                discovered_metadata.sdk_version,
            ),
            DiscoveredEndpoint::Lambda(arn, assume_role_arn) => DeploymentMetadata::new_lambda(
                arn,
                assume_role_arn,
                DeliveryOptions::new(discovered_metadata.headers),
                discovered_metadata.supported_protocol_versions,
                discovered_metadata.sdk_version,
            ),
        };

        if !apply_mode.should_apply() {
            let mut updater =
                updater::SchemaUpdater::new(Metadata::with_current(|m| m.schema()).deref().clone());

            // suppress logging output in case of a dry run
            tracing::subscriber::with_default(NoSubscriber::new(), || {
                updater.update_deployment(
                    deployment_id,
                    deployment_metadata,
                    discovered_metadata.services,
                )
            })?;

            let schema_information = updater.into_inner();
            Ok(schema_information
                .get_deployment_and_services(&deployment_id)
                .expect("deployment was just added"))
        } else {
            let schema_information = self
                .metadata_writer
                .global_metadata()
                .read_modify_write(|schema_information: Option<Arc<Schema>>| {
                    let mut updater = updater::SchemaUpdater::new(
                        schema_information
                            .map(|s| s.as_ref().clone())
                            .unwrap_or_default(),
                    );

                    updater.update_deployment(
                        deployment_id,
                        deployment_metadata.clone(),
                        discovered_metadata.services.clone(),
                    )?;
                    Ok(updater.into_inner())
                })
                .await?;

            let (deployment, services) = schema_information
                .get_deployment_and_services(&deployment_id)
                .expect("deployment was just updated");

            Ok((deployment, services))
        }
    }

    pub async fn delete_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Result<(), SchemaRegistryError> {
        self.metadata_writer
            .global_metadata()
            .read_modify_write(|schema_registry: Option<Arc<Schema>>| {
                let schema_information: Schema = schema_registry
                    .map(|s| s.as_ref().clone())
                    .unwrap_or_default();

                if schema_information.get_deployment(&deployment_id).is_some() {
                    let mut updater = updater::SchemaUpdater::new(schema_information);
                    updater.remove_deployment(deployment_id);
                    Ok(updater.into_inner())
                } else {
                    Err(updater::SchemaError::NotFound(format!(
                        "deployment with id '{deployment_id}'"
                    )))
                }
            })
            .await?;

        Ok(())
    }

    pub async fn modify_service(
        &self,
        service_name: String,
        changes: Vec<updater::ModifyServiceChange>,
    ) -> Result<ServiceMetadata, SchemaRegistryError> {
        let schema_information = self
            .metadata_writer
            .global_metadata()
            .read_modify_write(|schema_information: Option<Arc<Schema>>| {
                let schema_information = schema_information
                    .map(|s| s.as_ref().clone())
                    .unwrap_or_default();

                if schema_information
                    .resolve_latest_service(&service_name)
                    .is_some()
                {
                    let mut updater = updater::SchemaUpdater::new(schema_information);
                    updater.modify_service(service_name.clone(), changes.clone())?;
                    Ok(updater.into_inner())
                } else {
                    Err(updater::SchemaError::NotFound(format!(
                        "service with name '{service_name}'"
                    )))
                }
            })
            .await?;

        let response = schema_information
            .resolve_latest_service(&service_name)
            .expect("service was just modified");

        Ok(response)
    }

    pub async fn delete_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<(), SchemaRegistryError> {
        self.metadata_writer
            .global_metadata()
            .read_modify_write(|schema_information: Option<Arc<Schema>>| {
                let schema_information = schema_information
                    .map(|s| s.as_ref().clone())
                    .unwrap_or_default();

                if schema_information
                    .get_subscription(subscription_id)
                    .is_some()
                {
                    let mut updater = updater::SchemaUpdater::new(schema_information);
                    updater.remove_subscription(subscription_id);
                    Ok(updater.into_inner())
                } else {
                    Err(updater::SchemaError::NotFound(format!(
                        "subscription with id '{subscription_id}'"
                    )))
                }
            })
            .await?;

        Ok(())
    }

    pub fn list_services(&self) -> Vec<ServiceMetadata> {
        Metadata::with_current(|m| m.schema()).list_services()
    }

    pub fn get_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        Metadata::with_current(|m| m.schema()).resolve_latest_service(&service_name)
    }

    pub fn get_service_openapi(&self, service_name: impl AsRef<str>) -> Option<serde_json::Value> {
        Metadata::with_current(|m| m.schema()).resolve_latest_service_openapi(&service_name)
    }

    pub fn get_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        Metadata::with_current(|m| m.schema()).get_deployment_and_services(&deployment_id)
    }

    pub fn list_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        Metadata::with_current(|m| m.schema()).get_deployments()
    }

    pub fn list_service_handlers(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Vec<HandlerMetadata>> {
        Metadata::with_current(|m| m.schema())
            .resolve_latest_service(&service_name)
            .map(|m| m.handlers.into_values().collect())
    }

    pub fn get_service_handler(
        &self,
        service_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<HandlerMetadata> {
        Metadata::with_current(|m| m.schema())
            .resolve_latest_service(&service_name)
            .and_then(|m| m.handlers.get(handler_name.as_ref()).cloned())
    }

    pub fn get_subscription(&self, subscription_id: SubscriptionId) -> Option<Subscription> {
        Metadata::with_current(|m| m.schema()).get_subscription(subscription_id)
    }

    pub fn list_subscriptions(&self, filters: &[ListSubscriptionFilter]) -> Vec<Subscription> {
        Metadata::with_current(|m| m.schema()).list_subscriptions(filters)
    }
}

impl<V> SchemaRegistry<V>
where
    V: SubscriptionValidator,
{
    pub(crate) async fn create_subscription(
        &self,
        source: Uri,
        sink: Uri,
        options: Option<HashMap<String, String>>,
    ) -> Result<Subscription, SchemaRegistryError> {
        let mut subscription_id = None;

        let schema_information = self
            .metadata_writer
            .global_metadata()
            .read_modify_write(|schema_information: Option<Arc<Schema>>| {
                let mut updater = updater::SchemaUpdater::new(
                    schema_information
                        .map(|s| s.as_ref().clone())
                        .unwrap_or_default(),
                );
                subscription_id = Some(updater.add_subscription(
                    None,
                    source.clone(),
                    sink.clone(),
                    options.clone(),
                    &self.subscription_validator,
                )?);

                Ok::<_, updater::SchemaError>(updater.into_inner())
            })
            .await?;

        let subscription = schema_information
            .get_subscription(subscription_id.expect("subscription was just added"))
            .expect("subscription was just added");

        Ok(subscription)
    }
}
