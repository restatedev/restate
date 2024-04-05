// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod error;
mod updater;

use crate::schema_registry::error::{SchemaError, SchemaRegistryError};
use crate::schema_registry::updater::SchemaUpdater;
use http::Uri;
use restate_core::metadata_store::MetadataStoreClient;
use restate_core::{metadata, MetadataWriter};
use restate_schema::SchemaInformation;
use restate_schema_api::component::{
    ComponentMetadata, ComponentMetadataResolver, HandlerMetadata,
};
use restate_schema_api::deployment::{
    DeliveryOptions, Deployment, DeploymentMetadata, DeploymentResolver,
};
use restate_schema_api::subscription::{
    ListSubscriptionFilter, Subscription, SubscriptionResolver, SubscriptionValidator,
};
use restate_service_client::Endpoint;
use restate_service_protocol::discovery::{ComponentDiscovery, DiscoverEndpoint};
use restate_types::identifiers::{ComponentRevision, DeploymentId, SubscriptionId};
use restate_types::metadata_store::keys::SCHEMA_INFORMATION_KEY;
use std::collections::HashMap;
use std::ops::Deref;
use tracing::subscriber::NoSubscriber;

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

/// Responsible for updating the registered schema information. This includes the discovery of
/// new deployments.
#[derive(Clone)]
pub struct SchemaRegistry<V> {
    metadata_store_client: MetadataStoreClient,
    metadata_writer: MetadataWriter,
    component_discovery: ComponentDiscovery,
    subscription_validator: V,
}

impl<V> SchemaRegistry<V> {
    pub fn new(
        metadata_store_client: MetadataStoreClient,
        metadata_writer: MetadataWriter,
        component_discovery: ComponentDiscovery,
        subscription_validator: V,
    ) -> Self {
        Self {
            metadata_writer,
            metadata_store_client,
            component_discovery,
            subscription_validator,
        }
    }

    pub async fn register_deployment(
        &self,
        discover_endpoint: DiscoverEndpoint,
        force: Force,
        apply_mode: ApplyMode,
    ) -> Result<(DeploymentId, Vec<ComponentMetadata>), SchemaRegistryError> {
        // The number of concurrent discovery calls is bound by the number of concurrent
        // register_deployment calls. If it should become a problem that a user tries to register
        // the same endpoint too often, then we need to add a synchronization mechanism which
        // ensures that only a limited number of discover calls per endpoint are running.
        let discovered_metadata = self
            .component_discovery
            .discover(&discover_endpoint)
            .await?;

        let deployment_metadata = match discover_endpoint.into_inner() {
            (Endpoint::Http(uri, _), headers) => DeploymentMetadata::new_http(
                uri.clone(),
                discovered_metadata.protocol_type,
                DeliveryOptions::new(headers),
            ),
            (Endpoint::Lambda(arn, assume_role_arn), headers) => {
                DeploymentMetadata::new_lambda(arn, assume_role_arn, DeliveryOptions::new(headers))
            }
        };

        let (id, components) = if !apply_mode.should_apply() {
            let mut updater = SchemaUpdater::from(
                metadata()
                    .schema_information()
                    .map(|schema_information| schema_information.deref().clone())
                    .unwrap_or_default(),
            );

            // suppress logging output in case of a dry run
            let id = tracing::subscriber::with_default(NoSubscriber::new(), || {
                updater.add_deployment(
                    None,
                    deployment_metadata,
                    discovered_metadata.components,
                    force.force_enabled(),
                )
            })?;

            let schema_information = updater.into_inner();
            let (_, components) = schema_information
                .get_deployment_and_components(&id)
                .expect("deployment was just added");

            (id, components)
        } else {
            let mut new_deployment_id = None;
            let schema_information = self
                .metadata_store_client
                .read_modify_write(
                    SCHEMA_INFORMATION_KEY.clone(),
                    |schema_information: Option<SchemaInformation>| {
                        let mut updater =
                            SchemaUpdater::from(schema_information.unwrap_or_default());

                        new_deployment_id = Some(updater.add_deployment(
                            None,
                            deployment_metadata.clone(),
                            discovered_metadata.components.clone(),
                            force.force_enabled(),
                        )?);
                        Ok(updater.into_inner())
                    },
                )
                .await?;

            let new_deployment_id = new_deployment_id.expect("deployment was just added");
            let (_, components) = schema_information
                .get_deployment_and_components(&new_deployment_id)
                .expect("deployment was just added");

            self.metadata_writer.update(schema_information).await?;

            (new_deployment_id, components)
        };

        Ok((id, components))
    }

    pub async fn delete_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Result<(), SchemaRegistryError> {
        let schema_registry = self
            .metadata_store_client
            .read_modify_write(
                SCHEMA_INFORMATION_KEY.clone(),
                |schema_registry: Option<SchemaInformation>| {
                    let schema_information: SchemaInformation = schema_registry.unwrap_or_default();

                    if schema_information.get_deployment(&deployment_id).is_some() {
                        let mut updater = SchemaUpdater::from(schema_information);
                        updater.remove_deployment(deployment_id);
                        Ok(updater.into_inner())
                    } else {
                        Err(SchemaError::NotFound(format!(
                            "deployment with id '{deployment_id}'"
                        )))
                    }
                },
            )
            .await?;
        self.metadata_writer.update(schema_registry).await?;

        Ok(())
    }

    pub async fn modify_component(
        &self,
        component_name: String,
        public: bool,
    ) -> Result<ComponentMetadata, SchemaRegistryError> {
        let schema_information = self
            .metadata_store_client
            .read_modify_write(
                SCHEMA_INFORMATION_KEY.clone(),
                |schema_information: Option<SchemaInformation>| {
                    let schema_information = schema_information.unwrap_or_default();

                    if schema_information
                        .resolve_latest_component(&component_name)
                        .is_some()
                    {
                        let mut updater = SchemaUpdater::from(schema_information);
                        updater.modify_component(component_name.clone(), public);
                        Ok(updater.into_inner())
                    } else {
                        Err(SchemaError::NotFound(format!(
                            "component with name '{component_name}'"
                        )))
                    }
                },
            )
            .await?;

        let response = schema_information
            .resolve_latest_component(&component_name)
            .expect("component was just modified");

        self.metadata_writer.update(schema_information).await?;

        Ok(response)
    }

    pub async fn delete_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<(), SchemaRegistryError> {
        let schema_information = self
            .metadata_store_client
            .read_modify_write(
                SCHEMA_INFORMATION_KEY.clone(),
                |schema_information: Option<SchemaInformation>| {
                    let schema_information = schema_information.unwrap_or_default();

                    if schema_information
                        .get_subscription(subscription_id)
                        .is_some()
                    {
                        let mut updater = SchemaUpdater::from(schema_information);
                        updater.remove_subscription(subscription_id);
                        Ok(updater.into_inner())
                    } else {
                        Err(SchemaError::NotFound(format!(
                            "subscription with id '{subscription_id}'"
                        )))
                    }
                },
            )
            .await?;

        self.metadata_writer.update(schema_information).await?;

        Ok(())
    }

    pub fn list_components(&self) -> Vec<ComponentMetadata> {
        metadata()
            .schema_information()
            .map(|schema_information| schema_information.list_components())
            .unwrap_or_default()
    }

    pub fn get_component(&self, component_name: impl AsRef<str>) -> Option<ComponentMetadata> {
        metadata()
            .schema_information()
            .and_then(|schema_information| {
                schema_information.resolve_latest_component(&component_name)
            })
    }

    pub fn get_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Option<(Deployment, Vec<ComponentMetadata>)> {
        metadata()
            .schema_information()
            .and_then(|schema_information| {
                schema_information.get_deployment_and_components(&deployment_id)
            })
    }

    pub fn list_deployments(&self) -> Vec<(Deployment, Vec<(String, ComponentRevision)>)> {
        metadata()
            .schema_information()
            .map(|schema_information| schema_information.get_deployments())
            .unwrap_or_default()
    }

    pub fn list_component_handlers(
        &self,
        component_name: impl AsRef<str>,
    ) -> Option<Vec<HandlerMetadata>> {
        metadata()
            .schema_information()
            .and_then(|schema_information| {
                schema_information
                    .resolve_latest_component(&component_name)
                    .map(|m| m.handlers)
            })
    }

    pub fn get_component_handler(
        &self,
        component_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<HandlerMetadata> {
        metadata()
            .schema_information()
            .and_then(|schema_information| {
                schema_information
                    .resolve_latest_component(&component_name)
                    .and_then(|m| {
                        m.handlers
                            .into_iter()
                            .find(|handler| handler.name == handler_name.as_ref())
                    })
            })
    }

    pub fn get_subscription(&self, subscription_id: SubscriptionId) -> Option<Subscription> {
        metadata()
            .schema_information()
            .and_then(|schema_information| schema_information.get_subscription(subscription_id))
    }

    pub fn list_subscriptions(&self, filters: &[ListSubscriptionFilter]) -> Vec<Subscription> {
        metadata()
            .schema_information()
            .map(|schema_information| schema_information.list_subscriptions(filters))
            .unwrap_or_default()
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
            .metadata_store_client
            .read_modify_write(
                SCHEMA_INFORMATION_KEY.clone(),
                |schema_information: Option<SchemaInformation>| {
                    let mut updater = SchemaUpdater::from(schema_information.unwrap_or_default());
                    subscription_id = Some(updater.add_subscription(
                        None,
                        source.clone(),
                        sink.clone(),
                        options.clone(),
                        &self.subscription_validator,
                    )?);

                    Ok::<_, SchemaError>(updater.into_inner())
                },
            )
            .await?;

        let subscription = schema_information
            .get_subscription(subscription_id.expect("subscription was just added"))
            .expect("subscription was just added");
        self.metadata_writer.update(schema_information).await?;

        Ok(subscription)
    }
}
