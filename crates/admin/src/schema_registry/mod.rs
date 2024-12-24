// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use http::Uri;
use tracing::subscriber::NoSubscriber;

use restate_core::{Metadata, MetadataWriter};
use restate_service_protocol::discovery::{DiscoverEndpoint, DiscoveredEndpoint, ServiceDiscovery};
use restate_types::identifiers::{DeploymentId, ServiceRevision, SubscriptionId};
use restate_types::metadata_store::keys::SCHEMA_INFORMATION_KEY;
use restate_types::schema::deployment::{
    DeliveryOptions, Deployment, DeploymentMetadata, DeploymentResolver,
};
use restate_types::schema::service::{HandlerMetadata, ServiceMetadata, ServiceMetadataResolver};
use restate_types::schema::subscriptions::{
    ListSubscriptionFilter, Subscription, SubscriptionResolver, SubscriptionValidator,
};
use restate_types::schema::Schema;

use crate::schema_registry::error::{SchemaError, SchemaRegistryError, ServiceError};
use crate::schema_registry::updater::SchemaUpdater;

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

#[derive(Debug, Clone)]
pub enum ModifyServiceChange {
    Public(bool),
    IdempotencyRetention(Duration),
    WorkflowCompletionRetention(Duration),
    InactivityTimeout(Duration),
    AbortTimeout(Duration),
}

/// Responsible for updating the registered schema information. This includes the discovery of
/// new deployments.
#[derive(Clone)]
pub struct SchemaRegistry<V> {
    metadata_writer: MetadataWriter,
    service_discovery: ServiceDiscovery,
    subscription_validator: V,

    experimental_feature_kafka_ingress_next: bool,
}

impl<V> SchemaRegistry<V> {
    pub fn new(
        metadata_writer: MetadataWriter,
        service_discovery: ServiceDiscovery,
        subscription_validator: V,
        experimental_feature_kafka_ingress_next: bool,
    ) -> Self {
        Self {
            metadata_writer,
            service_discovery,
            subscription_validator,
            experimental_feature_kafka_ingress_next,
        }
    }

    pub async fn register_deployment(
        &self,
        discover_endpoint: DiscoverEndpoint,
        force: Force,
        apply_mode: ApplyMode,
    ) -> Result<(DeploymentId, Vec<ServiceMetadata>), SchemaRegistryError> {
        // The number of concurrent discovery calls is bound by the number of concurrent
        // register_deployment calls. If it should become a problem that a user tries to register
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
            ),
            DiscoveredEndpoint::Lambda(arn, assume_role_arn) => DeploymentMetadata::new_lambda(
                arn,
                assume_role_arn,
                DeliveryOptions::new(discovered_metadata.headers),
                discovered_metadata.supported_protocol_versions,
            ),
        };

        let (id, services) = if !apply_mode.should_apply() {
            let mut updater = SchemaUpdater::new(
                Metadata::with_current(|m| m.schema()).deref().clone(),
                self.experimental_feature_kafka_ingress_next,
            );

            // suppress logging output in case of a dry run
            let id = tracing::subscriber::with_default(NoSubscriber::new(), || {
                updater.add_deployment(
                    None,
                    deployment_metadata,
                    discovered_metadata.services,
                    force.force_enabled(),
                )
            })?;

            let schema_information = updater.into_inner();
            let (_, services) = schema_information
                .get_deployment_and_services(&id)
                .expect("deployment was just added");

            (id, services)
        } else {
            let mut new_deployment_id = None;
            let schema_information = self
                .metadata_writer
                .metadata_store_client()
                .read_modify_write(
                    SCHEMA_INFORMATION_KEY.clone(),
                    |schema_information: Option<Schema>| {
                        let mut updater = SchemaUpdater::new(
                            schema_information.unwrap_or_default(),
                            self.experimental_feature_kafka_ingress_next,
                        );

                        new_deployment_id = Some(updater.add_deployment(
                            None,
                            deployment_metadata.clone(),
                            discovered_metadata.services.clone(),
                            force.force_enabled(),
                        )?);
                        Ok(updater.into_inner())
                    },
                )
                .await?;

            let new_deployment_id = new_deployment_id.expect("deployment was just added");
            let (_, services) = schema_information
                .get_deployment_and_services(&new_deployment_id)
                .expect("deployment was just added");

            self.metadata_writer
                .update(Arc::new(schema_information))
                .await?;

            (new_deployment_id, services)
        };

        Ok((id, services))
    }

    pub async fn delete_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Result<(), SchemaRegistryError> {
        let schema_registry = self
            .metadata_writer
            .metadata_store_client()
            .read_modify_write(
                SCHEMA_INFORMATION_KEY.clone(),
                |schema_registry: Option<Schema>| {
                    let schema_information: Schema = schema_registry.unwrap_or_default();

                    if schema_information.get_deployment(&deployment_id).is_some() {
                        let mut updater = SchemaUpdater::new(
                            schema_information,
                            self.experimental_feature_kafka_ingress_next,
                        );
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
        self.metadata_writer
            .update(Arc::new(schema_registry))
            .await?;

        Ok(())
    }

    pub async fn modify_service(
        &self,
        service_name: String,
        changes: Vec<ModifyServiceChange>,
    ) -> Result<ServiceMetadata, SchemaRegistryError> {
        let schema_information = self
            .metadata_writer
            .metadata_store_client()
            .read_modify_write(
                SCHEMA_INFORMATION_KEY.clone(),
                |schema_information: Option<Schema>| {
                    let schema_information = schema_information.unwrap_or_default();

                    if schema_information
                        .resolve_latest_service(&service_name)
                        .is_some()
                    {
                        let mut updater = SchemaUpdater::new(
                            schema_information,
                            self.experimental_feature_kafka_ingress_next,
                        );
                        updater.modify_service(service_name.clone(), changes.clone())?;
                        Ok(updater.into_inner())
                    } else {
                        Err(SchemaError::NotFound(format!(
                            "service with name '{service_name}'"
                        )))
                    }
                },
            )
            .await?;

        let response = schema_information
            .resolve_latest_service(&service_name)
            .expect("service was just modified");

        self.metadata_writer
            .update(Arc::new(schema_information))
            .await?;

        Ok(response)
    }

    pub async fn delete_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<(), SchemaRegistryError> {
        let schema_information = self
            .metadata_writer
            .metadata_store_client()
            .read_modify_write(
                SCHEMA_INFORMATION_KEY.clone(),
                |schema_information: Option<Schema>| {
                    let schema_information = schema_information.unwrap_or_default();

                    if schema_information
                        .get_subscription(subscription_id)
                        .is_some()
                    {
                        let mut updater = SchemaUpdater::new(
                            schema_information,
                            self.experimental_feature_kafka_ingress_next,
                        );
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

        self.metadata_writer
            .update(Arc::new(schema_information))
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
            .map(|m| m.handlers)
    }

    pub fn get_service_handler(
        &self,
        service_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<HandlerMetadata> {
        Metadata::with_current(|m| m.schema())
            .resolve_latest_service(&service_name)
            .and_then(|m| {
                m.handlers
                    .into_iter()
                    .find(|handler| handler.name == handler_name.as_ref())
            })
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
            .metadata_store_client()
            .read_modify_write(
                SCHEMA_INFORMATION_KEY.clone(),
                |schema_information: Option<Schema>| {
                    let mut updater = SchemaUpdater::new(
                        schema_information.unwrap_or_default(),
                        self.experimental_feature_kafka_ingress_next,
                    );
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
        self.metadata_writer
            .update(Arc::new(schema_information))
            .await?;

        Ok(subscription)
    }
}

/// Newtype for service names
#[derive(Debug, Clone, PartialEq, Eq, Hash, derive_more::Display)]
#[display("{}", _0)]
pub struct ServiceName(String);

impl TryFrom<String> for ServiceName {
    type Error = ServiceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.to_lowercase().starts_with("restate")
            || value.to_lowercase().eq_ignore_ascii_case("openapi")
        {
            Err(ServiceError::ReservedName(value))
        } else {
            Ok(ServiceName(value))
        }
    }
}

impl AsRef<str> for ServiceName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl ServiceName {
    fn into_inner(self) -> String {
        self.0
    }
}

impl Borrow<String> for ServiceName {
    fn borrow(&self) -> &String {
        &self.0
    }
}
