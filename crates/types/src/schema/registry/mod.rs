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
use std::sync::Arc;

use codederror::{BoxedCodedError, CodedError};
use http::{StatusCode, Uri};
use tracing::subscriber::NoSubscriber;

use crate::deployment;
use crate::deployment::{
    DeploymentAddress, Headers, HttpDeploymentAddress, LambdaDeploymentAddress,
};
use crate::identifiers::{DeploymentId, LambdaARN, ServiceRevision, SubscriptionId};
use crate::live::Pinned;
use crate::schema::Schema;
use crate::schema::deployment::{Deployment, DeploymentResolver, DeploymentType};
use crate::schema::metadata::updater;
use crate::schema::metadata::updater::{SchemaError, SchemaUpdater, ServiceError};
use crate::schema::service::{HandlerMetadata, ServiceMetadata, ServiceMetadataResolver};
use crate::schema::subscriptions::{ListSubscriptionFilter, Subscription, SubscriptionResolver};

mod discovery_client;
mod metadata_service;
mod telemetry_client;

pub use crate::schema::metadata::updater::{
    AddDeploymentResult, AllowBreakingChanges, ModifyServiceRequest, Overwrite,
};
pub use discovery_client::*;
pub use metadata_service::*;
pub use telemetry_client::*;

// -- Schema registry error and other types

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[error(transparent)]
pub struct SchemaRegistryError(
    #[from]
    #[code]
    SchemaRegistryErrorInner,
);

impl SchemaRegistryError {
    pub fn internal(error: impl ToString) -> Self {
        Self(SchemaRegistryErrorInner::Internal(error.to_string()))
    }

    pub fn status_code(&self) -> StatusCode {
        match &self.0 {
            SchemaRegistryErrorInner::Schema(schema_error) => match schema_error {
                SchemaError::NotFound(_) => StatusCode::NOT_FOUND,
                SchemaError::Service(ServiceError::DifferentType { .. })
                | SchemaError::Service(ServiceError::RemovedHandlers { .. }) => {
                    StatusCode::CONFLICT
                }
                SchemaError::Service(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::BAD_REQUEST,
            },
            SchemaRegistryErrorInner::UpdateDeployment { .. } => StatusCode::BAD_REQUEST,
            SchemaRegistryErrorInner::Discovery(_) | SchemaRegistryErrorInner::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

impl From<SchemaError> for SchemaRegistryError {
    fn from(value: SchemaError) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
enum SchemaRegistryErrorInner {
    #[error(transparent)]
    Schema(
        #[from]
        #[code]
        SchemaError,
    ),
    #[error(
        "cannot update the deployment, as the deployment type is {actual_deployment_type} while the update options are for type {expected_deployment_type}"
    )]
    #[code(unknown)]
    UpdateDeployment {
        actual_deployment_type: &'static str,
        expected_deployment_type: &'static str,
    },
    #[error("{0}")]
    Discovery(
        #[source]
        #[code]
        BoxedCodedError,
    ),
    #[error("internal error: {0}")]
    #[code(unknown)]
    Internal(String),
}

/// Whether to apply the changes or not
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub enum ApplyMode {
    DryRun,
    #[default]
    Apply,
}

impl ApplyMode {
    pub(crate) fn should_apply(&self) -> bool {
        *self == Self::Apply
    }
}

pub struct RegisterDeploymentRequest {
    pub deployment_address: DeploymentAddress,
    pub additional_headers: Headers,
    pub metadata: deployment::Metadata,
    pub use_http_11: bool,
    pub allow_breaking: AllowBreakingChanges,
    pub overwrite: Overwrite,
    pub apply_mode: ApplyMode,
}

pub struct UpdateDeploymentRequest {
    pub update_deployment_address: Option<UpdateDeploymentAddress>,
    pub additional_headers: Option<Headers>,
    pub overwrite: Overwrite,
    pub apply_mode: ApplyMode,
}

pub enum UpdateDeploymentAddress {
    Lambda {
        arn: Option<LambdaARN>,
        assume_role_arn: Option<String>,
    },
    Http {
        uri: Option<Uri>,
        use_http_11: Option<bool>,
    },
}

/// This is the business logic driving the Admin API schema related endpoints.
#[derive(Clone)]
pub struct SchemaRegistry<Metadata, Discovery, Telemetry> {
    metadata_service: Metadata,
    discovery_client: Discovery,
    telemetry_client: Telemetry,
}

impl<Metadata, Discovery, Telemetry> SchemaRegistry<Metadata, Discovery, Telemetry> {
    pub fn new(
        metadata_service: Metadata,
        discovery_client: Discovery,
        telemetry_client: Telemetry,
    ) -> Self {
        Self {
            metadata_service,
            discovery_client,
            telemetry_client,
        }
    }
}

impl<Metadata: MetadataService, Discovery: DiscoveryClient, Telemetry: TelemetryClient>
    SchemaRegistry<Metadata, Discovery, Telemetry>
{
    pub async fn register_deployment(
        &self,
        RegisterDeploymentRequest {
            deployment_address,
            additional_headers,
            metadata,
            use_http_11,
            allow_breaking,
            overwrite,
            apply_mode,
        }: RegisterDeploymentRequest,
    ) -> Result<(AddDeploymentResult, Deployment, Vec<ServiceMetadata>), SchemaRegistryError> {
        // Verify first if we have the service. If we do, no need to do anything here.
        if overwrite == Overwrite::No {
            // Verify if we have a service for this endpoint already or not
            if let Some((deployment, services)) = self
                .metadata_service
                .get()
                .find_deployment(&deployment_address, &additional_headers)
            {
                return Ok((AddDeploymentResult::Unchanged, deployment, services));
            }
        }

        let discovery_request = DiscoveryRequest {
            address: deployment_address.clone(),
            use_http_11,
            additional_headers: additional_headers.clone(),
        };

        // The number of concurrent discovery calls is bound by the number of concurrent
        // {register,update}_deployment calls. If it should become a problem that a user tries to register
        // the same endpoint too often, then we need to add a synchronization mechanism which
        // ensures that only a limited number of discover calls per endpoint are running.
        let discovery_response = self
            .discovery_client
            .discover(discovery_request)
            .await
            .map_err(|e| e.into_boxed())
            .map_err(SchemaRegistryErrorInner::Discovery)
            .map_err(SchemaRegistryError::from)?;

        let sdk_version = discovery_response.sdk_version.clone();

        let add_deployment_request = updater::AddDeploymentRequest {
            deployment_address,
            additional_headers,
            metadata,
            discovery_response,
            allow_breaking_changes: allow_breaking,
            overwrite,
        };

        let (register_deployment_result, deployment, services) = if !apply_mode.should_apply() {
            // --- Dry run
            // Suppress logging output in case of a dry run
            let ((deployment_result, deployment_id), schemas) =
                tracing::subscriber::with_default(NoSubscriber::new(), || {
                    SchemaUpdater::update_and_return(
                        self.metadata_service.get().clone(),
                        |updater| updater.add_deployment(add_deployment_request),
                    )
                })?;

            let (deployment, services) = schemas
                .get_deployment_and_services(&deployment_id)
                .expect("deployment was just added");

            (deployment_result, deployment, services)
        } else {
            // --- Apply the deployment registration
            let ((new_deployment_result, new_deployment_id), schemas) = self
                .metadata_service
                .update(|schema| {
                    SchemaUpdater::update_and_return(schema, |updater| {
                        updater
                            .add_deployment(add_deployment_request.clone())
                            .map_err(Into::into)
                    })
                })
                .await?;

            let (deployment, services) = schemas
                .get_deployment_and_services(&new_deployment_id)
                .expect("deployment was just added");

            (new_deployment_result, deployment, services)
        };

        if apply_mode.should_apply() {
            self.telemetry_client
                .send_register_deployment_telemetry(sdk_version);
        }

        Ok((register_deployment_result, deployment, services))
    }
}
impl<Metadata: MetadataService, Discovery: DiscoveryClient, Telemetry>
    SchemaRegistry<Metadata, Discovery, Telemetry>
{
    pub async fn update_deployment(
        &self,
        deployment_id: DeploymentId,
        UpdateDeploymentRequest {
            update_deployment_address,
            additional_headers,
            overwrite,
            apply_mode,
        }: UpdateDeploymentRequest,
    ) -> Result<(Deployment, Vec<ServiceMetadata>), SchemaRegistryError> {
        let Some(existing_deployment) = self.metadata_service.get().get_deployment(&deployment_id)
        else {
            return Err(SchemaError::NotFound(deployment_id.to_string()).into());
        };

        // Merge with update changes requested
        let (deployment_address, use_http_11) =
            match (update_deployment_address, existing_deployment.ty) {
                (
                    Some(UpdateDeploymentAddress::Http {
                        uri: Some(uri),
                        use_http_11,
                    }),
                    _,
                ) => (
                    DeploymentAddress::Http(HttpDeploymentAddress::new(uri)),
                    use_http_11.unwrap_or(false),
                ),
                (
                    Some(UpdateDeploymentAddress::Lambda {
                        arn: Some(arn),
                        assume_role_arn,
                    }),
                    _,
                ) => (
                    DeploymentAddress::Lambda(LambdaDeploymentAddress::new(arn, assume_role_arn)),
                    false,
                ),
                (
                    Some(UpdateDeploymentAddress::Http {
                        uri: None,
                        use_http_11,
                    }),
                    DeploymentType::Http {
                        address,
                        http_version,
                        ..
                    },
                ) => (
                    DeploymentAddress::Http(HttpDeploymentAddress::new(address)),
                    use_http_11.unwrap_or(http_version == http::Version::HTTP_11),
                ),
                (
                    Some(UpdateDeploymentAddress::Lambda {
                        arn: None,
                        assume_role_arn: update_assume_role_arn,
                    }),
                    DeploymentType::Lambda {
                        arn,
                        assume_role_arn,
                        ..
                    },
                ) => (
                    DeploymentAddress::Lambda(LambdaDeploymentAddress::new(
                        arn,
                        update_assume_role_arn.or(assume_role_arn.map(Into::into)),
                    )),
                    false,
                ),
                (Some(UpdateDeploymentAddress::Lambda { .. }), DeploymentType::Http { .. }) => {
                    return Err(SchemaRegistryErrorInner::UpdateDeployment {
                        actual_deployment_type: "http",
                        expected_deployment_type: "lambda",
                    }
                    .into());
                }
                (Some(UpdateDeploymentAddress::Http { .. }), DeploymentType::Lambda { .. }) => {
                    return Err(SchemaRegistryErrorInner::UpdateDeployment {
                        actual_deployment_type: "lambda",
                        expected_deployment_type: "http",
                    }
                    .into());
                }
                (
                    None,
                    DeploymentType::Http {
                        address,
                        http_version,
                        ..
                    },
                ) => (
                    DeploymentAddress::Http(HttpDeploymentAddress::new(address)),
                    http_version == http::Version::HTTP_11,
                ),
                (
                    None,
                    DeploymentType::Lambda {
                        arn,
                        assume_role_arn,
                        ..
                    },
                ) => (
                    DeploymentAddress::Lambda(LambdaDeploymentAddress::new(
                        arn,
                        assume_role_arn.map(Into::into),
                    )),
                    false,
                ),
            };
        let additional_headers =
            additional_headers.unwrap_or(existing_deployment.additional_headers);

        let discovery_request = DiscoveryRequest {
            address: deployment_address.clone(),
            use_http_11,
            additional_headers: additional_headers.clone(),
        };

        // The number of concurrent discovery calls is bound by the number of concurrent
        // {register,update}_deployment calls. If it should become a problem that a user tries to register
        // the same endpoint too often, then we need to add a synchronization mechanism which
        // ensures that only a limited number of discover calls per endpoint are running.
        let discovery_response = self
            .discovery_client
            .discover(discovery_request)
            .await
            .map_err(|e| e.into_boxed())
            .map_err(SchemaRegistryErrorInner::Discovery)
            .map_err(SchemaRegistryError::from)?;

        let update_deployment_request = updater::UpdateDeploymentRequest {
            deployment_id,
            deployment_address,
            additional_headers,
            discovery_response,
            overwrite,
        };

        if !apply_mode.should_apply() {
            // --- Dry run
            // Suppress logging output in case of a dry run
            let schemas = tracing::subscriber::with_default(NoSubscriber::new(), || {
                SchemaUpdater::update(self.metadata_service.get().clone(), |updater| {
                    updater.update_deployment(update_deployment_request)
                })
            })?;

            Ok(schemas
                .get_deployment_and_services(&deployment_id)
                .expect("deployment was just added"))
        } else {
            // --- Apply the deployment update
            let (_, schemas) = self
                .metadata_service
                .update(|schema| {
                    Ok((
                        (),
                        SchemaUpdater::update(schema, |updater| {
                            updater.update_deployment(update_deployment_request.clone())
                        })?,
                    ))
                })
                .await?;

            let (deployment, services) = schemas
                .get_deployment_and_services(&deployment_id)
                .expect("deployment was just updated");

            Ok((deployment, services))
        }
    }
}
impl<Metadata: MetadataService, Discovery, Telemetry>
    SchemaRegistry<Metadata, Discovery, Telemetry>
{
    pub async fn delete_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Result<(), SchemaRegistryError> {
        self.metadata_service
            .update(|schema| {
                Ok((
                    (),
                    SchemaUpdater::update(schema, |updater| {
                        if updater.remove_deployment(deployment_id) {
                            Ok(())
                        } else {
                            Err(SchemaError::NotFound(format!(
                                "deployment with id '{deployment_id}'"
                            )))
                        }
                    })?,
                ))
            })
            .await?;

        Ok(())
    }

    pub async fn modify_service(
        &self,
        service_name: String,
        request: ModifyServiceRequest,
    ) -> Result<ServiceMetadata, SchemaRegistryError> {
        let (_, schema) = self
            .metadata_service
            .update(|schema| {
                if schema.resolve_latest_service(&service_name).is_some() {
                    Ok((
                        (),
                        SchemaUpdater::update(schema, |updater| {
                            updater.modify_service(&service_name, request.clone())
                        })?,
                    ))
                } else {
                    Err(SchemaError::NotFound(format!("service with name '{service_name}'")).into())
                }
            })
            .await?;

        let response = schema
            .resolve_latest_service(&service_name)
            .expect("service was just modified");

        Ok(response)
    }

    pub async fn delete_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<(), SchemaRegistryError> {
        self.metadata_service
            .update(|schema| {
                Ok((
                    (),
                    SchemaUpdater::update(schema, |updater| {
                        if updater.remove_subscription(subscription_id) {
                            Ok(())
                        } else {
                            Err(SchemaError::NotFound(format!(
                                "subscription with id '{subscription_id}'"
                            )))
                        }
                    })?,
                ))
            })
            .await?;

        Ok(())
    }

    pub fn list_services(&self) -> Vec<ServiceMetadata> {
        self.metadata_service.get().list_services()
    }

    pub fn get_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata> {
        self.metadata_service
            .get()
            .resolve_latest_service(&service_name)
    }

    pub fn get_service_openapi(&self, service_name: impl AsRef<str>) -> Option<serde_json::Value> {
        self.metadata_service
            .get()
            .resolve_latest_service_openapi(&service_name)
    }

    pub fn get_deployment(
        &self,
        deployment_id: DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        self.metadata_service
            .get()
            .get_deployment_and_services(&deployment_id)
    }

    pub fn list_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        self.metadata_service.get().get_deployments()
    }

    pub fn list_service_handlers(
        &self,
        service_name: impl AsRef<str>,
    ) -> Option<Vec<HandlerMetadata>> {
        self.metadata_service
            .get()
            .resolve_latest_service(&service_name)
            .map(|m| m.handlers.into_values().collect())
    }

    pub fn get_service_handler(
        &self,
        service_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<HandlerMetadata> {
        self.metadata_service
            .get()
            .resolve_latest_service(&service_name)
            .and_then(|m| m.handlers.get(handler_name.as_ref()).cloned())
    }

    pub fn get_subscription(&self, subscription_id: SubscriptionId) -> Option<Subscription> {
        self.metadata_service
            .get()
            .get_subscription(subscription_id)
    }

    pub fn list_subscriptions(&self, filters: &[ListSubscriptionFilter]) -> Vec<Subscription> {
        self.metadata_service.get().list_subscriptions(filters)
    }

    pub async fn create_subscription(
        &self,
        source: Uri,
        sink: Uri,
        options: Option<HashMap<String, String>>,
    ) -> Result<Subscription, SchemaRegistryError> {
        let (subscription_id, schema) = self
            .metadata_service
            .update(|schema| {
                SchemaUpdater::update_and_return(schema, |updater| {
                    updater.add_subscription(None, source.clone(), sink.clone(), options.clone())
                })
                .map_err(Into::into)
            })
            .await?;

        let subscription = schema
            .get_subscription(subscription_id)
            .expect("subscription was just added");

        Ok(subscription)
    }
}

#[cfg(any(test, feature = "test-util"))]
mod mocks {
    use super::*;
    use arc_swap::ArcSwap;
    use std::ops::Deref;

    impl TelemetryClient for () {
        fn send_register_deployment_telemetry(&self, _: Option<String>) {
            // Nothing
        }
    }

    impl MetadataService for ArcSwap<Schema> {
        fn get(&self) -> Pinned<Schema> {
            Pinned::new(self)
        }

        fn update<T: Send, F>(
            &self,
            modify: F,
        ) -> impl Future<Output = Result<(T, Arc<Schema>), SchemaRegistryError>> + Send
        where
            F: Fn(Schema) -> Result<(T, Schema), SchemaRegistryError>,
        {
            std::future::ready(
                modify(self.load().deref().deref().clone())
                    .map(|(t, schema)| (t, Arc::new(schema))),
            )
        }
    }
}
