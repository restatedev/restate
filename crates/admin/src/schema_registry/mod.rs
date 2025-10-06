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

use anyhow::Context;
use http::{HeaderMap, HeaderValue, Uri, uri::PathAndQuery};
use tracing::subscriber::NoSubscriber;
use tracing::trace;

use restate_core::{Metadata, MetadataWriter, ShutdownError, TaskCenter, TaskKind};
use restate_metadata_store::ReadModifyWriteError;
use restate_service_client::HttpClient;
use restate_service_protocol::discovery::{DiscoveryRequest, ServiceDiscovery};
use restate_types::deployment;
use restate_types::deployment::{
    DeploymentAddress, Headers, HttpDeploymentAddress, LambdaDeploymentAddress,
};
use restate_types::identifiers::{DeploymentId, LambdaARN, ServiceRevision, SubscriptionId};
use restate_types::schema::deployment::{
    DeliveryOptions, Deployment, DeploymentMetadata, DeploymentResolver, DeploymentType,
};
use restate_types::schema::service::{HandlerMetadata, ServiceMetadata, ServiceMetadataResolver};
use restate_types::schema::subscriptions::{
    ListSubscriptionFilter, Subscription, SubscriptionResolver, SubscriptionValidator,
};
pub(crate) use restate_types::schema::updater::RegisterDeploymentResult;
use restate_types::schema::{Schema, updater};

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum SchemaRegistryError {
    #[error(transparent)]
    Schema(
        #[from]
        #[code]
        updater::SchemaError,
    ),
    #[error(
        "cannot update the deployment, as the deployment type is {actual_deployment_type} while the update options are for type {expected_deployment_type}"
    )]
    #[code(unknown)]
    UpdateDeployment {
        actual_deployment_type: &'static str,
        expected_deployment_type: &'static str,
    },
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

/// Whether to apply the changes or not
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub(crate) enum ApplyMode {
    DryRun,
    #[default]
    Apply,
}

impl ApplyMode {
    pub(crate) fn should_apply(&self) -> bool {
        *self == Self::Apply
    }
}

pub(crate) struct RegisterDeploymentRequest {
    pub(crate) deployment_address: DeploymentAddress,
    pub(crate) additional_headers: Headers,
    pub(crate) metadata: deployment::Metadata,
    pub(crate) use_http_11: bool,
    pub(crate) allow_breaking: updater::AllowBreakingChanges,
    pub(crate) overwrite: updater::Overwrite,
    pub(crate) apply_mode: ApplyMode,
}

pub(crate) struct UpdateDeploymentRequest {
    pub(crate) update_deployment_address: UpdateDeploymentAddress,
    pub(crate) additional_headers: Option<Headers>,
    pub(crate) overwrite: updater::Overwrite,
    pub(crate) apply_mode: ApplyMode,
}

pub(crate) enum UpdateDeploymentAddress {
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
pub struct SchemaRegistry<V> {
    metadata_writer: MetadataWriter,
    service_discovery: ServiceDiscovery,
    telemetry_http_client: Option<HttpClient>,
    subscription_validator: V,
}

impl<V> SchemaRegistry<V> {
    pub fn new(
        metadata_writer: MetadataWriter,
        service_discovery: ServiceDiscovery,
        telemetry_http_client: Option<HttpClient>,
        subscription_validator: V,
    ) -> Self {
        Self {
            metadata_writer,
            service_discovery,
            telemetry_http_client,
            subscription_validator,
        }
    }

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
    ) -> Result<(RegisterDeploymentResult, Deployment, Vec<ServiceMetadata>), SchemaRegistryError>
    {
        // Verify first if we have the service. If we do, no need to do anything here.
        if matches!(overwrite, updater::Overwrite::No) {
            // Verify if we have a service for this endpoint already or not
            if let Some((deployment, services)) = Metadata::with_current(|m| m.schema_ref())
                .find_deployment(&deployment_address, &additional_headers)
            {
                return Ok((RegisterDeploymentResult::Unchanged, deployment, services));
            }
        }

        let discovery_request =
            DiscoveryRequest::new(deployment_address, use_http_11, additional_headers);

        // The number of concurrent discovery calls is bound by the number of concurrent
        // {register,update}_deployment calls. If it should become a problem that a user tries to register
        // the same endpoint too often, then we need to add a synchronization mechanism which
        // ensures that only a limited number of discover calls per endpoint are running.
        let discovery_response = self.service_discovery.discover(discovery_request).await?;

        // Construct deployment metadata from discovery response
        let deployment_metadata = DeploymentMetadata::new(
            discovery_response.deployment_type,
            DeliveryOptions::new(discovery_response.headers),
            discovery_response.supported_protocol_versions,
            discovery_response.sdk_version,
            metadata,
        );

        let (register_deployment_result, deployment, services) = if !apply_mode.should_apply() {
            // --- Dry run
            // Suppress logging output in case of a dry run
            let ((deployment_result, deployment_id), schemas) =
                tracing::subscriber::with_default(NoSubscriber::new(), || {
                    updater::SchemaUpdater::update_and_return(
                        Metadata::with_current(|m| m.schema()).deref().clone(),
                        |updater| {
                            updater.add_deployment(
                                deployment_metadata,
                                discovery_response.services,
                                allow_breaking,
                                overwrite,
                            )
                        },
                    )
                })?;

            let (deployment, services) = schemas
                .get_deployment_and_services(&deployment_id)
                .expect("deployment was just added");

            (deployment_result, deployment, services)
        } else {
            // --- Apply the deployment registration
            let mut new_deployment_id = None;
            let mut new_deployment_result = None;
            let schemas = self
                .metadata_writer
                .global_metadata()
                .read_modify_write(|schema_information: Option<Arc<Schema>>| {
                    let ((deployment_result, deployment_id), schemas) =
                        updater::SchemaUpdater::update_and_return(
                            schema_information
                                .map(|s| s.as_ref().clone())
                                .unwrap_or_default(),
                            |updater| {
                                updater.add_deployment(
                                    deployment_metadata.clone(),
                                    discovery_response.services.clone(),
                                    allow_breaking,
                                    overwrite,
                                )
                            },
                        )?;

                    new_deployment_result = Some(deployment_result);
                    new_deployment_id = Some(deployment_id);

                    Ok(schemas)
                })
                .await?;

            let new_deployment_id = new_deployment_id.expect("deployment was just added");
            let (deployment, services) = schemas
                .get_deployment_and_services(&new_deployment_id)
                .expect("deployment was just added");

            (new_deployment_result.unwrap(), deployment, services)
        };

        if apply_mode.should_apply() {
            self.send_register_deployment_telemetry(deployment.metadata.sdk_version.clone());
        }

        Ok((register_deployment_result, deployment, services))
    }

    fn send_register_deployment_telemetry(&self, sdk_version: Option<String>) {
        if let Some(telemetry_http_client) = &self.telemetry_http_client {
            let client = telemetry_http_client.clone();
            let _ = TaskCenter::spawn(TaskKind::Disposable, "telemetry-operation", async move {
                let (sdk_type, full_sdk_version_string) = if let Some(sdk_version) = &sdk_version {
                    (
                        sdk_version
                            .split_once('/')
                            .map(|(version, _)| version)
                            .unwrap_or_else(|| "unknown"),
                        sdk_version.as_str(),
                    )
                } else {
                    ("unknown", "unknown")
                };

                let uri = format!(
                    "{TELEMETRY_URI_PREFIX}?sdk={}&version={}",
                    urlencoding::encode(sdk_type),
                    urlencoding::encode(full_sdk_version_string)
                )
                .parse()
                .with_context(|| "cannot create telemetry uri")?;

                trace!(%uri, "Sending telemetry data");

                match client
                    .request(
                        uri,
                        None,
                        http::Method::GET,
                        http_body_util::Empty::new(),
                        PathAndQuery::from_static("/"),
                        HeaderMap::from_iter([(
                            http::header::USER_AGENT,
                            HeaderValue::from_static("restate-server"),
                        )]),
                    )
                    .await
                {
                    Ok(resp) => {
                        trace!(status = %resp.status(), "Sent telemetry data")
                    }
                    Err(err) => {
                        trace!(error = %err, "Failed to send telemetry data")
                    }
                }

                Ok(())
            });
        }
    }

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
        let Some(existing_deployment) =
            Metadata::with_current(|m| m.schema()).get_deployment(&deployment_id)
        else {
            return Err(updater::SchemaError::NotFound(deployment_id.to_string()).into());
        };

        // Merge with update changes requested
        let (deployment_address, use_http_11) =
            match (update_deployment_address, existing_deployment.metadata.ty) {
                (
                    UpdateDeploymentAddress::Http {
                        uri: Some(uri),
                        use_http_11,
                    },
                    _,
                ) => (
                    DeploymentAddress::Http(HttpDeploymentAddress::new(uri)),
                    use_http_11.unwrap_or(false),
                ),
                (
                    UpdateDeploymentAddress::Lambda {
                        arn: Some(arn),
                        assume_role_arn,
                    },
                    _,
                ) => (
                    DeploymentAddress::Lambda(LambdaDeploymentAddress::new(arn, assume_role_arn)),
                    false,
                ),
                (
                    UpdateDeploymentAddress::Http { uri, use_http_11 },
                    DeploymentType::Http {
                        address,
                        http_version,
                        ..
                    },
                ) => (
                    DeploymentAddress::Http(HttpDeploymentAddress::new(uri.unwrap_or(address))),
                    use_http_11.unwrap_or(http_version == http::Version::HTTP_11),
                ),
                (UpdateDeploymentAddress::Http { .. }, DeploymentType::Lambda { .. }) => {
                    return Err(SchemaRegistryError::UpdateDeployment {
                        actual_deployment_type: "lambda",
                        expected_deployment_type: "http",
                    });
                }
                (
                    UpdateDeploymentAddress::Lambda {
                        arn: update_arn,
                        assume_role_arn: update_assume_role_arn,
                    },
                    DeploymentType::Lambda {
                        arn,
                        assume_role_arn,
                        ..
                    },
                ) => (
                    DeploymentAddress::Lambda(LambdaDeploymentAddress::new(
                        update_arn.unwrap_or(arn),
                        update_assume_role_arn.or(assume_role_arn.map(Into::into)),
                    )),
                    false,
                ),
                (UpdateDeploymentAddress::Lambda { .. }, DeploymentType::Http { .. }) => {
                    return Err(SchemaRegistryError::UpdateDeployment {
                        actual_deployment_type: "http",
                        expected_deployment_type: "lambda",
                    });
                }
            };
        let additional_headers = additional_headers.unwrap_or(
            existing_deployment
                .metadata
                .delivery_options
                .additional_headers,
        );

        let discovery_request =
            DiscoveryRequest::new(deployment_address, use_http_11, additional_headers);

        // The number of concurrent discovery calls is bound by the number of concurrent
        // {register,update}_deployment calls. If it should become a problem that a user tries to register
        // the same endpoint too often, then we need to add a synchronization mechanism which
        // ensures that only a limited number of discover calls per endpoint are running.
        let discovery_response = self.service_discovery.discover(discovery_request).await?;

        // Construct deployment metadata from discovery response
        let deployment_metadata = DeploymentMetadata::new(
            discovery_response.deployment_type,
            DeliveryOptions::new(discovery_response.headers),
            discovery_response.supported_protocol_versions,
            discovery_response.sdk_version,
            existing_deployment.metadata.metadata,
        );

        if !apply_mode.should_apply() {
            // --- Dry run
            // Suppress logging output in case of a dry run
            let schemas = tracing::subscriber::with_default(NoSubscriber::new(), || {
                updater::SchemaUpdater::update(
                    Metadata::with_current(|m| m.schema()).deref().clone(),
                    |updater| {
                        updater.update_deployment(
                            deployment_id,
                            deployment_metadata,
                            discovery_response.services,
                            overwrite,
                        )
                    },
                )
            })?;

            Ok(schemas
                .get_deployment_and_services(&deployment_id)
                .expect("deployment was just added"))
        } else {
            // --- Apply the deployment update
            let schemas = self
                .metadata_writer
                .global_metadata()
                .read_modify_write(|schemas: Option<Arc<Schema>>| {
                    updater::SchemaUpdater::update(
                        schemas.map(|s| s.as_ref().clone()).unwrap_or_default(),
                        |updater| {
                            updater.update_deployment(
                                deployment_id,
                                deployment_metadata.clone(),
                                discovery_response.services.clone(),
                                overwrite,
                            )
                        },
                    )
                })
                .await?;

            let (deployment, services) = schemas
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
            .read_modify_write(|schemas: Option<Arc<Schema>>| {
                updater::SchemaUpdater::update(
                    schemas.map(|s| s.as_ref().clone()).unwrap_or_default(),
                    |updater| {
                        if updater.remove_deployment(deployment_id) {
                            Ok(())
                        } else {
                            Err(updater::SchemaError::NotFound(format!(
                                "deployment with id '{deployment_id}'"
                            )))
                        }
                    },
                )
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
            .read_modify_write(|schemas: Option<Arc<Schema>>| {
                let schemas = schemas.map(|s| s.as_ref().clone()).unwrap_or_default();

                if schemas.resolve_latest_service(&service_name).is_some() {
                    updater::SchemaUpdater::update(schemas, |updater| {
                        updater.modify_service(&service_name, changes.clone())
                    })
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
            .read_modify_write(|schemas: Option<Arc<Schema>>| {
                updater::SchemaUpdater::update(
                    schemas.map(|s| s.as_ref().clone()).unwrap_or_default(),
                    |updater| {
                        if updater.remove_subscription(subscription_id) {
                            Ok(())
                        } else {
                            Err(updater::SchemaError::NotFound(format!(
                                "subscription with id '{subscription_id}'"
                            )))
                        }
                    },
                )
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
                let (sub, schemas) = updater::SchemaUpdater::update_and_return(
                    schema_information
                        .map(|s| s.as_ref().clone())
                        .unwrap_or_default(),
                    |updater| {
                        updater.add_subscription(
                            None,
                            source.clone(),
                            sink.clone(),
                            options.clone(),
                            &self.subscription_validator,
                        )
                    },
                )?;
                subscription_id = Some(sub);

                Ok::<_, updater::SchemaError>(schemas)
            })
            .await?;

        let subscription = schema_information
            .get_subscription(subscription_id.expect("subscription was just added"))
            .expect("subscription was just added");

        Ok(subscription)
    }
}

static TELEMETRY_URI_PREFIX: &str = "https://restate.gateway.scarf.sh/sdk-registration/";
