// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use http::{HeaderMap, HeaderValue, uri::PathAndQuery};
use tracing::{debug, trace, warn};

use restate_core::{Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_metadata_store::{MetadataStoreClient, ReadModifyWriteError};
use restate_service_client::HttpClient;
use restate_types::identifiers::JsonSchemasId;
use restate_types::live::Pinned;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::{
    JSON_SCHEMAS_INDEX_KEY, SCHEMA_INFORMATION_KEY, deployment_json_schemas_key,
};
use restate_types::schema::registry::SchemaRegistryError;
use restate_types::schema::{DeploymentJsonSchemas, JsonSchemasIndex, Schema};
use restate_types::time::MillisSinceEpoch;

/// Unreferenced JSON schemas objects are only deleted once they were reserved at least this long
/// ago, so that objects of in-flight schema updates are not deleted.
const JSON_SCHEMAS_GRACE_PERIOD: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
pub struct MetadataService {
    metadata_writer: MetadataWriter,
    json_schemas_cache: moka::future::Cache<JsonSchemasId, Arc<DeploymentJsonSchemas>>,
}

impl MetadataService {
    pub(crate) fn new(metadata_writer: MetadataWriter) -> Self {
        Self {
            metadata_writer,
            // Objects are immutable, so entries never need to be invalidated
            json_schemas_cache: moka::future::Cache::builder()
                .max_capacity(256)
                .time_to_idle(Duration::from_secs(10 * 60))
                .build(),
        }
    }
}

impl restate_types::schema::registry::MetadataService for MetadataService {
    fn get(&self) -> Pinned<Schema> {
        Metadata::with_current(|m| m.schema_ref())
    }

    async fn get_json_schemas(&self, id: JsonSchemasId) -> Option<Arc<DeploymentJsonSchemas>> {
        let client = self.metadata_writer.raw_metadata_store_client();
        self.json_schemas_cache
            .optionally_get_with(id, async move {
                match client
                    .get::<DeploymentJsonSchemas>(deployment_json_schemas_key(id))
                    .await
                {
                    Ok(Some(json_schemas)) => Some(Arc::new(json_schemas)),
                    Ok(None) => {
                        warn!(%id, "JSON schemas are missing from the metadata store");
                        None
                    }
                    Err(err) => {
                        warn!(%id, %err, "Failed to fetch JSON schemas from the metadata store");
                        None
                    }
                }
            })
            .await
    }

    async fn store_json_schemas(
        &self,
        id: JsonSchemasId,
        json_schemas: &DeploymentJsonSchemas,
    ) -> Result<(), SchemaRegistryError> {
        restate_core::store_json_schemas(
            self.metadata_writer.raw_metadata_store_client(),
            [(id, json_schemas)],
        )
        .await
        .map_err(SchemaRegistryError::internal)
    }

    async fn update<T: Send, F>(&self, modify: F) -> Result<(T, Arc<Schema>), SchemaRegistryError>
    where
        F: (Fn(Schema) -> Result<(T, Schema), SchemaRegistryError>) + Send + Sync,
    {
        let mut t = None;
        let schemas = self
            .metadata_writer
            .global_metadata()
            .read_modify_write(|schema_information: Option<Arc<Schema>>| {
                let schema = schema_information
                    .map(|s| s.as_ref().clone())
                    .unwrap_or_default();

                let (new_t, new_schema) = modify(schema)?;
                t = Some(new_t);
                Ok(new_schema)
            })
            .await
            .map_err(|err| match err {
                ReadModifyWriteError::FailedOperation(err) => err,
                err => SchemaRegistryError::internal(err),
            })?;

        let _ = TaskCenter::spawn(TaskKind::Disposable, "json-schemas-sweep", {
            let client = self.metadata_writer.raw_metadata_store_client().clone();
            let schemas = Arc::clone(&schemas);
            async move { sweep_json_schemas(&client, &schemas, grace_period_start()).await }
        });

        Ok((t.unwrap(), schemas))
    }
}

fn grace_period_start() -> MillisSinceEpoch {
    MillisSinceEpoch::now() - JSON_SCHEMAS_GRACE_PERIOD
}

/// Deletes the JSON schemas objects that `schema` doesn't reference and that were reserved before
/// `reserved_before`. `schema` must be the latest known schema.
async fn sweep_json_schemas(
    client: &MetadataStoreClient,
    schema: &Schema,
    reserved_before: MillisSinceEpoch,
) -> anyhow::Result<()> {
    let Some(index) = client
        .get::<JsonSchemasIndex>(JSON_SCHEMAS_INDEX_KEY.clone())
        .await?
    else {
        return Ok(());
    };

    let unreferenced: HashSet<_> = index
        .unreferenced(schema, reserved_before)
        .into_iter()
        .collect();
    if unreferenced.is_empty() {
        return Ok(());
    }

    // Delete the objects before removing them from the index, so that a failure in between
    // leaves them in the index for the next sweep. Ids are never reused, so deleting is safe.
    for id in &unreferenced {
        client
            .inner()
            .delete(deployment_json_schemas_key(*id), Precondition::None)
            .await?;
    }

    match client
        .read_modify_write(
            JSON_SCHEMAS_INDEX_KEY.clone(),
            |index: Option<JsonSchemasIndex>| {
                index
                    .and_then(|index| index.remove(&unreferenced))
                    .ok_or("already removed")
            },
        )
        .await
    {
        // `FailedOperation` means that another sweeper removed them already
        Ok(_) | Err(ReadModifyWriteError::FailedOperation(_)) => {}
        Err(err) => return Err(err.into()),
    }

    debug!(
        "Deleted {} unreferenced JSON schemas objects",
        unreferenced.len()
    );
    Ok(())
}

/// Sweeps the JSON schemas objects left behind by interrupted schema updates.
pub(crate) async fn sweep_json_schemas_on_startup(
    client: MetadataStoreClient,
) -> anyhow::Result<()> {
    // Read the latest schema from the metadata store, the local one might be outdated
    let Some(schema) = client
        .get::<Schema>(SCHEMA_INFORMATION_KEY.clone())
        .await
        .context("failed to read the schema")?
    else {
        return Ok(());
    };
    sweep_json_schemas(&client, &schema, grace_period_start()).await
}

#[derive(Clone)]
pub struct TelemetryClient(pub(crate) Option<HttpClient>);

impl restate_types::schema::registry::TelemetryClient for TelemetryClient {
    fn send_register_deployment_telemetry(&self, sdk_version: Option<String>) {
        let Some(client) = &self.0 else {
            return;
        };

        let client = client.clone();
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

static TELEMETRY_URI_PREFIX: &str = "https://restate.gateway.scarf.sh/sdk-registration/";

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;
    use test_log::test;

    use restate_core::TestCoreEnvBuilder;
    use restate_types::Versioned;
    use restate_types::config::{Configuration, set_current_config};
    use restate_types::deployment::{DeploymentAddress, HttpDeploymentAddress};
    use restate_types::endpoint_manifest;
    use restate_types::identifiers::DeploymentId;
    use restate_types::schema::registry::{
        AllowBreakingChanges, ApplyMode, DiscoveryResponse, Overwrite, RegisterDeploymentRequest,
        SchemaRegistry,
    };

    const SERVICE: &str = "greeter.Greeter";
    const HANDLER: &str = "greet";

    fn greeter_service() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            abort_timeout: None,
            documentation: None,
            ingress_private: None,
            ty: endpoint_manifest::ServiceType::Service,
            name: SERVICE.parse().unwrap(),
            retry_policy_exponentiation_factor: None,
            retry_policy_initial_interval: None,
            retry_policy_max_attempts: None,
            retry_policy_max_interval: None,
            handlers: vec![endpoint_manifest::Handler {
                abort_timeout: None,
                documentation: None,
                idempotency_retention: None,
                name: HANDLER.parse().unwrap(),
                ty: None,
                input: Some(endpoint_manifest::InputPayload {
                    content_type: Some("application/json".to_owned()),
                    json_schema: Some(json!({"type": "object", "title": "Greeting"})),
                    required: Some(true),
                }),
                output: Some(endpoint_manifest::OutputPayload {
                    content_type: Some("application/json".to_owned()),
                    json_schema: Some(json!({"type": "string"})),
                    set_content_type_if_empty: None,
                }),
                retry_policy_exponentiation_factor: None,
                retry_policy_initial_interval: None,
                retry_policy_max_attempts: None,
                retry_policy_max_interval: None,
                metadata: Default::default(),
                inactivity_timeout: None,
                journal_retention: None,
                workflow_completion_retention: None,
                enable_lazy_state: None,
                eager_state_keys_whitelist: vec![],
                ingress_private: None,
                retry_policy_on_max_attempts: None,
            }],
            idempotency_retention: None,
            inactivity_timeout: None,
            journal_retention: None,
            metadata: Default::default(),
            enable_lazy_state: None,
            eager_state_keys_whitelist: vec![],
            retry_policy_on_max_attempts: None,
        }
    }

    fn register_request(overwrite: Overwrite) -> RegisterDeploymentRequest {
        RegisterDeploymentRequest {
            deployment_address: DeploymentAddress::Http(HttpDeploymentAddress::new(
                "http://localhost:9080".parse().unwrap(),
            )),
            additional_headers: Default::default(),
            metadata: Default::default(),
            use_http_11: false,
            allow_breaking: AllowBreakingChanges::No,
            overwrite,
            apply_mode: ApplyMode::Apply,
        }
    }

    async fn index_ids(client: &MetadataStoreClient) -> HashSet<JsonSchemasId> {
        client
            .get::<JsonSchemasIndex>(JSON_SCHEMAS_INDEX_KEY.clone())
            .await
            .unwrap()
            .map(|index| index.ids().collect())
            .unwrap_or_default()
    }

    async fn object_exists(client: &MetadataStoreClient, id: JsonSchemasId) -> bool {
        client
            .get::<DeploymentJsonSchemas>(deployment_json_schemas_key(id))
            .await
            .unwrap()
            .is_some()
    }

    fn json_schemas_ref(deployment_id: DeploymentId) -> JsonSchemasId {
        Metadata::with_current(|m| m.schema())
            .json_schemas_ref(&deployment_id)
            .expect("deployment references its JSON schemas")
    }

    #[test(restate_core::test)]
    async fn json_schemas_are_stored_separately_and_swept() {
        let mut config = Configuration::default();
        config.common.experimental.set_json_schemas_split(true);
        set_current_config(config);

        let builder = TestCoreEnvBuilder::with_incoming_only_connector();
        let client = builder.metadata_store_client.clone();
        let registry = SchemaRegistry::new(
            MetadataService::new(builder.metadata_writer.clone()),
            DiscoveryResponse::mock(vec![greeter_service()]),
            (),
        );
        let _env = builder.build().await;
        let far_future = MillisSinceEpoch::now() + Duration::from_secs(24 * 60 * 60);

        // Registering stores the JSON schemas next to the schema, and returns them hydrated
        let (_, deployment, services) = registry
            .register_deployment(register_request(Overwrite::No))
            .await
            .unwrap();
        assert!(services[0].handlers[HANDLER].input_json_schema.is_some());
        let first = json_schemas_ref(deployment.id);
        assert!(object_exists(&client, first).await);
        assert_eq!(index_ids(&client).await, HashSet::from([first]));

        let local_schema = Metadata::with_current(|m| m.schema());
        let handler = local_schema.assert_handler(SERVICE, HANDLER);
        assert_eq!(
            (handler.input_json_schema, handler.output_json_schema),
            (None, None)
        );

        // The read path fetches them
        let handler = registry
            .get_service_handler(SERVICE, HANDLER)
            .await
            .unwrap();
        assert_eq!(handler.output_json_schema, Some(json!({"type": "string"})));
        let openapi = registry
            .get_service_openapi(SERVICE, "http://localhost:8080".parse().unwrap())
            .await
            .unwrap();
        assert!(openapi.to_string().contains("Greeting"));
        let (_, services) = registry
            .get_deployment_and_services(deployment.id)
            .await
            .unwrap();
        assert_eq!(
            services[0].handlers[HANDLER].input_description,
            "JSON 'Greeting'"
        );
        let services = registry.list_services().await;
        assert!(services[0].handlers[HANDLER].input_json_schema.is_some());

        // Reading never puts them back into the managed schema
        let handler = Metadata::with_current(|m| m.schema()).assert_handler(SERVICE, HANDLER);
        assert_eq!(
            (handler.input_json_schema, handler.output_json_schema),
            (None, None)
        );

        // Overwriting the deployment stores a new object, the old one is only swept after the
        // grace period
        registry
            .register_deployment(register_request(Overwrite::Yes))
            .await
            .unwrap();
        let second = json_schemas_ref(deployment.id);
        assert_ne!(first, second);

        let schema = Metadata::with_current(|m| m.schema());
        sweep_json_schemas(&client, &schema, grace_period_start())
            .await
            .unwrap();
        assert_eq!(index_ids(&client).await, HashSet::from([first, second]));

        sweep_json_schemas(&client, &schema, far_future)
            .await
            .unwrap();
        assert_eq!(index_ids(&client).await, HashSet::from([second]));
        assert!(!object_exists(&client, first).await);
        assert!(object_exists(&client, second).await);

        // Removing the deployment leaves its object unreferenced
        registry.delete_deployment(deployment.id).await.unwrap();
        let schema = Metadata::with_current(|m| m.schema());
        sweep_json_schemas(&client, &schema, far_future)
            .await
            .unwrap();
        assert!(index_ids(&client).await.is_empty());
        assert!(!object_exists(&client, second).await);
    }

    #[test(restate_core::test)]
    async fn inline_json_schemas_are_migrated_on_boot() {
        set_current_config(Configuration::default());

        let builder = TestCoreEnvBuilder::with_incoming_only_connector();
        let client = builder.metadata_store_client.clone();
        let writer = builder.metadata_writer.clone();
        let registry = SchemaRegistry::new(
            MetadataService::new(writer.clone()),
            DiscoveryResponse::mock(vec![greeter_service()]),
            (),
        );
        let _env = builder.build().await;

        // Registered while disabled, the JSON schemas are inline
        let (_, deployment, _) = registry
            .register_deployment(register_request(Overwrite::No))
            .await
            .unwrap();
        let schema = Metadata::with_current(|m| m.schema());
        assert_eq!(schema.json_schemas_ref(&deployment.id), None);
        assert!(index_ids(&client).await.is_empty());

        // Once enabled, the next boot moves them out
        let mut config = Configuration::default();
        config.common.experimental.set_json_schemas_split(true);
        set_current_config(config);
        restate_core::migrate_metadata(&writer).await.unwrap();

        let id = json_schemas_ref(deployment.id);
        assert!(object_exists(&client, id).await);
        assert_eq!(index_ids(&client).await, HashSet::from([id]));
        let migrated = Metadata::with_current(|m| m.schema());
        assert!(migrated.version() > schema.version());
        assert!(migrated.inline_json_schemas().is_empty());
        let handler = registry
            .get_service_handler(SERVICE, HANDLER)
            .await
            .unwrap();
        assert_eq!(handler.output_json_schema, Some(json!({"type": "string"})));

        // Migrating again is a no-op
        restate_core::migrate_metadata(&writer).await.unwrap();
        assert_eq!(index_ids(&client).await, HashSet::from([id]));
        assert_eq!(
            Metadata::with_current(|m| m.schema()).version(),
            migrated.version()
        );
    }
}
