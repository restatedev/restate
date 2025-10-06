// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use anyhow::Context;
use http::{HeaderMap, HeaderValue, uri::PathAndQuery};
use restate_core::{Metadata, MetadataWriter, TaskCenter, TaskKind};
use restate_metadata_store::ReadModifyWriteError;
use restate_service_client::HttpClient;
use restate_types::live::Pinned;
use restate_types::schema::registry::SchemaRegistryError;
use restate_types::schema::{Schema, updater};
use tracing::trace;

#[derive(Clone)]
pub struct MetadataService(pub(crate) MetadataWriter);

impl restate_types::schema::registry::MetadataService for MetadataService {
    fn get(&self) -> Pinned<Schema> {
        Metadata::with_current(|m| m.schema_ref())
    }

    async fn update<T: Send, F>(&self, modify: F) -> Result<(T, Arc<Schema>), SchemaRegistryError>
    where
        F: (Fn(Schema) -> Result<(T, Schema), updater::SchemaError>) + Send + Sync,
    {
        let mut t = None;
        let schemas = self
            .0
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
                ReadModifyWriteError::FailedOperation(err) => SchemaRegistryError::Schema(err),
                err => SchemaRegistryError::Internal(err.to_string()),
            })?;

        Ok((t.unwrap(), schemas))
    }
}

#[derive(Clone)]
pub struct TelemetryClient(pub(crate) Option<HttpClient>);

impl restate_types::schema::registry::TelemetryClient for TelemetryClient {
    fn send_register_deployment_telemetry(&self, sdk_version: Option<String>) {
        if let Some(client) = &self.0 {
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
}

static TELEMETRY_URI_PREFIX: &str = "https://restate.gateway.scarf.sh/sdk-registration/";
