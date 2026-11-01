// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use tracing::info;

use restate_metadata_store::ReadModifyWriteError;
use restate_types::config::Configuration;
use restate_types::identifiers::JsonSchemasId;
use restate_types::metadata::GlobalMetadata;
use restate_types::metadata_store::keys::SCHEMA_INFORMATION_KEY;
use restate_types::nodes_config::Role;
use restate_types::schema::Schema;

use super::store_json_schemas;
use crate::MetadataWriter;

pub async fn migrate_metadata(writer: &MetadataWriter) -> anyhow::Result<()> {
    let split_json_schemas = {
        let config = Configuration::pinned();
        config.has_role(Role::Admin) && config.common.experimental.is_json_schemas_split_enabled()
    };
    if split_json_schemas {
        split_inline_json_schemas(writer).await?;
    }

    Ok(())
}

/// Moves the inline JSON schemas of deployments registered before `json_schemas_split` was
/// enabled into separately stored objects.
///
/// Objects left unreferenced, e.g. because another node migrated concurrently, are deleted by the
/// admin's sweep.
async fn split_inline_json_schemas(writer: &MetadataWriter) -> anyhow::Result<()> {
    let client = writer.raw_metadata_store_client();
    let Some(schema) = client.get::<Schema>(SCHEMA_INFORMATION_KEY.clone()).await? else {
        return Ok(());
    };
    let json_schemas: Vec<_> = schema
        .inline_json_schemas()
        .into_iter()
        .map(|(deployment_id, json_schemas)| (deployment_id, JsonSchemasId::new(), json_schemas))
        .collect();
    if json_schemas.is_empty() {
        return Ok(());
    }

    store_json_schemas(client, json_schemas.iter().map(|(_, id, js)| (*id, js))).await?;

    let mut migrated = 0;
    let result = client
        .read_modify_write(SCHEMA_INFORMATION_KEY.clone(), |schema: Option<Schema>| {
            let mut schema = schema.ok_or(Unchanged)?;
            migrated = 0;
            for (deployment_id, id, json_schemas) in &json_schemas {
                if schema.attach_json_schemas(*deployment_id, *id, json_schemas) {
                    migrated += 1;
                }
            }
            if migrated == 0 {
                return Err(Unchanged);
            }
            schema.touch();
            Ok(schema)
        })
        .await;

    match result {
        Ok(schema) => {
            writer.update(Arc::new(schema).into_container()).await?;
            info!("Moved the JSON schemas of {migrated} deployments out of the schema registry");
        }
        // The deployments changed or got migrated concurrently
        Err(ReadModifyWriteError::FailedOperation(Unchanged)) => {}
        Err(err) => return Err(err.into()),
    }

    Ok(())
}
#[derive(Debug, thiserror::Error)]
#[error("schema unchanged")]
struct Unchanged;
