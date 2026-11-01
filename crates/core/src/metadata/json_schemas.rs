// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;

use restate_metadata_store::MetadataStoreClient;
use restate_types::identifiers::JsonSchemasId;
use restate_types::metadata::Precondition;
use restate_types::metadata_store::keys::{JSON_SCHEMAS_INDEX_KEY, deployment_json_schemas_key};
use restate_types::schema::{DeploymentJsonSchemas, JsonSchemasIndex};
use restate_types::time::MillisSinceEpoch;

/// Stores new JSON schemas objects, after reserving their ids in the [`JsonSchemasIndex`] so that
/// they can be found and deleted if no schema ends up referencing them.
pub async fn store_json_schemas<'a>(
    client: &MetadataStoreClient,
    json_schemas: impl IntoIterator<Item = (JsonSchemasId, &'a DeploymentJsonSchemas)>,
) -> anyhow::Result<()> {
    let json_schemas: Vec<_> = json_schemas.into_iter().collect();
    if json_schemas.is_empty() {
        return Ok(());
    }

    let reserved_at = MillisSinceEpoch::now();
    client
        .read_modify_write(JSON_SCHEMAS_INDEX_KEY.clone(), |index| {
            Ok::<_, Infallible>(JsonSchemasIndex::reserve(
                index,
                json_schemas.iter().map(|(id, _)| *id),
                reserved_at,
            ))
        })
        .await?;

    futures::future::try_join_all(json_schemas.iter().map(|(id, json_schemas)| {
        client.put(
            deployment_json_schemas_key(*id),
            *json_schemas,
            Precondition::DoesNotExist,
        )
    }))
    .await?;

    Ok(())
}
