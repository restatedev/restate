// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::{debug, info};

use restate_metadata_store::ReadModifyWriteError;
use restate_types::schema::Schema;

use crate::MetadataWriter;

// todo(azmy): Remove in Restate v1.8
pub async fn migrate_metadata(writer: &MetadataWriter) -> anyhow::Result<()> {
    // Schema migrations to migrate old schema to new v2 format
    debug!("Checking whether schema metadata needs migration to v2 format");
    let result = writer
        .global_metadata()
        .read_modify_write::<Schema, _, MigrationError>(|schema| {
            let Some(schema) = schema else {
                return Err(MigrationError::NothingTodo);
            };

            if !schema.is_legacy_v1() {
                return Err(MigrationError::NothingTodo);
            }

            let mut schema = schema.as_ref().clone();
            schema.touch();

            Ok(schema)
        })
        .await;

    match result {
        Ok(_) => {
            info!("Successfully migrated schema metadata to v2 format");
        }
        Err(ReadModifyWriteError::FailedOperation(MigrationError::NothingTodo)) => {
            debug!("Schema metadata already up to date; no migration needed");
        }
        Err(err) => anyhow::bail!(err),
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum MigrationError {
    #[error("nothing todo")]
    NothingTodo,
}
