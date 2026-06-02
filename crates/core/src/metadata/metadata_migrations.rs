// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::debug;

use restate_metadata_store::ReadModifyWriteError;
use restate_types::schema::Schema;

use crate::MetadataWriter;

pub async fn migrate_metadata(writer: &MetadataWriter) -> anyhow::Result<()> {
    // Schema migrations to migrate old schema to new v2 format
    debug!("Starting metadata schema migration");
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
        Ok(_) | Err(ReadModifyWriteError::FailedOperation(MigrationError::NothingTodo)) => {}
        Err(err) => anyhow::bail!(err),
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum MigrationError {
    #[error("nothing todo")]
    NothingTodo,
}
