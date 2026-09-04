// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::WriteBatch;
use tokio_util::sync::CancellationToken;

use restate_types::config::Configuration;
use restate_types::{RESTATE_VERSION_1_7_9, RESTATE_VERSION_1_8_0, SemanticRestateVersion};
use restate_util_string::ReString;

use crate::migrations::MigrationContext;
use crate::migrations::migrate_to_scoped_state_table::{
    append_delete_state_data, migrate_to_scoped_state_table,
};
use crate::{MigrationError, PartitionStore};

use super::{StorageFeature, StorageFeatures};

static SCOPED_STATE_ONLY: ReString = ReString::from_static("scoped-state-table");

impl StorageFeature for super::MigratedToScopedStateTableFeature {
    fn persisted_name() -> &'static ReString {
        &SCOPED_STATE_ONLY
    }

    fn min_required_version() -> &'static SemanticRestateVersion {
        &RESTATE_VERSION_1_7_9
    }

    fn should_enable(
        config: &Configuration,
        current_version: &SemanticRestateVersion,
        is_store_empty: bool,
    ) -> bool {
        if current_version.is_equal_or_newer_than(&RESTATE_VERSION_1_8_0) {
            is_store_empty
                || config
                    .common
                    .experimental
                    .is_scoped_state_table_migration_enabled()
        } else {
            config
                .common
                .experimental
                .is_scoped_state_table_migration_enabled()
        }
    }

    fn is_enabled(features: &StorageFeatures) -> bool {
        features.is_migrated_to_scoped_state_table
    }

    fn set_enabled(features: &mut StorageFeatures) {
        features.is_migrated_to_scoped_state_table = true;
    }

    fn enable(
        storage: &mut PartitionStore,
        cancel: &CancellationToken,
        config: &Configuration,
        finalization: &mut WriteBatch,
    ) -> Result<(), MigrationError> {
        let partition_db = storage.partition_db().clone();
        let mut ctx = MigrationContext::new(
            config,
            &partition_db,
            storage.partition_key_range(),
            cancel.clone(),
        );
        migrate_to_scoped_state_table(&mut ctx)?;
        append_delete_state_data(&ctx, finalization);
        Ok(())
    }
}
