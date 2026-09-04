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
use restate_types::{RESTATE_VERSION_1_7_0, RESTATE_VERSION_1_8_0, SemanticRestateVersion};
use restate_util_string::ReString;

use crate::{MigrationError, PartitionStore};

use super::{StorageFeature, StorageFeatures};

static COMPOUND: ReString = ReString::from_static("scoped-promises-and-state-tables");

impl StorageFeature for super::MigratedToScopedPromiseAndStateTablesFeature {
    fn persisted_name() -> &'static ReString {
        &COMPOUND
    }

    fn min_required_version() -> &'static SemanticRestateVersion {
        &RESTATE_VERSION_1_7_0
    }

    fn should_enable(
        config: &Configuration,
        current_version: &SemanticRestateVersion,
        _is_store_empty: bool,
    ) -> bool {
        let both_options_on = config
            .common
            .experimental
            .is_scoped_promise_table_migration_enabled()
            && config
                .common
                .experimental
                .is_scoped_state_table_migration_enabled();

        !current_version.is_equal_or_newer_than(&RESTATE_VERSION_1_8_0) && both_options_on
    }

    fn is_enabled(features: &StorageFeatures) -> bool {
        (features.is_migrated_to_scoped_promise_table && features.is_migrated_to_scoped_state_table)
            || features.is_migrated_to_scoped_promise_and_state_tables
    }

    fn set_enabled(features: &mut StorageFeatures) {
        features.is_migrated_to_scoped_state_table = true;
        features.is_migrated_to_scoped_promise_table = true;
        features.is_migrated_to_scoped_promise_and_state_tables = true;
    }

    fn enable(
        storage: &mut PartitionStore,
        cancel: &CancellationToken,
        config: &Configuration,
        finalization: &mut WriteBatch,
    ) -> Result<(), MigrationError> {
        super::MigratedToScopedPromiseTableFeature::enable(storage, cancel, config, finalization)?;
        super::MigratedToScopedStateTableFeature::enable(storage, cancel, config, finalization)?;
        Ok(())
    }
}
