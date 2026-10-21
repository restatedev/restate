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

use restate_storage_api::StorageError;
use restate_types::config::Configuration;
use restate_types::{RESTATE_VERSION_1_8_2, RESTATE_VERSION_1_9_0, SemanticRestateVersion};
use restate_util_string::ReString;

use crate::stats::aggregated::backfill;
use crate::{MigrationError, PartitionStore};

use super::{StorageFeature, StorageFeatures};

impl StorageFeature for super::TrackStatusGaugesFeature {
    fn persisted_name() -> &'static ReString {
        const NAME: &ReString = &ReString::from_static("status-gauges");
        NAME
    }

    fn min_required_version() -> &'static SemanticRestateVersion {
        &RESTATE_VERSION_1_8_2
    }

    fn should_enable(
        config: &Configuration,
        current_version: &SemanticRestateVersion,
        _is_store_empty: bool,
    ) -> bool {
        current_version.is_equal_or_newer_than(&RESTATE_VERSION_1_9_0)
            || config.common.experimental.is_track_status_gauges_enabled()
    }

    fn should_disable(
        config: &Configuration,
        _current_version: &SemanticRestateVersion,
        _is_store_empty: bool,
    ) -> bool {
        config.common.experimental.is_status_gauges_reset_enabled()
    }

    fn is_enabled(features: &StorageFeatures) -> bool {
        features.is_track_status_gauges
    }

    fn set_enabled(features: &mut StorageFeatures) {
        features.is_track_status_gauges = true;
    }

    async fn enable(
        storage: &mut PartitionStore,
        cancel: &CancellationToken,
        _config: &Configuration,
        _finalization: &mut WriteBatch,
    ) -> Result<(), MigrationError> {
        backfill::run(storage, cancel).await
    }

    async fn disable(
        storage: &mut PartitionStore,
        _cancel: &CancellationToken,
        _config: &Configuration,
        finalization: &mut WriteBatch,
        features: &mut StorageFeatures,
    ) -> Result<(), MigrationError> {
        // Also clear partial output from an interrupted, unmarked backfill.
        backfill::append_reset(storage.partition_db(), finalization)
            .map_err(StorageError::Generic)?;
        features.is_track_status_gauges = false;
        Ok(())
    }
}
