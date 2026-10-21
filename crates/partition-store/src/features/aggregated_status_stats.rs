// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Opt-in before v1.9, automatic from v1.9, and persisted once enabled.
//! Initial activation requires an empty store until the backfill is implemented:
//! decrementing an uncounted existing entry would invalidate its unsigned gauge.

use rocksdb::WriteBatch;
use tokio_util::sync::CancellationToken;

use restate_storage_api::StorageError;
use restate_types::config::Configuration;
use restate_types::{RESTATE_VERSION_1_8_1, RESTATE_VERSION_1_9_0, SemanticRestateVersion};
use restate_util_string::ReString;

use crate::{MigrationError, PartitionStore};

use super::{StorageFeature, StorageFeatures};

impl StorageFeature for super::TrackStatusGaugesFeature {
    fn persisted_name() -> &'static ReString {
        const NAME: &ReString = &ReString::from_static("status-gauges");
        NAME
    }

    fn min_required_version() -> &'static SemanticRestateVersion {
        &RESTATE_VERSION_1_8_1
    }

    fn should_enable(
        config: &Configuration,
        current_version: &SemanticRestateVersion,
        is_store_empty: bool,
    ) -> bool {
        // This restriction is temporary and gets removed later in the commit stack
        // when backfill is implemented.
        is_store_empty
            && (current_version.is_equal_or_newer_than(&RESTATE_VERSION_1_9_0)
                || config.common.experimental.is_track_status_gauges_enabled())
    }

    fn is_enabled(features: &StorageFeatures) -> bool {
        features.is_track_status_gauges
    }

    fn set_enabled(features: &mut StorageFeatures) {
        features.is_track_status_gauges = true;
    }

    async fn enable(
        _storage: &mut PartitionStore,
        _cancel: &CancellationToken,
        _config: &Configuration,
        _finalization: &mut WriteBatch,
    ) -> Result<(), MigrationError> {
        // Empty stores skip this hook. Nonempty stores need a backfill before
        // publishing the feature marker and starting incremental maintenance.
        Err(StorageError::Generic(anyhow::anyhow!(
            "status gauges require a backfill before enabling on a nonempty store"
        ))
        .into())
    }
}
