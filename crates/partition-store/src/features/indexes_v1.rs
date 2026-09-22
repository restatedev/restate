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
use tracing::warn;

use restate_types::config::Configuration;
use restate_types::{RESTATE_VERSION_1_8_0, SemanticRestateVersion};
use restate_util_string::ReString;

use crate::{MigrationError, PartitionStore};

use super::{StorageFeature, StorageFeatures};

impl StorageFeature for super::IndexesV1Feature {
    fn persisted_name() -> &'static ReString {
        const NAME: &ReString = &ReString::from_static("indexes-v1");
        NAME
    }

    fn min_required_version() -> &'static SemanticRestateVersion {
        &RESTATE_VERSION_1_8_0
    }

    fn should_enable(
        config: &Configuration,
        _current_version: &SemanticRestateVersion,
        _is_store_empty: bool,
    ) -> bool {
        config.common.experimental.is_indexes_v1_enabled()
    }

    fn is_enabled(features: &StorageFeatures) -> bool {
        features.is_indexes_v1
    }

    fn set_enabled(features: &mut StorageFeatures) {
        features.is_indexes_v1 = true;
    }

    async fn enable(
        _storage: &mut PartitionStore,
        _cancel: &CancellationToken,
        _config: &Configuration,
        _finalization: &mut WriteBatch,
    ) -> Result<(), MigrationError> {
        warn!("!! IndexesV1 feature backfill is not yet implemented !!");
        Ok(())
    }
}
