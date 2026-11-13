// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
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
use restate_types::{RESTATE_VERSION_1_8_2, SemanticRestateVersion};
use restate_util_string::ReString;

use crate::{MigrationError, PartitionStore};

use super::{StorageFeature, StorageFeatures};

/// A fence, not a switch: registry reconciliation persists it in the batch of the
/// first registration (`PartitionStore::reconcile_registry`). It never enables
/// itself and has nothing to migrate.
impl StorageFeature for super::IndexRegistryFeature {
    fn persisted_name() -> &'static ReString {
        const NAME: &ReString = &ReString::from_static("index-registry");
        NAME
    }

    fn min_required_version() -> &'static SemanticRestateVersion {
        &RESTATE_VERSION_1_8_2
    }

    fn should_enable(
        _config: &Configuration,
        _current_version: &SemanticRestateVersion,
        _is_store_empty: bool,
    ) -> bool {
        false
    }

    fn is_enabled(features: &StorageFeatures) -> bool {
        features.is_index_registry
    }

    fn set_enabled(features: &mut StorageFeatures) {
        features.is_index_registry = true;
    }

    async fn enable(
        _storage: &mut PartitionStore,
        _cancel: &CancellationToken,
        _config: &Configuration,
        _finalization: &mut WriteBatch,
    ) -> Result<(), MigrationError> {
        Ok(())
    }
}
