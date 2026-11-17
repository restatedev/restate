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

use restate_storage_api::StorageError;
use restate_types::config::Configuration;
use restate_types::{RESTATE_VERSION_1_8_2, SemanticRestateVersion};
use restate_util_string::ReString;

use crate::fsm_table::{
    get_storage_features_from_partition_db, get_storage_version_from_partition_db,
    put_storage_features,
};
use crate::{MigrationError, PartitionDb, PartitionStore, PartitionStoreTransaction};

use super::{JcOrphanCleanupV1Feature, LoadedStorageFeatures, StorageFeature, StorageFeatures};

impl StorageFeature for JcOrphanCleanupV1Feature {
    fn persisted_name() -> &'static ReString {
        const NAME: &ReString = &ReString::from_static("jc-orphan-cleanup-v1");
        NAME
    }

    fn min_required_version() -> &'static SemanticRestateVersion {
        &RESTATE_VERSION_1_8_2
    }

    fn should_enable(_: &Configuration, _: &SemanticRestateVersion, _: bool) -> bool {
        // Completion evidence, not an automatic startup migration.
        false
    }

    fn is_enabled(features: &StorageFeatures) -> bool {
        features.is_jc_orphan_cleanup_v1
    }

    fn set_enabled(features: &mut StorageFeatures) {
        features.is_jc_orphan_cleanup_v1 = true;
    }

    async fn enable(
        _: &mut PartitionStore,
        _: &CancellationToken,
        _: &Configuration,
        _: &mut WriteBatch,
    ) -> Result<(), MigrationError> {
        Ok(())
    }
}

/// Stage completion with the final slice, returning the feature bits to publish
/// only after the commit succeeds.
pub(crate) fn stage_completion(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
) -> Result<StorageFeatures, StorageError> {
    let features = completion_features(db)?;
    put_storage_features(tx, db.partition().id(), features.ledger())?;
    Ok(*features.enabled())
}

fn completion_features(db: &PartitionDb) -> Result<LoadedStorageFeatures, StorageError> {
    let mut features = LoadedStorageFeatures::load(
        get_storage_features_from_partition_db(db)?,
        get_storage_version_from_partition_db(db)?,
        SemanticRestateVersion::current(),
    )
    .map_err(|err| StorageError::Generic(err.into()))?;
    features.set_enabled::<JcOrphanCleanupV1Feature>(Some(SemanticRestateVersion::current()));
    Ok(features)
}
