// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The status gauges kept before the registry, under static statistic IDs.
//!
//! Registered instances replace them (`crate::vqueue_table::STATUS_GAUGES`):
//! nothing maintains or reads the legacy keys any more, and this feature is never
//! enabled again. Opening a partition deletes the legacy keys and the feature's
//! marker once the store passed every compatibility check, before any other
//! change, so they never coexist with an active family.

use rocksdb::{ReadOptions, WriteBatch};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use zerocopy::IntoBytes;

use restate_rocksdb::{IoMode, Priority};
use restate_storage_api::StorageError;
use restate_types::config::Configuration;
use restate_types::{RESTATE_VERSION_1_8_2, SemanticRestateVersion};
use restate_util_string::ReString;

use crate::stats::{StatId, StatKeyPrefix, StatKind};
use crate::{MigrationError, PartitionStore};

use super::{LoadedStorageFeatures, StorageFeature, StorageFeatures};

impl StorageFeature for super::TrackStatusGaugesFeature {
    fn persisted_name() -> &'static ReString {
        const NAME: &ReString = &ReString::from_static("status-gauges");
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
        Err(StorageError::Generic(anyhow::anyhow!(
            "the legacy status gauges are retired; registered instances replace them"
        ))
        .into())
    }
}

/// The legacy status gauges' static IDs are contiguous, so one range covers them.
const _: () = assert!(StatId::VirtualObjectStats as u16 == StatId::ServiceStats as u16 + 2);

impl PartitionStore {
    /// Deletes the legacy status gauges and their marker, if either is present.
    /// The gauges are this partition's keys under the static IDs 1–3
    /// ([`StatId`]), whatever their aggregation kind, reserved byte and payload;
    /// other legacy IDs are left alone. Marker and keys go in one batch.
    ///
    /// The deletion is durable when this returns, including one written but not
    /// flushed by an interrupted earlier attempt on the same store: this attempt
    /// sees nothing left to delete, so it flushes regardless. Flushing nothing is
    /// a no-op.
    pub(crate) async fn remove_legacy_status_gauges(
        &mut self,
        features: &mut LoadedStorageFeatures,
    ) -> Result<(), MigrationError> {
        let db = self.partition_db().clone();
        // The eight-byte identity of ID `id`: key kind, padded partition, ID.
        let identity = |id: u16| {
            StatKeyPrefix::new(db.partition().id(), id, StatKind::AggregatedGauge).as_bytes()
                [..LEGACY_IDENTITY_LENGTH]
                .to_vec()
        };
        let start = identity(StatId::ServiceStats.as_u16());
        let end = identity(StatId::VirtualObjectStats.as_u16() + 1);

        let has_keys = {
            let mut options = ReadOptions::default();
            options.set_total_order_seek(true);
            options.set_iterate_range(start.clone()..end.clone());
            let mut iterator = db
                .rocksdb()
                .inner()
                .as_raw_db()
                .raw_iterator_cf_opt(db.cf_handle(), options);
            iterator.seek_to_first();
            iterator
                .status()
                .map_err(|err| StorageError::Generic(err.into()))?;
            iterator.valid()
        };
        let mut batch = WriteBatch::default();
        let marker = features.append_legacy_status_gauges_removal(
            db.cf_handle(),
            &mut batch,
            db.partition().id(),
        )?;
        let removed = marker || has_keys;
        if removed {
            batch.delete_range_cf(db.cf_handle(), &start, &end);
            let mut options = rocksdb::WriteOptions::default();
            options.disable_wal(true);
            db.rocksdb()
                .write_batch(
                    "legacy-status-gauges-removal",
                    Priority::High,
                    IoMode::Default,
                    options,
                    batch,
                )
                .await
                .map_err(|err| StorageError::Generic(err.into()))?;
        }
        db.flush_memtables(true)
            .await
            .map_err(|err| StorageError::Generic(err.into()))?;
        if !removed {
            return Ok(());
        }
        if marker {
            features.mark_persisted();
        }
        self.set_storage_features(*features.enabled());
        warn!(
            partition_id = %db.partition().id(),
            "Deleted the status gauges kept before the index and statistics registry. \
             Enable experimental-enable-complete-status-gauges to rebuild them."
        );
        Ok(())
    }
}

/// Key kind, padded partition and static ID: everything before the aggregation kind.
const LEGACY_IDENTITY_LENGTH: usize = 8;
