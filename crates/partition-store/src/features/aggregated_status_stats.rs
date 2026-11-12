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

use rocksdb::WriteBatch;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use restate_rocksdb::{IoMode, Priority};
use restate_storage_api::StorageError;
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::{RESTATE_VERSION_1_8_2, SemanticRestateVersion};
use restate_util_string::ReString;

use crate::keys::KeyKind;
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

impl PartitionStore {
    /// Deletes the legacy status gauges and their marker.
    ///
    /// One range delete covers every key of the retired [`KeyKind::LegacyStats`]
    /// kind that belongs to this partition, whatever its statistic ID,
    /// aggregation kind or payload; nothing else ever used that kind. The delete
    /// is written at every open without looking for keys first: it is cheap and
    /// idempotent, so an open that comes before an earlier deletion was flushed
    /// simply repeats it. Nothing is flushed here; the deletion becomes durable
    /// with the next regular memtable flush.
    pub(crate) async fn remove_legacy_status_gauges(
        &mut self,
        features: &mut LoadedStorageFeatures,
    ) -> Result<(), MigrationError> {
        let db = self.partition_db().clone();
        let partition_id = db.partition().id();
        let (start, end) = legacy_status_gauge_range(partition_id);
        let mut batch = WriteBatch::default();
        let marker = features.append_legacy_status_gauges_removal(
            db.cf_handle(),
            &mut batch,
            partition_id,
        )?;
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
        if marker {
            features.mark_persisted();
            self.set_storage_features(*features.enabled());
            warn!(
                partition_id = %partition_id,
                "Deleted the status gauges kept before the index and statistics registry. \
                 Enable experimental-enable-complete-status-gauges to rebuild them."
            );
        }
        Ok(())
    }
}

/// The key range holding every legacy status gauge of `partition_id`: from the
/// partition's prefix up to, excluding, the next prefix in byte order.
fn legacy_status_gauge_range(partition_id: PartitionId) -> (Vec<u8>, Vec<u8>) {
    let start = legacy_status_gauge_prefix(partition_id);
    // The prefix plus one as a big-endian integer. The padding bytes are zero,
    // so a carry never reaches the key kind.
    let mut end = start.clone();
    for byte in end.iter_mut().rev() {
        if *byte == u8::MAX {
            *byte = 0;
        } else {
            *byte += 1;
            break;
        }
    }
    (start, end)
}

/// `key kind (2) | padding (2) | partition id (2)` of the retired layout.
#[allow(deprecated)]
fn legacy_status_gauge_prefix(partition_id: PartitionId) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(6);
    prefix.extend_from_slice(KeyKind::LegacyStats.as_bytes());
    prefix.extend_from_slice(&[0, 0]);
    prefix.extend_from_slice(&(*partition_id).to_be_bytes());
    prefix
}

/// A legacy status gauge key of the retired layout, `key kind (2) | padding (2) |
/// partition id (2) | statistic id (2) | aggregation kind (1) | reserved (1)`,
/// for tests that seed such keys.
#[cfg(test)]
pub(crate) fn legacy_status_gauge_key(partition_id: PartitionId, id: u16) -> Vec<u8> {
    let mut key = legacy_status_gauge_prefix(partition_id);
    key.extend_from_slice(&id.to_be_bytes());
    key.push(crate::stats::StatKind::StageStatusBucketedGauge as u8);
    key.push(0);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_range_covers_exactly_one_partition() {
        let (start, end) = legacy_status_gauge_range(PartitionId::from(7));
        assert_eq!(start, b"zS\x00\x00\x00\x07");
        assert_eq!(end, b"zS\x00\x00\x00\x08");
        let key = legacy_status_gauge_key(PartitionId::from(7), 3);
        assert!(start.as_slice() <= key.as_slice() && key.as_slice() < end.as_slice());
        assert!(legacy_status_gauge_key(PartitionId::from(8), 1).as_slice() >= end.as_slice());

        // The last partition's range ends in the padding, never in the key kind.
        let (start, end) = legacy_status_gauge_range(PartitionId::from(u16::MAX));
        assert_eq!(start, b"zS\x00\x00\xff\xff");
        assert_eq!(end, b"zS\x00\x01\x00\x00");
    }
}
