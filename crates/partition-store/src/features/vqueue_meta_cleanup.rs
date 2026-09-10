// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use anyhow::Context;
use bilrost::BorrowedMessage;
use bytes::BytesMut;
use rocksdb::{ReadOptions, WriteBatch, WriteOptions};
use tokio_util::sync::CancellationToken;
use tracing::info;

use restate_rocksdb::{Priority, StorageTaskKind};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::VQueueMetaRef;
use restate_types::config::Configuration;
use restate_types::sharding::KeyRange;
use restate_types::{RESTATE_VERSION_1_7_10, RESTATE_VERSION_1_8_0, SemanticRestateVersion};
use restate_util_string::ReString;
use restate_util_time::DurationExt;

use super::{StorageFeature, StorageFeatures, VqueueMetadataCleanupV1Feature};
use crate::migrations::MigrationError;
use crate::scan::{PhysicalScan, TableScan};
use crate::vqueue_table::MetaKey;
use crate::{PartitionDb, PartitionStore, convert_to_upper_bound};

static VQUEUE_METADATA_CLEANUP_V1: ReString = ReString::from_static("vqueue-metadata-cleanup-v1");

#[cfg(not(test))]
const MAX_BATCH_SIZE: usize = 1024 * 1024;
#[cfg(test)]
const MAX_BATCH_SIZE: usize = 1;
#[cfg(not(test))]
const MAX_ROWS_PER_CHUNK: usize = 100_000;
#[cfg(test)]
const MAX_ROWS_PER_CHUNK: usize = 3;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

impl StorageFeature for VqueueMetadataCleanupV1Feature {
    fn persisted_name() -> &'static ReString {
        &VQUEUE_METADATA_CLEANUP_V1
    }

    fn min_required_version() -> &'static SemanticRestateVersion {
        &RESTATE_VERSION_1_7_10
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
                    .is_vqueue_obsolete_cleanup_enabled()
        } else {
            config
                .common
                .experimental
                .is_vqueue_obsolete_cleanup_enabled()
        }
    }

    fn is_enabled(features: &StorageFeatures) -> bool {
        features.is_vqueue_metadata_cleanup_v1
    }

    fn set_enabled(features: &mut StorageFeatures) {
        features.is_vqueue_metadata_cleanup_v1 = true;
    }

    async fn enable(
        storage: &mut PartitionStore,
        cancel: &CancellationToken,
        _config: &Configuration,
        _finalization: &mut WriteBatch,
    ) -> Result<(), MigrationError> {
        purge_obsolete_vqueue_metadata(storage, cancel).await
    }
}

async fn purge_obsolete_vqueue_metadata(
    storage: &PartitionStore,
    cancel: &CancellationToken,
) -> Result<(), MigrationError> {
    let started = Instant::now();
    let mut last_reported = Instant::now();
    let mut scanned = 0u64;
    let mut purged = 0u64;
    let partition_db = storage.partition_db().clone();
    let key_range = storage.partition_key_range();
    let rocks = partition_db.rocksdb().clone();
    let mut next_key = None;

    loop {
        let chunk = rocks
            .clone()
            .run_background_read_op(
                "purge-obsolete-vqueue-metadata",
                StorageTaskKind::BackgroundIterator,
                Priority::Low,
                {
                    let partition_db = partition_db.clone();
                    let cancel = cancel.clone();
                    move |_| purge_chunk(&partition_db, key_range, next_key, &cancel)
                },
            )
            .await
            .map_err(|_| StorageError::OperationalError)??;

        scanned += chunk.scanned;
        purged += chunk.purged;

        if scanned > 0 && last_reported.elapsed() >= PROGRESS_INTERVAL {
            info!(
                scanned,
                purged,
                elapsed = %started.elapsed().friendly(),
                "Purging obsolete vqueue metadata"
            );
            last_reported = Instant::now();
        }

        let Some(key) = chunk.next_key else {
            break;
        };
        next_key = Some(key);
    }

    if purged > 0 {
        info!(
            scanned,
            purged,
            elapsed = %started.elapsed().friendly(),
            "Finished purging obsolete vqueue metadata"
        );
    }
    Ok(())
}

struct CleanupChunk {
    next_key: Option<Vec<u8>>,
    scanned: u64,
    purged: u64,
}

fn purge_chunk(
    partition_db: &PartitionDb,
    key_range: KeyRange,
    start_key: Option<Vec<u8>>,
    cancel: &CancellationToken,
) -> Result<CleanupChunk, MigrationError> {
    let rocks = partition_db.rocksdb();
    let cf = partition_db.cf_handle();
    let mut arena = BytesMut::new();
    let mut read_options = ReadOptions::default();
    read_options.fill_cache(false);
    let mut iterator = partition_db.scan(
        PhysicalScan::from(
            TableScan::ScanPartitionKeyRange::<MetaKey>(key_range),
            &mut arena,
        ),
        read_options,
    )?;
    if let Some(start_key) = start_key.as_deref() {
        iterator.seek(start_key);
    }

    let mut write_options = WriteOptions::default();
    write_options.disable_wal(true);
    let mut write_batch = WriteBatch::with_capacity_bytes(MAX_BATCH_SIZE);
    let mut last_key = Vec::with_capacity(MetaKey::serialized_length_fixed());
    let mut scanned = 0u64;
    let mut purged = 0u64;
    let mut pending_purges = 0u64;

    while iterator.valid() && scanned < MAX_ROWS_PER_CHUNK as u64 {
        if cancel.is_cancelled() {
            return Err(MigrationError::MigrationCancelled);
        }

        let (key, value) = iterator.item().expect("valid iterator has an item");
        let meta = VQueueMetaRef::decode_borrowed(value).map_err(StorageError::BilrostDecode)?;
        last_key.clear();
        last_key.extend_from_slice(key);
        scanned += 1;

        if meta.is_obsolete() {
            write_batch.delete_cf(cf, key);
            pending_purges += 1;
        }
        iterator.next();

        if write_batch.size_in_bytes() >= MAX_BATCH_SIZE {
            rocks
                .inner()
                .write_batch(&write_batch, &write_options)
                .context("failed to delete obsolete vqueue metadata batch")
                .map_err(StorageError::Generic)?;
            write_batch.clear();
            purged += pending_purges;
            pending_purges = 0;
        }
    }

    iterator
        .status()
        .context("iterating over vqueue metadata")
        .map_err(StorageError::Generic)?;
    let next_key = iterator.valid().then(|| {
        assert!(
            convert_to_upper_bound(&mut last_key),
            "vqueue metadata key must have a successor"
        );
        last_key
    });
    drop(iterator);

    if !write_batch.is_empty() {
        rocks
            .inner()
            .write_batch(&write_batch, &write_options)
            .context("failed to delete final obsolete vqueue metadata batch")
            .map_err(StorageError::Generic)?;
        purged += pending_purges;
    }

    Ok(CleanupChunk {
        next_key,
        scanned,
        purged,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_respects_version_and_config() {
        assert_eq!(
            VqueueMetadataCleanupV1Feature::min_required_version(),
            &*RESTATE_VERSION_1_7_10
        );

        let mut config = Configuration::default();
        assert!(!VqueueMetadataCleanupV1Feature::should_enable(
            &config,
            &SemanticRestateVersion::new(1, 7, 9),
            false,
        ));
        assert!(!VqueueMetadataCleanupV1Feature::should_enable(
            &config,
            &RESTATE_VERSION_1_7_10,
            false,
        ));
        config.common.experimental.set_vqueue_obsolete_cleanup(true);
        assert!(VqueueMetadataCleanupV1Feature::should_enable(
            &config,
            &RESTATE_VERSION_1_7_10,
            false,
        ));

        let config = Configuration::default();
        assert!(!VqueueMetadataCleanupV1Feature::should_enable(
            &config,
            &RESTATE_VERSION_1_8_0,
            false,
        ));
        assert!(VqueueMetadataCleanupV1Feature::should_enable(
            &config,
            &RESTATE_VERSION_1_8_0,
            true,
        ));
    }
}
