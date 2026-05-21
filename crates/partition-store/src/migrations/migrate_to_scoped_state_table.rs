// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use bytes::BufMut;
use rocksdb::WriteBatch;
use tracing::debug;

use restate_storage_api::StorageError;
use restate_types::sharding::PartitionKey;

use crate::keys::{EncodeTableKeyPrefix, KeyKind};
use crate::scan::{PhysicalScan, TableScan};
use crate::state_table::StateKey;

use super::MigrationContext;

/// Scan the unscoped state table and copy every entry into the scoped state
/// table with `scope = None`. The value bytes are copied through unchanged.
///
/// We use direct rocksdb access because no async operations are needed.
#[allow(dead_code)]
pub fn migrate_to_scoped_state_table(ctx: &mut MigrationContext<'_>) -> Result<(), StorageError> {
    let rocks = ctx.partition_db.rocksdb();
    let key_range = ctx.key_range;
    let mut counter = 0;

    let mut iterator = ctx.partition_db.scan(PhysicalScan::from(
        TableScan::FullScanPartitionKeyRange::<StateKey>(key_range),
        &mut ctx.arena,
    ))?;
    iterator.seek_to_first();

    // 1 MiB batches
    let mut wb = WriteBatch::with_capacity_bytes(1024 * 1024);
    let arena = &mut ctx.arena;
    arena.clear();

    let mut opts = rocksdb::WriteOptions::default();
    // We disable WAL since bifrost is our durable distributed log.
    opts.disable_wal(true);

    while iterator.valid() {
        // safe to unwrap because the iterator is valid
        let (mut key, value) = iterator.item().unwrap();
        // Advance past the legacy `KeyKind::State` prefix and the partition_key.
        // The remaining bytes are the wire-identical suffix
        // (service_name | service_key | state_key) shared with `ScopedStateKey`.
        let kind = KeyKind::deserialize(&mut key)?;
        debug_assert_eq!(kind, KeyKind::State);
        let partition_key: PartitionKey = crate::keys::deserialize(&mut key)?;

        KeyKind::ScopedState.serialize(arena);
        crate::keys::serialize(&partition_key, arena);
        // unscoped
        arena.put_u8(b'u');
        arena.put_slice(key);
        let new_key = arena.split().freeze();

        wb.put_cf(ctx.partition_db.cf_handle(), new_key, value);
        counter += 1;

        // non-scientific threshold to trigger the commit.
        if wb.size_in_bytes() >= 800 {
            rocks
                .inner()
                .write_batch(&wb, &opts)
                .context("failed to write batch")?;
            wb.clear();
        }

        iterator.next();
    }

    // ensures we didn't stop because of an iterator error
    iterator
        .status()
        .context("iterating over state entries")
        .map_err(StorageError::Generic)?;

    // just in case!
    if !wb.is_empty() {
        // commit, including the last batch of records
        rocks
            .inner()
            .write_batch(&wb, &opts)
            .context("failed to write batch")?;
    }

    debug!("Finished migrating {} state entries", counter);

    Ok(())
}

/// Deletes the legacy unscoped state range.
#[allow(dead_code)]
pub fn delete_state_data(ctx: &mut MigrationContext<'_>) -> Result<(), StorageError> {
    let mut wb = WriteBatch::default();
    let mut opts = rocksdb::WriteOptions::default();
    // We disable WAL since bifrost is our durable distributed log.
    opts.disable_wal(true);

    let mut start_key_buf = [0u8; KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>()];
    EncodeTableKeyPrefix::serialize_to(
        &StateKey::builder().partition_key(ctx.key_range.start()),
        &mut start_key_buf.as_mut(),
    );

    let mut end_key_buf = [0u8; KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>()];
    EncodeTableKeyPrefix::serialize_to(
        &StateKey::builder().partition_key(ctx.key_range.end()),
        &mut end_key_buf.as_mut(),
    );
    // End key is exclusive in delete range, so the end prefix is one byte
    // beyond the max partition key on this key kind prefix.
    let success = crate::convert_to_upper_bound(&mut end_key_buf);
    assert!(success, "end key overflowed");
    wb.delete_range_cf(ctx.partition_db.cf_handle(), start_key_buf, end_key_buf);

    ctx.partition_db
        .rocksdb()
        .inner()
        .write_batch(&wb, &opts)
        .context("failed to write batch")?;

    Ok(())
}

#[cfg(test)]
#[path = "../tests/migrations_test/migrate_to_scoped_state_table.rs"]
mod tests;
