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
use crate::promise_table::PromiseKey;
use crate::scan::{PhysicalScan, TableScan};

use super::{MigrationContext, MigrationError};

/// Length of a `KeyKind | partition_key` prefix.
const KEY_PREFIX_LEN: usize = KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>();

/// Scan the unscoped promise table and copy every entry into the scoped
/// promise table with `scope = None`. The value bytes are copied through
/// unchanged.
///
/// We use direct rocksdb access because no async operations are needed.
pub fn migrate_to_scoped_promise_table(
    ctx: &mut MigrationContext<'_>,
) -> Result<(), MigrationError> {
    let rocks = ctx.partition_db.rocksdb();
    let key_range = ctx.key_range;
    let mut counter = 0;

    let mut iterator = ctx.partition_db.scan(
        PhysicalScan::from(
            TableScan::ScanPartitionKeyRange::<PromiseKey>(key_range),
            &mut ctx.arena,
        ),
        rocksdb::ReadOptions::default(),
    )?;
    iterator.seek_to_first();

    // 1 MiB batches
    let mut wb = WriteBatch::with_capacity_bytes(1024 * 1024);
    ctx.arena.clear();

    let mut opts = rocksdb::WriteOptions::default();
    // We disable WAL since bifrost is our durable distributed log.
    opts.disable_wal(true);

    while iterator.valid() {
        ctx.fail_if_cancelled()?;
        // safe to unwrap because the iterator is valid
        let (mut key, value) = iterator.item().unwrap();
        // Advance past the legacy `KeyKind::Promise` prefix and the partition_key.
        // The remaining bytes are the wire-identical suffix
        // (service_name | service_key | key) shared with `ScopedPromiseKey`.
        let kind = KeyKind::deserialize(&mut key)?;
        debug_assert_eq!(kind, KeyKind::Promise);
        let partition_key: PartitionKey = crate::keys::deserialize(&mut key)?;

        KeyKind::ScopedPromise.serialize(&mut ctx.arena);
        crate::keys::serialize(&partition_key, &mut ctx.arena);
        // unscoped
        ctx.arena.put_u8(b'u');
        ctx.arena.put_slice(key);
        let new_key = ctx.arena.split().freeze();

        wb.put_cf(ctx.partition_db.cf_handle(), new_key, value);
        counter += 1;

        // non-scientific threshold to trigger the commit.
        if wb.size_in_bytes() >= 800 {
            rocks
                .inner()
                .write_batch(&wb, &opts)
                .context("failed to write batch")
                .map_err(StorageError::Generic)?;
            wb.clear();
        }

        iterator.next();
    }

    // ensures we didn't stop because of an iterator error
    iterator
        .status()
        .context("iterating over promises")
        .map_err(StorageError::Generic)?;

    // just in case!
    if !wb.is_empty() {
        // commit, including the last batch of records
        rocks
            .inner()
            .write_batch(&wb, &opts)
            .context("failed to write batch")
            .map_err(StorageError::Generic)?;
    }

    debug!("Finished migrating {} promises", counter);

    Ok(())
}

/// Appends a `delete_range_cf` for the legacy unscoped promise range to `wb`.
///
/// The caller is responsible for committing `wb`. Bundling the range delete
/// with the schema-version bump in a single [`WriteBatch`] keeps the two
/// changes atomic with respect to RocksDB's memtable / SST flush.
pub fn append_delete_promise_data(ctx: &MigrationContext<'_>, wb: &mut WriteBatch) {
    let mut start_key_buf = [0u8; KEY_PREFIX_LEN];
    EncodeTableKeyPrefix::serialize_to(
        &PromiseKey::builder().partition_key(ctx.key_range.start()),
        &mut start_key_buf.as_mut(),
    );

    let mut end_key_buf = [0u8; KEY_PREFIX_LEN];
    EncodeTableKeyPrefix::serialize_to(
        &PromiseKey::builder().partition_key(ctx.key_range.end()),
        &mut end_key_buf.as_mut(),
    );
    // End key is exclusive in delete range, so the end prefix is one byte
    // beyond the max partition key on this key kind prefix.
    let success = crate::convert_to_upper_bound(&mut end_key_buf);
    assert!(success, "end key overflowed");
    wb.delete_range_cf(ctx.partition_db.cf_handle(), start_key_buf, end_key_buf);
}

#[cfg(test)]
#[path = "../tests/migrations_test/migrate_to_scoped_promise_table.rs"]
mod tests;
