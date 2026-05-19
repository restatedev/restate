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
use bilrost::Message;
use bytes::BufMut;
use rocksdb::WriteBatch;
use tracing::debug;

use restate_storage_api::StorageError;
use restate_storage_api::lock_table::{AcquiredBy, LockState};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue as _;
use restate_storage_api::service_status_table::VirtualObjectStatus;
use restate_types::sharding::PartitionKey;

use crate::keys::{DecodeTableKey, EncodeTableKeyPrefix, KeyKind};
use crate::scan::{PhysicalScan, TableScan};
use crate::service_status_table::ServiceStatusKey;

use super::MigrationContext;

/// scan the sys_service_status table and migrate all entries that hold a lock
/// into the locks table. Once the migration is done.
///
/// We are using direct access to rocksdb because we are not performing any async operations.
#[allow(dead_code)]
pub fn migrate_to_locks_table(ctx: &mut MigrationContext<'_>) -> Result<(), StorageError> {
    let rocks = ctx.partition_db.rocksdb();
    let key_range = ctx.key_range;
    let mut counter = 0;

    let mut iterator = ctx.partition_db.scan(PhysicalScan::from(
        TableScan::FullScanPartitionKeyRange::<ServiceStatusKey>(key_range),
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
        let (mut key, mut value) = iterator.item().unwrap();
        let state_key = ServiceStatusKey::deserialize_from(&mut key)?;
        let state_value = VirtualObjectStatus::decode(&mut value)?;
        let (partition_key, service_name, service_key) = state_key.split();

        if let VirtualObjectStatus::Locked(invocation_id) = state_value {
            KeyKind::Lock.serialize(arena);
            crate::keys::serialize(&partition_key, arena);
            // unscoped
            arena.put_u8(b'u');
            // Encode the key:
            arena.put_slice(service_name.as_ref());
            arena.put_u8(b'/');
            arena.put_slice(service_key.as_ref());

            let key = arena.split().freeze();

            // Value
            let value = LockState {
                // We don't know the original acquisition time, so we use now()!
                acquired_at: ctx.clock.next(),
                acquired_by: AcquiredBy::InvocationId(invocation_id),
            };
            value.encode(arena).unwrap();
            let value = arena.split().freeze();

            wb.put_cf(ctx.partition_db.cf_handle(), key, value);
            counter += 1;
        }

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
        .context("iterating over service status")
        .map_err(StorageError::Generic)?;

    // just in case!
    if !wb.is_empty() {
        // commit, including the last batch of records
        rocks
            .inner()
            .write_batch(&wb, &opts)
            .context("failed to write batch")?;
    }

    debug!("Finished migrating {} locks", counter);

    Ok(())
}

/// Deletes old service statuses.
#[allow(dead_code)]
pub fn delete_service_status_data(ctx: &mut MigrationContext<'_>) -> Result<(), StorageError> {
    let mut wb = WriteBatch::default();
    let mut opts = rocksdb::WriteOptions::default();
    // We disable WAL since bifrost is our durable distributed log.
    opts.disable_wal(true);

    // Delete old service statuses.
    let mut start_key_buf = [0u8; KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>()];
    EncodeTableKeyPrefix::serialize_to(
        &ServiceStatusKey::builder().partition_key(ctx.key_range.start()),
        &mut start_key_buf.as_mut(),
    );

    let mut end_key_buf = [0u8; KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>()];
    EncodeTableKeyPrefix::serialize_to(
        &ServiceStatusKey::builder().partition_key(ctx.key_range.end()),
        &mut end_key_buf.as_mut(),
    );
    // End key is exclusive in delete range, so the end prefix is one byte
    // beyond the max partition key on this key kind prefix.
    let success = crate::convert_to_upper_bound(&mut end_key_buf);
    assert!(success, "end key overflowed");
    wb.delete_range_cf(ctx.partition_db.cf_handle(), start_key_buf, end_key_buf);

    // commit, including the last batch of records
    ctx.partition_db
        .rocksdb()
        .inner()
        .write_batch(&wb, &opts)
        .context("failed to write batch")?;

    Ok(())
}

#[cfg(test)]
#[path = "../tests/migrations_test/migrate_to_locks_table.rs"]
mod tests;
