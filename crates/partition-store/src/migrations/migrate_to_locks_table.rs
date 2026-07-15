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
use rocksdb::WriteBatch;

use restate_storage_api::StorageError;
use restate_types::sharding::PartitionKey;

use crate::keys::{EncodeTableKeyPrefix, KeyKind};
use crate::service_status_table::ServiceStatusKey;

use super::MigrationContext;

/// Deletes old service statuses.
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
