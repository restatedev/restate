// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::BytesMut;
use enum_map::{enum_map, EnumMap};
use restate_rocksdb::{CfName, RocksDb};
use restate_types::identifiers::PartitionId;
use rocksdb::{BoundColumnFamily, DBCompressionType};

use crate::TableKind;

pub const PARTITION_DATA_CF_PREFIX: &str = "partition-data-";
pub const PARTITION_KV_CF_PREFIX: &str = "partition-kv-";

pub struct PartitionScopedDb<'a> {
    partition_id: PartitionId,
    rocksdb: &'a Arc<RocksDb>,
    table_to_cf: EnumMap<TableKind, Arc<BoundColumnFamily<'a>>>,
    key_buffer: BytesMut,
    value_buffer: BytesMut,
}

impl<'a> Clone for PartitionScopedDb<'a> {
    fn clone(&self) -> Self {
        Self {
            partition_id: self.partition_id.clone(),
            rocksdb: self.rocksdb,
            table_to_cf: self.table_to_cf.clone(),
            key_buffer: BytesMut::default(),
            value_buffer: BytesMut::default(),
        }
    }
}

impl<'a> PartitionScopedDb<'a> {
    pub fn new(partition_id: PartitionId, rocksdb: &'a Arc<RocksDb>) -> Option<Self> {
        let data_cf = rocksdb.cf_handle(&data_cf_for_partition(&partition_id))?;
        let kv_cf = rocksdb.cf_handle(&kv_cf_for_partition(&partition_id))?;

        let table_to_cf = enum_map! {
        // optimized for point lookups
        TableKind::State => kv_cf.clone(),
        TableKind::ServiceStatus => kv_cf.clone(),
        TableKind::InvocationStatus => kv_cf.clone(),
        TableKind::Idempotency => kv_cf.clone(),
        TableKind::Deduplication => kv_cf.clone(),
        TableKind::PartitionStateMachine => kv_cf.clone(),
        // optimized for prefix scan lookups
        TableKind::Journal => data_cf.clone(),
        TableKind::Inbox => data_cf.clone(),
        TableKind::Outbox => data_cf.clone(),
        TableKind::Timers => data_cf.clone(),
        };

        Some(Self {
            partition_id,
            rocksdb,
            table_to_cf,
            key_buffer: BytesMut::default(),
            value_buffer: BytesMut::default(),
        })
    }

    pub(crate) fn table_handle(&self, table_kind: TableKind) -> &Arc<BoundColumnFamily> {
        &self.table_to_cf[table_kind]
    }
}

pub(crate) fn data_cf_for_partition(partition_id: &PartitionId) -> CfName {
    CfName::from(format!("{}{}", PARTITION_DATA_CF_PREFIX, partition_id))
}

pub(crate) fn kv_cf_for_partition(partition_id: &PartitionId) -> CfName {
    CfName::from(format!("{}{}", PARTITION_KV_CF_PREFIX, partition_id))
}

pub(crate) fn partition_ids_to_cfs(partition_ids: &[PartitionId]) -> Vec<CfName> {
    partition_ids
        .iter()
        .map(data_cf_for_partition)
        .chain(partition_ids.iter().map(kv_cf_for_partition))
        .collect()
}

pub(crate) fn data_cf_options(mut cf_options: rocksdb::Options) -> rocksdb::Options {
    // Most of the changes are highly temporal, we try to delay flushing
    // As much as we can to increase the chances to observe a deletion.
    //
    cf_options.set_max_write_buffer_number(3);
    cf_options.set_min_write_buffer_number_to_merge(2);
    //
    // Set compactions per level
    //
    cf_options.set_num_levels(7);
    cf_options.set_compression_per_level(&[
        DBCompressionType::None,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Zstd,
    ]);

    cf_options
}

pub(crate) fn kv_cf_options(mut cf_options: rocksdb::Options) -> rocksdb::Options {
    // Most of the changes are highly temporal, we try to delay flushing
    // As much as we can to increase the chances to observe a deletion.
    //
    cf_options.set_max_write_buffer_number(3);
    cf_options.set_min_write_buffer_number_to_merge(2);
    //
    // Set compactions per level
    //
    cf_options.set_num_levels(7);
    cf_options.set_compression_per_level(&[
        DBCompressionType::None,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Zstd,
    ]);

    cf_options
}
