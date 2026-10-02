// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_rocksdb::{Priority, RocksDbReadPerfGuard};
use restate_storage_api::output_table::{
    ReadInvocationOutputTable, ScanInvocationOutputTable, ScanOutputTableRange,
    WriteInvocationOutputTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::invocation::ResponseResult;
use restate_types::sharding::{PartitionKey, WithPartitionKey};

use crate::TableKind::InvocationOutput;
use crate::error::break_on_err;
use crate::keys::{DecodeTableKey, KeyKind, define_table_key};
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan};

define_table_key!(
    InvocationOutput,
    KeyKind::InvocationOutput,
    InvocationOutputKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

fn put_output<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    output_message: &ResponseResult,
) -> Result<()> {
    let key = InvocationOutputKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
    };

    storage.put_kv_proto(key, output_message)
}

fn delete_output<S: StorageAccess>(storage: &mut S, invocation_id: &InvocationId) -> Result<()> {
    let key = InvocationOutputKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
    };

    storage.delete_key(&key)
}

fn get_invocation_output<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<Option<ResponseResult>> {
    let _x = RocksDbReadPerfGuard::new("get-output");
    let outbox_key = InvocationOutputKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
    };

    storage.get_value_proto(outbox_key)
}

impl ReadInvocationOutputTable for PartitionStore {
    async fn get_invocation_output(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Option<ResponseResult>> {
        get_invocation_output(self, invocation_id)
    }
}

impl ScanInvocationOutputTable for PartitionStore {
    fn for_each_output<
        F: FnMut((InvocationId, ResponseResult)) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: ScanOutputTableRange,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        let scan = match range {
            ScanOutputTableRange::PartitionKey(partition_key) => {
                TableScan::ScanPartitionKeyRange::<InvocationOutputKeyBuilder>(partition_key)
            }
            ScanOutputTableRange::InvocationId(invocation_id) => {
                let start = InvocationOutputKey::builder()
                    .partition_key(invocation_id.start().partition_key())
                    .invocation_uuid(invocation_id.start().invocation_uuid());

                let end = InvocationOutputKey::builder()
                    .partition_key(invocation_id.end().partition_key())
                    .invocation_uuid(invocation_id.end().invocation_uuid());

                TableScan::RangeInclusive(start, end)
            }
        };

        self.iterator_for_each(
            "df-invocation-output",
            Priority::Low,
            scan,
            move |(mut key, mut value)| {
                let output_key = break_on_err(InvocationOutputKey::deserialize_from(&mut key))?;
                let (partition_key, invocation_uuid) = output_key.split();
                let output = break_on_err(ResponseResult::decode(&mut value))?;

                f((
                    InvocationId::from_parts(partition_key, invocation_uuid),
                    output,
                ))
                .map_break(Ok)
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadInvocationOutputTable for PartitionStoreTransaction<'_> {
    async fn get_invocation_output(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Option<ResponseResult>> {
        get_invocation_output(self, invocation_id)
    }
}

impl WriteInvocationOutputTable for PartitionStoreTransaction<'_> {
    fn put_invocation_output(
        &mut self,
        invocation_id: &InvocationId,
        result: &ResponseResult,
    ) -> Result<()> {
        put_output(self, invocation_id, result)
    }

    fn delete_invocation_output(&mut self, invocation_id: &InvocationId) -> Result<()> {
        delete_output(self, invocation_id)
    }
}
