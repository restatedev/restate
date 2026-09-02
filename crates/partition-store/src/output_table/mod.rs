// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_rocksdb::RocksDbReadPerfGuard;
use restate_storage_api::Result;
use restate_storage_api::output_table::{ReadOutputTable, WriteOutputTable};
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::invocation::ResponseResult;
use restate_types::sharding::{PartitionKey, WithPartitionKey};

use crate::TableKind::Output;
use crate::keys::{KeyKind, define_table_key};
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess};

define_table_key!(
    Output,
    KeyKind::Output,
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

fn get_output<S: StorageAccess>(
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

impl ReadOutputTable for PartitionStore {
    async fn get_output(&mut self, invocation_id: &InvocationId) -> Result<Option<ResponseResult>> {
        get_output(self, invocation_id)
    }
}

impl ReadOutputTable for PartitionStoreTransaction<'_> {
    async fn get_output(&mut self, invocation_id: &InvocationId) -> Result<Option<ResponseResult>> {
        get_output(self, invocation_id)
    }
}

impl WriteOutputTable for PartitionStoreTransaction<'_> {
    fn put_output(&mut self, invocation_id: &InvocationId, result: &ResponseResult) -> Result<()> {
        put_output(self, invocation_id, result)
    }

    fn delete_output(&mut self, invocation_id: &InvocationId) -> Result<()> {
        delete_output(self, invocation_id)
    }
}
