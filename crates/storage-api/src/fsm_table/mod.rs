// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{protobuf_storage_encode_decode, Result};
use restate_types::identifiers::PartitionId;
use restate_types::storage::{StorageDecode, StorageEncode};
use std::future::Future;

#[derive(Debug, Clone, Copy, derive_more::From, derive_more::Into)]
pub struct SequenceNumber(u64);

protobuf_storage_encode_decode!(SequenceNumber);

pub trait ReadOnlyFsmTable {
    fn get<T>(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
    ) -> impl Future<Output = Result<Option<T>>> + Send
    where
        T: StorageDecode;
}

pub trait FsmTable: ReadOnlyFsmTable {
    fn put(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
        state_value: impl StorageEncode,
    ) -> impl Future<Output = ()> + Send;

    fn clear(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
    ) -> impl Future<Output = ()> + Send;
}
