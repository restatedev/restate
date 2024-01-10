// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Result;
use bytes::Bytes;
use futures_util::Stream;
use restate_types::identifiers::PartitionId;
use std::future::Future;

pub trait ReadOnlyFsmTable {
    fn get(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send;

    fn get_all_states(
        &mut self,
        partition_id: PartitionId,
    ) -> impl Stream<Item = Result<(u64, Bytes)>> + Send;
}

pub trait FsmTable: ReadOnlyFsmTable {
    fn put(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
        state_value: impl AsRef<[u8]>,
    ) -> impl Future<Output = ()> + Send;

    fn clear(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
    ) -> impl Future<Output = ()> + Send;
}
