// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{GetFuture, GetStream, PutFuture};
use bytes::Bytes;
use restate_types::identifiers::PartitionId;

pub trait FsmTable {
    fn get(&mut self, partition_id: PartitionId, state_id: u64) -> GetFuture<Option<Bytes>>;

    fn put(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
        state_value: impl AsRef<[u8]>,
    ) -> PutFuture;

    fn clear(&mut self, partition_id: PartitionId, state_id: u64) -> PutFuture;

    fn get_all_states(&mut self, partition_id: PartitionId) -> GetStream<(u64, Bytes)>;
}
