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
use restate_types::identifiers::PartitionId;

pub trait DeduplicationTable {
    fn get_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producing_partition_id: PartitionId,
    ) -> GetFuture<Option<u64>>;

    fn put_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producing_partition_id: PartitionId,
        sequence_number: u64,
    ) -> PutFuture;

    fn get_all_sequence_numbers(
        &mut self,
        partition_id: PartitionId,
    ) -> GetStream<(PartitionId, u64)>;
}
