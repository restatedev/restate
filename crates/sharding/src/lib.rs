// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod key_range;
mod partition_id;
mod partitioner;
pub mod subsharding;

use std::sync::Arc;

pub use key_range::KeyRange;
pub use partition_id::PartitionId;
pub use partitioner::{EqualSizedPartitionPartitioner, EqualSizedPartitions};

/// Identifying to which partition a key belongs. This is unlike the [`PartitionId`]
/// which identifies a consecutive range of partition keys.
pub type PartitionKey = u64;

/// Trait for data structures that have a partition key
pub trait WithPartitionKey {
    /// Returns the partition key
    fn partition_key(&self) -> PartitionKey;
}

impl<T> WithPartitionKey for &T
where
    T: WithPartitionKey,
{
    fn partition_key(&self) -> PartitionKey {
        (*self).partition_key()
    }
}

impl<T> WithPartitionKey for Arc<T>
where
    T: WithPartitionKey,
{
    fn partition_key(&self) -> PartitionKey {
        self.as_ref().partition_key()
    }
}
