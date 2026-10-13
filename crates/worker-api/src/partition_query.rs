// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use futures::StreamExt;
use futures::stream::{self, BoxStream};

use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;

use crate::invoker::InvocationStatusReport;
use crate::{SchedulerStatusEntry, UserLimitCounterEntry};

/// Failure to read a partition's live query data. An unavailable source is not an empty result.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PartitionQueryError {
    #[error("partition {0} is not available to this query backend")]
    PartitionUnavailable(PartitionId),
    #[error("leader query source for partition {0} is not available")]
    LeaderUnavailable(PartitionId),
    #[error("leadership changed while reading partition {0}")]
    LeaderChanged(PartitionId),
    #[error("unexpected leader query response for partition {0}")]
    UnexpectedResponse(PartitionId),
}

pub type PartitionQueryStream<T> = BoxStream<'static, Result<T, PartitionQueryError>>;

/// Query-facing access to partition-owned live data, independent of SQL and processor commands.
///
/// Implementations resolve the exact partition, intersect the requested range with its range,
/// and return owned rows in ascending partition-key order. A disjoint range is empty; failure
/// to access a live source is an error. Streams must stop on their first error and release
/// pending work when dropped. Results describe live state, not a database snapshot.
///
/// Persisted reads still use the existing storage adapters. Database fencing, draining, and
/// leased read views are a separate extension of this boundary.
pub trait PartitionQueryAccess: Send + Sync + 'static {
    fn scan_invoker_status(
        &self,
        partition_id: PartitionId,
        keys: KeyRange,
    ) -> PartitionQueryStream<InvocationStatusReport>;

    fn scan_scheduler_status(
        &self,
        partition_id: PartitionId,
        keys: KeyRange,
    ) -> PartitionQueryStream<SchedulerStatusEntry>;

    fn scan_user_limit_counters(
        &self,
        partition_id: PartitionId,
        keys: KeyRange,
    ) -> PartitionQueryStream<UserLimitCounterEntry>;
}

/// Live-data capability for an offline partition universe. Live data is intentionally absent;
/// an unknown partition remains an error. Restored database access is supplied separately.
#[derive(Debug, Clone)]
pub struct OfflinePartitionQueryAccess {
    partitions: BTreeMap<PartitionId, KeyRange>,
}

impl OfflinePartitionQueryAccess {
    pub fn new(partitions: impl IntoIterator<Item = (PartitionId, KeyRange)>) -> Self {
        Self {
            partitions: partitions.into_iter().collect(),
        }
    }

    fn empty<T: Send + 'static>(&self, partition_id: PartitionId) -> PartitionQueryStream<T> {
        if self.partitions.contains_key(&partition_id) {
            stream::empty().boxed()
        } else {
            stream::once(
                async move { Err(PartitionQueryError::PartitionUnavailable(partition_id)) },
            )
            .boxed()
        }
    }
}

impl PartitionQueryAccess for OfflinePartitionQueryAccess {
    fn scan_invoker_status(
        &self,
        partition_id: PartitionId,
        _keys: KeyRange,
    ) -> PartitionQueryStream<InvocationStatusReport> {
        self.empty(partition_id)
    }

    fn scan_scheduler_status(
        &self,
        partition_id: PartitionId,
        _keys: KeyRange,
    ) -> PartitionQueryStream<SchedulerStatusEntry> {
        self.empty(partition_id)
    }

    fn scan_user_limit_counters(
        &self,
        partition_id: PartitionId,
        _keys: KeyRange,
    ) -> PartitionQueryStream<UserLimitCounterEntry> {
        self.empty(partition_id)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[test]
    fn offline_live_data_is_empty_but_unknown_partitions_fail() {
        futures::executor::block_on(async {
            let partition = PartitionId::MIN;
            let access = OfflinePartitionQueryAccess::new([(partition, KeyRange::new(100, 199))]);
            assert!(
                access
                    .scan_invoker_status(partition, KeyRange::FULL)
                    .next()
                    .await
                    .is_none()
            );
            assert!(
                access
                    .scan_scheduler_status(partition, KeyRange::new(0, 99))
                    .next()
                    .await
                    .is_none()
            );
            assert!(
                access
                    .scan_user_limit_counters(partition, KeyRange::FULL)
                    .next()
                    .await
                    .is_none()
            );
            let unknown = PartitionId::new_unchecked(2);
            let mut rows = access.scan_invoker_status(unknown, KeyRange::FULL);
            assert_eq!(
                rows.next().await.unwrap().unwrap_err(),
                PartitionQueryError::PartitionUnavailable(unknown)
            );
            assert!(rows.next().await.is_none());
        });
    }
}
