// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Partition-scoped leader introspection. Registrations are owned by leader tasks and guarded
//! by epochs, so a stale guard cannot remove a replacement. Query futures clone their reader
//! under the registry lock, then release the lock before awaiting a response.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::{StreamExt, TryStreamExt, stream};

use restate_invoker_impl::ChannelStatusReader;
use restate_platform::sync::Mutex;
use restate_types::identifiers::{PartitionId, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_worker_api::invoker::InvocationStatusReport;
use restate_worker_api::{
    LeaderQueryKind, LeaderQueryRequest, LeaderQueryResponse, LeaderQuerySender,
    PartitionQueryAccess, PartitionQueryError, PartitionQueryStream, SchedulerStatusEntry,
    UserLimitCounterEntry,
};

type Epoch = u64;
static EPOCH: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
struct Registration<T> {
    range: KeyRange,
    epoch: Epoch,
    reader: T,
}

/// Live query capabilities, registered only while their partition's owning tasks are active.
#[derive(Debug, Clone, Default)]
pub struct PartitionLeaderHandlesRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

#[derive(Debug, Default)]
struct RegistryInner {
    invoker_status: BTreeMap<PartitionId, Registration<ChannelStatusReader>>,
    leader_query_tx: BTreeMap<PartitionId, Registration<LeaderQuerySender>>,
}

impl PartitionLeaderHandlesRegistry {
    pub fn register_invoker_status(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        reader: ChannelStatusReader,
    ) -> InvokerStatusGuard {
        let epoch = EPOCH.fetch_add(1, Ordering::Relaxed);
        self.inner.lock().invoker_status.insert(
            partition_id,
            Registration {
                range,
                epoch,
                reader,
            },
        );
        InvokerStatusGuard {
            registry: self.clone(),
            partition_id,
            epoch,
        }
    }

    pub fn register_leader_query(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        reader: LeaderQuerySender,
    ) -> LeaderQueryGuard {
        let epoch = EPOCH.fetch_add(1, Ordering::Relaxed);
        self.inner.lock().leader_query_tx.insert(
            partition_id,
            Registration {
                range,
                epoch,
                reader,
            },
        );
        LeaderQueryGuard {
            registry: self.clone(),
            partition_id,
            epoch,
        }
    }

    /// Removes registrations intersecting the range of an abnormally stopped processor.
    pub fn unregister_all(&self, keys: KeyRange) {
        let mut inner = self.inner.lock();
        inner
            .invoker_status
            .retain(|_, entry| entry.range.intersect(&keys).is_none());
        inner
            .leader_query_tx
            .retain(|_, entry| entry.range.intersect(&keys).is_none());
    }

    async fn query_leader(
        &self,
        partition_id: PartitionId,
        keys: KeyRange,
        kind: LeaderQueryKind,
    ) -> Result<Option<(KeyRange, LeaderQueryResponse)>, PartitionQueryError> {
        let entry = self
            .inner
            .lock()
            .leader_query_tx
            .get(&partition_id)
            .cloned()
            .ok_or(PartitionQueryError::LeaderUnavailable(partition_id))?;
        let Some(keys) = keys.intersect(&entry.range) else {
            return Ok(None);
        };
        let request = match kind {
            LeaderQueryKind::SchedulerStatus => LeaderQueryRequest::SchedulerStatus { keys },
            LeaderQueryKind::UserLimitCounters => LeaderQueryRequest::UserLimitCounters { keys },
        };
        let (command, response) = restate_futures_util::command::Command::prepare(request);
        entry
            .reader
            .send(command)
            .map_err(|_| PartitionQueryError::LeaderUnavailable(partition_id))?;
        let response = response
            .await
            .map_err(|_| PartitionQueryError::LeaderUnavailable(partition_id))?;
        if self
            .inner
            .lock()
            .leader_query_tx
            .get(&partition_id)
            .map(|e| e.epoch)
            != Some(entry.epoch)
        {
            return Err(PartitionQueryError::LeaderChanged(partition_id));
        }
        if matches!(response, LeaderQueryResponse::NotLeader(_)) {
            return Err(PartitionQueryError::LeaderUnavailable(partition_id));
        }
        Ok(Some((keys, response)))
    }
}

fn rows_stream<T: Send + 'static>(
    read: impl Future<Output = Result<Vec<T>, PartitionQueryError>> + Send + 'static,
) -> PartitionQueryStream<T> {
    stream::once(read)
        .map_ok(|rows| stream::iter(rows.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
}

impl PartitionQueryAccess for PartitionLeaderHandlesRegistry {
    fn scan_invoker_status(
        &self,
        partition_id: PartitionId,
        keys: KeyRange,
    ) -> PartitionQueryStream<InvocationStatusReport> {
        let registry = self.clone();
        rows_stream(async move {
            let entry = registry
                .inner
                .lock()
                .invoker_status
                .get(&partition_id)
                .cloned()
                .ok_or(PartitionQueryError::LeaderUnavailable(partition_id))?;
            let Some(keys) = keys.intersect(&entry.range) else {
                return Ok(Vec::new());
            };
            let mut rows = entry
                .reader
                .try_read_status(keys)
                .await
                .map_err(|_| PartitionQueryError::LeaderUnavailable(partition_id))?;
            if registry
                .inner
                .lock()
                .invoker_status
                .get(&partition_id)
                .map(|e| e.epoch)
                != Some(entry.epoch)
            {
                return Err(PartitionQueryError::LeaderChanged(partition_id));
            }
            rows.retain(|row| keys.contains(&row.invocation_id().partition_key()));
            // ChannelStatusReader returns rows ordered by invocation ID (partition key first).
            Ok(rows)
        })
    }

    fn scan_scheduler_status(
        &self,
        partition_id: PartitionId,
        keys: KeyRange,
    ) -> PartitionQueryStream<SchedulerStatusEntry> {
        let registry = self.clone();
        rows_stream(async move {
            match registry
                .query_leader(partition_id, keys, LeaderQueryKind::SchedulerStatus)
                .await?
            {
                None => Ok(Vec::new()),
                Some((keys, LeaderQueryResponse::SchedulerStatus(mut rows))) => {
                    rows.retain(|(id, _)| keys.contains(&id.partition_key()));
                    rows.sort_by(|(a, _), (b, _)| a.cmp(b));
                    Ok(rows)
                }
                Some(_) => Err(PartitionQueryError::UnexpectedResponse(partition_id)),
            }
        })
    }

    fn scan_user_limit_counters(
        &self,
        partition_id: PartitionId,
        keys: KeyRange,
    ) -> PartitionQueryStream<UserLimitCounterEntry> {
        let registry = self.clone();
        rows_stream(async move {
            match registry
                .query_leader(partition_id, keys, LeaderQueryKind::UserLimitCounters)
                .await?
            {
                None => Ok(Vec::new()),
                Some((keys, LeaderQueryResponse::UserLimitCounters(mut rows))) => {
                    rows.retain(|row| keys.contains(&row.partition_key));
                    rows.sort_by_key(|row| row.partition_key);
                    Ok(rows)
                }
                Some(_) => Err(PartitionQueryError::UnexpectedResponse(partition_id)),
            }
        })
    }
}

#[must_use = "dropping the guard unregisters the invoker-status entry"]
#[derive(Debug)]
pub struct InvokerStatusGuard {
    registry: PartitionLeaderHandlesRegistry,
    partition_id: PartitionId,
    epoch: Epoch,
}

impl Drop for InvokerStatusGuard {
    fn drop(&mut self) {
        let mut inner = self.registry.inner.lock();
        if inner
            .invoker_status
            .get(&self.partition_id)
            .map(|e| e.epoch)
            == Some(self.epoch)
        {
            inner.invoker_status.remove(&self.partition_id);
        }
    }
}

#[must_use = "dropping the guard unregisters the leader-query entry"]
#[derive(Debug)]
pub struct LeaderQueryGuard {
    registry: PartitionLeaderHandlesRegistry,
    partition_id: PartitionId,
    epoch: Epoch,
}

impl Drop for LeaderQueryGuard {
    fn drop(&mut self) {
        let mut inner = self.registry.inner.lock();
        if inner
            .leader_query_tx
            .get(&self.partition_id)
            .map(|e| e.epoch)
            == Some(self.epoch)
        {
            inner.leader_query_tx.remove(&self.partition_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{StreamExt, TryStreamExt};

    use restate_types::identifiers::PartitionId;
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::VQueueId;
    use restate_worker_api::{
        LeaderQueryRequest, LeaderQueryResponse, PartitionQueryAccess, PartitionQueryError, channel,
    };

    use super::PartitionLeaderHandlesRegistry;

    #[test]
    fn epoch_protects_stale_drops() {
        let registry = PartitionLeaderHandlesRegistry::default();
        let partition = PartitionId::MIN;
        let range = KeyRange::new(0, 1000);
        let (tx, _rx) = channel();
        let old = registry.register_leader_query(partition, range, tx.clone());
        let fresh = registry.register_leader_query(partition, range, tx);
        drop(old);
        assert_eq!(
            registry
                .inner
                .lock()
                .leader_query_tx
                .get(&partition)
                .unwrap()
                .epoch,
            fresh.epoch
        );
        drop(fresh);
        assert!(registry.inner.lock().leader_query_tx.is_empty());

        let (tx, _rx) = channel();
        let _guard = registry.register_leader_query(partition, range, tx.clone());
        let _overlap = registry.register_leader_query(
            PartitionId::new_unchecked(2),
            KeyRange::new(500, 2000),
            tx,
        );
        registry.unregister_all(range);
        assert!(registry.inner.lock().leader_query_tx.is_empty());
    }

    #[tokio::test]
    async fn queries_are_partition_scoped_and_fail_on_source_loss() {
        let registry = PartitionLeaderHandlesRegistry::default();
        let partition = PartitionId::MIN;
        let range = KeyRange::new(100, 199);
        let (tx, mut rx) = channel();
        let guard = registry.register_leader_query(partition, range, tx);

        assert!(
            registry
                .scan_scheduler_status(partition, KeyRange::new(0, 99))
                .next()
                .await
                .is_none()
        );
        assert!(rx.try_recv().is_err(), "disjoint ranges issue no request");
        let unknown = PartitionId::new_unchecked(2);
        assert_eq!(
            registry
                .scan_scheduler_status(unknown, range)
                .next()
                .await
                .unwrap()
                .unwrap_err(),
            PartitionQueryError::LeaderUnavailable(unknown)
        );

        let query = registry
            .scan_scheduler_status(partition, KeyRange::new(150, 300))
            .try_collect::<Vec<_>>();
        let respond = async {
            let command = rx.recv().await.unwrap();
            assert!(
                matches!(command.payload(), LeaderQueryRequest::SchedulerStatus { keys } if *keys == KeyRange::new(150, 199))
            );
            command
                .reply(LeaderQueryResponse::SchedulerStatus(vec![
                    (VQueueId::custom(175, "later"), Default::default()),
                    (VQueueId::custom(250, "outside"), Default::default()),
                    (VQueueId::custom(155, "earlier"), Default::default()),
                ]))
                .unwrap();
        };
        let (rows, ()) = tokio::join!(query, respond);
        assert_eq!(
            rows.unwrap()
                .iter()
                .map(|(id, _)| id.partition_key())
                .collect::<Vec<_>>(),
            vec![155, 175]
        );

        let mut pending = registry.scan_scheduler_status(partition, range);
        assert!(futures::poll!(pending.next()).is_pending());
        let (_, reply) = rx.try_recv().unwrap().into_inner();
        drop(pending);
        assert!(
            reply.is_closed(),
            "dropping a query cancels its pending response"
        );

        let query = registry
            .scan_scheduler_status(partition, range)
            .try_collect::<Vec<_>>();
        let step_down = async {
            let command = rx.recv().await.unwrap();
            drop(guard);
            command
                .reply(LeaderQueryResponse::SchedulerStatus(Vec::new()))
                .unwrap();
        };
        let (rows, ()) = tokio::join!(query, step_down);
        assert_eq!(
            rows.unwrap_err(),
            PartitionQueryError::LeaderChanged(partition)
        );

        let (tx, rx) = channel();
        let _guard = registry.register_leader_query(partition, range, tx);
        drop(rx);
        assert_eq!(
            registry
                .scan_user_limit_counters(partition, range)
                .next()
                .await
                .unwrap()
                .unwrap_err(),
            PartitionQueryError::LeaderUnavailable(partition)
        );
    }
}
