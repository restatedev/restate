// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU16;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::{self, Sender};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, instrument, warn};

use restate_core::{ShutdownError, TaskCenter, TaskHandle, TaskId, TaskKind, cancellation_watcher};
use restate_storage_api::invocation_status_table::ScanInvocationStatusTable;
use restate_types::errors::ConversionError;
use restate_types::identifiers::{InvocationId, PartitionId};
use restate_types::sharding::{
    KeyRange,
    subsharding::{ShardIdx, ShardPlan},
};
use restate_util_time::DurationExt;

const CLEANER_EFFECT_QUEUE_SIZE: usize = 10;

// Divide the interval into 5mins slices. For example, a 1 hour cleanup interval then would sweep every
// partition in 12 slices.
const INTERVAL_SLICE_DURATION: Duration = Duration::from_mins(5);
// For configurations with very large intervals, we clamp the number of slices to 1000 to avoid the churn
// of small scans, and instead spread the 1000 slices over longer intervals.
const MAX_NUM_SLICES: u16 = 1000;

struct KeyRangeSlicer {
    shard_plan: ShardPlan,
    next_slice: ShardIdx,
}

impl KeyRangeSlicer {
    fn new(key_range: KeyRange, num_slices: NonZeroU16) -> Self {
        Self {
            shard_plan: ShardPlan::new(key_range, num_slices),
            next_slice: 0,
        }
    }

    fn next(&mut self) -> KeyRange {
        let range = *self
            .shard_plan
            .find_shard_unchecked(self.next_slice)
            .key_range();
        self.next_slice = (self.next_slice + 1) % self.shard_plan.shard_count();
        range
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CleanerEffect {
    PurgeInvocation(InvocationId),
    PurgeJournal(InvocationId),
}

pub(super) struct CleanerHandle {
    task_id: TaskId,
    rx: ReceiverStream<CleanerEffect>,
}

impl CleanerHandle {
    pub fn stop(self) -> Option<TaskHandle<()>> {
        TaskCenter::cancel_task(self.task_id)
    }

    pub fn effects(&mut self) -> impl Stream<Item = CleanerEffect> {
        &mut self.rx
    }
}

/// The cleaner runs periodically and scans the invocation status table for expired invocations and journals.
/// It then issues cleaner effects to to eventually purge those invocations from the storage.
///
/// The `cleanup_interval` knob controls the cycle for which the cleaner is expected to would have done a full
/// sweep of the invocation status table. Internally, the cleaner divides the interval into smaller sweeps each
/// scanning a subset of the partition key range. This is meant to avoid spikes of cleanup activities that might
/// overwhelm the processor.
pub(super) struct Cleaner<Storage> {
    partition_id: PartitionId,
    storage: Storage,
    key_range: KeyRange,
    cleanup_interval: Duration,
}

impl<Storage> Cleaner<Storage>
where
    Storage: ScanInvocationStatusTable + Send + Sync + 'static,
{
    pub(super) fn new(
        storage: Storage,
        partition_id: PartitionId,
        key_range: KeyRange,
        cleanup_interval: Duration,
    ) -> Self {
        Self {
            partition_id,
            storage,
            key_range,
            cleanup_interval,
        }
    }

    pub(super) fn start(self) -> Result<CleanerHandle, ShutdownError> {
        let (tx, rx) = mpsc::channel(CLEANER_EFFECT_QUEUE_SIZE);
        let task_id = TaskCenter::spawn_child(TaskKind::Cleaner, "cleaner", self.run(tx))?;

        Ok(CleanerHandle {
            task_id,
            rx: ReceiverStream::new(rx),
        })
    }

    #[instrument(skip_all)]
    async fn run(self, tx: Sender<CleanerEffect>) -> anyhow::Result<()> {
        debug!(
            partition_id=%self.partition_id,
            cleanup_interval=?self.cleanup_interval,
            "Running cleaner"
        );

        // the cleaner is currently quite an expensive scan and we don't strictly need to do it on startup, so we will wait
        // for 20-40% of the interval (so, 12-24 minutes by default) before doing the first one
        let initial_wait = self.cleanup_interval.mul_f32(0.2).add_jitter(1.0);

        let num_slices = self
            .cleanup_interval
            .as_secs()
            .div_ceil(INTERVAL_SLICE_DURATION.as_secs())
            .clamp(1, MAX_NUM_SLICES as u64) as u16;
        let num_slices = NonZeroU16::new(num_slices).expect("clamped to at least one");
        let slice_interval = self.cleanup_interval.div_f32(num_slices.get() as f32);
        let mut key_range_slicer = KeyRangeSlicer::new(self.key_range, num_slices);

        // the first tick will fire after initial_wait
        let mut interval = tokio::time::interval_at(Instant::now() + initial_wait, slice_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.do_cleanup(&tx, key_range_slicer.next()).await {
                        warn!(
                            partition_id=%self.partition_id,
                            "Error when trying to cleanup completed invocations: {e:?}"
                        );
                    }
                },
                _ = cancellation_watcher() => {
                    break;
                }
            }
        }

        debug!("Stopping cleaner");

        Ok(())
    }

    pub(super) async fn do_cleanup(
        &self,
        tx: &Sender<CleanerEffect>,
        range_slice: KeyRange,
    ) -> anyhow::Result<()> {
        debug!(partition_id=%self.partition_id, "Starting invocation cleanup");
        let start = tokio::time::Instant::now();
        let mut purged_invocation_count = 0;
        let mut purged_journal_count = 0;

        let now = SystemTime::now();

        let effects_stream = self
            .storage
            .filter_map_invocation_status_ranged_lazy(range_slice, move |(invocation_id, invocation_status_v2_lazy)| {
                let restate_storage_api::protobuf_types::v1::invocation_status_v2::Status::Completed =
                    invocation_status_v2_lazy.inner.status()
                else {
                    return Ok(None);
                };

                let Some(completed_time) = invocation_status_v2_lazy.inner.completed_transition_time else {
                    // If completed time is unavailable, the invocation is on the old invocation table,
                    //  thus it will be cleaned up with the old timer.
                    return Ok(None);
                };
                let completed_time = restate_types::time::MillisSinceEpoch::new(completed_time);

                let completion_retention_duration =
                    invocation_status_v2_lazy.completion_retention_duration()?;

                // Check if the invocation status itself has expired
                if let Some(status_expiration_time) =
                    SystemTime::from(completed_time).checked_add(completion_retention_duration)
                    && now >= status_expiration_time
                {
                    return Ok(Some(CleanerEffect::PurgeInvocation(invocation_id)));
                }

                // We don't cleanup the status yet, let's check if there's a journal to cleanup
                // When length != 0 it means that the purge journal feature was activated from the SDK side (through annotations and the new manifest),
                // or from the relative experimental feature in the Admin API. In this case, the user opted-in this feature and it can't go back to 1.3
                if invocation_status_v2_lazy.inner.journal_length != 0 {
                    let journal_retention_duration = invocation_status_v2_lazy.journal_retention_duration()?;

                    if let Some(journal_expiration_time) =
                        SystemTime::from(completed_time).checked_add(journal_retention_duration)
                        && now >= journal_expiration_time
                    {
                        return Ok(Some(CleanerEffect::PurgeJournal(invocation_id)));
                    }
                }

                Result::<Option<_>, ConversionError>::Ok(None)
            })?;
        tokio::pin!(effects_stream);

        while let Some(effect) = effects_stream
            .next()
            .await
            .transpose()
            .context("Cannot read the next expired item of the invocation status table")?
        {
            match &effect {
                CleanerEffect::PurgeInvocation(_) => purged_invocation_count += 1,
                CleanerEffect::PurgeJournal(_) => purged_journal_count += 1,
            }
            tx.send(effect)
                .await
                .context("Cannot send cleaner effect")?;
        }

        debug!(
            partition_id=%self.partition_id,
            purged_invocation_count,
            purged_journal_count,
            "Completed invocation cleanup in {:?}",
            start.elapsed()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::RangeBounds;

    use super::*;

    use futures::{Stream, stream};
    use prost::Message;
    use test_log::test;

    use restate_storage_api::invocation_status_table::{
        InvokedInvocationStatusLite, ScanInvocationStatusTableRange,
    };
    use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
    use restate_storage_api::{StorageError, protobuf_types};
    use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey};
    use restate_types::sharding::WithPartitionKey;
    use restate_types::time::MillisSinceEpoch;

    #[derive(Clone)]
    struct MockCompletedInvocation {
        invocation_id: InvocationId,
        completed_transition_time: Option<u64>,
        completion_retention_duration: Duration,
        journal_retention_duration: Duration,
        journal_length: u32,
    }

    #[allow(dead_code)]
    struct MockInvocationStatusReader {
        invocations: Vec<MockCompletedInvocation>,
        scanned_ranges_tx: mpsc::UnboundedSender<KeyRange>,
    }

    impl MockInvocationStatusReader {
        fn new(
            invocations: Vec<MockCompletedInvocation>,
        ) -> (Self, mpsc::UnboundedReceiver<KeyRange>) {
            let (scanned_ranges_tx, scanned_ranges_rx) = mpsc::unbounded_channel();
            (
                Self {
                    invocations,
                    scanned_ranges_tx,
                },
                scanned_ranges_rx,
            )
        }
    }

    impl ScanInvocationStatusTable for MockInvocationStatusReader {
        fn for_each_invocation_status_lazy<
            E: Into<anyhow::Error> + 'static,
            F: for<'a> FnMut(
                    (InvocationId, &'a InvocationStatusV2Lazy<'a>),
                ) -> std::ops::ControlFlow<std::result::Result<(), E>>
                + Send
                + Sync
                + 'static,
        >(
            &self,
            _: ScanInvocationStatusTableRange,
            _: F,
        ) -> restate_storage_api::Result<impl Future<Output = restate_storage_api::Result<()>> + Send>
        {
            unimplemented!();

            #[allow(unreachable_code)]
            Ok(std::future::pending())
        }

        fn filter_map_invocation_status_ranged_lazy<
            O: Send + 'static,
            E: Into<anyhow::Error>,
            F: for<'a> FnMut(
                    (InvocationId, &'a InvocationStatusV2Lazy<'a>),
                ) -> std::result::Result<Option<O>, E>
                + Send
                + Sync
                + 'static,
        >(
            &self,
            key_range: KeyRange,
            mut f: F,
        ) -> restate_storage_api::Result<impl Stream<Item = restate_storage_api::Result<O>> + Send>
        {
            self.scanned_ranges_tx
                .send(key_range)
                .expect("scan observer must be open");
            Ok(
                stream::iter(self.invocations.clone()).filter_map(move |expired_invocation| {
                    if !key_range.contains(&expired_invocation.invocation_id.partition_key()) {
                        return std::future::ready(None);
                    }
                    let completion_retention_duration = protobuf_types::v1::Duration::from(
                        expired_invocation.completion_retention_duration,
                    )
                    .encode_to_vec();
                    let journal_retention_duration = protobuf_types::v1::Duration::from(
                        expired_invocation.journal_retention_duration,
                    )
                    .encode_to_vec();

                    std::future::ready({
                        match f((
                            expired_invocation.invocation_id,
                            &InvocationStatusV2Lazy {
                                inner: protobuf_types::v1::InvocationStatusV2Lazy {
                                    status: 5,
                                    completed_transition_time: expired_invocation
                                        .completed_transition_time,
                                    journal_length: expired_invocation.journal_length,
                                    ..Default::default()
                                },
                                completion_retention_duration_lazy: Some(
                                    &completion_retention_duration,
                                ),
                                journal_retention_duration_lazy: Some(&journal_retention_duration),
                                ..Default::default()
                            },
                        )) {
                            Ok(Some(val)) => Some(Ok(val)),
                            Ok(None) => None,
                            Err(err) => Some(Err(StorageError::Conversion(err.into()))),
                        }
                    })
                }),
            )
        }

        fn scan_legacy_invoked_invocations(
            &self,
        ) -> restate_storage_api::Result<
            impl Stream<Item = restate_storage_api::Result<InvokedInvocationStatusLite>> + Send,
        > {
            Ok(stream::empty())
        }
    }

    #[test(restate_core::test)]
    pub async fn cleanup_works_across_slices() {
        let key_range = KeyRange::FULL;

        let expired_invocation =
            InvocationId::from_parts(key_range.start(), InvocationUuid::mock_random());
        let expired_journal =
            InvocationId::from_parts(key_range.midpoint(), InvocationUuid::mock_random());
        let expired_invocation_2 =
            InvocationId::from_parts(key_range.end(), InvocationUuid::mock_random());
        let not_expired_invocation_1 =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());
        let not_expired_invocation_2 =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());

        let now = MillisSinceEpoch::now().as_u64();

        let (mock_storage, _scanned_ranges_rx) = MockInvocationStatusReader::new(vec![
            MockCompletedInvocation {
                invocation_id: expired_invocation,
                completed_transition_time: Some(now),
                completion_retention_duration: Duration::ZERO,
                journal_retention_duration: Duration::ZERO,
                journal_length: 0,
            },
            MockCompletedInvocation {
                invocation_id: expired_invocation_2,
                completed_transition_time: Some(now),
                completion_retention_duration: Duration::ZERO,
                journal_retention_duration: Duration::ZERO,
                journal_length: 0,
            },
            MockCompletedInvocation {
                invocation_id: expired_journal,
                completed_transition_time: Some(now),
                completion_retention_duration: Duration::MAX,
                journal_retention_duration: Duration::ZERO,
                journal_length: 2,
            },
            MockCompletedInvocation {
                invocation_id: not_expired_invocation_1,
                completed_transition_time: Some(now),
                completion_retention_duration: Duration::MAX,
                journal_retention_duration: Duration::ZERO,
                journal_length: 0,
            },
            MockCompletedInvocation {
                invocation_id: not_expired_invocation_2,
                completed_transition_time: None,
                completion_retention_duration: Duration::ZERO,
                journal_retention_duration: Duration::ZERO,
                journal_length: 0,
            },
        ]);

        let cleaner = Cleaner::new(mock_storage, 0.into(), key_range, Duration::from_mins(20));
        let mut key_range_slicer = KeyRangeSlicer::new(key_range, NonZeroU16::new(4).unwrap());
        let (tx, mut rx) = mpsc::channel(10);

        // Full range is divided into 4 slices
        for expectation in [
            // First expired invocation has partition key 0, so first quarter
            Some(CleanerEffect::PurgeInvocation(expired_invocation)),
            // Nothing in the 2nd quarter
            None,
            // 2nd expired invocation has key of FULL::midpoint(), this is the first key in the 3rd quarter
            Some(CleanerEffect::PurgeJournal(expired_journal)),
            // 3rd expired invocation has key of FULL::end(), this is the last key in the 4th quarter
            Some(CleanerEffect::PurgeInvocation(expired_invocation_2)),
            // We cycle back when all ranges are exhausted
            Some(CleanerEffect::PurgeInvocation(expired_invocation)),
        ] {
            cleaner
                .do_cleanup(&tx, key_range_slicer.next())
                .await
                .unwrap();

            match expectation {
                Some(expected) => assert_eq!(rx.recv().await, Some(expected)),
                None => std::assert_matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            }
        }
    }

    #[test(restate_core::test(start_paused = true))]
    pub async fn cleanup_visits_all_keys() {
        let key_range = KeyRange::FULL;

        let (mock_storage, mut scanned_ranges_rx) = MockInvocationStatusReader::new(vec![]);

        let cleaner = Cleaner::new(mock_storage, 0.into(), key_range, Duration::from_mins(20));
        let handle = cleaner.start().unwrap();

        // Slice interval is 5 mins, so with 20mins cleanup interval, we should visit all the keys
        // in 4 iterations.
        let mut called_with = Vec::with_capacity(4);
        for _ in 0..4 {
            called_with.push(
                scanned_ranges_rx
                    .recv()
                    .await
                    .expect("cleaner must scan all ranges"),
            );
        }

        assert_eq!(called_with[0].start(), key_range.start());
        assert_eq!(called_with[3].end(), key_range.end());

        // Next iteration would wrap around to the beginning
        let range = scanned_ranges_rx.recv().await.expect("not closed");
        assert_eq!(range.start(), key_range.start());

        if let Some(task) = handle.stop() {
            task.await.expect("cleaner must stop cleanly");
        }
    }
}
