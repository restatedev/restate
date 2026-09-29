// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::task::{Poll, Waker, ready};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::sync::mpsc::{self, Sender};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::time::FutureExt;
use tracing::{debug, instrument, warn};

use restate_core::{
    ShutdownError, TaskCenter, TaskHandle, TaskId, TaskKind, cancellation_token,
    cancellation_watcher,
};
use restate_storage_api::invocation_status_table::ScanInvocationStatusTable;
use restate_types::errors::ConversionError;
use restate_types::identifiers::{InvocationId, PartitionId};
use restate_util_time::DurationExt;

const CLEANER_EFFECT_QUEUE_SIZE: usize = 10;

// Buffer up to that many effects in memory from storage.
// Note: it's important to keep the CleanerEffect enum size in check.
// Currently, it's 48 bytes, so with 4096 effects, that's 200KiB per partition.
const CLEANER_EFFECT_BUFFER_SIZE: usize = 4096;

#[derive(Debug, Clone)]
pub enum CleanerEffect {
    PurgeInvocation(InvocationId),
    PurgeJournal(InvocationId),
}

impl CleanerEffect {
    pub fn invocation_id(&self) -> InvocationId {
        match self {
            CleanerEffect::PurgeInvocation(invocation_id)
            | CleanerEffect::PurgeJournal(invocation_id) => *invocation_id,
        }
    }
}

pub(super) struct CleanerHandle {
    task_id: TaskId,
    rx: ReceiverStream<CleanerEffect>,
    // Maximum number of purges the leader may have proposed but not yet applied. Purges are cheap
    // to propose but expensive to apply. Bounding them in flight keeps other commands from
    // queueing behind a long backlog of purges in the log.
    max_in_flight: usize,
    // Purges handed out to the leader that have not been applied yet.
    in_flight: usize,
    // Woken once a slot frees up after the window was full.
    window_waker: Option<Waker>,
}

impl CleanerHandle {
    pub fn stop(self) -> Option<TaskHandle<()>> {
        TaskCenter::cancel_task(self.task_id)
    }

    /// The cleaner effects, paced by an in-flight window: once `max_in_flight` purges wait to be
    /// applied, the stream stays pending until [`Self::on_purge_applied`] frees a slot.
    pub fn effects(&mut self) -> impl Stream<Item = CleanerEffect> + Unpin + '_ {
        futures::stream::poll_fn(move |cx| self.poll_next_effect(cx))
    }

    fn poll_next_effect(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Option<CleanerEffect>> {
        if self.in_flight >= self.max_in_flight {
            self.window_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let effect = ready!(self.rx.poll_next_unpin(cx));
        if effect.is_some() {
            self.in_flight += 1;
        }
        Poll::Ready(effect)
    }

    /// Frees a slot of the in-flight window. Called whenever the leader applies a purge.
    pub fn on_purge_applied(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
        if let Some(waker) = self.window_waker.take() {
            waker.wake();
        }
    }
}

pub(super) struct Cleaner<Storage> {
    partition_id: PartitionId,
    storage: Storage,
    cleanup_interval: Duration,
    max_in_flight_purges: NonZeroUsize,
}

impl<Storage> Cleaner<Storage>
where
    Storage: ScanInvocationStatusTable + Send + Sync + 'static,
{
    pub(super) fn new(
        storage: Storage,
        partition_id: PartitionId,
        cleanup_interval: Duration,
        max_in_flight_purges: NonZeroUsize,
    ) -> Self {
        Self {
            partition_id,
            storage,
            cleanup_interval,
            max_in_flight_purges,
        }
    }

    pub(super) fn start(self) -> Result<CleanerHandle, ShutdownError> {
        let (tx, rx) = mpsc::channel(CLEANER_EFFECT_QUEUE_SIZE);
        let max_in_flight = self.max_in_flight_purges.get();
        let task_id = TaskCenter::spawn_child(TaskKind::Cleaner, "cleaner", self.run(tx))?;

        Ok(CleanerHandle {
            task_id,
            rx: ReceiverStream::new(rx),
            max_in_flight,
            in_flight: 0,
            window_waker: None,
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

        // the first tick will fire after initial_wait
        let mut interval =
            tokio::time::interval_at(Instant::now() + initial_wait, self.cleanup_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.do_cleanup(&tx).with_cancellation_token(&cancellation_token()).await {
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            warn!(
                                partition_id=%self.partition_id,
                                "Error when trying to cleanup completed invocations: {e:?}"
                            );
                        }
                        None => {
                            debug!(
                                partition_id=%self.partition_id,
                                "Aborting cleanup midway due to cancellation"
                            );
                            break;
                        }
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

    pub(super) async fn do_cleanup(&self, tx: &Sender<CleanerEffect>) -> anyhow::Result<()> {
        debug!(partition_id=%self.partition_id, "Starting invocation cleanup");
        let start = tokio::time::Instant::now();
        let mut purged_invocation_count = 0;
        let mut purged_journal_count = 0;

        let now = SystemTime::now();

        let mut after: Option<InvocationId> = None;

        loop {
            let mut effects: Vec<_> = self
            .storage
            .filter_map_invocation_status_lazy(after, move |(invocation_id, invocation_status_v2_lazy)| {
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
            })?.take(CLEANER_EFFECT_BUFFER_SIZE + 1 /* An extra element for pagination */).try_collect().await
                        .context("Cannot read the next expired item of the invocation status table")?;

            let has_more = effects.len() > CLEANER_EFFECT_BUFFER_SIZE;
            if has_more {
                after = Some(effects.pop().unwrap().invocation_id());
            }

            for effect in effects {
                match &effect {
                    CleanerEffect::PurgeInvocation(_) => purged_invocation_count += 1,
                    CleanerEffect::PurgeJournal(_) => purged_journal_count += 1,
                }
                tx.send(effect)
                    .await
                    .context("Cannot send cleaner effect")?;
            }
            if !has_more {
                break;
            }
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
    use super::*;

    use futures::{FutureExt, Stream, stream};
    use googletest::prelude::*;
    use prost::Message;
    use restate_storage_api::invocation_status_table::ScanInvocationStatusTableRange;
    use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
    use restate_storage_api::{StorageError, protobuf_types};
    use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey};
    use restate_types::time::MillisSinceEpoch;
    use test_log::test;

    #[derive(Clone)]
    struct MockCompletedInvocation {
        invocation_id: InvocationId,
        completed_transition_time: Option<u64>,
        completion_retention_duration: Duration,
        journal_retention_duration: Duration,
        journal_length: u32,
    }

    #[allow(dead_code)]
    struct MockInvocationStatusReader(Vec<MockCompletedInvocation>);

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

        fn filter_map_invocation_status_lazy<
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
            after: Option<InvocationId>,
            mut f: F,
        ) -> restate_storage_api::Result<impl Stream<Item = restate_storage_api::Result<O>> + Send>
        {
            // Resume inclusively from `after`, like the partition store does.
            let invocations = self.0.clone().into_iter().skip_while(move |invocation| {
                after.is_some_and(|after| invocation.invocation_id != after)
            });
            Ok(
                stream::iter(invocations).filter_map(move |expired_invocation| {
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
    }

    // Start paused makes sure the timer is immediately fired
    #[test(restate_core::test(start_paused = true))]
    pub async fn cleanup_works() {
        let expired_invocation =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());
        let expired_journal =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());
        let not_expired_invocation_1 =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());
        let not_expired_invocation_2 =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());
        let expired_invocation_2 =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());

        let now = MillisSinceEpoch::now().as_u64();

        let mock_storage = MockInvocationStatusReader(vec![
            MockCompletedInvocation {
                invocation_id: expired_invocation,
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
            MockCompletedInvocation {
                invocation_id: expired_invocation_2,
                completed_transition_time: Some(now),
                completion_retention_duration: Duration::ZERO,
                journal_retention_duration: Duration::ZERO,
                journal_length: 0,
            },
        ]);

        let mut handle = Cleaner::new(
            mock_storage,
            0.into(),
            Duration::from_secs(1),
            NonZeroUsize::new(2).unwrap(),
        )
        .start()
        .unwrap();

        // cleanup will run after around 200ms
        tokio::time::advance(Duration::from_secs(1)).await;

        let received: Vec<_> = handle.effects().ready_chunks(10).next().await.unwrap();

        assert_that!(
            received,
            all!(
                len(eq(2)),
                contains(pat!(CleanerEffect::PurgeInvocation(eq(expired_invocation)))),
                contains(pat!(CleanerEffect::PurgeJournal(eq(expired_journal))))
            )
        );

        // The in-flight window is full until a purge is applied
        assert!(handle.effects().next().now_or_never().is_none());
        handle.on_purge_applied();

        assert_that!(
            handle.effects().next().await,
            some(pat!(CleanerEffect::PurgeInvocation(eq(
                expired_invocation_2
            ))))
        );
    }

    #[test(restate_core::test(start_paused = true))]
    async fn cleanup_paginates() {
        let now = MillisSinceEpoch::now().as_u64();
        // Two full pages and one more effect on a third page
        let invocations: Vec<_> = (0..2 * CLEANER_EFFECT_BUFFER_SIZE + 1)
            .map(|_| MockCompletedInvocation {
                invocation_id: InvocationId::from_parts(
                    PartitionKey::MIN,
                    InvocationUuid::mock_random(),
                ),
                completed_transition_time: Some(now),
                completion_retention_duration: Duration::ZERO,
                journal_retention_duration: Duration::ZERO,
                journal_length: 0,
            })
            .collect();
        let expected: Vec<_> = invocations.iter().map(|i| i.invocation_id).collect();

        let cleaner = Cleaner::new(
            MockInvocationStatusReader(invocations),
            0.into(),
            Duration::from_secs(1),
            NonZeroUsize::new(1).unwrap(),
        );

        // Sized for exactly the expected effects: sending any effect twice blocks the cleanup,
        // which then fails on the timeout.
        let (tx, rx) = mpsc::channel(expected.len());
        tokio::time::timeout(Duration::from_secs(1), cleaner.do_cleanup(&tx))
            .await
            .expect("cleanup terminates")
            .unwrap();
        drop(tx);

        let received: Vec<_> = ReceiverStream::new(rx)
            .map(|effect| effect.invocation_id())
            .collect()
            .await;
        assert_eq!(received, expected);
    }
}
