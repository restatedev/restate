// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::Context;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::{self, Sender};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, instrument, warn};

use restate_core::{ShutdownError, TaskCenter, TaskHandle, TaskId, TaskKind, cancellation_watcher};
use restate_storage_api::invocation_status_table::ScanInvocationStatusTable;
use restate_types::identifiers::{InvocationId, PartitionId};
use restate_types::retries::with_jitter;

const CLEANER_EFFECT_QUEUE_SIZE: usize = 10;

#[derive(Debug, Clone)]
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

pub(super) struct Cleaner<Storage> {
    partition_id: PartitionId,
    storage: Storage,
    cleanup_interval: Duration,
}

impl<Storage> Cleaner<Storage>
where
    Storage: ScanInvocationStatusTable + Send + Sync + 'static,
{
    pub(super) fn new(
        storage: Storage,
        partition_id: PartitionId,
        cleanup_interval: Duration,
    ) -> Self {
        Self {
            partition_id,
            storage,
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
        let initial_wait = with_jitter(self.cleanup_interval.mul_f32(0.2), 1.0);

        // the first tick will fire after initial_wait
        let mut interval =
            tokio::time::interval_at(Instant::now() + initial_wait, self.cleanup_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.do_cleanup(&tx).await {
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

    pub(super) async fn do_cleanup(&self, tx: &Sender<CleanerEffect>) -> anyhow::Result<()> {
        debug!(partition_id=%self.partition_id, "Starting invocation cleanup");
        let start = tokio::time::Instant::now();
        let mut purged_invocation_count = 0;
        let mut purged_journal_count = 0;

        let invocations_stream = self.storage.scan_expired_invocations()?;
        tokio::pin!(invocations_stream);

        while let Some(expired_invocation) = invocations_stream
            .next()
            .await
            .transpose()
            .context("Cannot read the next expired item of the invocation status table")?
        {
            if expired_invocation.invocation_expired {
                tx.send(CleanerEffect::PurgeInvocation(
                    expired_invocation.invocation_id,
                ))
                .await
                .context("Cannot append to bifrost purge invocation")?;

                purged_invocation_count += 1;
                continue;
            }

            // We don't cleanup the status yet, let's check if there's a journal to cleanup
            // When length != 0 it means that the purge journal feature was activated from the SDK side (through annotations and the new manifest),
            // or from the relative experimental feature in the Admin API. In this case, the user opted-in this feature and it can't go back to 1.3
            if expired_invocation.journal_expired {
                tx.send(CleanerEffect::PurgeJournal(
                    expired_invocation.invocation_id,
                ))
                .await
                .context("Cannot append to bifrost purge journal")?;
                purged_journal_count += 1;
                continue;
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
    use std::ops::RangeInclusive;

    use super::*;

    use futures::{Stream, stream};
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::{
        ExpiredInvocation, InvokedInvocationStatusLite, ScanInvocationStatusTable,
    };
    use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
    use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey};
    use test_log::test;

    #[allow(dead_code)]
    struct MockInvocationStatusReader(Vec<ExpiredInvocation>);

    impl ScanInvocationStatusTable for MockInvocationStatusReader {
        fn for_each_invocation_status_lazy<
            E: Into<anyhow::Error>,
            F: for<'a> FnMut(
                    (InvocationId, &'a InvocationStatusV2Lazy<'a>),
                ) -> std::ops::ControlFlow<std::result::Result<(), E>>
                + Send
                + Sync
                + 'static,
        >(
            &self,
            _: RangeInclusive<PartitionKey>,
            _: F,
        ) -> restate_storage_api::Result<impl Future<Output = restate_storage_api::Result<()>> + Send>
        {
            unimplemented!();

            #[allow(unreachable_code)]
            Ok(std::future::pending())
        }

        fn scan_invoked_invocations(
            &self,
        ) -> restate_storage_api::Result<
            impl Stream<Item = restate_storage_api::Result<InvokedInvocationStatusLite>> + Send,
        > {
            Ok(stream::empty())
        }

        fn scan_expired_invocations(
            &self,
        ) -> restate_storage_api::Result<
            impl Stream<Item = restate_storage_api::Result<ExpiredInvocation>> + Send,
        > {
            Ok(stream::iter(self.0.clone()).map(Ok))
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

        let mock_storage = MockInvocationStatusReader(vec![
            ExpiredInvocation {
                invocation_id: expired_invocation,
                invocation_expired: true,
                journal_expired: false,
            },
            ExpiredInvocation {
                invocation_id: expired_journal,
                invocation_expired: false,
                journal_expired: true,
            },
            ExpiredInvocation {
                invocation_id: not_expired_invocation_1,
                invocation_expired: false,
                journal_expired: false,
            },
        ]);

        let mut handle = Cleaner::new(mock_storage, 0.into(), Duration::from_secs(1))
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
    }
}
