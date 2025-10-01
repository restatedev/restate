// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use futures::StreamExt;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, instrument, warn};

use restate_bifrost::Bifrost;
use restate_core::cancellation_watcher;
use restate_storage_api::invocation_status_table::{InvocationStatus, ScanInvocationStatusTable};
use restate_types::identifiers::WithPartitionKey;
use restate_types::identifiers::{LeaderEpoch, PartitionKey};
use restate_types::invocation::PurgeInvocationRequest;
use restate_types::retries::with_jitter;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};

pub(super) struct Cleaner<Storage> {
    leader_epoch: LeaderEpoch,
    partition_key_range: RangeInclusive<PartitionKey>,
    storage: Storage,
    bifrost: Bifrost,
    cleanup_interval: Duration,
}

impl<Storage> Cleaner<Storage>
where
    Storage: ScanInvocationStatusTable + Send + Sync + 'static,
{
    pub(super) fn new(
        leader_epoch: LeaderEpoch,
        storage: Storage,
        bifrost: Bifrost,
        partition_key_range: RangeInclusive<PartitionKey>,
        cleanup_interval: Duration,
    ) -> Self {
        Self {
            leader_epoch,
            partition_key_range,
            storage,
            bifrost,
            cleanup_interval,
        }
    }

    #[instrument(skip_all)]
    pub(super) async fn run(self) -> anyhow::Result<()> {
        let Self {
            leader_epoch,
            partition_key_range,
            storage,
            bifrost,
            cleanup_interval,
        } = self;

        debug!(?cleanup_interval, "Running cleaner");

        let bifrost_envelope_source = Source::Processor {
            partition_id: None,
            partition_key: None,
            leader_epoch,
        };

        // the cleaner is currently quite an expensive scan and we don't strictly need to do it on startup, so we will wait
        // for 20-40% of the interval (so, 12-24 minutes by default) before doing the first one
        let initial_wait = with_jitter(cleanup_interval.mul_f32(0.2), 1.0);

        // the first tick will fire after initial_wait
        let mut interval =
            tokio::time::interval_at(Instant::now() + initial_wait, cleanup_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = Self::do_cleanup(&storage, &bifrost, partition_key_range.clone(), &bifrost_envelope_source).await {
                        warn!("Error when trying to cleanup completed invocations: {e:?}");
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
        storage: &Storage,
        bifrost: &Bifrost,
        partition_key_range: RangeInclusive<PartitionKey>,
        bifrost_envelope_source: &Source,
    ) -> anyhow::Result<()> {
        debug!("Executing completed invocations cleanup");

        let invocations_stream = storage.scan_invocation_statuses(partition_key_range)?;
        tokio::pin!(invocations_stream);

        while let Some((invocation_id, invocation_status)) = invocations_stream
            .next()
            .await
            .transpose()
            .context("Cannot read the next item of the invocation status table")?
        {
            let InvocationStatus::Completed(completed_invocation) = invocation_status else {
                continue;
            };

            let Some(completed_time) = completed_invocation.timestamps.completed_transition_time()
            else {
                // If completed time is unavailable, the invocation is on the old invocation table,
                //  thus it will be cleaned up with the old timer.
                continue;
            };

            let now = SystemTime::now();
            if let Some(status_expiration_time) = SystemTime::from(completed_time)
                .checked_add(completed_invocation.completion_retention_duration)
                && now >= status_expiration_time
            {
                restate_bifrost::append_to_bifrost(
                    bifrost,
                    Arc::new(Envelope {
                        header: Header {
                            source: bifrost_envelope_source.clone(),
                            dest: Destination::Processor {
                                partition_key: invocation_id.partition_key(),
                                dedup: None,
                            },
                        },
                        command: Command::PurgeInvocation(PurgeInvocationRequest {
                            invocation_id,
                            response_sink: None,
                        }),
                    }),
                )
                .await
                .context("Cannot append to bifrost purge invocation")?;
                continue;
            }

            // We don't cleanup the status yet, let's check if there's a journal to cleanup
            // When length != 0 it means that the purge journal feature was activated from the SDK side (through annotations and the new manifest),
            // or from the relative experimental feature in the Admin API. In this case, the user opted-in this feature and it can't go back to 1.3
            if completed_invocation.journal_metadata.length != 0 {
                let Some(journal_expiration_time) = SystemTime::from(completed_time)
                    .checked_add(completed_invocation.journal_retention_duration)
                else {
                    // If sum overflow, then the cleanup time lies far enough in the future
                    continue;
                };

                if now >= journal_expiration_time {
                    restate_bifrost::append_to_bifrost(
                        bifrost,
                        Arc::new(Envelope {
                            header: Header {
                                source: bifrost_envelope_source.clone(),
                                dest: Destination::Processor {
                                    partition_key: invocation_id.partition_key(),
                                    dedup: None,
                                },
                            },
                            command: Command::PurgeJournal(PurgeInvocationRequest {
                                invocation_id,
                                response_sink: None,
                            }),
                        }),
                    )
                    .await
                    .context("Cannot append to bifrost purge journal")?;
                    continue;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{Stream, stream};
    use googletest::prelude::*;
    use restate_core::{Metadata, TaskCenter, TaskKind, TestCoreEnvBuilder};
    use restate_storage_api::StorageError;
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InFlightInvocationMetadata, InvocationStatus,
        InvokedInvocationStatusLite, JournalMetadata, ScanInvocationStatusTable,
    };
    use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
    use restate_types::Version;
    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::partition_table::{FindPartition, PartitionTable};
    use test_log::test;

    #[allow(dead_code)]
    struct MockInvocationStatusReader(Vec<(InvocationId, InvocationStatus)>);

    impl ScanInvocationStatusTable for MockInvocationStatusReader {
        fn scan_invocation_statuses(
            &self,
            _: RangeInclusive<PartitionKey>,
        ) -> std::result::Result<
            impl Stream<Item = restate_storage_api::Result<(InvocationId, InvocationStatus)>> + Send,
            StorageError,
        > {
            Ok(stream::iter(self.0.clone()).map(Ok))
        }

        fn for_each_invocation_status_lazy<
            E: Into<anyhow::Error>,
            F: for<'a> FnMut(
                    (InvocationId, InvocationStatusV2Lazy<'a>),
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
    }

    // Start paused makes sure the timer is immediately fired
    #[test(restate_core::test(start_paused = true))]
    pub async fn cleanup_works() {
        let env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_partition_table(PartitionTable::with_equally_sized_partitions(
                Version::MIN,
                1,
            ))
            .build()
            .await;
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;

        let expired_invocation =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());
        let expired_journal =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());
        let not_expired_invocation_1 =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());
        let not_expired_invocation_2 =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());
        let not_completed_invocation =
            InvocationId::from_parts(PartitionKey::MIN, InvocationUuid::mock_random());

        let mock_storage = MockInvocationStatusReader(vec![
            (
                expired_invocation,
                InvocationStatus::Completed(CompletedInvocation {
                    completion_retention_duration: Duration::ZERO,
                    ..CompletedInvocation::mock_neo()
                }),
            ),
            (
                expired_journal,
                InvocationStatus::Completed(CompletedInvocation {
                    completion_retention_duration: Duration::MAX,
                    journal_retention_duration: Duration::ZERO,
                    journal_metadata: JournalMetadata {
                        length: 2,
                        commands: 2,
                        span_context: Default::default(),
                    },
                    ..CompletedInvocation::mock_neo()
                }),
            ),
            (
                not_expired_invocation_1,
                InvocationStatus::Completed(CompletedInvocation {
                    completion_retention_duration: Duration::MAX,
                    ..CompletedInvocation::mock_neo()
                }),
            ),
            (
                not_expired_invocation_2,
                // Old status invocations are still processed with the cleanup timer in the PP
                InvocationStatus::Completed(CompletedInvocation::mock_old()),
            ),
            (
                not_completed_invocation,
                InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
            ),
        ]);

        TaskCenter::spawn(
            TaskKind::Cleaner,
            "cleaner",
            Cleaner::new(
                LeaderEpoch::INITIAL,
                mock_storage,
                bifrost.clone(),
                RangeInclusive::new(PartitionKey::MIN, PartitionKey::MAX),
                Duration::from_secs(1),
            )
            .run(),
        )
        .unwrap();

        // cleanup will run after around 200ms
        tokio::time::sleep(Duration::from_secs(1)).await;

        // All the invocation ids were created with same partition keys, hence same partition id.
        let partition_id = Metadata::with_current(|m| {
            m.partition_table_snapshot()
                .find_partition_id(expired_invocation.partition_key())
        })
        .unwrap();

        let log_entries: Vec<_> = bifrost
            .read_all(partition_id.into())
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.try_decode::<Envelope>().unwrap().unwrap().command)
            .collect();

        assert_that!(
            log_entries,
            all!(
                len(eq(2)),
                contains(pat!(Command::PurgeInvocation(pat!(
                    PurgeInvocationRequest {
                        invocation_id: eq(expired_invocation),
                    }
                )))),
                contains(pat!(Command::PurgeJournal(pat!(PurgeInvocationRequest {
                    invocation_id: eq(expired_journal),
                })))),
            )
        );
    }
}
