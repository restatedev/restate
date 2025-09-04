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
use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
use restate_types::errors::ConversionError;
use tokio::time::MissedTickBehavior;
use tracing::{debug, instrument, warn};

use restate_bifrost::Bifrost;
use restate_core::{Metadata, cancellation_watcher};
use restate_storage_api::invocation_status_table::ScanInvocationStatusTable;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::invocation::PurgeInvocationRequest;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};

pub(super) struct Cleaner<Storage> {
    partition_id: PartitionId,
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
        partition_id: PartitionId,
        leader_epoch: LeaderEpoch,
        storage: Storage,
        bifrost: Bifrost,
        partition_key_range: RangeInclusive<PartitionKey>,
        cleanup_interval: Duration,
    ) -> Self {
        Self {
            partition_id,
            leader_epoch,
            partition_key_range,
            storage,
            bifrost,
            cleanup_interval,
        }
    }

    #[instrument(skip_all, fields(restate.partition.id = %self.partition_id))]
    pub(super) async fn run(self) -> anyhow::Result<()> {
        let Self {
            partition_id,
            leader_epoch,
            partition_key_range,
            storage,
            bifrost,
            cleanup_interval,
        } = self;
        debug!("Running cleaner");

        let my_node_id = Metadata::with_current(|m| m.my_node_id());
        let bifrost_envelope_source = Source::Processor {
            partition_id,
            partition_key: None,
            leader_epoch,
            node_id: my_node_id.as_plain(),
            generational_node_id: Some(my_node_id),
        };

        let mut interval = tokio::time::interval(cleanup_interval);
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
        let start = tokio::time::Instant::now();
        let mut purged_invocation_count = 0;
        let mut purged_journal_count = 0;

        let invocations_stream =
            storage.filter_map_invocation_status_lazy(partition_key_range, read_expired)?;
        tokio::pin!(invocations_stream);

        while let Some(expired_invocation) = invocations_stream
            .next()
            .await
            .transpose()
            .context("Cannot read the next expired item of the invocation status table")?
        {
            if expired_invocation.invocation_expired {
                restate_bifrost::append_to_bifrost(
                    bifrost,
                    Arc::new(Envelope {
                        header: Header {
                            source: bifrost_envelope_source.clone(),
                            dest: Destination::Processor {
                                partition_key: expired_invocation.invocation_id.partition_key(),
                                dedup: None,
                            },
                        },
                        command: Command::PurgeInvocation(PurgeInvocationRequest {
                            invocation_id: expired_invocation.invocation_id,
                            response_sink: None,
                        }),
                    }),
                )
                .await
                .context("Cannot append to bifrost purge invocation")?;
                purged_invocation_count += 1;
                continue;
            }

            // We don't cleanup the status yet, let's check if there's a journal to cleanup
            // When length != 0 it means that the purge journal feature was activated from the SDK side (through annotations and the new manifest),
            // or from the relative experimental feature in the Admin API. In this case, the user opted-in this feature and it can't go back to 1.3
            if expired_invocation.journal_expired {
                restate_bifrost::append_to_bifrost(
                    bifrost,
                    Arc::new(Envelope {
                        header: Header {
                            source: bifrost_envelope_source.clone(),
                            dest: Destination::Processor {
                                partition_key: expired_invocation.invocation_id.partition_key(),
                                dedup: None,
                            },
                        },
                        command: Command::PurgeJournal(PurgeInvocationRequest {
                            invocation_id: expired_invocation.invocation_id,
                            response_sink: None,
                        }),
                    }),
                )
                .await
                .context("Cannot append to bifrost purge journal")?;
                purged_journal_count += 1;
                continue;
            }
        }

        debug!(
            purged_invocation_count,
            purged_journal_count,
            "Executed completed invocations cleanup in {:?}",
            start.elapsed()
        );

        Ok(())
    }
}

struct ExpiredInvocation {
    invocation_id: InvocationId,
    invocation_expired: bool,
    journal_expired: bool,
}

fn read_expired(
    (invocation_id, invocation_status_v2_lazy): (InvocationId, InvocationStatusV2Lazy),
) -> Result<Option<ExpiredInvocation>, ConversionError> {
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
    let now = SystemTime::now();

    let completion_retention_duration =
        invocation_status_v2_lazy.completion_retention_duration()?;

    let invocation_expired = if let Some(status_expiration_time) =
        SystemTime::from(completed_time).checked_add(completion_retention_duration)
    {
        now >= status_expiration_time
    } else {
        false
    };

    let journal_expired = if invocation_status_v2_lazy.inner.journal_length != 0 {
        let journal_retention_duration = invocation_status_v2_lazy.journal_retention_duration()?;

        if let Some(journal_expiration_time) =
            SystemTime::from(completed_time).checked_add(journal_retention_duration)
        {
            now >= journal_expiration_time
        } else {
            // If sum overflow, then the cleanup time lies far enough in the future
            false
        }
    } else {
        false
    };

    if !invocation_expired && !journal_expired {
        return Ok(None);
    }

    Ok(Some(ExpiredInvocation {
        invocation_id,
        invocation_expired,
        journal_expired,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{Stream, stream};
    use googletest::prelude::*;
    use prost::Message;
    use restate_core::{Metadata, TaskCenter, TaskKind, TestCoreEnvBuilder};
    use restate_storage_api::invocation_status_table::{
        InvokedInvocationStatusLite, ScanInvocationStatusTable,
    };
    use restate_storage_api::protobuf_types::v1::lazy::InvocationStatusV2Lazy;
    use restate_storage_api::{StorageError, protobuf_types};
    use restate_types::Version;
    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::partition_table::{FindPartition, PartitionTable};
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

        fn filter_map_invocation_status_lazy<
            O: Send + 'static,
            E: Into<anyhow::Error>,
            F: for<'a> FnMut(
                    (InvocationId, InvocationStatusV2Lazy<'a>),
                ) -> std::result::Result<Option<O>, E>
                + Send
                + Sync
                + 'static,
        >(
            &self,
            _: RangeInclusive<PartitionKey>,
            mut f: F,
        ) -> restate_storage_api::Result<impl Stream<Item = restate_storage_api::Result<O>> + Send>
        {
            Ok(
                stream::iter(self.0.clone()).filter_map(move |expired_invocation| {
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
                            InvocationStatusV2Lazy {
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
        ]);

        TaskCenter::spawn(
            TaskKind::Cleaner,
            "cleaner",
            Cleaner::new(
                PartitionId::MIN,
                LeaderEpoch::INITIAL,
                mock_storage,
                bifrost.clone(),
                RangeInclusive::new(PartitionKey::MIN, PartitionKey::MAX),
                Duration::from_secs(1),
            )
            .run(),
        )
        .unwrap();

        // By yielding once we let the cleaner task run, and perform the cleanup
        tokio::task::yield_now().await;

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
