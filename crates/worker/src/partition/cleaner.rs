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
use tokio::time::MissedTickBehavior;
use tracing::{debug, instrument, warn};

use restate_bifrost::Bifrost;
use restate_core::{Metadata, cancellation_watcher};
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadOnlyInvocationStatusTable,
};
use restate_types::identifiers::WithPartitionKey;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::invocation::PurgeInvocationRequest;
use restate_wal_protocol::{
    Command, Destination, Envelope, Header, Source, append_envelope_to_bifrost,
};

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
    Storage: ReadOnlyInvocationStatusTable + Send + Sync + 'static,
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
        debug!("Executing completed invocations cleanup");

        let invocations_stream = storage.all_invocation_statuses(partition_key_range)?;
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

            // SAFETY: it's ok to use the completed_transition_time here,
            //  because only the leader runs this cleaner code, so there's no need to use an agreed time.
            let Some(completed_time) =
                (unsafe { completed_invocation.timestamps.completed_transition_time() })
            else {
                // If completed time is unavailable, the invocation is on the old invocation table,
                //  thus it will be cleaned up with the old timer.
                continue;
            };
            let Some(expiration_time) = SystemTime::from(completed_time)
                .checked_add(completed_invocation.completion_retention_duration)
            else {
                // If sum overflow, then the cleanup time lies far enough in the future
                continue;
            };

            if SystemTime::now() >= expiration_time {
                append_envelope_to_bifrost(
                    bifrost,
                    Arc::new(Envelope {
                        header: Header {
                            source: bifrost_envelope_source.clone(),
                            dest: Destination::Processor {
                                partition_key: invocation_id.partition_key(),
                                dedup: None,
                            },
                        },
                        command: Command::PurgeInvocation(PurgeInvocationRequest { invocation_id }),
                    }),
                )
                .await
                .context("Cannot append to bifrost")?;
            };
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
        InvokedInvocationStatusLite,
    };
    use restate_types::Version;
    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::partition_table::{FindPartition, PartitionTable};
    use std::future::Future;
    use test_log::test;

    #[allow(dead_code)]
    struct MockInvocationStatusReader(Vec<(InvocationId, InvocationStatus)>);

    impl ReadOnlyInvocationStatusTable for MockInvocationStatusReader {
        fn get_invocation_status(
            &mut self,
            _: &InvocationId,
        ) -> impl Future<Output = restate_storage_api::Result<InvocationStatus>> + Send {
            todo!();
            #[allow(unreachable_code)]
            std::future::pending()
        }

        fn all_invoked_invocations(
            &mut self,
        ) -> std::result::Result<
            impl Stream<Item = restate_storage_api::Result<InvokedInvocationStatusLite>> + Send,
            StorageError,
        > {
            todo!();
            #[allow(unreachable_code)]
            Ok(stream::empty())
        }

        fn all_invocation_statuses(
            &self,
            _: RangeInclusive<PartitionKey>,
        ) -> std::result::Result<
            impl Stream<Item = restate_storage_api::Result<(InvocationId, InvocationStatus)>> + Send,
            StorageError,
        > {
            Ok(stream::iter(self.0.clone()).map(Ok))
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

        let mut log_entries = bifrost.read_all(partition_id.into()).await.unwrap();
        let bifrost_message = log_entries
            .remove(0)
            .try_decode::<Envelope>()
            .unwrap()
            .unwrap();

        assert_that!(
            bifrost_message.command,
            pat!(Command::PurgeInvocation(pat!(PurgeInvocationRequest {
                invocation_id: eq(expired_invocation)
            })))
        );
        assert_that!(log_entries, empty());
    }
}
