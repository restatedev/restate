// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use futures::StreamExt;
use restate_bifrost::Bifrost;
use restate_core::cancellation_watcher;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadOnlyInvocationStatusTable,
};
use restate_types::identifiers::WithPartitionKey;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
use restate_types::invocation::PurgeInvocationRequest;
use restate_types::GenerationalNodeId;
use restate_wal_protocol::{
    append_envelope_to_bifrost, Command, Destination, Envelope, Header, Source,
};
use std::ops::RangeInclusive;
use std::time::{Duration, SystemTime};
use tracing::debug;

pub(super) struct Cleaner<Storage> {
    partition_id: PartitionId,
    leader_epoch: LeaderEpoch,
    node_id: GenerationalNodeId,
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
        node_id: GenerationalNodeId,
        storage: Storage,
        bifrost: Bifrost,
        partition_key_range: RangeInclusive<PartitionKey>,
        cleanup_interval: Duration,
    ) -> Self {
        Self {
            partition_id,
            leader_epoch,
            node_id,
            partition_key_range,
            storage,
            bifrost,
            cleanup_interval,
        }
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        let Self {
            partition_id,
            leader_epoch,
            node_id,
            partition_key_range,
            storage,
            bifrost,
            cleanup_interval,
        } = self;
        debug!(restate.node = %node_id, restate.partition.id = %partition_id, "Running cleaner");

        let bifrost_envelope_source = Source::Processor {
            partition_id,
            partition_key: None,
            leader_epoch,
            node_id,
        };

        let mut interval = tokio::time::interval(cleanup_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    Self::do_cleanup(&storage, &bifrost, partition_key_range.clone(), &bifrost_envelope_source).await?
                },
                _ = cancellation_watcher() => {
                    break;
                }
            }
        }

        debug!(restate.node = %node_id, restate.partition.id = %partition_id, "Stopping cleaner");

        Ok(())
    }

    pub(super) async fn do_cleanup(
        storage: &Storage,
        bifrost: &Bifrost,
        partition_key_range: RangeInclusive<PartitionKey>,
        bifrost_envelope_source: &Source,
    ) -> anyhow::Result<()> {
        let invocations_stream = storage.all_invocation_statuses(partition_key_range);
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
            let Some(completed_time) =
                (unsafe { completed_invocation.timestamps.completed_transition_time() })
            else {
                continue;
            };
            let Some(expiration_time) = SystemTime::from(completed_time)
                .checked_add(completed_invocation.completion_retention)
            else {
                continue;
            };

            if SystemTime::now() >= expiration_time {
                append_envelope_to_bifrost(
                    bifrost,
                    Envelope {
                        header: Header {
                            source: bifrost_envelope_source.clone(),
                            dest: Destination::Processor {
                                partition_key: invocation_id.partition_key(),
                                dedup: None,
                            },
                        },
                        command: Command::PurgeInvocation(PurgeInvocationRequest { invocation_id }),
                    },
                )
                .await
                .context("Cannot append to bifrost")?;
            };
        }

        Ok(())
    }
}
