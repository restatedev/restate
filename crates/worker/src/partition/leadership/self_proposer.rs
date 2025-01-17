// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::leadership::Error;
use futures::never::Never;
use restate_bifrost::{Bifrost, CommitToken, ErrorRecoveryStrategy};
use restate_core::my_node_id;
use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber};
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::logs::LogId;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};
use std::sync::Arc;

// Constants since it's very unlikely that we can derive a meaningful configuration
// that the user can reason about.
//
// The queue size is small to reduce the tail latency. This comes at the cost of throughput but
// this runs within a single processor and the expected throughput is bound by the overall
// throughput of the processor itself.
const BIFROST_QUEUE_SIZE: usize = 20;
const MAX_BIFROST_APPEND_BATCH: usize = 5000;

static BIFROST_APPENDER_TASK: &str = "bifrost-appender";

pub struct SelfProposer {
    partition_id: PartitionId,
    epoch_sequence_number: EpochSequenceNumber,
    bifrost_appender: restate_bifrost::AppenderHandle<Envelope>,
}

impl SelfProposer {
    pub fn new(
        partition_id: PartitionId,
        epoch_sequence_number: EpochSequenceNumber,
        bifrost: &Bifrost,
    ) -> Result<Self, Error> {
        let bifrost_appender = bifrost
            .create_background_appender(
                LogId::from(partition_id),
                ErrorRecoveryStrategy::extend_preferred(),
                BIFROST_QUEUE_SIZE,
                MAX_BIFROST_APPEND_BATCH,
            )?
            .start("self-appender")?;

        Ok(Self {
            partition_id,
            epoch_sequence_number,
            bifrost_appender,
        })
    }

    pub async fn propose(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
    ) -> Result<(), Error> {
        let envelope = Envelope::new(self.create_header(partition_key), cmd);

        // Only blocks if background append is pushing back (queue full)
        self.bifrost_appender
            .sender()
            .enqueue(Arc::new(envelope))
            .await
            .map_err(|_| Error::SelfProposer)?;

        Ok(())
    }

    pub async fn propose_with_notification(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
    ) -> Result<CommitToken, Error> {
        let envelope = Envelope::new(self.create_header(partition_key), cmd);

        let commit_token = self
            .bifrost_appender
            .sender()
            .enqueue_with_notification(Arc::new(envelope))
            .await
            .map_err(|_| Error::SelfProposer)?;

        Ok(commit_token)
    }

    fn create_header(&mut self, partition_key: PartitionKey) -> Header {
        let esn = self.epoch_sequence_number;
        self.epoch_sequence_number = self.epoch_sequence_number.next();

        let my_node_id = my_node_id();
        Header {
            dest: Destination::Processor {
                partition_key,
                dedup: Some(DedupInformation::self_proposal(esn)),
            },
            source: Source::Processor {
                partition_id: self.partition_id,
                partition_key: Some(partition_key),
                leader_epoch: self.epoch_sequence_number.leader_epoch,
                // Kept for backward compatibility.
                node_id: my_node_id.as_plain(),
                generational_node_id: Some(my_node_id),
            },
        }
    }

    /// Waits for self proposer to fail. This method will only complete with an error if the self
    /// proposer has failed. There is no guarantee up to which point the self proposer has finished
    /// processing the proposed commands.
    pub async fn join_on_err(&mut self) -> Result<Never, Error> {
        let result = self.bifrost_appender.join().await;

        Err(match result {
            Ok(()) => Error::task_terminated_unexpectedly(BIFROST_APPENDER_TASK),
            Err(err) => Error::task_failed(BIFROST_APPENDER_TASK, err),
        })
    }
}
