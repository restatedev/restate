// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use futures::never::Never;

use restate_bifrost::{Bifrost, CommitToken, ErrorRecoveryStrategy};
use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber};
use restate_types::{identifiers::PartitionKey, logs::LogId};
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};

use crate::partition::leadership::Error;

// Constants since it's very unlikely that we can derive a meaningful configuration
// that the user can reason about.
//
// The queue size is small to reduce the tail latency. This comes at the cost of throughput but
// this runs within a single processor and the expected throughput is bound by the overall
// throughput of the processor itself.
const BIFROST_QUEUE_SIZE: usize = 50;
const MAX_BIFROST_APPEND_BATCH: usize = 5000;

static BIFROST_APPENDER_TASK: &str = "bifrost-appender";

pub struct SelfProposer {
    epoch_sequence_number: EpochSequenceNumber,
    bifrost_appender: restate_bifrost::AppenderHandle<Envelope>,
}

impl SelfProposer {
    pub fn new(
        log_id: LogId,
        epoch_sequence_number: EpochSequenceNumber,
        bifrost: &Bifrost,
    ) -> Result<Self, Error> {
        let bifrost_appender = bifrost
            .create_background_appender(
                log_id,
                ErrorRecoveryStrategy::ExtendChainPreferred,
                BIFROST_QUEUE_SIZE,
                MAX_BIFROST_APPEND_BATCH,
            )?
            .start("self-appender")?;

        Ok(Self {
            epoch_sequence_number,
            bifrost_appender,
        })
    }

    pub async fn mark_as_leader(&mut self) {
        // we wouldn't fail if this didn't work out, subsequent operations will fail anyway.
        let _ = self.bifrost_appender.sender().mark_as_preferred().await;
    }

    pub async fn mark_as_non_leader(&mut self) {
        // we wouldn't fail if this didn't work out, subsequent operations will fail anyway.
        let _ = self.bifrost_appender.sender().forget_preference().await;
    }

    /// Propose many commands to bifrost
    ///
    /// Note that propose_many will return an error if the number of commands is greater than the
    /// internal channel's max capacity.
    pub async fn propose_many(
        &mut self,
        cmds: impl ExactSizeIterator<Item = (PartitionKey, Command)>,
    ) -> Result<(), Error> {
        // allocate a sequence number range for the batch
        let leader_epoch = self.epoch_sequence_number.leader_epoch;

        let start_seq = self.epoch_sequence_number.sequence_number;
        let end_seq = start_seq + cmds.len() as u64;

        let envelopes = cmds.enumerate().map(|(idx, (partition_key, cmd))| {
            let esn = EpochSequenceNumber {
                leader_epoch,
                sequence_number: start_seq + idx as u64,
            };
            let header = Header {
                dest: Destination::Processor {
                    partition_key,
                    dedup: Some(DedupInformation::self_proposal(esn)),
                },
                source: Source::Processor {
                    partition_id: None,
                    partition_key: Some(partition_key),
                    leader_epoch,
                },
            };
            Arc::new(Envelope::new(header, cmd))
        });

        // Only blocks if background append is pushing back (queue full)
        self.bifrost_appender
            .sender()
            .enqueue_many(envelopes)
            .await
            .map_err(|_| Error::SelfProposer)?;

        // update the sequence number range for the next batch
        self.epoch_sequence_number = EpochSequenceNumber {
            leader_epoch,
            sequence_number: end_seq,
        };

        Ok(())
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

        Header {
            dest: Destination::Processor {
                partition_key,
                dedup: Some(DedupInformation::self_proposal(esn)),
            },
            source: Source::Processor {
                partition_id: None,
                partition_key: Some(partition_key),
                leader_epoch: self.epoch_sequence_number.leader_epoch,
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
