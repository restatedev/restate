// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use futures::never::Never;
use metrics::{Histogram, counter, histogram};

use restate_bifrost::{Bifrost, CommitToken, EnqueueError, ErrorRecoveryStrategy, InputRecord};
use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber};
use restate_types::config::Configuration;
use restate_types::{
    identifiers::{PartitionId, PartitionKey},
    logs::LogId,
    net::ingest::IngestRecord,
    time::NanosSinceEpoch,
};
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};

use crate::metric_definitions::{
    PARTITION_LABEL, PARTITION_SELF_PROPOSER_BACKPRESSURE,
    PARTITION_SELF_PROPOSER_ENQUEUE_DURATION_SECONDS,
};
use crate::partition::leadership::Error;

static BIFROST_APPENDER_TASK: &str = "bifrost-appender";

pub struct SelfProposer {
    partition_id: PartitionId,
    epoch_sequence_number: EpochSequenceNumber,
    bifrost_appender: restate_bifrost::AppenderHandle<Envelope>,
}

impl SelfProposer {
    pub fn new(
        partition_id: PartitionId,
        log_id: LogId,
        epoch_sequence_number: EpochSequenceNumber,
        bifrost: &Bifrost,
    ) -> Result<Self, Error> {
        let (queue_capacity, max_append_batch_size) = {
            let config = Configuration::pinned();
            (
                config.worker.self_proposer_queue_capacity(),
                config.worker.self_proposer_max_append_batch_size(),
            )
        };
        let bifrost_appender = bifrost
            .create_background_appender(
                log_id,
                ErrorRecoveryStrategy::ExtendChainPreferred,
                queue_capacity,
                max_append_batch_size,
            )?
            .start("self-appender")?;

        Ok(Self {
            partition_id,
            epoch_sequence_number,
            bifrost_appender,
        })
    }

    pub fn mark_as_leader(&mut self) {
        // we wouldn't fail if this didn't work out, subsequent operations will fail anyway.
        self.bifrost_appender.sender().mark_as_preferred();
    }

    pub fn mark_as_non_leader(&mut self) {
        // we wouldn't fail if this didn't work out, subsequent operations will fail anyway.
        self.bifrost_appender.sender().forget_preference();
    }

    /// Self-propose many commands to Bifrost, attaching ESN-based dedup information.
    ///
    /// Note that self_propose_many will return an error if the number of commands is greater than
    /// the internal channel's max capacity.
    pub async fn self_propose_many(
        &mut self,
        cmds: impl ExactSizeIterator<Item = (PartitionKey, Command)>,
    ) -> Result<(), Error> {
        let num_commands = cmds.len();
        // allocate a sequence number range for the batch
        let leader_epoch = self.epoch_sequence_number.leader_epoch;

        let start_seq = self.epoch_sequence_number.sequence_number;
        let end_seq = start_seq + num_commands as u64;

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
                    partition_key: Some(partition_key),
                    leader_epoch,
                },
            };
            Arc::new(Envelope::new(header, cmd))
        });

        let (enqueue_duration, start) = Self::start_enqueue(
            self.partition_id,
            self.bifrost_appender.sender().capacity() < num_commands,
        );
        let result = self.bifrost_appender.sender().enqueue_many(envelopes).await;
        enqueue_duration.record(start.elapsed());
        result.map_err(|e| Error::SelfProposer(e.to_string()))?;

        // update the sequence number range for the next batch
        self.epoch_sequence_number = EpochSequenceNumber {
            leader_epoch,
            sequence_number: end_seq,
        };

        Ok(())
    }

    /// Self-propose a single command to Bifrost, attaching ESN-based dedup information.
    pub async fn self_propose(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
    ) -> Result<(), Error> {
        let envelope = Envelope::new(self.create_self_propose_header(partition_key), cmd);

        let (enqueue_duration, start) = Self::start_enqueue(
            self.partition_id,
            self.bifrost_appender.sender().capacity() == 0,
        );
        let result = self
            .bifrost_appender
            .sender()
            .enqueue(Arc::new(envelope))
            .await;
        enqueue_duration.record(start.elapsed());
        result.map_err(|e| Error::SelfProposer(e.to_string()))?;

        Ok(())
    }

    /// Append a command to Bifrost **without** dedup information, returning a [`CommitToken`].
    ///
    /// Unlike [`Self::self_propose`], this does not attach an epoch sequence number. Records
    /// appended this way are never filtered by the dedup mechanism during leadership transitions,
    /// which makes them safe for fire-and-forget ingress commands (signals, invocation responses).
    pub async fn append_with_notification(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
    ) -> Result<CommitToken, Error> {
        let header = Header {
            dest: Destination::Processor {
                partition_key,
                dedup: None,
            },
            source: Source::Processor {
                partition_key: Some(partition_key),
                leader_epoch: self.epoch_sequence_number.leader_epoch,
            },
        };
        let envelope = Envelope::new(header, cmd);

        let (enqueue_duration, start) = Self::start_enqueue(
            self.partition_id,
            self.bifrost_appender.sender().capacity() == 0,
        );
        let result = self
            .bifrost_appender
            .sender()
            .enqueue_with_notification(Arc::new(envelope))
            .await;
        enqueue_duration.record(start.elapsed());
        let commit_token = result.map_err(|e| Error::SelfProposer(e.to_string()))?;

        Ok(commit_token)
    }

    /// Forward externally-created records to Bifrost, returning a [`CommitToken`].
    ///
    /// The records already carry their own dedup information in their headers; no ESN is attached.
    /// Internally this uses `enqueue_unchecked` which does not check the record size. Hence
    /// the only limit here is the networking max message size.
    pub async fn forward_many_with_notification(
        &mut self,
        records: impl ExactSizeIterator<Item = IngestRecord>,
    ) -> Result<CommitToken, EnqueueError<()>> where {
        let enqueue_duration = histogram!(
            PARTITION_SELF_PROPOSER_ENQUEUE_DURATION_SECONDS,
            PARTITION_LABEL => self.partition_id.to_string(),
        );
        let start = Instant::now();
        let partition_id = self.partition_id;
        let sender = self.bifrost_appender.sender();

        // This should ideally be implemented
        // by using `sender.enqueue_many`
        // but since we have no guarantee over the
        // underlying channel size a `reserve_many()` might
        // return a misleading Closed error
        //
        // sender
        //     .enqueue_many(records)
        //     .await
        //     .map_err(|e| Error::SelfProposer(e.to_string()))?;
        //
        // so instead we do this.

        for record in records {
            if sender.capacity() == 0 {
                Self::record_backpressure(partition_id);
            }
            // Skip decoding the envelope; build the InputRecord directly from the raw bytes.
            // The ingestion client should only handle payloads of type Envelope.
            let input = unsafe {
                InputRecord::from_bytes_unchecked(
                    NanosSinceEpoch::now(),
                    record.keys,
                    record.record,
                )
            };

            if let Err(err) = sender.enqueue_unchecked(input).await {
                enqueue_duration.record(start.elapsed());
                return Err(err.drop_payload());
            }
        }

        if sender.capacity() == 0 {
            Self::record_backpressure(partition_id);
        }
        let result = sender.notify_committed().await;
        enqueue_duration.record(start.elapsed());
        result
    }

    fn start_enqueue(partition_id: PartitionId, backpressured: bool) -> (Histogram, Instant) {
        if backpressured {
            Self::record_backpressure(partition_id);
        }
        (
            histogram!(
                PARTITION_SELF_PROPOSER_ENQUEUE_DURATION_SECONDS,
                PARTITION_LABEL => partition_id.to_string(),
            ),
            Instant::now(),
        )
    }

    fn record_backpressure(partition_id: PartitionId) {
        counter!(
            PARTITION_SELF_PROPOSER_BACKPRESSURE,
            PARTITION_LABEL => partition_id.to_string(),
        )
        .increment(1);
    }

    fn create_self_propose_header(&mut self, partition_key: PartitionKey) -> Header {
        let esn = self.epoch_sequence_number;
        self.epoch_sequence_number = self.epoch_sequence_number.next();

        Header {
            dest: Destination::Processor {
                partition_key,
                dedup: Some(DedupInformation::self_proposal(esn)),
            },
            source: Source::Processor {
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
