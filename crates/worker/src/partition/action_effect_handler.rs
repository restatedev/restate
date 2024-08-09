// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{btree_map, BTreeMap};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::SystemTime;

use restate_bifrost::Bifrost;
use restate_core::{task_center, Metadata};
use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber};
use restate_types::identifiers::{PartitionId, PartitionKey, WithPartitionKey};
use restate_types::logs::LogId;
use restate_types::partition_table::FindPartition;
use restate_types::time::MillisSinceEpoch;
use restate_types::Version;
use restate_wal_protocol::timer::TimerKeyValue;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};

use super::leadership::ActionEffect;

// Constants since it's very unlikely that we can derive a meaningful configuration
// that the user can reason about.
const BIFROST_QUEUE_SIZE: usize = 1000;
const MAX_BIFROST_APPEND_BATCH: usize = 100;

/// Responsible for proposing [ActionEffect].
pub(super) struct ActionEffectHandler {
    partition_id: PartitionId,
    epoch_sequence_number: EpochSequenceNumber,
    partition_key_range: RangeInclusive<PartitionKey>,
    bifrost: Bifrost,
    bifrost_appenders: BTreeMap<LogId, restate_bifrost::AppenderHandle<Envelope>>,
    metadata: Metadata,
}

impl ActionEffectHandler {
    pub(super) fn new(
        partition_id: PartitionId,
        epoch_sequence_number: EpochSequenceNumber,
        partition_key_range: RangeInclusive<PartitionKey>,
        bifrost: Bifrost,
        metadata: Metadata,
    ) -> Self {
        Self {
            partition_id,
            epoch_sequence_number,
            partition_key_range,
            bifrost,
            bifrost_appenders: Default::default(),
            metadata,
        }
    }

    pub(super) async fn handle(
        &mut self,
        effects: impl IntoIterator<Item = ActionEffect>,
    ) -> anyhow::Result<()> {
        for actuator_output in effects {
            let envelope = match actuator_output {
                ActionEffect::Invoker(invoker_output) => {
                    let header = self.create_header(invoker_output.invocation_id.partition_key());
                    Envelope::new(header, Command::InvokerEffect(invoker_output))
                }
                ActionEffect::Shuffle(outbox_truncation) => {
                    // todo: Until we support partition splits we need to get rid of outboxes or introduce partition
                    //  specific destination messages that are identified by a partition_id
                    let header = self.create_header(*self.partition_key_range.start());
                    Envelope::new(header, Command::TruncateOutbox(outbox_truncation.index()))
                }
                ActionEffect::Timer(timer) => {
                    let header = self.create_header(timer.invocation_id().partition_key());
                    Envelope::new(header, Command::Timer(timer))
                }
                ActionEffect::ScheduleCleanupTimer(invocation_id, duration) => {
                    //  We need this self proposal because we need to agree between leaders and followers on the wakeup time.
                    //  We can get rid of this once we'll have a synchronized clock between leaders/followers.
                    let header = self.create_header(invocation_id.partition_key());
                    Envelope::new(
                        header,
                        Command::ScheduleTimer(TimerKeyValue::clean_invocation_status(
                            MillisSinceEpoch::from(SystemTime::now() + duration),
                            invocation_id,
                        )),
                    )
                }
            };

            let log_id = {
                // make sure we drop pinned partition table before awaiting
                let partition_table = self.metadata.wait_for_partition_table(Version::MIN).await?;
                LogId::from(partition_table.find_partition_id(envelope.partition_key())?)
            };

            let sender = match self.bifrost_appenders.entry(log_id) {
                btree_map::Entry::Vacant(entry) => {
                    let appender = self
                        .bifrost
                        .create_background_appender(
                            log_id,
                            BIFROST_QUEUE_SIZE,
                            MAX_BIFROST_APPEND_BATCH,
                        )?
                        .start(task_center(), "effect-appender", Some(self.partition_id))?;
                    entry.insert(appender).sender()
                }
                btree_map::Entry::Occupied(sender) => sender.into_mut().sender(),
            };
            // Only blocks if background append is pushing back (queue full)
            sender.enqueue(Arc::new(envelope)).await?;
        }

        Ok(())
    }

    /// Creates a header with itself as the source and destination.
    fn create_header(&mut self, partition_key: PartitionKey) -> Header {
        let esn = self.epoch_sequence_number.next();
        self.epoch_sequence_number = esn;

        Header {
            dest: Destination::Processor {
                partition_key,
                dedup: Some(DedupInformation::self_proposal(esn)),
            },
            source: Source::Processor {
                partition_id: self.partition_id,
                partition_key: Some(partition_key),
                leader_epoch: self.epoch_sequence_number.leader_epoch,
                node_id: self.metadata.my_node_id(),
            },
        }
    }
}
