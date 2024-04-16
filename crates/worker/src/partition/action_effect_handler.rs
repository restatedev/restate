// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::leadership::ActionEffect;
use restate_bifrost::Bifrost;
use restate_core::metadata;
use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber};
use restate_storage_api::timer_table::{Timer, TimerKey};
use restate_types::identifiers::{PartitionId, PartitionKey, WithPartitionKey};
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::effects::BuiltinServiceEffects;
use restate_wal_protocol::timer::TimerValue;
use restate_wal_protocol::{
    append_envelope_to_bifrost, Command, Destination, Envelope, Header, Source,
};
use std::ops::RangeInclusive;
use std::time::SystemTime;

/// Responsible for proposing [ActionEffect].
pub(super) struct ActionEffectHandler {
    partition_id: PartitionId,
    epoch_sequence_number: EpochSequenceNumber,
    partition_key_range: RangeInclusive<PartitionKey>,
    bifrost: Bifrost,
}

impl ActionEffectHandler {
    pub(super) fn new(
        partition_id: PartitionId,
        epoch_sequence_number: EpochSequenceNumber,
        partition_key_range: RangeInclusive<PartitionKey>,
        bifrost: Bifrost,
    ) -> Self {
        Self {
            partition_id,
            epoch_sequence_number,
            partition_key_range,
            bifrost,
        }
    }

    pub(super) async fn handle(&mut self, actuator_output: ActionEffect) -> anyhow::Result<()> {
        match actuator_output {
            ActionEffect::Invoker(invoker_output) => {
                let header = self.create_header(invoker_output.invocation_id.partition_key());
                append_envelope_to_bifrost(
                    &mut self.bifrost,
                    Envelope::new(header, Command::InvokerEffect(invoker_output)),
                )
                .await?;
            }
            ActionEffect::Shuffle(outbox_truncation) => {
                // todo: Until we support partition splits we need to get rid of outboxes or introduce partition
                //  specific destination messages that are identified by a partition_id
                let header = self.create_header(*self.partition_key_range.start());
                append_envelope_to_bifrost(
                    &mut self.bifrost,
                    Envelope::new(header, Command::TruncateOutbox(outbox_truncation.index())),
                )
                .await?;
            }
            ActionEffect::Timer(timer) => {
                let partition_key = timer.invocation_id().partition_key();
                let header = self.create_header(partition_key);
                append_envelope_to_bifrost(
                    &mut self.bifrost,
                    Envelope::new(header, Command::Timer(timer)),
                )
                .await?;
            }
            ActionEffect::BuiltInInvoker(invoker_output) => {
                // TODO Super BAD code to mitigate https://github.com/restatedev/restate/issues/851
                //  until we properly fix it.
                //  By proposing the effects one by one we avoid the read your own writes issue,
                //  because for each proposal the state machine goes through a transaction commit,
                //  to make sure the next command can see the effects of the previous one.
                //  A problematic example case is a sequence of CreateVirtualJournal and AppendJournalEntry:
                //  to append a journal entry we must have stored the JournalMetadata first.
                let (fid, effects) = invoker_output.into_inner();

                for effect in effects {
                    let header = self.create_header(fid.partition_key());
                    append_envelope_to_bifrost(
                        &mut self.bifrost,
                        Envelope::new(
                            header.clone(),
                            Command::BuiltInInvokerEffect(BuiltinServiceEffects::new(
                                fid.clone(),
                                vec![effect],
                            )),
                        ),
                    )
                    .await?;
                }
            }
            ActionEffect::ScheduleCleanupTimer(invocation_id, duration) => {
                // We need this self proposal because we need to agree between leaders and followers on the wakeup time.
                //  We can get rid of this once we'll have a synchronized clock between leaders/followers.
                let header = self.create_header(invocation_id.partition_key());
                append_envelope_to_bifrost(
                    &mut self.bifrost,
                    Envelope::new(
                        header.clone(),
                        Command::ScheduleTimer(TimerValue::new(
                            TimerKey {
                                timestamp: MillisSinceEpoch::from(SystemTime::now() + duration)
                                    .as_u64(),
                                invocation_uuid: invocation_id.invocation_uuid(),
                                journal_index: 0,
                            },
                            Timer::CleanInvocationStatus(invocation_id),
                        )),
                    ),
                )
                .await?;
            }
        };

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
                node_id: metadata().my_node_id().as_plain(),
            },
        }
    }
}
