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
use crate::partition::ConsensusWriter;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, WithPartitionKey};
use restate_types::NodeId;
use restate_wal_protocol::effects::BuiltinServiceEffects;
use restate_wal_protocol::{AckMode, Command, Destination, Envelope, Header, Source};
use std::ops::RangeInclusive;

/// Responsible for proposing [ActionEffect].
pub(super) struct ActionEffectHandler {
    partition_id: PartitionId,
    leader_epoch: LeaderEpoch,
    partition_key_range: RangeInclusive<PartitionKey>,
    consensus_writer: ConsensusWriter,
}

impl ActionEffectHandler {
    pub(super) fn new(
        partition_id: PartitionId,
        leader_epoch: LeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        consensus_writer: ConsensusWriter,
    ) -> Self {
        Self {
            partition_id,
            leader_epoch,
            partition_key_range,
            consensus_writer,
        }
    }

    pub(super) async fn handle(&self, actuator_output: ActionEffect) {
        match actuator_output {
            ActionEffect::Invoker(invoker_output) => {
                let header = self.create_header(invoker_output.full_invocation_id.partition_key());
                // Err only if the consensus module is shutting down
                let _ = self
                    .consensus_writer
                    .send(Envelope::new(
                        header,
                        Command::InvokerEffect(invoker_output),
                    ))
                    .await;
            }
            ActionEffect::Shuffle(outbox_truncation) => {
                // todo: Until we support partition splits we need to get rid of outboxes or introduce partition
                //  specific destination messages that are identified by a partition_id
                let header = self.create_header(*self.partition_key_range.start());
                // Err only if the consensus module is shutting down
                let _ = self
                    .consensus_writer
                    .send(Envelope::new(
                        header,
                        Command::TruncateOutbox(outbox_truncation.index()),
                    ))
                    .await;
            }
            ActionEffect::Timer(timer) => {
                let partition_key = timer.invocation_id().partition_key();
                let header = self.create_header(partition_key);
                // Err only if the consensus module is shutting down
                let _ = self
                    .consensus_writer
                    .send(Envelope::new(header, Command::Timer(timer)))
                    .await;
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

                let header = self.create_header(fid.partition_key());

                for effect in effects {
                    // Err only if the consensus module is shutting down
                    let _ = self
                        .consensus_writer
                        .send(Envelope::new(
                            header.clone(),
                            Command::BuiltInInvokerEffect(BuiltinServiceEffects::new(
                                fid.clone(),
                                vec![effect],
                            )),
                        ))
                        .await;
                }
            }
        };
    }

    /// Creates a header with itself as the source and destination and w/o acking.
    fn create_header(&self, partition_key: PartitionKey) -> Header {
        Header {
            ack_mode: AckMode::None,
            dest: Destination::Processor { partition_key },
            source: Source::Processor {
                partition_id: self.partition_id,
                partition_key: Some(partition_key),
                leader_epoch: self.leader_epoch,
                // todo: Add support for deduplicating self proposals
                sequence_number: None,
                node_id: NodeId::my_node_id().expect("NodeId should be set").id(),
            },
        }
    }
}
