// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{AckCommand, AckMode, Effects, Error};

use crate::partition::state_machine::commands::DeduplicationSource;
use crate::partition::state_machine::{
    Action, ActionCollector, InterpretationResult, StateMachine,
};
use crate::partition::storage::Transaction;
use restate_storage_api::deduplication_table::SequenceNumberSource;
use restate_types::journal::raw::RawEntryCodec;
use restate_types::message::MessageIndex;

#[derive(Debug)]
pub struct DeduplicatingStateMachine<Codec> {
    inner: StateMachine<Codec>,
}

impl<Codec> DeduplicatingStateMachine<Codec> {
    pub fn new(inbox_seq_number: MessageIndex, outbox_seq_number: MessageIndex) -> Self {
        DeduplicatingStateMachine {
            inner: StateMachine::new(inbox_seq_number, outbox_seq_number),
        }
    }
}

impl<Codec> DeduplicatingStateMachine<Codec>
where
    Codec: RawEntryCodec,
{
    pub async fn apply<
        TransactionType: restate_storage_api::Transaction,
        Collector: ActionCollector,
    >(
        &mut self,
        command: AckCommand,
        effects: &mut Effects,
        mut transaction: Transaction<TransactionType>,
        mut message_collector: Collector,
        is_leader: bool,
    ) -> Result<InterpretationResult<Transaction<TransactionType>, Collector>, Error> {
        let (fsm_command, ack_mode) = command.into_inner();

        match ack_mode {
            AckMode::Ack(ack_target) => {
                message_collector.collect(Action::SendAckResponse(ack_target.acknowledge()));
            }
            AckMode::Dedup(deduplication_source) => {
                let (source, seq_number) = match deduplication_source {
                    DeduplicationSource::Shuffle {
                        seq_number,
                        producing_partition_id,
                        ..
                    } => (
                        SequenceNumberSource::Partition(producing_partition_id),
                        seq_number,
                    ),
                    DeduplicationSource::Ingress {
                        seq_number,
                        ref source_id,
                        ..
                    } => (
                        SequenceNumberSource::Ingress(source_id.clone().into()),
                        seq_number,
                    ),
                };

                if let Some(last_known_seq_number) =
                    transaction.load_dedup_seq_number(source.clone()).await?
                {
                    if seq_number <= last_known_seq_number {
                        message_collector.collect(Action::SendAckResponse(
                            deduplication_source.duplicate(last_known_seq_number),
                        ));
                        return Ok(InterpretationResult::new(transaction, message_collector));
                    }
                }

                transaction.store_dedup_seq_number(source, seq_number).await;
                message_collector
                    .collect(Action::SendAckResponse(deduplication_source.acknowledge()));
            }
            AckMode::None => {}
        }

        self.inner
            .apply(
                fsm_command,
                effects,
                transaction,
                message_collector,
                is_leader,
            )
            .await
    }
}
