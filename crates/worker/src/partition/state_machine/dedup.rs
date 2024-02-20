// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{Effects, Error};

use crate::partition::state_machine::{
    Action, ActionCollector, InterpretationResult, StateMachine,
};
use crate::partition::storage::Transaction;
use crate::partition::types::{AckResponse, IngressAckResponse, ShuffleAckResponse};
use restate_storage_api::deduplication_table::SequenceNumberSource;
use restate_types::journal::raw::RawEntryCodec;
use restate_types::message::{AckKind, MessageIndex};
use restate_wal_protocol::{AckMode, Envelope, Source};

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
        envelope: Envelope,
        effects: &mut Effects,
        mut transaction: Transaction<TransactionType>,
        mut message_collector: Collector,
        is_leader: bool,
    ) -> Result<InterpretationResult<Transaction<TransactionType>, Collector>, Error> {
        match envelope.header.ack_mode {
            AckMode::Ack => {
                let ack_response =
                    create_ack_response(&envelope.header.source, |sequence_number| {
                        AckKind::Acknowledge(sequence_number)
                    });
                message_collector.collect(Action::SendAckResponse(ack_response));
            }
            AckMode::Dedup => {
                let (source, seq_number) = match &envelope.header.source {
                    Source::Processor {
                        partition_id,
                        sequence_number,
                        ..
                    } => (
                        SequenceNumberSource::Partition(*partition_id),
                        sequence_number.expect("sequence number must be present"),
                    ),
                    Source::Ingress {
                        dedup_key,
                        sequence_number,
                        ..
                    } => (
                        SequenceNumberSource::Ingress(
                            dedup_key.clone().expect("dedup key must be present").into(),
                        ),
                        *sequence_number,
                    ),
                    Source::ControlPlane { .. } => {
                        unimplemented!("control plane should not require deduping")
                    }
                };

                if let Some(last_known_seq_number) =
                    transaction.load_dedup_seq_number(source.clone()).await?
                {
                    if seq_number <= last_known_seq_number {
                        let ack_response =
                            create_ack_response(&envelope.header.source, |seq_number| {
                                AckKind::Duplicate {
                                    seq_number,
                                    last_known_seq_number,
                                }
                            });
                        message_collector.collect(Action::SendAckResponse(ack_response));
                        return Ok(InterpretationResult::new(transaction, message_collector));
                    }
                }

                transaction.store_dedup_seq_number(source, seq_number).await;
                let ack_response = create_ack_response(&envelope.header.source, |seq_number| {
                    AckKind::Acknowledge(seq_number)
                });
                message_collector.collect(Action::SendAckResponse(ack_response));
            }
            AckMode::None => {}
        }

        self.inner
            .apply(
                envelope.command,
                effects,
                transaction,
                message_collector,
                is_leader,
            )
            .await
    }
}

fn create_ack_response(
    source: &Source,
    ack_kind: impl FnOnce(MessageIndex) -> AckKind,
) -> AckResponse {
    match source {
        Source::Processor {
            partition_id,
            sequence_number,
            ..
        } => AckResponse::Shuffle(ShuffleAckResponse {
            shuffle_target: *partition_id,
            kind: ack_kind(sequence_number.expect("sequence number must be present")),
        }),
        Source::Ingress {
            node_id,
            sequence_number,
            dedup_key,
            ..
        } => AckResponse::Ingress(IngressAckResponse {
            _from_node_id: *node_id,
            dedup_source: dedup_key.clone(),
            kind: ack_kind(*sequence_number),
        }),
        Source::ControlPlane { .. } => unimplemented!("control plane should not require acks"),
    }
}
