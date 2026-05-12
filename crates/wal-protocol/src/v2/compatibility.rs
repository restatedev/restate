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

use bilrost::OwnedMessage;

use restate_encoding::U128;
use restate_limiter::RuleBook;
use restate_storage_api::deduplication_table::{
    DedupInformation, DedupSequenceNumber, EpochSequenceNumber, ProducerId,
};
use restate_types::{logs::Keys, storage::StorageCodecKind};
use restate_util_string::ReString;

use super::{Raw, commands};
use crate::{
    v1,
    v2::{
        self, Envelope,
        commands::{TruncateOutboxCommand, UpsertRuleBookCommand},
    },
};

impl TryFrom<v1::Envelope> for v2::Envelope<Raw> {
    type Error = anyhow::Error;

    fn try_from(value: v1::Envelope) -> Result<Self, Self::Error> {
        let v1::Destination::Processor {
            dedup,
            partition_key,
        } = value.header.dest;

        let dedup = match dedup {
            None => v2::Dedup::None,
            Some(info) => match (info.producer_id, info.sequence_number) {
                (ProducerId::Partition(id), DedupSequenceNumber::Sn(seq)) => {
                    v2::Dedup::ForeignPartition { partition: id, seq }
                }
                (ProducerId::Partition(_), _) => anyhow::bail!("invalid deduplication information"),
                (ProducerId::Other(_), DedupSequenceNumber::Esn(sn)) => v2::Dedup::SelfProposal {
                    leader_epoch: sn.leader_epoch,
                    seq: sn.sequence_number,
                },
                (ProducerId::Other(prefix), DedupSequenceNumber::Sn(seq)) => v2::Dedup::Arbitrary {
                    prefix: Some(ReString::from(&prefix[..])),
                    producer_id: 0.into(),
                    seq,
                },
                (ProducerId::Producer(producer_id), DedupSequenceNumber::Sn(seq)) => {
                    v2::Dedup::Arbitrary {
                        prefix: None,
                        producer_id: U128::from(u128::from(producer_id)),
                        seq,
                    }
                }
                (ProducerId::Producer(_), _) => {
                    anyhow::bail!("invalid deduplication information")
                }
            },
        };

        let envelope = match value.command {
            v1::Command::AnnounceLeader(payload) => Envelope::new(dedup, *payload).into_raw(),
            v1::Command::AttachInvocation(payload) => {
                Envelope::new(dedup, commands::AttachInvocationCommand::from(payload)).into_raw()
            }
            v1::Command::InvocationResponse(payload) => {
                Envelope::new(dedup, commands::InvocationResponseCommand::from(payload)).into_raw()
            }
            v1::Command::Invoke(payload) => {
                Envelope::new(dedup, commands::InvokeCommand::from(*payload)).into_raw()
            }
            v1::Command::InvokerEffect(payload) => {
                Envelope::new(dedup, commands::InvokerEffectCommand::from(*payload)).into_raw()
            }
            v1::Command::NotifyGetInvocationOutputResponse(payload) => Envelope::new(
                dedup,
                commands::NotifyGetInvocationOutputResponseCommand::from(payload),
            )
            .into_raw(),
            v1::Command::NotifySignal(payload) => {
                Envelope::new(dedup, commands::NotifySignalCommand::from(payload)).into_raw()
            }
            v1::Command::PatchState(payload) => {
                Envelope::new(dedup, commands::PatchStateCommand::from(payload)).into_raw()
            }
            v1::Command::ProxyThrough(payload) => Envelope::new(
                dedup,
                commands::ProxyThroughCommand {
                    invocation: (*payload).into(),
                    proxy_partition: Keys::Single(partition_key),
                },
            )
            .into_raw(),
            v1::Command::PurgeInvocation(payload) => {
                Envelope::new(dedup, commands::PurgeInvocationCommand::from(payload)).into_raw()
            }
            v1::Command::PurgeJournal(payload) => {
                Envelope::new(dedup, commands::PurgeJournalCommand::from(payload)).into_raw()
            }
            v1::Command::RestartAsNewInvocation(payload) => Envelope::new(
                dedup,
                commands::RestartAsNewInvocationCommand::from(payload),
            )
            .into_raw(),
            v1::Command::ResumeInvocation(payload) => {
                Envelope::new(dedup, commands::ResumeInvocationCommand::from(payload)).into_raw()
            }
            v1::Command::ScheduleTimer(payload) => {
                Envelope::new(dedup, commands::ScheduleTimerCommand::from(payload)).into_raw()
            }
            v1::Command::TerminateInvocation(payload) => {
                Envelope::new(dedup, commands::TerminateInvocationCommand::from(payload)).into_raw()
            }
            v1::Command::Timer(payload) => {
                Envelope::new(dedup, commands::TimerCommand::from(payload)).into_raw()
            }
            v1::Command::TruncateOutbox(payload) => Envelope::new(
                dedup,
                TruncateOutboxCommand {
                    index: payload,
                    // this actually should be a key-range but v1 unfortunately
                    // only hold the "start" of the range.
                    // will be fixed in v2
                    partition_key_range: Keys::Single(partition_key),
                },
            )
            .into_raw(),
            v1::Command::UpdatePartitionDurability(payload) => {
                Envelope::new(dedup, payload).into_raw()
            }
            v1::Command::UpsertSchema(payload) => Envelope::new(dedup, payload).into_raw(),
            v1::Command::VersionBarrier(payload) => Envelope::new(dedup, payload).into_raw(),
            v1::Command::UpsertRuleBook(payload) => {
                let rule_book = RuleBook::decode(payload.rule_book)?;
                Envelope::new(
                    dedup,
                    UpsertRuleBookCommand {
                        partition_key_range: payload.partition_key_range,
                        rule_book: Arc::new(rule_book),
                    },
                )
                .into_raw()
            }
            v1::Command::VQSchedulerDecisions(payload) => Envelope::from_bytes_unchecked(
                v2::CommandKind::VQSchedulerDecisions,
                StorageCodecKind::Bilrost,
                dedup,
                payload,
            ),
            v1::Command::VQueuesPause(payload) => Envelope::from_bytes_unchecked(
                v2::CommandKind::VQueuesPause,
                StorageCodecKind::Bilrost,
                dedup,
                payload,
            ),
            v1::Command::VQueuesResume(payload) => Envelope::from_bytes_unchecked(
                v2::CommandKind::VQueuesResume,
                StorageCodecKind::Bilrost,
                dedup,
                payload,
            ),
        };

        Ok(envelope)
    }
}

impl From<v2::Dedup> for Option<DedupInformation> {
    fn from(value: v2::Dedup) -> Self {
        match value {
            v2::Dedup::None => None,
            v2::Dedup::SelfProposal { leader_epoch, seq } => {
                Some(DedupInformation::self_proposal(EpochSequenceNumber {
                    leader_epoch,
                    sequence_number: seq,
                }))
            }
            v2::Dedup::ForeignPartition { partition, seq } => {
                Some(DedupInformation::cross_partition(partition, seq))
            }
            v2::Dedup::Arbitrary {
                prefix: Some(prefix),
                producer_id: _,
                seq,
            } => {
                // TODO(azmy): remove prefix from dedup info
                // and drop `DedupInformation::ingress()`
                #[allow(deprecated)]
                Some(DedupInformation::ingress(prefix.to_string(), seq))
            }
            v2::Dedup::Arbitrary {
                prefix: None,
                producer_id,
                seq,
            } => Some(DedupInformation::producer(producer_id.into(), seq)),
        }
    }
}
