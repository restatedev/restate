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
use restate_storage_api::{
    deduplication_table::{DedupInformation, DedupSequenceNumber, EpochSequenceNumber, ProducerId},
    vqueue_table::scheduler::SchedulerDecisions,
};
use restate_types::logs::Keys;
use restate_util_string::ReString;

use super::{Raw, records};
use crate::{
    v1,
    v2::{
        self, Record,
        records::{ProxyThroughPayload, TruncateOutboxPayload, UpsertRuleBookPayload},
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
            v1::Command::AnnounceLeader(payload) => {
                records::AnnounceLeader::envelope(dedup, *payload).into_raw()
            }
            v1::Command::AttachInvocation(payload) => {
                records::AttachInvocation::envelope(dedup, payload).into_raw()
            }
            v1::Command::InvocationResponse(payload) => {
                records::InvocationResponse::envelope(dedup, payload).into_raw()
            }
            v1::Command::Invoke(payload) => records::Invoke::envelope(dedup, payload).into_raw(),
            v1::Command::InvokerEffect(payload) => {
                records::InvokerEffect::envelope(dedup, payload).into_raw()
            }
            v1::Command::NotifyGetInvocationOutputResponse(payload) => {
                records::NotifyGetInvocationOutputResponse::envelope(dedup, payload).into_raw()
            }
            v1::Command::NotifySignal(payload) => {
                records::NotifySignal::envelope(dedup, payload).into_raw()
            }
            v1::Command::PatchState(payload) => {
                records::PatchState::envelope(dedup, payload).into_raw()
            }
            v1::Command::ProxyThrough(payload) => records::ProxyThrough::envelope(
                dedup,
                ProxyThroughPayload {
                    invocation: payload.into(),
                    proxy_partition: Keys::Single(partition_key),
                },
            )
            .into_raw(),
            v1::Command::PurgeInvocation(payload) => {
                records::PurgeInvocation::envelope(dedup, payload).into_raw()
            }
            v1::Command::PurgeJournal(payload) => {
                records::PurgeJournal::envelope(dedup, payload).into_raw()
            }
            v1::Command::RestartAsNewInvocation(payload) => {
                records::RestartAsNewInvocation::envelope(dedup, payload).into_raw()
            }
            v1::Command::ResumeInvocation(payload) => {
                records::ResumeInvocation::envelope(dedup, payload).into_raw()
            }
            v1::Command::ScheduleTimer(payload) => {
                records::ScheduleTimer::envelope(dedup, payload).into_raw()
            }
            v1::Command::TerminateInvocation(payload) => {
                records::TerminateInvocation::envelope(dedup, payload).into_raw()
            }
            v1::Command::Timer(payload) => records::Timer::envelope(dedup, payload).into_raw(),
            v1::Command::TruncateOutbox(payload) => records::TruncateOutbox::envelope(
                dedup,
                TruncateOutboxPayload {
                    index: payload,
                    // this actually should be a key-range but v1 unfortunately
                    // only hold the "start" of the range.
                    // will be fixed in v2
                    partition_key_range: Keys::Single(partition_key),
                },
            )
            .into_raw(),
            v1::Command::UpdatePartitionDurability(payload) => {
                records::UpdatePartitionDurability::envelope(dedup, payload).into_raw()
            }
            v1::Command::UpsertSchema(payload) => {
                records::UpsertSchema::envelope(dedup, payload).into_raw()
            }
            v1::Command::VersionBarrier(payload) => {
                records::VersionBarrier::envelope(dedup, payload).into_raw()
            }
            v1::Command::VQSchedulerDecisions(payload) => {
                // bytes are bilrost encoded SchedulerDecision.
                let payload = SchedulerDecisions::bilrost_decode(payload)?;
                records::VQSchedulerDecisions::envelope(dedup, payload).into_raw()
            }
            v1::Command::UpsertRuleBook(payload) => {
                let rule_book = RuleBook::decode(payload.rule_book)?;
                records::UpsertRuleBook::envelope(
                    dedup,
                    UpsertRuleBookPayload {
                        partition_key_range: payload.partition_key_range,
                        rule_book: Arc::new(rule_book),
                    },
                )
                .into_raw()
            }
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
