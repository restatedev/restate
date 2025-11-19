// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::deduplication_table::{DedupSequenceNumber, ProducerId};

use super::{Raw, records};
use crate::{v1, v2};

impl TryFrom<v1::Envelope> for v2::IncomingEnvelope<Raw> {
    type Error = anyhow::Error;

    fn try_from(value: v1::Envelope) -> Result<Self, Self::Error> {
        let source = match value.header.source {
            v1::Source::Ingress {} => v2::Source::Ingress,
            v1::Source::Processor {
                partition_id,
                partition_key,
                leader_epoch,
            } => v2::Source::Processor {
                partition_id,
                partition_key,
                leader_epoch,
            },
            v1::Source::ControlPlane {} => v2::Source::ControlPlane,
        };

        let v1::Destination::Processor { dedup, .. } = value.header.dest;

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
                    prefix: prefix.into(),
                    seq,
                },
            },
        };

        let envelope = match value.command {
            v1::Command::AnnounceLeader(payload) => {
                Self::from_parts::<records::AnnounceLeader>(source, dedup, *payload)
            }
            v1::Command::AttachInvocation(payload) => {
                Self::from_parts::<records::AttachInvocation>(source, dedup, payload.into())
            }
            v1::Command::InvocationResponse(payload) => {
                Self::from_parts::<records::InvocationResponse>(source, dedup, payload.into())
            }
            v1::Command::Invoke(payload) => {
                Self::from_parts::<records::Invoke>(source, dedup, payload.into())
            }
            v1::Command::InvokerEffect(payload) => {
                Self::from_parts::<records::InvokerEffect>(source, dedup, payload.into())
            }
            v1::Command::NotifyGetInvocationOutputResponse(payload) => {
                Self::from_parts::<records::NotifyGetInvocationOutputResponse>(
                    source,
                    dedup,
                    payload.into(),
                )
            }
            v1::Command::NotifySignal(payload) => {
                Self::from_parts::<records::NotifySignal>(source, dedup, payload.into())
            }
            v1::Command::PatchState(payload) => {
                Self::from_parts::<records::PatchState>(source, dedup, payload.into())
            }
            v1::Command::ProxyThrough(payload) => {
                Self::from_parts::<records::ProxyThrough>(source, dedup, payload.into())
            }
            v1::Command::PurgeInvocation(payload) => {
                Self::from_parts::<records::PurgeInvocation>(source, dedup, payload.into())
            }
            v1::Command::PurgeJournal(payload) => {
                Self::from_parts::<records::PurgeJournal>(source, dedup, payload.into())
            }
            v1::Command::RestartAsNewInvocation(payload) => {
                Self::from_parts::<records::RestartAsNewInvocation>(source, dedup, payload.into())
            }
            v1::Command::ResumeInvocation(payload) => {
                Self::from_parts::<records::ResumeInvocation>(source, dedup, payload.into())
            }
            v1::Command::ScheduleTimer(payload) => {
                Self::from_parts::<records::ScheduleTimer>(source, dedup, payload.into())
            }
            v1::Command::TerminateInvocation(payload) => {
                Self::from_parts::<records::TerminateInvocation>(source, dedup, payload.into())
            }
            v1::Command::Timer(payload) => {
                Self::from_parts::<records::Timer>(source, dedup, payload.into())
            }
            v1::Command::TruncateOutbox(payload) => {
                Self::from_parts::<records::TruncateOutbox>(source, dedup, payload.into())
            }
            v1::Command::UpdatePartitionDurability(payload) => {
                Self::from_parts::<records::UpdatePartitionDurability>(source, dedup, payload)
            }
            v1::Command::UpsertSchema(payload) => {
                Self::from_parts::<records::UpsertSchema>(source, dedup, payload)
            }
            v1::Command::VersionBarrier(payload) => {
                Self::from_parts::<records::VersionBarrier>(source, dedup, payload)
            }
        };

        Ok(envelope)
    }
}

// todo(azmy): Convert from v2 Envelope to v1 for backward compatible
// log writing
