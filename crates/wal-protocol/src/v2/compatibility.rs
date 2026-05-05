// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::deduplication_table::{
    DedupInformation, DedupSequenceNumber, EpochSequenceNumber, ProducerId,
};
use restate_types::{
    logs::{HasRecordKeys, Keys},
    storage::StorageDecodeError,
};

use super::{Raw, records};
use crate::{
    v1,
    v2::{
        self, Record,
        records::{ProxyThroughRequest, TruncateOutboxRequest},
    },
};

impl TryFrom<v1::Envelope> for v2::Envelope<Raw> {
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
                    prefix: prefix.into(),
                    seq,
                },
            },
        };

        let envelope = match value.command {
            v1::Command::AnnounceLeader(payload) => {
                records::AnnounceLeader::new(source, dedup, *payload).into_raw()
            }
            v1::Command::AttachInvocation(payload) => {
                records::AttachInvocation::new(source, dedup, payload).into_raw()
            }
            v1::Command::InvocationResponse(payload) => {
                records::InvocationResponse::new(source, dedup, payload).into_raw()
            }
            v1::Command::Invoke(payload) => records::Invoke::new(source, dedup, payload).into_raw(),
            v1::Command::InvokerEffect(payload) => {
                records::InvokerEffect::new(source, dedup, payload).into_raw()
            }
            v1::Command::NotifyGetInvocationOutputResponse(payload) => {
                records::NotifyGetInvocationOutputResponse::new(source, dedup, payload).into_raw()
            }
            v1::Command::NotifySignal(payload) => {
                records::NotifySignal::new(source, dedup, payload).into_raw()
            }
            v1::Command::PatchState(payload) => {
                records::PatchState::new(source, dedup, payload).into_raw()
            }
            v1::Command::ProxyThrough(payload) => records::ProxyThrough::new(
                source,
                dedup,
                ProxyThroughRequest {
                    invocation: payload.into(),
                    proxy_partition: Keys::Single(partition_key),
                },
            )
            .into_raw(),
            v1::Command::PurgeInvocation(payload) => {
                records::PurgeInvocation::new(source, dedup, payload).into_raw()
            }
            v1::Command::PurgeJournal(payload) => {
                records::PurgeJournal::new(source, dedup, payload).into_raw()
            }
            v1::Command::RestartAsNewInvocation(payload) => {
                records::RestartAsNewInvocation::new(source, dedup, payload).into_raw()
            }
            v1::Command::ResumeInvocation(payload) => {
                records::ResumeInvocation::new(source, dedup, payload).into_raw()
            }
            v1::Command::ScheduleTimer(payload) => {
                records::ScheduleTimer::new(source, dedup, payload).into_raw()
            }
            v1::Command::TerminateInvocation(payload) => {
                records::TerminateInvocation::new(source, dedup, payload).into_raw()
            }
            v1::Command::Timer(payload) => records::Timer::new(source, dedup, payload).into_raw(),
            v1::Command::TruncateOutbox(payload) => records::TruncateOutbox::new(
                source,
                dedup,
                TruncateOutboxRequest {
                    index: payload,
                    // this actually should be a key-range but v1 unfortunately
                    // only hold the "start" of the range.
                    // will be fixed in v2
                    partition_key_range: Keys::Single(partition_key),
                },
            )
            .into_raw(),
            v1::Command::UpdatePartitionDurability(payload) => {
                records::UpdatePartitionDurability::new(source, dedup, payload).into_raw()
            }
            v1::Command::UpsertSchema(payload) => {
                records::UpsertSchema::new(source, dedup, payload).into_raw()
            }
            v1::Command::VersionBarrier(payload) => {
                records::VersionBarrier::new(source, dedup, payload).into_raw()
            }
        };

        Ok(envelope)
    }
}

impl From<(Keys, v2::Header)> for v1::Header {
    fn from((keys, value): (Keys, v2::Header)) -> Self {
        let source = match value.source {
            v2::Source::Ingress => v1::Source::Ingress {},
            v2::Source::ControlPlane => v1::Source::ControlPlane {},
            v2::Source::Processor {
                partition_id,
                partition_key,
                leader_epoch,
            } => v1::Source::Processor {
                partition_id,
                partition_key,
                leader_epoch,
            },
        };

        // this is only for backward compatibility
        // but in reality the partition_id in the dest
        // should never be used. Instead the associated
        // record keys should.
        let partition_key = match keys {
            Keys::None => 0,
            Keys::Single(pk) => pk,
            Keys::Pair(pk, _) => pk,
            Keys::RangeInclusive(range) => *range.start(),
        };

        v1::Header {
            source,
            dest: v1::Destination::Processor {
                partition_key,
                dedup: value.dedup.into(),
            },
        }
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
            v2::Dedup::Arbitrary { prefix, seq } => Some(DedupInformation::ingress(prefix, seq)),
        }
    }
}

// compatibility from v2 to v1. We will keep writing v1 envelops
// until the following release.

impl TryFrom<v2::Envelope<records::AnnounceLeader>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::AnnounceLeader>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::AnnounceLeader(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::VersionBarrier>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::VersionBarrier>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::VersionBarrier(inner),
        })
    }
}

impl TryFrom<v2::Envelope<records::UpdatePartitionDurability>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(
        value: v2::Envelope<records::UpdatePartitionDurability>,
    ) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::UpdatePartitionDurability(inner),
        })
    }
}

impl TryFrom<v2::Envelope<records::PatchState>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::PatchState>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::PatchState(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::TerminateInvocation>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::TerminateInvocation>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::TerminateInvocation(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::PurgeInvocation>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::PurgeInvocation>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::PurgeInvocation(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::PurgeJournal>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::PurgeJournal>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::PurgeJournal(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::Invoke>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::Invoke>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::Invoke(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::TruncateOutbox>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::TruncateOutbox>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::TruncateOutbox(inner.index),
        })
    }
}

impl TryFrom<v2::Envelope<records::ProxyThrough>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::ProxyThrough>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::ProxyThrough(inner.invocation.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::AttachInvocation>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::AttachInvocation>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::AttachInvocation(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::ResumeInvocation>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::ResumeInvocation>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::ResumeInvocation(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::RestartAsNewInvocation>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::RestartAsNewInvocation>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::RestartAsNewInvocation(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::InvokerEffect>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::InvokerEffect>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::InvokerEffect(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::Timer>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::Timer>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::Timer(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::ScheduleTimer>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::ScheduleTimer>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::ScheduleTimer(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::InvocationResponse>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::InvocationResponse>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::InvocationResponse(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::NotifyGetInvocationOutputResponse>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(
        value: v2::Envelope<records::NotifyGetInvocationOutputResponse>,
    ) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::NotifyGetInvocationOutputResponse(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::NotifySignal>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::NotifySignal>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::NotifySignal(inner.into()),
        })
    }
}

impl TryFrom<v2::Envelope<records::UpsertSchema>> for v1::Envelope {
    type Error = StorageDecodeError;

    fn try_from(value: v2::Envelope<records::UpsertSchema>) -> Result<Self, Self::Error> {
        let (header, inner) = value.split()?;

        let header = v1::Header::from((inner.record_keys(), header));

        Ok(Self {
            header,
            command: v1::Command::UpsertSchema(inner),
        })
    }
}
