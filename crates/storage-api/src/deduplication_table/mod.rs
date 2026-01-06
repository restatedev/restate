// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;

use bytestring::ByteString;

use restate_types::identifiers::{LeaderEpoch, PartitionId};
use restate_types::message::MessageIndex;
use serde::{Deserialize, Serialize};

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DedupInformation {
    pub producer_id: ProducerId,
    pub sequence_number: DedupSequenceNumber,
}

impl DedupInformation {
    pub fn cross_partition(producer_id: PartitionId, sequence_number: MessageIndex) -> Self {
        DedupInformation {
            producer_id: ProducerId::Partition(producer_id),
            sequence_number: DedupSequenceNumber::Sn(sequence_number),
        }
    }

    pub fn self_proposal(esn: EpochSequenceNumber) -> Self {
        DedupInformation {
            producer_id: ProducerId::self_producer(),
            sequence_number: DedupSequenceNumber::Esn(esn),
        }
    }

    pub fn ingress(producer_id: impl Into<ByteString>, sequence_number: MessageIndex) -> Self {
        DedupInformation {
            producer_id: ProducerId::Other(producer_id.into()),
            sequence_number: DedupSequenceNumber::Sn(sequence_number),
        }
    }

    pub fn producer(producer_id: u128, message_index: MessageIndex) -> Self {
        DedupInformation {
            producer_id: ProducerId::Producer(producer_id.into()),
            sequence_number: DedupSequenceNumber::Sn(message_index),
        }
    }
}

static SELF_PRODUCER: ByteString = ByteString::from_static("SELF");

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ProducerId {
    Partition(PartitionId),
    Other(ByteString),
    Producer(U128),
}

impl ProducerId {
    pub fn self_producer() -> Self {
        ProducerId::Other(SELF_PRODUCER.clone())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DedupSequenceNumber {
    /// Sequence number to deduplicate messages. Use this type if you want to deduplicate
    /// messages independent of the sender's leader epoch. This requires that the sequence
    /// number is deterministically generated for outgoing messages across leader epochs.
    Sn(MessageIndex),
    /// Epoch sequence number to deduplicate messages. Use this type if you want to deduplicate
    /// messages being produced during a given leader epoch and fence off messages coming from
    /// an older leader epoch.
    Esn(EpochSequenceNumber),
}

impl PartitionStoreProtobufValue for DedupSequenceNumber {
    type ProtobufType = crate::protobuf_types::v1::DedupSequenceNumber;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EpochSequenceNumber {
    pub leader_epoch: LeaderEpoch,
    pub sequence_number: MessageIndex,
}

impl EpochSequenceNumber {
    pub fn new(leader_epoch: LeaderEpoch) -> Self {
        EpochSequenceNumber {
            leader_epoch,
            sequence_number: 0,
        }
    }

    pub fn next(mut self) -> Self {
        self.sequence_number += 1;
        self
    }

    pub fn advance_by(mut self, n: u64) -> Self {
        self.sequence_number += n;
        self
    }
}

impl std::ops::RangeBounds<EpochSequenceNumber>
    for std::ops::Range<std::ops::Bound<EpochSequenceNumber>>
{
    fn start_bound(&self) -> std::ops::Bound<&EpochSequenceNumber> {
        self.start.as_ref()
    }

    fn end_bound(&self) -> std::ops::Bound<&EpochSequenceNumber> {
        self.end.as_ref()
    }
}

/// Epoch sequence numbers are lexicographically ordered with respect to leader_epoch and then
/// sequence_number.
impl PartialOrd for EpochSequenceNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            self.leader_epoch
                .cmp(&other.leader_epoch)
                .then_with(|| self.sequence_number.cmp(&other.sequence_number)),
        )
    }
}

pub trait ReadDeduplicationTable {
    fn get_dedup_sequence_number(
        &mut self,
        producer_id: &ProducerId,
    ) -> impl Future<Output = Result<Option<DedupSequenceNumber>>> + Send;
}

pub trait WriteDeduplicationTable {
    fn put_dedup_seq_number(
        &mut self,
        producer_id: ProducerId,
        dedup_sequence_number: &DedupSequenceNumber,
    ) -> Result<()>;
}

// Flexbuffers does not support u128 so we need to
// make this representation for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct U128 {
    h: u64,
    l: u64,
}

impl From<u128> for U128 {
    fn from(value: u128) -> Self {
        Self {
            h: (value >> 64) as u64,
            l: value as u64,
        }
    }
}

impl From<U128> for u128 {
    fn from(value: U128) -> Self {
        let v = (value.h as u128) << 64;
        v | (value.l as u128)
    }
}

#[cfg(test)]
mod test {
    use crate::deduplication_table::U128;

    #[test]
    fn test_u128() {
        let x = u128::MAX;
        let y = U128::from(x);
        let z = u128::from(y);
        assert_eq!(x, z);
    }
}
