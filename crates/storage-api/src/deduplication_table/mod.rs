// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{protobuf_storage_encode_decode, Result};
use bytestring::ByteString;
use futures_util::Stream;
use restate_types::identifiers::{LeaderEpoch, PartitionId};
use restate_types::message::MessageIndex;
use std::cmp::Ordering;
use std::future::Future;

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
}

static SELF_PRODUCER: ByteString = ByteString::from_static("SELF");

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ProducerId {
    Partition(PartitionId),
    Other(ByteString),
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

protobuf_storage_encode_decode!(DedupSequenceNumber);

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

pub trait ReadOnlyDeduplicationTable {
    fn get_dedup_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producer_id: &ProducerId,
    ) -> impl Future<Output = Result<Option<DedupSequenceNumber>>> + Send;

    fn get_all_sequence_numbers(
        &mut self,
        partition_id: PartitionId,
    ) -> impl Stream<Item = Result<DedupInformation>> + Send;
}

pub trait DeduplicationTable: ReadOnlyDeduplicationTable {
    fn put_dedup_seq_number(
        &mut self,
        partition_id: PartitionId,
        producer_id: ProducerId,
        dedup_sequence_number: DedupSequenceNumber,
    ) -> impl Future<Output = ()> + Send;
}
