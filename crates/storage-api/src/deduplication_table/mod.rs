// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Result;
use bytestring::ByteString;
use futures_util::Stream;
use restate_types::dedup::{DedupInformation, DedupSequenceNumber, ProducerId};
use restate_types::identifiers::PartitionId;
use std::future::Future;

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq, Clone)]
pub enum SequenceNumberSource {
    Partition(PartitionId),
    Ingress(ByteString),
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
