// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use restate_types::SemanticRestateVersion;
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;
use restate_types::time::MillisSinceEpoch;

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

pub trait ReadOnlyFsmTable {
    fn get_inbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_;

    fn get_outbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_;

    fn get_applied_lsn(&mut self) -> impl Future<Output = Result<Option<Lsn>>> + Send + '_;

    fn get_min_restate_version(
        &mut self,
    ) -> impl Future<Output = Result<SemanticRestateVersion>> + Send + '_;

    fn get_partition_durability(
        &mut self,
    ) -> impl Future<Output = Result<Option<PartitionDurability>>> + Send + '_;
}

pub trait FsmTable {
    fn put_applied_lsn(&mut self, lsn: Lsn) -> impl Future<Output = Result<()>> + Send;

    fn put_inbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> impl Future<Output = Result<()>> + Send;

    fn put_outbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> impl Future<Output = Result<()>> + Send;

    fn put_min_restate_version(
        &mut self,
        version: &SemanticRestateVersion,
    ) -> impl Future<Output = Result<()>> + Send;

    fn put_partition_durability(
        &mut self,
        durability: &PartitionDurability,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Debug, Clone, Copy, derive_more::From, derive_more::Into)]
pub struct SequenceNumber(pub u64);

impl PartitionStoreProtobufValue for SequenceNumber {
    type ProtobufType = crate::protobuf_types::v1::SequenceNumber;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionDurability {
    /// The partition has applied this LSN durably to the replica-set and/or has been
    /// persisted in a snapshot in the snapshot repository.
    pub durable_point: Lsn,
    /// Timestamp which the durability point was updated
    pub modification_time: MillisSinceEpoch,
}

// Ord PartitionDurability based on durable_point only
impl Ord for PartitionDurability {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.durable_point.cmp(&other.durable_point)
    }
}

impl PartialOrd for PartitionDurability {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartitionStoreProtobufValue for PartitionDurability {
    type ProtobufType = crate::protobuf_types::v1::PartitionDurability;
}
