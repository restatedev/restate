// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use bytes::BytesMut;

use restate_limiter::RuleBook;
use restate_types::identifiers::LeaderEpoch;
use restate_types::logs::{Lsn, SequenceNumber as _};
use restate_types::message::MessageIndex;
use restate_types::partitions::features::PersistedFeatures;
use restate_types::partitions::state::ReplicaSetState;
use restate_types::replication::ReplicationProperty;
use restate_types::schema::SchemaUnindexedLite;
use restate_types::storage::{
    StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError, decode,
    encode,
};
use restate_types::time::MillisSinceEpoch;
use restate_types::{
    GenerationalNodeId, SemanticRestateVersion, Version, bilrost_storage_encode_decode,
};

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

pub trait ReadFsmTable {
    fn get_inbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_;

    fn get_outbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_;

    /// The applied LSN: the one in the [`FsmPosition`] if the partition has one, else the one
    /// persisted on its own. A position persisted before any record was applied has none.
    fn get_applied_lsn(&mut self) -> impl Future<Output = Result<Option<Lsn>>> + Send + '_;

    /// The partition's position, `None` while it doesn't track byte offsets.
    /// *Since v1.8.2*
    fn get_fsm_position(&mut self)
    -> impl Future<Output = Result<Option<FsmPosition>>> + Send + '_;

    fn get_min_restate_version(
        &mut self,
    ) -> impl Future<Output = Result<SemanticRestateVersion>> + Send + '_;

    fn get_partition_durability(
        &mut self,
    ) -> impl Future<Output = Result<Option<PartitionDurability>>> + Send + '_;

    fn get_schema(
        &mut self,
    ) -> impl Future<Output = Result<Option<SchemaUnindexedLite>>> + Send + '_;

    fn get_partition_config_state(
        &mut self,
    ) -> impl Future<Output = Result<Option<CachedEpochMetadata>>> + Send + '_;

    /// The rule book persisted for this partition. Returns `None` when the
    /// partition has not yet observed any rule-book update, in which case
    /// callers should treat it as the empty default.
    /// *Since v1.7.0*
    fn get_rule_book(&mut self) -> impl Future<Output = Result<Option<RuleBook>>> + Send + '_;

    /// The set of state-machine features enabled for this partition. Defaults to
    /// [`PersistedFeatures::default`] (all features disabled) when the
    /// partition has not yet applied a [`VersionBarrierCommand`] carrying feature
    /// changes.
    /// *Since v1.7.0*
    fn get_state_machine_features(
        &mut self,
    ) -> impl Future<Output = Result<PersistedFeatures>> + Send + '_;
}

pub trait WriteFsmTable {
    fn put_applied_lsn(&mut self, lsn: Lsn) -> Result<()>;

    fn put_inbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()>;

    fn put_outbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()>;

    fn put_min_restate_version(&mut self, version: &SemanticRestateVersion) -> Result<()>;

    fn put_partition_durability(&mut self, durability: &PartitionDurability) -> Result<()>;

    fn put_schema(&mut self, schema: &SchemaUnindexedLite) -> Result<()>;

    fn put_partition_config_state(&mut self, state: &CachedEpochMetadata) -> Result<()>;

    /// Persist the rule book for this partition.
    /// *Since v1.7.0*
    fn put_rule_book(&mut self, rule_book: &RuleBook) -> Result<()>;

    /// Persist the set of state-machine features enabled for this partition.
    /// *Since v1.7.0*
    fn put_state_machine_features(&mut self, features: &PersistedFeatures) -> Result<()>;
}

/// Where a partition stands in its log: its applied LSN, and how far it has applied its log in
/// bytes.
///
/// The byte offsets are positions: they count from the record that turned byte offsets on for
/// the partition, its byte offsets base LSN, and wrap at 2^64. They only move forward, except
/// that a replica restored from a snapshot continues from the snapshot's position. A partition
/// persists its position with every commit while it tracks byte offsets; without a base LSN
/// ([`Lsn::INVALID`]), it doesn't.
///
/// Every field has a fixed-width encoding, so the size of an encoded position depends only on
/// which of its fields are zero (omitted). New fields can be added under new tags; servers that
/// don't know them skip them.
///
/// *Since v1.8.2*
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Message)]
pub struct FsmPosition {
    /// The partition's applied LSN.
    #[bilrost(tag(1), encoding(fixed))]
    pub applied_lsn: Lsn,
    /// The LSN of the record that turned byte offsets on. The byte offsets count the records
    /// after it. The same on every replica.
    #[bilrost(tag(2), encoding(fixed))]
    pub byte_offsets_base_lsn: Lsn,
    /// How far the partition has applied its log, in bytes: the size of the records it read
    /// from its log after the base LSN, up to the applied LSN, as the log stores them. Records
    /// it skips as duplicates count too; records outside the partition's key range never reach
    /// it and don't. Replicas at the same applied LSN normally agree on it, but it's an estimate
    /// that can differ slightly between them, for example between versions that size records
    /// differently; nothing depends on them agreeing.
    #[bilrost(tag(3), encoding(fixed))]
    pub log_byte_offset: u64,
}

bilrost_storage_encode_decode!(FsmPosition);

impl FsmPosition {
    /// The position of a partition at `applied_lsn` that doesn't track byte offsets.
    pub fn new(applied_lsn: Lsn) -> Self {
        Self {
            applied_lsn,
            byte_offsets_base_lsn: Lsn::INVALID,
            log_byte_offset: 0,
        }
    }

    /// Whether the partition tracks byte offsets.
    pub fn has_byte_offsets(&self) -> bool {
        self.byte_offsets_base_lsn != Lsn::INVALID
    }

    /// Turns byte offsets on at the record at `lsn`: they start at zero and count the records
    /// after it.
    pub fn start_byte_offsets(&mut self, lsn: Lsn) {
        self.byte_offsets_base_lsn = lsn;
        self.log_byte_offset = 0;
    }

    /// Advances to the record or gap that ends at `lsn`, of `bytes` as the log stores it. The log
    /// byte offset counts it if it comes after the base LSN.
    pub fn advance(&mut self, lsn: Lsn, bytes: u64) {
        self.applied_lsn = lsn;
        if self.has_byte_offsets() && lsn > self.byte_offsets_base_lsn {
            self.log_byte_offset = self.log_byte_offset.wrapping_add(bytes);
        }
    }
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

/// Stores the current and next replica set state from the latest AnnounceLeader.
/// *Since v1.6*
#[derive(Debug, Clone, bilrost::Message)]
pub struct CachedEpochMetadata {
    #[bilrost(tag(1))]
    pub version: Version,
    #[bilrost(tag(2))]
    pub leader_node_id: GenerationalNodeId,
    #[bilrost(tag(3))]
    pub leader_epoch: LeaderEpoch,
    /// The current replica set state at the time of the announcement.
    #[bilrost(tag(4))]
    pub current: CurrentReplicaSetState,
    /// The next replica set state
    #[bilrost(tag(5))]
    pub next: Option<NextReplicaSetState>,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct CurrentReplicaSetState {
    #[bilrost(tag(1))]
    pub replica_set: ReplicaSetState,
    #[bilrost(tag(2))]
    pub modified_at: MillisSinceEpoch,
    #[bilrost(tag(3))]
    pub replication: ReplicationProperty,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct NextReplicaSetState {
    #[bilrost(tag(1))]
    pub replica_set: ReplicaSetState,
}

impl From<ReplicaSetState> for NextReplicaSetState {
    fn from(value: ReplicaSetState) -> Self {
        Self { replica_set: value }
    }
}

impl From<NextReplicaSetState> for ReplicaSetState {
    fn from(value: NextReplicaSetState) -> Self {
        value.replica_set
    }
}

impl StorageEncode for CachedEpochMetadata {
    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::Bilrost
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        encode::encode_bilrost(self, buf)
    }
}

impl StorageDecode for CachedEpochMetadata {
    fn decode<B: bytes::Buf>(buf: B, kind: StorageCodecKind) -> Result<Self, StorageDecodeError>
    where
        Self: Sized,
    {
        assert_eq!(kind, StorageCodecKind::Bilrost);

        decode::decode_bilrost(buf)
    }
}

#[cfg(test)]
mod tests {
    use restate_types::logs::Lsn;

    use super::FsmPosition;

    /// The log byte offset counts the records after the one that turned byte offsets on, and
    /// wraps at 2^64; without byte offsets, a position only follows the applied LSN.
    #[test]
    fn fsm_position_counts_the_records_after_the_base_lsn() {
        let mut position = FsmPosition::new(Lsn::new(1));
        position.advance(Lsn::new(2), 20);
        assert!(!position.has_byte_offsets());
        assert_eq!(position, FsmPosition::new(Lsn::new(2)));

        position.start_byte_offsets(Lsn::new(3));
        for (lsn, bytes) in [(3, 30), (4, 40), (5, 50)] {
            position.advance(Lsn::new(lsn), bytes);
        }
        assert!(position.has_byte_offsets());
        assert_eq!(
            position,
            FsmPosition {
                applied_lsn: Lsn::new(5),
                byte_offsets_base_lsn: Lsn::new(3),
                log_byte_offset: 90,
            }
        );

        position.log_byte_offset = u64::MAX - 5;
        position.advance(Lsn::new(6), 10);
        assert_eq!(position.log_byte_offset, 4);
    }
}
