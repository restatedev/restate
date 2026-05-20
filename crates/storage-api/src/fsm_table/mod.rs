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
use strum::EnumCount;

use restate_limiter::RuleBook;
use restate_types::identifiers::LeaderEpoch;
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;
use restate_types::partitions::state::ReplicaSetState;
use restate_types::replication::ReplicationProperty;
use restate_types::schema::Schema;
use restate_types::storage::{
    StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError, decode,
    encode,
};
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, SemanticRestateVersion, Version};

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

/// Partition storage format version. Strictly monotonically increasing — each variant
/// represents the cumulative storage layout produced by applying every preceding
/// migration. Variants are tagged as either *local* (safe to run on each replica
/// independently at PP startup before any commands are applied) or *coordinated*
/// (must be reached via a `MigrationBarrierCommand` so every replica observes the
/// cutover at the same WAL LSN).
///
/// The actual migration runners live in `restate-partition-store` since they need
/// concrete access to the partition store; the type itself lives here so it can be
/// referenced by the `FsmTable` traits.
// NOTE: The representation numbers here must be strictly monotonically increasing.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, strum::FromRepr, strum::EnumCount)]
#[repr(u16)]
pub enum StorageFormatVersion {
    /// Before 1.5
    None = 0,
    /// Migrations:
    /// * Invocation status V1 -> V2
    ///
    /// *Since v1.5.0*
    V1_5 = 1,
    /// Migrations (coordinated):
    /// * Inbox, invocation status, state, promise, timer tables → vqueue layout.
    ///
    /// Coordinated migration — must be reached via a `MigrationBarrierCommand` so every
    /// replica observes the cutover at the same WAL LSN.
    ///
    /// *Since v1.7.0*
    Vqueues = 2,
}

pub const LATEST_STORAGE_FORMAT: StorageFormatVersion =
    StorageFormatVersion::from_repr((StorageFormatVersion::COUNT as u16) - 1).unwrap();

/// The storage format an empty partition store can be initialized with. It is the last version
/// which only requires local migration steps.
pub const INIT_STORAGE_FORMAT: StorageFormatVersion = StorageFormatVersion::V1_5;

impl From<u16> for StorageFormatVersion {
    fn from(value: u16) -> Self {
        StorageFormatVersion::from_repr(value).unwrap_or(StorageFormatVersion::V1_5)
    }
}

impl StorageFormatVersion {
    /// True if this version's migration (from `self.prev()` to `self`) needs to be
    /// coordinated across the replica set via a `MigrationBarrierCommand` so every
    /// replica observes the cutover at the same WAL LSN. Local migrations can run on
    /// each replica independently at PP startup before any commands are applied.
    pub const fn requires_coordinated_migration(self) -> bool {
        match self {
            StorageFormatVersion::None => false,
            StorageFormatVersion::V1_5 => false,
            StorageFormatVersion::Vqueues => true,
        }
    }

    /// The next version in the sequence. Callers must ensure `self != LATEST_STORAGE_FORMAT`
    /// before invoking — calling on the last variant clamps to `V1_5` via the lossy
    /// `From<u16>` impl (an artifact of the wire-compat fallback).
    pub fn next(self) -> Self {
        (self as u16)
            .checked_add(1)
            .expect("storage format must be <= u16::MAX")
            .into()
    }
}

pub trait ReadFsmTable {
    fn get_inbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_;

    fn get_outbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_;

    fn get_applied_lsn(&mut self) -> impl Future<Output = Result<Option<Lsn>>> + Send + '_;

    fn get_min_restate_version(
        &mut self,
    ) -> impl Future<Output = Result<SemanticRestateVersion>> + Send + '_;

    fn get_partition_durability(
        &mut self,
    ) -> impl Future<Output = Result<Option<PartitionDurability>>> + Send + '_;

    fn get_schema(&mut self) -> impl Future<Output = Result<Option<Schema>>> + Send + '_;

    fn get_partition_config_state(
        &mut self,
    ) -> impl Future<Output = Result<Option<CachedEpochMetadata>>> + Send + '_;

    /// The rule book persisted for this partition. Returns `None` when the
    /// partition has not yet observed any rule-book update, in which case
    /// callers should treat it as the empty default.
    /// *Since v1.7.0*
    fn get_rule_book(&mut self) -> impl Future<Output = Result<Option<RuleBook>>> + Send + '_;

    /// The partition's storage format version (FSM variable `STORAGE_VERSION`).
    /// Identifies which storage-layout migrations have been applied. Used by the
    /// `MigrationBarrierCommand` apply path for idempotency, and by runtime
    /// feature gates that must wait until a barrier has crossed the WAL.
    /// *Since v1.7.0*
    fn get_storage_version(
        &mut self,
    ) -> impl Future<Output = Result<StorageFormatVersion>> + Send + '_;
}

pub trait WriteFsmTable {
    fn put_applied_lsn(&mut self, lsn: Lsn) -> Result<()>;

    fn put_inbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()>;

    fn put_outbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()>;

    fn put_min_restate_version(&mut self, version: &SemanticRestateVersion) -> Result<()>;

    fn put_partition_durability(&mut self, durability: &PartitionDurability) -> Result<()>;

    fn put_schema(&mut self, schema: &Schema) -> Result<()>;

    fn put_partition_config_state(&mut self, state: &CachedEpochMetadata) -> Result<()>;

    /// Persist the rule book for this partition.
    /// *Since v1.7.0*
    fn put_rule_book(&mut self, rule_book: &RuleBook) -> Result<()>;

    /// Set the partition's storage format version (FSM variable `STORAGE_VERSION`).
    /// *Since v1.7.0*
    fn put_storage_version(&mut self, version: StorageFormatVersion) -> Result<()>;
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
    fn decode<B: bytes::Buf>(
        buf: &mut B,
        kind: StorageCodecKind,
    ) -> Result<Self, StorageDecodeError>
    where
        Self: Sized,
    {
        assert_eq!(kind, StorageCodecKind::Bilrost);

        decode::decode_bilrost(buf)
    }
}
