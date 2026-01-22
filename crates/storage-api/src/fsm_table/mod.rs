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

use bytes::BytesMut;

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
}

pub trait WriteFsmTable {
    fn put_applied_lsn(&mut self, lsn: Lsn) -> Result<()>;

    fn put_inbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()>;

    fn put_outbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()>;

    fn put_min_restate_version(&mut self, version: &SemanticRestateVersion) -> Result<()>;

    fn put_partition_durability(&mut self, durability: &PartitionDurability) -> Result<()>;

    fn put_schema(&mut self, schema: &Schema) -> Result<()>;

    fn put_partition_config_state(&mut self, state: &CachedEpochMetadata) -> Result<()>;
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
