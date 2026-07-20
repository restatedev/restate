// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::TableKind::PartitionStateMachine;
use crate::keys::{EncodeTableKey, KeyKind, define_table_key};
use crate::{
    PaddedPartitionId, PartitionDb, PartitionStore, PartitionStoreTransaction, StorageAccess,
};
use restate_limiter::RuleBook;
use restate_storage_api::fsm_table::{
    CachedEpochMetadata, PartitionDurability, ReadFsmTable, SequenceNumber, WriteFsmTable,
};
use restate_storage_api::protobuf_types::{PartitionStoreProtobufValue, ProtobufStorageWrapper};
use restate_storage_api::{Result, StorageError};
use restate_types::SemanticRestateVersion;
use restate_types::identifiers::PartitionId;
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;
use restate_types::partitions::StorageVersion;
use restate_types::partitions::features::PersistedFeatures;
use restate_types::schema::Schema;
use restate_types::storage::{StorageCodec, StorageDecode};

define_table_key!(
    PartitionStateMachine,
    KeyKind::Fsm,
    PartitionStateMachineKey(partition_id: PaddedPartitionId, state_id: u64)
);

#[inline]
fn create_key(
    partition_id: impl Into<PaddedPartitionId>,
    state_id: u64,
) -> PartitionStateMachineKey {
    PartitionStateMachineKey {
        partition_id: partition_id.into(),
        state_id,
    }
}

// 'ps' | PaddedPartitionId | state_id
static_assertions::const_assert_eq!(PartitionStateMachineKey::serialized_length_fixed(), 18);

impl PartitionStateMachineKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PaddedPartitionId>()
            + std::mem::size_of::<u64>()
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::serialized_length_fixed()] {
        let mut buf = [0u8; Self::serialized_length_fixed()];
        EncodeTableKey::serialize_to(self, &mut buf.as_mut());
        buf
    }
}

pub(crate) mod fsm_variable {
    pub(crate) const INBOX_SEQ_NUMBER: u64 = 0;
    pub(crate) const OUTBOX_SEQ_NUMBER: u64 = 1;

    pub(crate) const APPLIED_LSN: u64 = 2;
    pub(crate) const RESTATE_VERSION_BARRIER: u64 = 3;
    pub(crate) const PARTITION_DURABILITY: u64 = 4;

    /// Schema versions are represented as a strictly monotonically increasing number.
    /// This represent the partition storage schema version, not the user services schema.
    pub(crate) const STORAGE_VERSION: u64 = 5;

    pub(crate) const SERVICES_SCHEMA_METADATA: u64 = 6;

    /// Stores the current and next partition configuration from the latest AnnounceLeader.
    /// *Since v1.6*
    pub(crate) const PARTITION_CONFIG_STATE: u64 = 7;

    /// Set to 1 once the one-time cleanup of orphaned `jc` index entries has completed.
    /// These orphans were caused by a bug in `delete_journal` that used the wrong scan
    /// prefix when deleting `JournalCompletionIdToCommandIndex` entries.
    ///
    /// Can be removed in v1.8 once we are confident this cleanup has been executed on all
    /// deployments.
    /// *Since v1.7.0*
    pub(crate) const JC_ORPHAN_CLEANUP_DONE: u64 = 8;

    /// Cluster-global rule book persisted per-partition. Each partition writes
    /// the same logical rule book (via `Command::UpsertRuleBook` log entries),
    /// and reads it back on PP startup so leader transitions inherit the same
    /// rule set without an extra metadata-store round trip.
    /// *Since v1.7.0*
    pub(crate) const RULE_BOOK: u64 = 9;

    /// Set of state-machine features enabled for this partition. Updated by
    /// `VersionBarrierCommand` entries carrying feature changes.
    /// *Since v1.7.0*
    pub(crate) const STATE_MACHINE_FEATURES: u64 = 10;
}

fn get<T: PartitionStoreProtobufValue, S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    state_id: u64,
) -> Result<Option<T>>
where
    <<T as PartitionStoreProtobufValue>::ProtobufType as TryInto<T>>::Error: Into<anyhow::Error>,
{
    storage.get_value_proto(create_key(partition_id, state_id))
}

/// Forces a read from persistent storage, bypassing memtables and block cache.
fn get_durable<T: PartitionStoreProtobufValue, S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    state_id: u64,
) -> Result<Option<T>>
where
    <<T as PartitionStoreProtobufValue>::ProtobufType as TryInto<T>>::Error: Into<anyhow::Error>,
{
    storage.get_durable_value(create_key(partition_id, state_id))
}

fn put<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    state_id: u64,
    state_value: &(impl PartitionStoreProtobufValue + Clone + 'static),
) -> Result<()> {
    let key = PartitionStateMachineKey {
        partition_id: partition_id.into(),
        state_id,
    };
    storage.put_kv_proto(key, state_value)
}

pub async fn get_locally_durable_lsn(partition_store: &mut PartitionStore) -> Result<Option<Lsn>> {
    get_durable::<SequenceNumber, _>(
        partition_store,
        partition_store.partition_id(),
        fsm_variable::APPLIED_LSN,
    )
    .map(|opt| opt.map(|seq_number| Lsn::from(u64::from(seq_number))))
}

pub(crate) fn get_storage_version_from_partition_db(db: &PartitionDb) -> Result<StorageVersion> {
    let sequence_number: Option<SequenceNumber> =
        get_proto_from_partition_db(db, fsm_variable::STORAGE_VERSION)?;

    Ok(if let Some(sequence_number) = sequence_number {
        let raw = u16::try_from(sequence_number.0).map_err(|_| StorageError::DataIntegrityError)?;
        StorageVersion::try_from(raw)?
    } else {
        StorageVersion::None
    })
}

pub(crate) async fn put_storage_version<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    last_executed_migration: u16,
) -> Result<()> {
    put(
        storage,
        partition_id,
        fsm_variable::STORAGE_VERSION,
        &SequenceNumber::from(last_executed_migration as u64),
    )
}

/// Append a `STORAGE_VERSION = version` put to `wb`.
pub(crate) fn append_storage_version_to_wb(
    cf_handle: &std::sync::Arc<rocksdb::BoundColumnFamily<'_>>,
    wb: &mut rocksdb::WriteBatch,
    partition_id: PartitionId,
    version: StorageVersion,
) -> Result<()> {
    use bytes::BytesMut;
    use restate_types::storage::StorageCodec;

    let key = create_key(partition_id, fsm_variable::STORAGE_VERSION);
    let key_buffer = key.to_bytes();

    let value = SequenceNumber::from(version as u64);
    let mut value_buffer = BytesMut::new();
    StorageCodec::encode(
        &ProtobufStorageWrapper::<<SequenceNumber as PartitionStoreProtobufValue>::ProtobufType>(
            value.into(),
        ),
        &mut value_buffer,
    )
    .map_err(|e| restate_storage_api::StorageError::Generic(e.into()))?;

    wb.put_cf(cf_handle, key_buffer, &value_buffer);
    Ok(())
}

pub(crate) async fn is_jc_orphan_cleanup_done<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
) -> Result<bool> {
    get::<SequenceNumber, _>(storage, partition_id, fsm_variable::JC_ORPHAN_CLEANUP_DONE)
        .map(|opt| opt.is_some())
}

pub(crate) async fn put_jc_orphan_cleanup_done<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
) -> Result<()> {
    put(
        storage,
        partition_id,
        fsm_variable::JC_ORPHAN_CLEANUP_DONE,
        &SequenceNumber::from(1u64),
    )
}

/// Reads a `StorageCodec`-encoded value directly from the partition db's column family.
///
/// [`PartitionDb`] does not implement [`StorageAccess`], so we read the raw bytes from
/// RocksDB and decode via the standard prost path rather than the `BytesMut` arena used
/// by [`PartitionStore`].
fn get_storage_codec_from_partition_db<V: StorageDecode>(
    db: &PartitionDb,
    state_id: u64,
) -> Result<Option<V>> {
    let cf = db.cf_handle();
    let key = create_key(db.partition().id(), state_id);
    db.rocksdb()
        .inner()
        .as_raw_db()
        .get_pinned_cf(cf, key.to_bytes())
        .map_err(|err| StorageError::Generic(err.into()))?
        .map(|value| {
            let mut slice = value.as_ref();
            StorageCodec::decode::<V, _>(&mut slice)
        })
        .transpose()
        .map_err(|err| StorageError::Conversion(err.into()))
}

fn get_proto_from_partition_db<T: PartitionStoreProtobufValue>(
    db: &PartitionDb,
    state_id: u64,
) -> Result<Option<T>>
where
    <<T as PartitionStoreProtobufValue>::ProtobufType as TryInto<T>>::Error: Into<anyhow::Error>,
{
    get_storage_codec_from_partition_db::<ProtobufStorageWrapper<T::ProtobufType>>(db, state_id)?
        .map(|wrapper| wrapper.0.try_into())
        .transpose()
        .map_err(|err| StorageError::Conversion(err.into()))
}

impl ReadFsmTable for PartitionDb {
    async fn get_inbox_seq_number(&mut self) -> Result<MessageIndex> {
        get_proto_from_partition_db::<SequenceNumber>(self, fsm_variable::INBOX_SEQ_NUMBER)
            .map(|opt| opt.map(Into::into).unwrap_or_default())
    }

    async fn get_outbox_seq_number(&mut self) -> Result<MessageIndex> {
        get_proto_from_partition_db::<SequenceNumber>(self, fsm_variable::OUTBOX_SEQ_NUMBER)
            .map(|opt| opt.map(Into::into).unwrap_or_default())
    }

    async fn get_applied_lsn(&mut self) -> Result<Option<Lsn>> {
        get_proto_from_partition_db::<SequenceNumber>(self, fsm_variable::APPLIED_LSN)
            .map(|opt| opt.map(|seq_number| Lsn::from(u64::from(seq_number))))
    }

    async fn get_min_restate_version(&mut self) -> Result<SemanticRestateVersion> {
        get_proto_from_partition_db::<SemanticRestateVersion>(
            self,
            fsm_variable::RESTATE_VERSION_BARRIER,
        )
        .map(|opt| opt.unwrap_or_default())
    }

    async fn get_partition_durability(&mut self) -> Result<Option<PartitionDurability>> {
        get_proto_from_partition_db::<PartitionDurability>(self, fsm_variable::PARTITION_DURABILITY)
    }

    async fn get_schema(&mut self) -> Result<Option<Schema>> {
        get_storage_codec_from_partition_db(self, fsm_variable::SERVICES_SCHEMA_METADATA)
    }

    async fn get_partition_config_state(&mut self) -> Result<Option<CachedEpochMetadata>> {
        get_storage_codec_from_partition_db(self, fsm_variable::PARTITION_CONFIG_STATE)
    }

    async fn get_rule_book(&mut self) -> Result<Option<RuleBook>> {
        get_storage_codec_from_partition_db(self, fsm_variable::RULE_BOOK)
    }

    async fn get_state_machine_features(&mut self) -> Result<PersistedFeatures> {
        get_storage_codec_from_partition_db(self, fsm_variable::STATE_MACHINE_FEATURES)
            .map(|opt| opt.unwrap_or_default())
    }
}

impl ReadFsmTable for PartitionStore {
    async fn get_inbox_seq_number(&mut self) -> Result<MessageIndex> {
        get::<SequenceNumber, _>(self, self.partition_id(), fsm_variable::INBOX_SEQ_NUMBER)
            .map(|opt| opt.map(Into::into).unwrap_or_default())
    }

    async fn get_outbox_seq_number(&mut self) -> Result<MessageIndex> {
        get::<SequenceNumber, _>(self, self.partition_id(), fsm_variable::OUTBOX_SEQ_NUMBER)
            .map(|opt| opt.map(Into::into).unwrap_or_default())
    }

    async fn get_applied_lsn(&mut self) -> Result<Option<Lsn>> {
        get::<SequenceNumber, _>(self, self.partition_id(), fsm_variable::APPLIED_LSN)
            .map(|opt| opt.map(|seq_number| Lsn::from(u64::from(seq_number))))
    }

    async fn get_min_restate_version(&mut self) -> Result<SemanticRestateVersion> {
        get::<SemanticRestateVersion, _>(
            self,
            self.partition_id(),
            fsm_variable::RESTATE_VERSION_BARRIER,
        )
        .map(|opt| opt.unwrap_or_default())
    }

    async fn get_partition_durability(&mut self) -> Result<Option<PartitionDurability>> {
        get::<PartitionDurability, _>(
            self,
            self.partition_id(),
            fsm_variable::PARTITION_DURABILITY,
        )
    }

    async fn get_schema(&mut self) -> Result<Option<Schema>> {
        let key = create_key(self.partition_id(), fsm_variable::SERVICES_SCHEMA_METADATA);
        self.get_value_storage_codec(key)
    }

    async fn get_partition_config_state(&mut self) -> Result<Option<CachedEpochMetadata>> {
        let key = create_key(self.partition_id(), fsm_variable::PARTITION_CONFIG_STATE);
        self.get_value_storage_codec(key)
    }

    async fn get_rule_book(&mut self) -> Result<Option<RuleBook>> {
        let key = create_key(self.partition_id(), fsm_variable::RULE_BOOK);
        self.get_value_storage_codec(key)
    }

    async fn get_state_machine_features(&mut self) -> Result<PersistedFeatures> {
        let key = create_key(self.partition_id(), fsm_variable::STATE_MACHINE_FEATURES);
        self.get_value_storage_codec(key)
            .map(|opt| opt.unwrap_or_default())
    }
}

impl WriteFsmTable for PartitionStoreTransaction<'_> {
    fn put_applied_lsn(&mut self, lsn: Lsn) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::APPLIED_LSN,
            &SequenceNumber::from(u64::from(lsn)),
        )
    }

    fn put_inbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::INBOX_SEQ_NUMBER,
            &SequenceNumber::from(seq_number),
        )
    }

    fn put_outbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::OUTBOX_SEQ_NUMBER,
            &SequenceNumber::from(seq_number),
        )
    }

    fn put_min_restate_version(&mut self, version: &SemanticRestateVersion) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::RESTATE_VERSION_BARRIER,
            version,
        )
    }

    fn put_partition_durability(&mut self, durability: &PartitionDurability) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::PARTITION_DURABILITY,
            durability,
        )
    }

    fn put_schema(&mut self, schema: &Schema) -> Result<()> {
        let key = create_key(self.partition_id(), fsm_variable::SERVICES_SCHEMA_METADATA);
        self.put_kv_storage_codec(key, schema)
    }

    fn put_partition_config_state(&mut self, state: &CachedEpochMetadata) -> Result<()> {
        let key = create_key(self.partition_id(), fsm_variable::PARTITION_CONFIG_STATE);
        self.put_kv_storage_codec(key, state)
    }

    fn put_rule_book(&mut self, rule_book: &RuleBook) -> Result<()> {
        let key = create_key(self.partition_id(), fsm_variable::RULE_BOOK);
        self.put_kv_storage_codec(key, rule_book)
    }

    fn put_state_machine_features(&mut self, features: &PersistedFeatures) -> Result<()> {
        let key = create_key(self.partition_id(), fsm_variable::STATE_MACHINE_FEATURES);
        self.put_kv_storage_codec(key, features)
    }
}
