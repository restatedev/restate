// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::warn;

use restate_limiter::RuleBook;
use restate_storage_api::fsm_table::{
    CachedEpochMetadata, FsmPosition, PartitionDurability, ReadFsmTable, SequenceNumber,
    WriteFsmTable,
};
use restate_storage_api::protobuf_types::{PartitionStoreProtobufValue, ProtobufStorageWrapper};
use restate_storage_api::{Result, StorageError};
use restate_types::SemanticRestateVersion;
use restate_types::identifiers::PartitionId;
use restate_types::logs::{Lsn, SequenceNumber as _};
use restate_types::message::MessageIndex;
use restate_types::partitions::StorageVersion;
use restate_types::partitions::features::PersistedFeatures;
use restate_types::schema::SchemaUnindexedLite;
use restate_types::storage::{StorageCodec, StorageDecode};

use crate::TableKind::PartitionStateMachine;
use crate::features::PersistedEnabledFeatures;
use crate::keys::{EncodeTableKey, KeyKind, define_table_key};
use crate::{
    PaddedPartitionId, PartitionDb, PartitionSeal, PartitionStore, PartitionStoreTransaction,
    StorageAccess, StorageRead,
};

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

    /// The partition's applied LSN while it doesn't track byte offsets. The commit that turns
    /// byte offsets on deletes it; from then on, [`FSM_POSITION`] holds the applied LSN instead.
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

    /// A local marker (not replicated) written to indicate that the partition store
    /// is unsafe and should be not used by processors. The data is json serialized
    /// and contains the reason for the seal. The intention is to prevent processors
    /// from using this partition store until it has been removed and replaced with
    /// an unsealed safe one.
    ///
    /// *Since v1.7.3*
    pub(crate) const SEAL_MARKER: u64 = 11;

    /// A local set of storage features (not replicated) written to mark which
    /// storage features are fully enabled. For instance, a feature for a secondary
    /// index will be enabled _after_ the index gets persisted consistently with the data.
    ///
    /// Persisted as plain JSON.
    ///
    /// *Since v1.7.9*
    pub(crate) const STORAGE_FEATURES: u64 = 12;

    /// The partition's [`FsmPosition`](restate_storage_api::fsm_table::FsmPosition): its applied
    /// LSN, and how far it has applied its log and written its partition store in bytes since the
    /// record that turned byte offsets on. Written with every commit while the partition tracks
    /// byte offsets, in place of [`APPLIED_LSN`], which the commit that turns them on deletes. If
    /// both are present, the position takes precedence. The byte offsets wrap at 2^64.
    ///
    /// The partition store byte offset is local (not replicated), like [`SEAL_MARKER`] and
    /// [`STORAGE_FEATURES`]: it counts this store's write batches. After a crash, the store replays
    /// the commits it hadn't flushed in different batches, and a store restored from a snapshot
    /// continues from the snapshot's. The applied LSN and the base LSN are the same on
    /// every replica; replicas at the same applied LSN normally agree on the log byte offset too,
    /// but it can differ slightly between them.
    ///
    /// *Since v1.8.2*
    pub(crate) const FSM_POSITION: u64 = 13;

    /// The highest outbox message index whose blob reference has been released.
    /// Used to deduplicate blob releases re-proposed after a leadership change.
    ///
    /// *Since v1.8.0*
    pub(crate) const RELEASED_OUTBOX_BLOB_INDEX: u64 = 14;
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

pub fn get_locally_durable_lsn(partition_store: &mut PartitionStore) -> Result<Option<Lsn>> {
    read_applied_lsn(partition_store.partition_db(), rocksdb::ReadTier::Persisted)
}

/// The applied LSN of partition `partition_id`, from its column family `cf` in a partition store
/// that a tool opened with RocksDB directly. It's read the way the partition reads it: from its
/// [`FsmPosition`] if it has one, else from the applied LSN persisted on its own.
pub fn read_applied_lsn_from_db(
    db: &rocksdb::DB,
    cf: &impl rocksdb::AsColumnFamilyRef,
    partition_id: PartitionId,
) -> Result<Option<Lsn>> {
    read_applied_lsn_from(db, cf, partition_id, rocksdb::ReadTier::All)
}

fn read_applied_lsn(db: &PartitionDb, read_tier: rocksdb::ReadTier) -> Result<Option<Lsn>> {
    read_applied_lsn_from(
        db.rocksdb().inner().as_raw_db(),
        db.cf_handle(),
        db.partition().id(),
        read_tier,
    )
}

/// The partition's applied LSN as [`resolve_applied_lsn`] picks it, with [`FSM_POSITION`] and
/// [`APPLIED_LSN`] read at the same point in time, so that a commit or flush between two reads
/// can't make them look different.
///
/// [`FSM_POSITION`]: fsm_variable::FSM_POSITION
/// [`APPLIED_LSN`]: fsm_variable::APPLIED_LSN
fn read_applied_lsn_from(
    db: &rocksdb::DB,
    cf: &impl rocksdb::AsColumnFamilyRef,
    partition_id: PartitionId,
    read_tier: rocksdb::ReadTier,
) -> Result<Option<Lsn>> {
    let keys = [fsm_variable::FSM_POSITION, fsm_variable::APPLIED_LSN]
        .map(|state_id| create_key(partition_id, state_id).to_bytes());
    let mut read_opts = rocksdb::ReadOptions::default();
    read_opts.set_read_tier(read_tier);
    // a multi-get reads all keys from one view of the column family, also of the persisted tier
    let [position, applied_lsn]: [_; 2] = db
        .multi_get_cf_opt(keys.iter().map(|key| (cf, key)), &read_opts)
        .try_into()
        .expect("a value per key");
    let decode = |value: std::result::Result<Option<Vec<u8>>, rocksdb::Error>| {
        value.map_err(|err| StorageError::Generic(err.into()))
    };
    let position = decode(position)?
        .map(|value| StorageCodec::decode::<FsmPosition, _>(&mut value.as_slice()))
        .transpose()
        .map_err(|err| StorageError::Conversion(err.into()))?;
    let applied_lsn = decode(applied_lsn)?
        .map(|value| {
            StorageCodec::decode::<
                ProtobufStorageWrapper<
                    <SequenceNumber as PartitionStoreProtobufValue>::ProtobufType,
                >,
                _,
            >(&mut value.as_slice())
        })
        .transpose()
        .map_err(|err| StorageError::Conversion(err.into()))?
        .map(|wrapper| SequenceNumber::from(wrapper.0));
    Ok(resolve_applied_lsn(partition_id, position, applied_lsn))
}

/// The applied LSN of the partition's [`FsmPosition`] if it has one, else its [`APPLIED_LSN`].
/// The commit that writes the first position deletes [`APPLIED_LSN`], and the processor doesn't
/// write it after that, so both are only present if something wrote the applied LSN on its own.
///
/// [`APPLIED_LSN`]: fsm_variable::APPLIED_LSN
fn resolve_applied_lsn(
    partition_id: PartitionId,
    position: Option<FsmPosition>,
    applied_lsn: Option<SequenceNumber>,
) -> Option<Lsn> {
    let applied_lsn = applied_lsn.map(|seq_number| Lsn::from(u64::from(seq_number)));
    let Some(position) = position else {
        return applied_lsn;
    };
    if let Some(applied_lsn) = applied_lsn
        && applied_lsn != position.applied_lsn
    {
        warn!(
            %partition_id,
            %applied_lsn,
            fsm_position_applied_lsn = %position.applied_lsn,
            "The partition's applied LSN differs from the one in its FSM position; using the FSM position's",
        );
    }
    // Before the first record is applied, the position has no applied LSN either.
    Some(position.applied_lsn).filter(|lsn| *lsn != Lsn::INVALID)
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

pub(crate) fn get_storage_features_from_partition_db(
    db: &PartitionDb,
) -> Result<Option<PersistedEnabledFeatures>> {
    let cf = db.cf_handle();
    let key = create_key(db.partition().id(), fsm_variable::STORAGE_FEATURES);
    db.rocksdb()
        .inner()
        .as_raw_db()
        .get_pinned_cf(cf, key.to_bytes())
        .map_err(|err| StorageError::Generic(err.into()))?
        .map(|value| {
            serde_json::from_slice(&value).map_err(|err| StorageError::Conversion(err.into()))
        })
        .transpose()
}

#[cfg(test)]
pub(crate) async fn put_storage_features_json<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    value: &[u8],
) -> Result<()> {
    let key = PartitionStateMachineKey {
        partition_id: partition_id.into(),
        state_id: fsm_variable::STORAGE_FEATURES,
    };
    storage.put_kv_raw(key, value)?;
    Ok(())
}

#[cfg(test)]
pub(crate) async fn delete_storage_features<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
) -> Result<()> {
    storage.delete_key(&PartitionStateMachineKey {
        partition_id: partition_id.into(),
        state_id: fsm_variable::STORAGE_FEATURES,
    })
}

pub(crate) fn get_min_restate_version_from_partition_db(
    db: &PartitionDb,
) -> Result<SemanticRestateVersion> {
    get_proto_from_partition_db(db, fsm_variable::RESTATE_VERSION_BARRIER)
        .map(|version| version.unwrap_or_default())
}

pub(crate) fn get_state_machine_features_from_partition_db(
    db: &PartitionDb,
) -> Result<PersistedFeatures> {
    get_storage_codec_from_partition_db(db, fsm_variable::STATE_MACHINE_FEATURES)
        .map(|features| features.unwrap_or_default())
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

pub(crate) fn append_storage_version_to_wb(
    cf_handle: &std::sync::Arc<rocksdb::BoundColumnFamily<'_>>,
    wb: &mut rocksdb::WriteBatch,
    partition_id: PartitionId,
    version: StorageVersion,
) -> Result<()> {
    let key = create_key(partition_id, fsm_variable::STORAGE_VERSION);
    let mut value = bytes::BytesMut::new();
    StorageCodec::encode(
        &ProtobufStorageWrapper::<<SequenceNumber as PartitionStoreProtobufValue>::ProtobufType>(
            SequenceNumber::from(version as u64).into(),
        ),
        &mut value,
    )
    .map_err(|err| StorageError::Generic(err.into()))?;
    wb.put_cf(cf_handle, key.to_bytes(), value);
    Ok(())
}

pub(crate) fn put_storage_features<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    features: &PersistedEnabledFeatures,
) -> Result<()> {
    let key = PartitionStateMachineKey {
        partition_id: partition_id.into(),
        state_id: fsm_variable::STORAGE_FEATURES,
    };
    let value = serde_json::to_vec(features).map_err(|err| StorageError::Conversion(err.into()))?;
    storage.put_kv_raw(key, value)?;
    Ok(())
}

pub(crate) fn append_storage_features_to_wb(
    cf_handle: &std::sync::Arc<rocksdb::BoundColumnFamily<'_>>,
    wb: &mut rocksdb::WriteBatch,
    partition_id: PartitionId,
    features: &PersistedEnabledFeatures,
) -> Result<()> {
    let key = create_key(partition_id, fsm_variable::STORAGE_FEATURES);
    let value = serde_json::to_vec(features).map_err(|err| StorageError::Conversion(err.into()))?;
    wb.put_cf(cf_handle, key.to_bytes(), value);
    Ok(())
}

pub(crate) async fn put_min_restate_version<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    version: &SemanticRestateVersion,
) -> Result<()> {
    put(
        storage,
        partition_id,
        fsm_variable::RESTATE_VERSION_BARRIER,
        version,
    )
}

pub(crate) fn append_min_restate_version_to_wb(
    cf_handle: &std::sync::Arc<rocksdb::BoundColumnFamily<'_>>,
    wb: &mut rocksdb::WriteBatch,
    partition_id: PartitionId,
    version: SemanticRestateVersion,
) -> Result<()> {
    let key = create_key(partition_id, fsm_variable::RESTATE_VERSION_BARRIER);

    let mut value = bytes::BytesMut::new();
    StorageCodec::encode(
        &ProtobufStorageWrapper::<
            <SemanticRestateVersion as PartitionStoreProtobufValue>::ProtobufType,
        >(version.into()),
        &mut value,
    )
    .map_err(|err| StorageError::Generic(err.into()))?;
    wb.put_cf(cf_handle, key.to_bytes(), value);
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

pub(crate) async fn get_partition_seal<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
) -> Result<Option<PartitionSeal>> {
    let key = PartitionStateMachineKey {
        partition_id: partition_id.into(),
        state_id: fsm_variable::SEAL_MARKER,
    };

    storage.get_kv_raw(key, |_k, v| {
        v.map(|v| serde_json::from_slice(v).map_err(|e| StorageError::Conversion(e.into())))
            .transpose()
    })
}

pub(crate) async fn seal_partition(
    storage: &mut PartitionStore,
    seal: &PartitionSeal,
) -> Result<()> {
    let key = PartitionStateMachineKey {
        partition_id: storage.partition_id().into(),
        state_id: fsm_variable::SEAL_MARKER,
    };

    storage.put_kv_raw_with_wal(
        key,
        serde_json::to_vec(seal).map_err(|e| StorageError::Conversion(e.into()))?,
    )?;
    Ok(())
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

    async fn get_released_outbox_blob_index(&mut self) -> Result<Option<MessageIndex>> {
        get_proto_from_partition_db::<SequenceNumber>(
            self,
            fsm_variable::RELEASED_OUTBOX_BLOB_INDEX,
        )
        .map(|opt| opt.map(Into::into))
    }

    async fn get_applied_lsn(&mut self) -> Result<Option<Lsn>> {
        read_applied_lsn(self, rocksdb::ReadTier::All)
    }

    async fn get_fsm_position(&mut self) -> Result<Option<FsmPosition>> {
        get_storage_codec_from_partition_db(self, fsm_variable::FSM_POSITION)
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

    async fn get_schema(&mut self) -> Result<Option<SchemaUnindexedLite>> {
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

    async fn get_released_outbox_blob_index(&mut self) -> Result<Option<MessageIndex>> {
        get::<SequenceNumber, _>(
            self,
            self.partition_id(),
            fsm_variable::RELEASED_OUTBOX_BLOB_INDEX,
        )
        .map(|opt| opt.map(Into::into))
    }

    async fn get_applied_lsn(&mut self) -> Result<Option<Lsn>> {
        read_applied_lsn(self.partition_db(), rocksdb::ReadTier::All)
    }

    async fn get_fsm_position(&mut self) -> Result<Option<FsmPosition>> {
        let key = create_key(self.partition_id(), fsm_variable::FSM_POSITION);
        self.get_value_storage_codec(key)
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

    async fn get_schema(&mut self) -> Result<Option<SchemaUnindexedLite>> {
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
        // While the partition tracks byte offsets, only its position holds the applied LSN.
        if self
            .fsm_position()
            .is_some_and(|position| position.has_byte_offsets())
        {
            self.mark_fsm_position_pending();
            return Ok(());
        }
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

    fn put_released_outbox_blob_index(&mut self, index: MessageIndex) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::RELEASED_OUTBOX_BLOB_INDEX,
            &SequenceNumber::from(index),
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

    fn put_schema(&mut self, schema: &SchemaUnindexedLite) -> Result<()> {
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

/// Persists the partition's position. Only the transaction that maintains it writes it, so that
/// its applied LSN is always the one persisted with it.
pub(crate) fn put_fsm_position(
    txn: &mut PartitionStoreTransaction<'_>,
    position: &FsmPosition,
) -> Result<()> {
    let key = create_key(txn.partition_id(), fsm_variable::FSM_POSITION);
    txn.put_kv_storage_codec(key, position)
}

/// The applied LSN persisted on its own, whether or not the partition has a position.
#[cfg(test)]
pub(crate) fn get_applied_lsn_on_its_own(db: &PartitionDb) -> Result<Option<Lsn>> {
    Ok(
        get_proto_from_partition_db::<SequenceNumber>(db, fsm_variable::APPLIED_LSN)?
            .map(|seq_number| Lsn::from(u64::from(seq_number))),
    )
}

/// Deletes the applied LSN persisted on its own, once the partition's position holds it.
pub(crate) fn delete_applied_lsn(txn: &mut PartitionStoreTransaction<'_>) -> Result<()> {
    let key = create_key(txn.partition_id(), fsm_variable::APPLIED_LSN);
    txn.delete_key(&key)
}
