// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::Result;
use restate_storage_api::fsm_table::{
    PartitionDurability, ReadFsmTable, SequenceNumber, WriteFsmTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_types::SemanticRestateVersion;
use restate_types::identifiers::PartitionId;
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;
use restate_types::schema::Schema;

use crate::TableKind::PartitionStateMachine;
use crate::keys::{KeyKind, define_table_key};
use crate::{PaddedPartitionId, PartitionStore, PartitionStoreTransaction, StorageAccess};

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

pub(crate) async fn get_storage_version<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
) -> Result<u16> {
    get::<SequenceNumber, _>(storage, partition_id, fsm_variable::STORAGE_VERSION)
        .map(|opt| opt.map(|s| s.0 as u16).unwrap_or_default())
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
}
