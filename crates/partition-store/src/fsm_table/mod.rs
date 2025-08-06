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
    FsmTable, PartitionDurability, ReadOnlyFsmTable, SequenceNumber,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_types::SemanticRestateVersion;
use restate_types::identifiers::PartitionId;
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;

use crate::TableKind::PartitionStateMachine;
use crate::keys::{KeyKind, define_table_key};
use crate::{PaddedPartitionId, PartitionStore, PartitionStoreTransaction, StorageAccess};

define_table_key!(
    PartitionStateMachine,
    KeyKind::Fsm,
    PartitionStateMachineKey(partition_id: PaddedPartitionId, state_id: u64)
);

pub(crate) mod fsm_variable {
    pub(crate) const INBOX_SEQ_NUMBER: u64 = 0;
    pub(crate) const OUTBOX_SEQ_NUMBER: u64 = 1;

    pub(crate) const APPLIED_LSN: u64 = 2;
    pub(crate) const RESTATE_VERSION_BARRIER: u64 = 3;
    pub(crate) const PARTITION_DURABILITY: u64 = 4;

    /// Migrations are represented as incremental number.
    pub(crate) const LAST_EXECUTED_MIGRATION: u64 = 5;
}

fn get<T: PartitionStoreProtobufValue, S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    state_id: u64,
) -> Result<Option<T>>
where
    <<T as PartitionStoreProtobufValue>::ProtobufType as TryInto<T>>::Error: Into<anyhow::Error>,
{
    let key = PartitionStateMachineKey::default()
        .partition_id(partition_id.into())
        .state_id(state_id);
    storage.get_value(key)
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
    let key = PartitionStateMachineKey::default()
        .partition_id(partition_id.into())
        .state_id(state_id);
    storage.get_durable_value(key)
}

fn put<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    state_id: u64,
    state_value: &(impl PartitionStoreProtobufValue + Clone + 'static),
) -> Result<()> {
    let key = PartitionStateMachineKey::default()
        .partition_id(partition_id.into())
        .state_id(state_id);
    storage.put_kv(key, state_value)
}

pub async fn get_locally_durable_lsn(partition_store: &mut PartitionStore) -> Result<Option<Lsn>> {
    get_durable::<SequenceNumber, _>(
        partition_store,
        partition_store.partition_id(),
        fsm_variable::APPLIED_LSN,
    )
    .map(|opt| opt.map(|seq_number| Lsn::from(u64::from(seq_number))))
}

pub(crate) async fn get_last_executed_migration(
    storage: &mut PartitionStoreTransaction<'_>,
) -> Result<u16> {
    get::<SequenceNumber, _>(
        storage,
        storage.partition_id(),
        fsm_variable::LAST_EXECUTED_MIGRATION,
    )
    .map(|opt| opt.map(|s| s.0 as u16).unwrap_or_default())
}

pub(crate) async fn put_last_executed_migration(
    storage: &mut PartitionStoreTransaction<'_>,
    last_executed_migration: u16,
) -> Result<()> {
    put(
        storage,
        storage.partition_id(),
        fsm_variable::INBOX_SEQ_NUMBER,
        &SequenceNumber::from(last_executed_migration as u64),
    )
}

impl ReadOnlyFsmTable for PartitionStore {
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
}

impl FsmTable for PartitionStoreTransaction<'_> {
    async fn put_applied_lsn(&mut self, lsn: Lsn) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::APPLIED_LSN,
            &SequenceNumber::from(u64::from(lsn)),
        )
    }

    async fn put_inbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::INBOX_SEQ_NUMBER,
            &SequenceNumber::from(seq_number),
        )
    }

    async fn put_outbox_seq_number(&mut self, seq_number: MessageIndex) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::OUTBOX_SEQ_NUMBER,
            &SequenceNumber::from(seq_number),
        )
    }

    async fn put_min_restate_version(&mut self, version: &SemanticRestateVersion) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::RESTATE_VERSION_BARRIER,
            version,
        )
    }

    async fn put_partition_durability(&mut self, durability: &PartitionDurability) -> Result<()> {
        put(
            self,
            self.partition_id(),
            fsm_variable::PARTITION_DURABILITY,
            durability,
        )
    }
}
