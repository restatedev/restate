// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::fsm_table::{FsmTable, ReadOnlyFsmTable};
use restate_storage_api::Result;
use restate_types::identifiers::PartitionId;
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;

use crate::keys::{define_table_key, KeyKind};
use crate::protobuf_types::PartitionStoreProtobufValue;
use crate::TableKind::PartitionStateMachine;
use crate::{PaddedPartitionId, PartitionStore, PartitionStoreTransaction, StorageAccess};

define_table_key!(
    PartitionStateMachine,
    KeyKind::Fsm,
    PartitionStateMachineKey(partition_id: PaddedPartitionId, state_id: u64)
);

#[derive(Debug, Clone, Copy, derive_more::From, derive_more::Into)]
pub(crate) struct SequenceNumber(pub(crate) u64);

impl PartitionStoreProtobufValue for SequenceNumber {
    type ProtobufType = crate::protobuf_types::v1::SequenceNumber;
}

mod fsm_variable {
    pub(crate) const INBOX_SEQ_NUMBER: u64 = 0;
    pub(crate) const OUTBOX_SEQ_NUMBER: u64 = 1;

    pub(crate) const APPLIED_LSN: u64 = 2;
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

fn put<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    state_id: u64,
    state_value: &(impl PartitionStoreProtobufValue + Clone + 'static),
) {
    let key = PartitionStateMachineKey::default()
        .partition_id(partition_id.into())
        .state_id(state_id);
    storage.put_kv(key, state_value);
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
}

impl ReadOnlyFsmTable for PartitionStoreTransaction<'_> {
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
}

impl FsmTable for PartitionStoreTransaction<'_> {
    async fn put_applied_lsn(&mut self, lsn: Lsn) {
        put(
            self,
            self.partition_id(),
            fsm_variable::APPLIED_LSN,
            &SequenceNumber::from(u64::from(lsn)),
        )
    }

    async fn put_inbox_seq_number(&mut self, seq_number: MessageIndex) {
        put(
            self,
            self.partition_id(),
            fsm_variable::INBOX_SEQ_NUMBER,
            &SequenceNumber::from(seq_number),
        )
    }

    async fn put_outbox_seq_number(&mut self, seq_number: MessageIndex) {
        put(
            self,
            self.partition_id(),
            fsm_variable::OUTBOX_SEQ_NUMBER,
            &SequenceNumber::from(seq_number),
        )
    }
}
