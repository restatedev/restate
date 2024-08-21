// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{protobuf_storage_encode_decode, Result};
use futures_util::FutureExt;
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;
use restate_types::storage::{StorageDecode, StorageEncode};
use std::future::Future;

#[derive(Debug, Clone, Copy, derive_more::From, derive_more::Into)]
pub struct SequenceNumber(u64);

protobuf_storage_encode_decode!(SequenceNumber);

mod fsm_variable {
    pub(crate) const INBOX_SEQ_NUMBER: u64 = 0;
    pub(crate) const OUTBOX_SEQ_NUMBER: u64 = 1;

    pub(crate) const APPLIED_LSN: u64 = 2;
}

pub trait ReadOnlyFsmTable {
    fn get<T>(&mut self, state_id: u64) -> impl Future<Output = Result<Option<T>>> + Send
    where
        T: StorageDecode;

    fn get_inbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_ {
        self.get::<SequenceNumber>(fsm_variable::INBOX_SEQ_NUMBER)
            .map(|result| result.map(|seq_number| seq_number.map(Into::into).unwrap_or_default()))
    }

    fn get_outbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_ {
        self.get::<SequenceNumber>(fsm_variable::OUTBOX_SEQ_NUMBER)
            .map(|result| result.map(|seq_number| seq_number.map(Into::into).unwrap_or_default()))
    }

    fn get_applied_lsn(&mut self) -> impl Future<Output = Result<Option<Lsn>>> + Send + '_ {
        self.get::<SequenceNumber>(fsm_variable::APPLIED_LSN)
            .map(|result| {
                result
                    .map(|seq_number| seq_number.map(|seq_number| Lsn::from(u64::from(seq_number))))
            })
    }
}

pub trait FsmTable: ReadOnlyFsmTable {
    fn put(
        &mut self,
        state_id: u64,
        state_value: impl StorageEncode,
    ) -> impl Future<Output = ()> + Send;

    fn clear(&mut self, state_id: u64) -> impl Future<Output = ()> + Send;

    fn put_applied_lsn(&mut self, lsn: Lsn) -> impl Future<Output = ()> + Send {
        self.put(
            fsm_variable::APPLIED_LSN,
            SequenceNumber::from(u64::from(lsn)),
        )
    }

    fn put_inbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> impl Future<Output = ()> + Send {
        self.put(
            fsm_variable::INBOX_SEQ_NUMBER,
            SequenceNumber::from(seq_number),
        )
    }

    fn put_outbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> impl Future<Output = ()> + Send {
        self.put(
            fsm_variable::OUTBOX_SEQ_NUMBER,
            SequenceNumber::from(seq_number),
        )
    }
}
