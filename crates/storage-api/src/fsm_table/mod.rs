// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Result;
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;
use std::future::Future;

pub trait ReadOnlyFsmTable {
    fn get_inbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_;

    fn get_outbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_;

    fn get_applied_lsn(&mut self) -> impl Future<Output = Result<Option<Lsn>>> + Send + '_;
}

pub trait FsmTable: ReadOnlyFsmTable {
    fn put_applied_lsn(&mut self, lsn: Lsn) -> impl Future<Output = Result<()>> + Send;

    fn put_inbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> impl Future<Output = Result<()>> + Send;

    fn put_outbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> impl Future<Output = Result<()>> + Send;
}
