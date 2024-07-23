// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

extern crate prost_0_12 as prost;
extern crate prost_types_0_12 as prost_types;

use std::future::Future;

/// Storage error
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("generic storage error: {0}")]
    Generic(#[from] anyhow::Error),
    #[error("failed to convert Rust objects to/from protobuf: {0}")]
    Conversion(anyhow::Error),
    #[error("Integrity constrained is violated")]
    DataIntegrityError,
    #[error("Operational error that can be caused during a graceful shutdown")]
    OperationalError,
}

pub type Result<T> = std::result::Result<T, StorageError>;

pub mod deduplication_table;
pub mod fsm_table;
pub mod idempotency_table;
pub mod inbox_table;
pub mod invocation_status_table;
pub mod journal_table;
pub mod outbox_table;
pub mod promise_table;
pub mod service_status_table;
pub mod state_table;
mod storage;
pub mod timer_table;

pub trait Storage {
    type TransactionType<'a>: Transaction
    where
        Self: 'a;

    fn transaction(&mut self) -> Self::TransactionType<'_>;
}

pub trait Transaction:
    state_table::StateTable
    + invocation_status_table::InvocationStatusTable
    + service_status_table::VirtualObjectStatusTable
    + inbox_table::InboxTable
    + outbox_table::OutboxTable
    + deduplication_table::DeduplicationTable
    + journal_table::JournalTable
    + fsm_table::FsmTable
    + timer_table::TimerTable
    + idempotency_table::IdempotencyTable
    + promise_table::PromiseTable
    + Send
{
    fn commit(self) -> impl Future<Output = Result<()>> + Send;
}
