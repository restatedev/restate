// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;

/// Storage error
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("generic storage error: {0}")]
    Generic(#[from] anyhow::Error),
    #[error("failed to convert Rust objects to/from protobuf: {0}")]
    Conversion(anyhow::Error),
    #[error("Integrity constrained is violated")]
    DataIntegrityError,
}

//
// The following future definitions are temporary upto the point
// where async interfaces will be a thing in Rust.
//
pub type GetFuture<'a, T> = BoxFuture<'a, Result<T, StorageError>>;
pub type PutFuture = futures_util::future::Ready<()>;
pub type GetStream<'a, T> = BoxStream<'a, Result<T, StorageError>>;

pub fn ready() -> PutFuture {
    futures_util::future::ready(())
}

pub mod deduplication_table;
pub mod fsm_table;
pub mod inbox_table;
pub mod journal_table;
pub mod outbox_table;
pub mod state_table;
pub mod status_table;
pub mod timer_table;

pub trait Storage {
    type TransactionType<'a>: Transaction
    where
        Self: 'a;

    fn transaction(&self) -> Self::TransactionType<'_>;
}

pub trait Transaction:
    state_table::StateTable
    + status_table::StatusTable
    + inbox_table::InboxTable
    + outbox_table::OutboxTable
    + deduplication_table::DeduplicationTable
    + journal_table::JournalTable
    + fsm_table::FsmTable
    + timer_table::TimerTable
    + Send
{
    fn commit<'a>(self) -> GetFuture<'a, ()>
    where
        Self: 'a;
}
