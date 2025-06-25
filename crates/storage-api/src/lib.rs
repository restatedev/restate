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

/// Storage error
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("generic storage error: {0}")]
    Generic(#[from] anyhow::Error),
    #[error("failed to convert Rust objects to/from protobuf: {0}")]
    Conversion(anyhow::Error),
    #[error("Integrity constraint is violated")]
    DataIntegrityError,
    #[error("Operational error that can be caused during a graceful shutdown")]
    OperationalError,
    #[error("Snapshot export failed: {0}")]
    SnapshotExport(anyhow::Error),
    #[error("Precondition failed: {0}")]
    PreconditionFailed(anyhow::Error),
}

pub type Result<T> = std::result::Result<T, StorageError>;

pub mod deduplication_table;
pub mod fsm_table;
pub mod idempotency_table;
pub mod inbox_table;
pub mod invocation_status_table;
pub mod journal_table;
pub mod journal_table_v2;
pub mod outbox_table;
pub mod promise_table;
pub mod protobuf_types;
pub mod service_status_table;
pub mod state_table;
pub mod timer_table;

/// Isolation level of a storage transaction
#[derive(Debug, Default)]
pub enum IsolationLevel {
    /// Read committed writes during the execution of the transaction. Note, that this level does
    /// not ensure repeatable reads since you might see committed writes from multiple transactions.
    /// Use this level, if you know that you are the only writer.
    #[default]
    Committed,
    /// Ensure repeatable reads during the execution of the transaction. Use this level,
    /// if you need to do multiple reads and tolerate concurrent write operations.
    ///
    /// Note that using this level might be more costly than [`IsolationLevel::Committed`].
    RepeatableReads,
}

pub trait Storage {
    type TransactionType<'a>: Transaction
    where
        Self: 'a;

    /// Create a transaction with no read isolation level. This method should only be used if you
    /// are the only writer to the [`Storage`] implementation. Otherwise, use
    /// [`Storage::transaction_with_isolation`] and specify a proper [`IsolationLevel`] level.
    fn transaction(&mut self) -> Self::TransactionType<'_> {
        self.transaction_with_isolation(IsolationLevel::Committed)
    }

    /// Create a transaction with the given read isolation level.
    fn transaction_with_isolation(
        &mut self,
        read_isolation: IsolationLevel,
    ) -> Self::TransactionType<'_>;
}

pub trait Transaction:
    state_table::StateTable
    + invocation_status_table::InvocationStatusTable
    + service_status_table::VirtualObjectStatusTable
    + inbox_table::InboxTable
    + outbox_table::OutboxTable
    + deduplication_table::DeduplicationTable
    + journal_table::JournalTable
    + journal_table_v2::JournalTable
    + fsm_table::FsmTable
    + timer_table::TimerTable
    + idempotency_table::IdempotencyTable
    + promise_table::PromiseTable
    + Send
{
    fn commit(self) -> impl Future<Output = Result<()>> + Send;
}
