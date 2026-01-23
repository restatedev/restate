// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod error;
#[cfg(any(test, feature = "test-util"))]
pub mod loglet_tests;
mod provider;
pub use restate_types::logs::TailOffsetWatch;

// exports
pub use error::*;
pub use provider::{Improvement, LogletProvider, LogletProviderFactory};

use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Poll, ready};

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream};
use tokio::sync::oneshot;

use restate_types::logs::{KeyFilter, LogletOffset, Record, TailState};

use crate::LogEntry;
use crate::Result;

/// A loglet represents a logical log stream provided by a provider implementation.
///
/// Loglets are required to follow these rules:
/// - Loglet implementations must be Send + Sync (internal mutability is required)
/// - Loglets must strictly adhere to the consistency requirements as the interface calls
///   that is, if an append returns an offset, it **must** be durably committed.
/// - Loglets are allowed to buffer writes internally as long as the order of records
///   follows the order of append calls.
///
///  ```text
///       Semantics of offsets
///       [  1  2  3  4  5  6  7  ]
///       [  ----  A  B  C  ----- ]
///             ^           ^
//     Trim Point           Tail
///                      ^ Last Committed
///                   ^  -- Last released - can be delivered to readers
/// ```
///
///  An empty loglet. A log is empty when trim_point.next() == tail.prev()
///
/// ```text
///       Semantics of offsets
///       [  1  2  3  4  5  6  7  ]
///       [  -------------------- ]
///                      ^  ^
//              Trim Point  Tail
///                      ^ Last Committed
///                      ^  -- Last released (optional and internal)
///
///       1 -> LogletOffset::OLDEST
///       0 -> LogletOffset::INVALID
/// ```

#[async_trait]
pub trait Loglet: Send + Sync {
    /// A string describing this instance of the loglet, used for debugging purposes.
    fn debug_str(&self) -> Cow<'static, str>;

    /// Create a read stream that streams record from a single loglet instance.
    ///
    /// `to`: The offset of the last record to be read (inclusive). If `None`, the
    /// stream is an open-ended tailing read stream.
    async fn create_read_stream(
        self: Arc<Self>,
        filter: KeyFilter,
        from: LogletOffset,
        to: Option<LogletOffset>,
    ) -> Result<SendableLogletReadStream, OperationError>;

    /// Create a stream watching the state of tail for this loglet
    ///
    /// The stream will return the last known TailState with seal notification semantics
    /// similar to `find_tail()` except that it won't trigger a linearizable tail check when
    /// polled. This can be used as a trailing tail indicator.
    ///
    /// Note that it's legal to observe the last unsealed tail becoming sealed. The
    /// last_known_unsealed (or the last unsealed offset emitted on this stream) defines the
    /// point at which readers should stop **before**, therefore, when reading, if the next offset
    /// to read == the tail, it means that you can only read this offset if the tail watch moves
    /// beyond it to a higher tail while remaining unsealed.
    fn watch_tail(&self) -> BoxStream<'static, TailState<LogletOffset>>;

    /// Enqueues a batch of records for the loglet to write. This function blocks when the loglet cannot
    /// accept this batch as a form of back-pressure. Once the batch is accepted, it will be committed
    /// in the background and the caller doesn't need to await for the commit status.
    ///
    /// That said, if commit confirmation is needed, use the returned future `LogletCommit` to wait
    /// for commit completion and to acquire the **last offset** in that batch.
    ///
    /// Note: For pipelined writes. The order of records/batches is determined by the order of
    /// calling this function from a single source. The user must wait for this function to return
    /// before appending the next batch in a stream of ordered records.
    ///
    /// However, awaiting the returned `LogletCommit` is optional and can happen concurrently and in
    /// any order. In the case of pipelined writes, loglets must acknowledge writes
    /// only after all previously enqueued writes have been durably committed. The loglet should
    /// retry failing appends indefinitely until the loglet is sealed. In that case, such commits
    /// might still appear to future readers but without returning the commit acknowledgement to
    /// the original writer.
    async fn enqueue_batch(&self, payloads: Arc<[Record]>) -> Result<LogletCommit, OperationError>;

    /// The tail is *the first unwritten position* in the loglet.
    ///
    /// Finds the durable tail of the loglet (last record offset that was durably committed) then
    /// it returns the offset **after** it. Virtually, the returned offset is the offset returned
    /// after the next `append()` call.
    ///
    /// If the loglet is empty, the loglet should return TailState::Open(Offset::OLDEST).
    async fn find_tail(
        &self,
        opts: FindTailOptions,
    ) -> Result<TailState<LogletOffset>, OperationError>;

    /// The offset of the slot **before** the first readable record (if it exists), or the offset
    /// before the next slot that will be written to. Must not return Self::INVALID. If the loglet
    /// is never trimmed, this must return `None`.
    async fn get_trim_point(&self) -> Result<Option<LogletOffset>, OperationError>;

    /// Trim the loglet prefix up to and including the `trim_point`.
    /// If trim_point equal or higher than the loglet tail, the loglet trims its data until the tail.
    ///
    /// It's acceptable to pass `trim_point` beyond the tail of the loglet (Offset::MAX is legal).
    /// The behaviour in this case is equivalent to trim(find_tail() - 1).
    ///
    /// Passing `Offset::INVALID` is a no-op. (success)
    /// Passing `Offset::OLDEST` trims the first record in the loglet (if exists).
    async fn trim(&self, trim_point: LogletOffset) -> Result<(), OperationError>;

    /// Seal the loglet. This operation is idempotent.
    ///
    /// Appends **SHOULD NOT** succeed after a `seal()` call is successful. And appends **MUST
    /// NOT** succeed after the offset returned by the *first* TailState::Sealed() response.
    async fn seal(&self) -> Result<(), OperationError>;
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub enum FindTailOptions {
    /// Stable and accurate tail find.
    #[default]
    ConsistentRead,
    /// A fast but possibly stale tail approximation. This should be cheap to run based on the
    /// loglet implementation. If an efficient approximation is not possible, it must
    /// fall back to a ConsistentRead check.
    Fast,
}

/// A stream of log records from a single loglet. Loglet streams are _always_ tailing streams.
pub trait LogletReadStream: Stream<Item = Result<LogEntry<LogletOffset>, OperationError>> {
    /// Current read pointer. This points to the next offset to be read.
    fn read_pointer(&self) -> LogletOffset;

    /// Returns true if the stream is terminated.
    fn is_terminated(&self) -> bool;
}

pub type SendableLogletReadStream = Pin<Box<dyn LogletReadStream + Send>>;

pub struct LogletCommitResolver {
    tx: oneshot::Sender<Result<LogletOffset, AppendError>>,
}

impl LogletCommitResolver {
    pub fn sealed(self) {
        let _ = self.tx.send(Err(AppendError::Sealed));
    }

    pub fn offset(self, offset: LogletOffset) {
        let _ = self.tx.send(Ok(offset));
    }

    pub fn error(self, err: AppendError) {
        let _ = self.tx.send(Err(err));
    }
}

pub struct LogletCommit {
    rx: oneshot::Receiver<Result<LogletOffset, AppendError>>,
}

impl LogletCommit {
    pub fn sealed() -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(AppendError::Sealed));
        Self { rx }
    }

    pub fn reconfiguration_needed(reason: impl Into<Cow<'static, str>>) -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(AppendError::ReconfigurationNeeded(reason.into())));
        Self { rx }
    }

    pub fn resolved(offset: LogletOffset) -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(offset));
        Self { rx }
    }

    pub fn deferred() -> (Self, LogletCommitResolver) {
        let (tx, rx) = oneshot::channel();
        (Self { rx }, LogletCommitResolver { tx })
    }
}

impl std::future::Future for LogletCommit {
    type Output = Result<LogletOffset, AppendError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match ready!(self.rx.poll_unpin(cx)) {
            Ok(res) => Poll::Ready(res),
            Err(_) => Poll::Ready(Err(AppendError::ReconfigurationNeeded(
                "loglet gave up on this batch".into(),
            ))),
        }
    }
}
