// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod error;
#[cfg(test)]
pub mod loglet_tests;
mod provider;
pub(crate) mod util;

// exports
pub use error::*;
use futures::stream::BoxStream;
pub use provider::{LogletProvider, LogletProviderFactory};

use std::ops::Add;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

use restate_types::logs::{Keys, Lsn, SequenceNumber};

use crate::LogRecord;
use crate::{Result, TailState};

// Inner loglet offset
#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
)]
pub struct LogletOffset(pub(crate) u64);

impl Add<usize> for LogletOffset {
    type Output = Self;
    fn add(self, rhs: usize) -> Self {
        // we always assume that we are running on a 64bit cpu arch.
        Self(self.0.saturating_add(rhs as u64))
    }
}

impl SequenceNumber for LogletOffset {
    const MAX: Self = LogletOffset(u64::MAX);
    const INVALID: Self = LogletOffset(0);
    const OLDEST: Self = LogletOffset(1);

    /// Saturates to Self::MAX
    fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Saturates to Self::OLDEST.
    fn prev(self) -> Self {
        Self(std::cmp::max(Self::OLDEST.0, self.0.saturating_sub(1)))
    }
}

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
///       1 -> Offset::OLDEST
///       0 -> Offset::INVALID
/// ```
pub trait Loglet: LogletBase<Offset = LogletOffset> {}
impl<T> Loglet for T where T: LogletBase<Offset = LogletOffset> {}

#[async_trait]
pub trait LogletBase: Send + Sync + std::fmt::Debug {
    type Offset: SequenceNumber;

    /// Create a read stream that streams record from a single loglet instance.
    ///
    /// `to`: The offset of the last record to be read (inclusive). If `None`, the
    /// stream is an open-ended tailing read stream.
    async fn create_read_stream(
        self: Arc<Self>,
        from: Self::Offset,
        to: Option<Self::Offset>,
    ) -> Result<SendableLogletReadStream<Self::Offset>, OperationError>;

    /// Append a record to the loglet.
    async fn append(&self, data: &Bytes, keys: &Keys) -> Result<Self::Offset, AppendError>;

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
    fn watch_tail(&self) -> BoxStream<'static, TailState<Self::Offset>>;

    /// Append a batch of records to the loglet. The returned offset (on success) is the offset of
    /// the first record in the batch)
    async fn append_batch(&self, payloads: &[(Bytes, Keys)]) -> Result<Self::Offset, AppendError>;

    /// The tail is *the first unwritten position* in the loglet.
    ///
    /// Finds the durable tail of the loglet (last record offset that was durably committed) then
    /// it returns the offset **after** it. Virtually, the returned offset is the offset returned
    /// after the next `append()` call.
    ///
    /// If the loglet is empty, the loglet should return TailState::Open(Offset::OLDEST).
    async fn find_tail(&self) -> Result<TailState<Self::Offset>, OperationError>;

    /// The offset of the slot **before** the first readable record (if it exists), or the offset
    /// before the next slot that will be written to. Must not return Self::INVALID. If the loglet
    /// is never trimmed, this must return `None`.
    async fn get_trim_point(&self) -> Result<Option<Self::Offset>, OperationError>;

    /// Trim the loglet prefix up to and including the `trim_point`.
    /// If trim_point equal or higher than the loglet tail, the loglet trims its data until the tail.
    ///
    /// It's acceptable to pass `trim_point` beyond the tail of the loglet (Offset::MAX is legal).
    /// The behaviour in this case is equivalent to trim(find_tail() - 1).
    ///
    /// Passing `Offset::INVALID` is a no-op. (success)
    /// Passing `Offset::OLDEST` trims the first record in the loglet (if exists).
    async fn trim(&self, trim_point: Self::Offset) -> Result<(), OperationError>;

    /// Seal the loglet. This operation is idempotent.
    ///
    /// Appends **SHOULD NOT** succeed after a `seal()` call is successful. And appends **MUST
    /// NOT** succeed after the offset returned by the *first* TailState::Sealed() response.
    async fn seal(&self) -> Result<(), OperationError>;
}

/// A stream of log records from a single loglet. Loglet streams are _always_ tailing streams.
pub trait LogletReadStream<S: SequenceNumber>:
    Stream<Item = Result<LogRecord<S, Bytes>, OperationError>>
{
    /// Current read pointer. This points to the next offset to be read.
    fn read_pointer(&self) -> S;

    /// Returns true if the stream is terminated.
    fn is_terminated(&self) -> bool;
}

pub type SendableLogletReadStream<S = Lsn> = Pin<Box<dyn LogletReadStream<S> + Send>>;
