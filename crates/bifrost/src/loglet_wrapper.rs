// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::task::Poll;

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};

use restate_core::ShutdownError;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{KeyFilter, Lsn, SequenceNumber};
use tracing::instrument;

use crate::loglet::{AppendError, Loglet, LogletOffset, OperationError, SendableLogletReadStream};
use crate::record::ErasedInputRecord;
use crate::{Commit, LogEntry, LsnExt};
use crate::{Result, TailState};

#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Clone, thiserror::Error)]
enum LogletWrapperError {
    #[error("attempted to read outside the tail boundary of the loglet, requested read LSN is {attempt_lsn}, tail is at {tail_lsn}")]
    OutOfBoundsRead { attempt_lsn: Lsn, tail_lsn: Lsn },
}

/// Wraps loglets with the base LSN of the segment
#[derive(Clone, Debug)]
pub struct LogletWrapper {
    segment_index: SegmentIndex,
    /// The offset of the first record in the segment (if exists).
    /// A segment on a clean chain is created with Lsn::OLDEST but this doesn't mean that this
    /// record exists. It only means that we want to offset the loglet offsets by base_lsn -
    /// Loglet::Offset::OLDEST.
    pub(crate) base_lsn: Lsn,
    /// If set, it points to the first first LSN outside the boundary of this loglet (bifrost's tail semantics)
    pub(crate) tail_lsn: Option<Lsn>,
    loglet: Arc<dyn Loglet>,
}

impl LogletWrapper {
    pub fn new(
        segment_index: SegmentIndex,
        base_lsn: Lsn,
        tail_lsn: Option<Lsn>,
        loglet: Arc<dyn Loglet>,
    ) -> Self {
        Self {
            segment_index,
            base_lsn,
            tail_lsn,
            loglet,
        }
    }

    /// Panics if `tail_lsn` is lower than the loglet's `base_lsn`
    pub fn set_tail_lsn(&mut self, tail_lsn: Lsn) {
        debug_assert!(tail_lsn >= self.base_lsn);
        self.tail_lsn = Some(tail_lsn)
    }

    pub fn segment_index(&self) -> SegmentIndex {
        self.segment_index
    }

    pub async fn create_read_stream(
        self,
        filter: KeyFilter,
        start_lsn: Lsn,
    ) -> Result<LogletReadStreamWrapper, OperationError> {
        let tail_lsn = self.tail_lsn;
        self.create_read_stream_with_tail(filter, start_lsn, tail_lsn)
            .await
    }

    pub async fn create_read_stream_with_tail(
        self,
        filter: KeyFilter,
        start_lsn: Lsn,
        tail_lsn: Option<Lsn>,
    ) -> Result<LogletReadStreamWrapper, OperationError> {
        // Translates LSN to loglet offset
        Ok(LogletReadStreamWrapper::new(
            self.clone(),
            self.loglet
                .create_read_stream(
                    filter,
                    start_lsn.into_offset(self.base_lsn),
                    // We go back one LSN because `to` is inclusive and `tail_lsn` is exclusive.
                    // transposed to loglet offset (if set)
                    tail_lsn.map(|tail| tail.prev().into_offset(self.base_lsn)),
                )
                .await?,
            self.base_lsn,
        ))
    }

    /// Read or wait for the record at `from` offset, or the next available record if `from` isn't
    /// defined for the loglet.
    #[allow(unused)]
    #[cfg(any(test, feature = "test-util"))]
    pub async fn read(&self, from: Lsn) -> Result<LogEntry<Lsn>, OperationError> {
        let mut stream = self
            .clone()
            .create_read_stream(KeyFilter::Any, from)
            .await?;
        stream.next().await.unwrap_or_else(|| {
            // We are trying to read past the the last record.
            Err(OperationError::terminal(
                LogletWrapperError::OutOfBoundsRead {
                    attempt_lsn: from,
                    tail_lsn: self.tail_lsn.unwrap_or(Lsn::INVALID),
                },
            ))
        })
    }

    #[allow(unused)]
    #[cfg(any(test, feature = "test-util"))]
    pub async fn read_opt(&self, from: Lsn) -> Result<Option<LogEntry<Lsn>>, OperationError> {
        let tail_lsn = match self.tail_lsn {
            Some(tail) => tail,
            None => self.find_tail().await?.offset(),
        };
        let mut stream = self
            .clone()
            .create_read_stream_with_tail(KeyFilter::Any, from, Some(tail_lsn))
            .await?;
        stream.next().await.transpose()
    }

    #[allow(unused)]
    #[cfg(any(test, feature = "test-util"))]
    pub async fn append(&self, payload: ErasedInputRecord) -> Result<Lsn, AppendError> {
        let commit = self
            .enqueue_batch(Arc::new([payload]))
            .await
            .map_err(AppendError::Shutdown)?;
        commit.await
    }

    pub fn watch_tail(&self) -> impl Stream<Item = TailState<Lsn>> {
        let base_lsn = self.base_lsn;
        self.loglet.watch_tail().map(move |tail_state| {
            let offset = std::cmp::max(tail_state.offset(), LogletOffset::OLDEST);
            TailState::new(tail_state.is_sealed(), base_lsn.offset_by(offset))
        })
    }

    #[instrument(
        level = "trace",
        skip(self, payloads),
        ret,
        fields(
            segment_index = %self.segment_index,
            loglet = ?self.loglet,
            count = payloads.len(),
        )
    )]
    pub async fn append_batch(
        &self,
        payloads: Arc<[ErasedInputRecord]>,
    ) -> Result<Lsn, AppendError> {
        self.enqueue_batch(payloads)
            .await
            .map_err(AppendError::Shutdown)?
            .await
    }

    pub async fn enqueue_batch(
        &self,
        payloads: Arc<[ErasedInputRecord]>,
    ) -> Result<Commit, ShutdownError> {
        if self.tail_lsn.is_some() {
            return Ok(Commit::sealed());
        }
        let commit = self.loglet.enqueue_batch(payloads).await?;
        Ok(Commit::passthrough(self.base_lsn, commit))
    }

    pub async fn find_tail(&self) -> Result<TailState<Lsn>, OperationError> {
        if let Some(tail) = self.tail_lsn {
            return Ok(TailState::Sealed(tail));
        }

        Ok(self
            .loglet
            .find_tail()
            .await?
            .map(|o| self.base_lsn.offset_by(o)))
    }

    pub async fn get_trim_point(&self) -> Result<Option<Lsn>, OperationError> {
        let offset = self.loglet.get_trim_point().await?;
        Ok(offset.map(|o| self.base_lsn.offset_by(o)))
    }

    // trim_point is inclusive.
    pub async fn trim(&self, trim_point: Lsn) -> Result<(), OperationError> {
        // trimming to INVALID is no-op
        if trim_point == Lsn::INVALID {
            return Ok(());
        }
        // saturate to the loglet max possible offset.
        let trim_point = trim_point.min(Lsn::new(LogletOffset::MAX.into()));
        let trim_point = trim_point.into_offset(self.base_lsn);
        self.loglet.trim(trim_point).await
    }

    pub async fn seal(&self) -> Result<(), OperationError> {
        if self.tail_lsn.is_some() {
            return Ok(());
        }

        self.loglet.seal().await
    }
}

impl PartialEq for LogletWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.segment_index == other.segment_index
    }
}

/// Wraps loglet read streams with the base LSN of the segment
pub struct LogletReadStreamWrapper {
    pub(crate) base_lsn: Lsn,
    loglet: LogletWrapper,
    inner_read_stream: SendableLogletReadStream,
}

impl LogletReadStreamWrapper {
    pub fn new(
        loglet: LogletWrapper,
        inner_read_stream: SendableLogletReadStream,
        base_lsn: Lsn,
    ) -> Self {
        Self {
            loglet,
            inner_read_stream,
            base_lsn,
        }
    }

    /// The first LSN outside the boundary of this stream (bifrost's tail semantics)
    /// The read stream will return None and terminate before it reads this LSN
    #[inline(always)]
    pub fn tail_lsn(&self) -> Option<Lsn> {
        self.loglet.tail_lsn
    }

    pub fn set_tail_lsn(&mut self, tail_lsn: Lsn) {
        self.loglet.set_tail_lsn(tail_lsn)
    }

    #[inline(always)]
    pub fn loglet(&self) -> &LogletWrapper {
        &self.loglet
    }

    #[allow(unused)]
    pub fn read_pointer(&self) -> Lsn {
        self.base_lsn
            .offset_by(self.inner_read_stream.read_pointer())
    }

    #[allow(unused)]
    pub fn is_terminated(&self) -> bool {
        self.inner_read_stream.is_terminated()
    }
}

impl Stream for LogletReadStreamWrapper {
    type Item = Result<LogEntry<Lsn>, OperationError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.tail_lsn().is_some_and(|tail| {
            self.base_lsn
                .offset_by(self.inner_read_stream.read_pointer())
                >= tail
        }) {
            // Read until the permitted tail already.
            return Poll::Ready(None);
        }
        match self.inner_read_stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(record))) => {
                Poll::Ready(Some(Ok(record.with_base_lsn(self.base_lsn))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

static_assertions::assert_impl_all!(LogletWrapper: Send, Sync, Clone);
