// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::task::Poll;

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use tracing::instrument;

use restate_types::logs::metadata::{LogletConfig, SegmentIndex};
use restate_types::logs::{KeyFilter, LogletId, LogletOffset, Lsn, SequenceNumber};
use restate_types::logs::{Record, TailState};

use crate::Result;
use crate::loglet::{
    AppendError, FindTailOptions, Loglet, OperationError, SendableLogletReadStream,
};
use crate::{Commit, LogEntry, LsnExt};

#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Clone, thiserror::Error)]
enum LogletWrapperError {
    #[error(
        "attempted to read outside the tail boundary of the loglet, requested read LSN is {attempt_lsn}, tail is at {tail_lsn}"
    )]
    OutOfBoundsRead { attempt_lsn: Lsn, tail_lsn: Lsn },
}

/// Wraps loglets with the base LSN of the segment
#[derive(Clone, derive_more::Debug)]
pub struct LogletWrapper {
    segment_index: SegmentIndex,
    /// The offset of the first record in the segment (if exists).
    /// A segment on a clean chain is created with Lsn::OLDEST but this doesn't mean that this
    /// record exists. It only means that we want to offset the loglet offsets by base_lsn -
    /// Loglet::Offset::OLDEST.
    pub(crate) base_lsn: Lsn,
    /// If set, it points to the first first LSN outside the boundary of this loglet (bifrost's tail semantics)
    pub(crate) tail_lsn: Option<Lsn>,
    #[debug(skip)]
    pub(crate) config: LogletConfig,
    #[debug("{}", loglet.debug_str())]
    loglet: Arc<dyn Loglet>,
}

impl LogletWrapper {
    pub fn new(
        segment_index: SegmentIndex,
        base_lsn: Lsn,
        tail_lsn: Option<Lsn>,
        config: LogletConfig,
        loglet: Arc<dyn Loglet>,
    ) -> Self {
        Self {
            segment_index,
            base_lsn,
            tail_lsn,
            config,
            loglet,
        }
    }

    pub fn loglet_id(&self) -> Option<LogletId> {
        self.loglet.id()
    }

    pub fn segment_index(&self) -> SegmentIndex {
        self.segment_index
    }

    pub fn debug_str(&self) -> Cow<'static, str> {
        self.loglet.debug_str()
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
            None => self.find_tail(FindTailOptions::default()).await?.offset(),
        };
        let mut stream = self
            .clone()
            .create_read_stream_with_tail(KeyFilter::Any, from, Some(tail_lsn))
            .await?;
        stream.next().await.transpose()
    }

    #[allow(unused)]
    #[cfg(any(test, feature = "test-util"))]
    pub async fn append(&self, payload: Record) -> Result<Lsn, AppendError> {
        let commit = self.enqueue_batch(Arc::new([payload])).await?;
        commit.await
    }

    pub fn watch_tail(&self) -> impl Stream<Item = TailState<Lsn>> + use<> {
        let base_lsn = self.base_lsn;
        self.loglet.watch_tail().map(move |tail_state| {
            let offset = std::cmp::max(tail_state.offset(), LogletOffset::OLDEST);
            TailState::new(tail_state.is_sealed(), base_lsn.offset_by(offset))
        })
    }

    #[instrument(
        level = "trace",
        skip(payloads),
        ret,
        fields(batch_size = payloads.len())
    )]
    pub async fn append_batch(&self, payloads: Arc<[Record]>) -> Result<Lsn, AppendError> {
        self.enqueue_batch(payloads).await?.await
    }

    pub async fn enqueue_batch(&self, payloads: Arc<[Record]>) -> Result<Commit, OperationError> {
        if self.tail_lsn.is_some() {
            return Ok(Commit::sealed());
        }
        let commit = self.loglet.enqueue_batch(payloads).await?;
        Ok(Commit::passthrough(self.base_lsn, commit))
    }

    pub async fn find_tail(&self, opts: FindTailOptions) -> Result<TailState<Lsn>, OperationError> {
        if let Some(tail) = self.tail_lsn {
            return Ok(TailState::Sealed(tail));
        }

        Ok(self
            .loglet
            .find_tail(opts)
            .await?
            .map(|o| self.base_lsn.offset_by(o)))
    }

    pub async fn get_trim_point(&self) -> Result<Option<Lsn>, OperationError> {
        let offset = self.loglet.get_trim_point().await?;
        Ok(offset
            .map(|o| self.base_lsn.offset_by(o))
            .map(|actual_trim_point| {
                // If this loglet is sealed, the reported trim-point must fall within its boundaries
                match self.tail_lsn {
                    Some(tail) => actual_trim_point.min(tail.prev()),
                    None => actual_trim_point,
                }
            }))
    }

    // trim_point is inclusive.
    pub async fn trim(&self, trim_point: Lsn) -> Result<(), OperationError> {
        // trimming to INVALID, or to a point before this segment starts, is a no-op
        if trim_point == Lsn::INVALID || trim_point < self.base_lsn {
            return Ok(());
        }
        // saturate to the highest LSN this loglet can address. The saturation has to be
        // relative to base_lsn: clamping the absolute LSN to LogletOffset::MAX stops
        // trimming once a log passes 2^32 records, and panics in into_offset() for
        // segments whose base_lsn is itself above LogletOffset::MAX.
        let max_lsn = self.base_lsn.offset_by(LogletOffset::MAX);
        let trim_point = trim_point.min(max_lsn).into_offset(self.base_lsn);
        self.loglet.trim(trim_point).await
    }

    pub async fn seal(&self) -> Result<(), OperationError> {
        if self.tail_lsn.is_some() {
            return Ok(());
        }

        self.loglet.seal().await
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn inner(&self) -> &Arc<dyn Loglet> {
        &self.loglet
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
        self.loglet.tail_lsn = Some(tail_lsn)
    }

    pub fn loglet_id(&self) -> Option<LogletId> {
        self.loglet.loglet_id()
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::stream::BoxStream;

    use crate::loglet::{FindTailOptions, LogletCommit};

    use super::*;

    /// A loglet that only records the offsets it is asked to trim to.
    #[derive(Default)]
    struct RecordingLoglet {
        trims: Mutex<Vec<LogletOffset>>,
    }

    impl RecordingLoglet {
        fn last_trim(&self) -> Option<LogletOffset> {
            self.trims.lock().unwrap().last().copied()
        }
    }

    #[async_trait]
    impl Loglet for RecordingLoglet {
        fn id(&self) -> Option<LogletId> {
            None
        }

        fn debug_str(&self) -> Cow<'static, str> {
            "recording-loglet".into()
        }

        async fn create_read_stream(
            self: Arc<Self>,
            _filter: KeyFilter,
            _from: LogletOffset,
            _to: Option<LogletOffset>,
        ) -> Result<SendableLogletReadStream, OperationError> {
            unimplemented!()
        }

        fn watch_tail(&self) -> BoxStream<'static, TailState<LogletOffset>> {
            unimplemented!()
        }

        async fn enqueue_batch(
            &self,
            _payloads: Arc<[Record]>,
        ) -> Result<LogletCommit, OperationError> {
            unimplemented!()
        }

        async fn find_tail(
            &self,
            _opts: FindTailOptions,
        ) -> Result<TailState<LogletOffset>, OperationError> {
            unimplemented!()
        }

        async fn get_trim_point(&self) -> Result<Option<LogletOffset>, OperationError> {
            Ok(self.last_trim())
        }

        async fn trim(&self, trim_point: LogletOffset) -> Result<(), OperationError> {
            self.trims.lock().unwrap().push(trim_point);
            Ok(())
        }

        async fn seal(&self) -> Result<(), OperationError> {
            Ok(())
        }
    }

    fn wrapper(base_lsn: u64, loglet: Arc<RecordingLoglet>) -> LogletWrapper {
        LogletWrapper::new(
            SegmentIndex::from(31),
            Lsn::new(base_lsn),
            None,
            LogletConfig::for_testing(),
            loglet,
        )
    }

    fn trim(wrapper: &LogletWrapper, lsn: u64) -> Result<(), OperationError> {
        futures::executor::block_on(wrapper.trim(Lsn::new(lsn)))
    }

    // The LSNs below come from a production log whose tail passed 2^32 records.
    // Segment 31 starts below u32::MAX, segment 32 starts above it.
    const SEGMENT_31_BASE: u64 = 3_435_583_584;
    const SEGMENT_32_BASE: u64 = 4_505_162_431;
    const SEGMENT_33_BASE: u64 = 4_505_225_006;

    /// A trim point above u32::MAX must map to the segment-relative offset, not get
    /// clamped in LSN space. On the buggy code the loglet receives offset 859_383_712
    /// (== u32::MAX - base + 1), exactly what the log servers reported in production.
    #[test]
    fn trim_point_above_u32_max_is_segment_relative() {
        let loglet = Arc::new(RecordingLoglet::default());
        let wrapper = wrapper(SEGMENT_31_BASE, loglet.clone());
        let last_record = SEGMENT_32_BASE - 1;

        trim(&wrapper, last_record).unwrap();

        let expected = LogletOffset::new((last_record - SEGMENT_31_BASE + 1) as u32); // 1_069_578_847
        assert_eq!(loglet.last_trim(), Some(expected));
    }

    /// Trimming a segment whose base is above u32::MAX must not panic.
    #[test]
    fn trim_segment_with_base_above_u32_max_does_not_panic() {
        let loglet = Arc::new(RecordingLoglet::default());
        let wrapper = wrapper(SEGMENT_32_BASE, loglet.clone());
        let last_record = SEGMENT_33_BASE - 1;

        trim(&wrapper, last_record).unwrap();

        let expected = LogletOffset::new((last_record - SEGMENT_32_BASE + 1) as u32); // 62_575
        assert_eq!(loglet.last_trim(), Some(expected));
    }

    /// A trim point below the segment's base has nothing to trim in this segment.
    #[test]
    fn trim_point_below_segment_base_is_a_noop() {
        let loglet = Arc::new(RecordingLoglet::default());
        let wrapper = wrapper(SEGMENT_32_BASE, loglet.clone());

        trim(&wrapper, SEGMENT_32_BASE - 1_000).unwrap();

        assert_eq!(loglet.last_trim(), None);
    }

    /// Relative offsets beyond the loglet's u32 range saturate to the loglet maximum.
    #[test]
    fn trim_point_saturates_at_loglet_max_offset() {
        let loglet = Arc::new(RecordingLoglet::default());
        let wrapper = wrapper(1, loglet.clone());

        trim(&wrapper, u64::from(u32::MAX) + 1_000).unwrap();

        assert_eq!(loglet.last_trim(), Some(LogletOffset::MAX));
    }
}
