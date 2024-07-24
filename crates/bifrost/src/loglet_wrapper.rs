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

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

use restate_types::logs::{Lsn, SequenceNumber};

use crate::loglet::{
    AppendError, Loglet, LogletBase, LogletOffset, OperationError, SendableLogletReadStream,
};
use crate::{LogRecord, LsnExt};
use crate::{Result, TailState};

#[derive(Debug, Clone, thiserror::Error)]
enum LogletWrapperError {
    #[error("attempted to read outside the tail boundary of the loglet, requested read LSN is {attempt_lsn}, tail is at {tail_lsn}")]
    OutOfBoundsRead { attempt_lsn: Lsn, tail_lsn: Lsn },
}

/// Wraps loglets with the base LSN of the segment
#[derive(Clone, Debug)]
pub struct LogletWrapper {
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
    pub fn new(base_lsn: Lsn, tail_lsn: Option<Lsn>, loglet: Arc<dyn Loglet>) -> Self {
        Self {
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

    pub async fn create_wrapped_read_stream(
        self,
        start_lsn: Lsn,
    ) -> Result<LogletReadStreamWrapper> {
        // Translates LSN to loglet offset
        Ok(LogletReadStreamWrapper::new(
            self.clone(),
            self.loglet
                .create_read_stream(
                    start_lsn.into_offset(self.base_lsn),
                    // We go back one LSN because `to` is inclusive and `tail_lsn` is exclusive.
                    // transposed to loglet offset (if set)
                    self.tail_lsn
                        .map(|tail| tail.prev().into_offset(self.base_lsn)),
                )
                .await?,
            self.base_lsn,
        ))
    }
}

impl PartialEq for LogletWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.base_lsn == other.base_lsn && Arc::ptr_eq(&self.loglet, &other.loglet)
    }
}

#[async_trait]
impl LogletBase for LogletWrapper {
    type Offset = Lsn;

    /// This should never be used directly. Instead, use `create_wrapped_read_stream()` instead.
    async fn create_read_stream(
        self: Arc<Self>,
        _after: Self::Offset,
        _to: Option<Self::Offset>,
    ) -> Result<SendableLogletReadStream<Self::Offset>> {
        unreachable!("create_read_stream on LogletWrapper should never be used directly")
    }

    async fn append(&self, data: Bytes) -> Result<Lsn, AppendError> {
        if self.tail_lsn.is_some() {
            return Err(AppendError::Sealed);
        }

        let offset = self.loglet.append(data).await?;
        // Return the LSN given the loglet offset.
        Ok(self.base_lsn.offset_by(offset))
    }

    async fn append_batch(&self, payloads: &[Bytes]) -> Result<Lsn, AppendError> {
        if self.tail_lsn.is_some() {
            return Err(AppendError::Sealed);
        }
        let offset = self.loglet.append_batch(payloads).await?;
        Ok(self.base_lsn.offset_by(offset))
    }

    async fn find_tail(&self) -> Result<TailState<Lsn>, OperationError> {
        if let Some(tail) = self.tail_lsn {
            return Ok(TailState::Sealed(tail));
        }

        Ok(self
            .loglet
            .find_tail()
            .await?
            .map(|o| self.base_lsn.offset_by(o)))
    }

    async fn get_trim_point(&self) -> Result<Option<Lsn>, OperationError> {
        let offset = self.loglet.get_trim_point().await?;
        Ok(offset.map(|o| self.base_lsn.offset_by(o)))
    }

    // trim_point is inclusive.
    async fn trim(&self, trim_point: Self::Offset) -> Result<(), OperationError> {
        // trimming to INVALID is no-op
        if trim_point == Self::Offset::INVALID {
            return Ok(());
        }
        let trim_point = trim_point.into_offset(self.base_lsn);
        self.loglet.trim(trim_point).await
    }

    async fn seal(&self) -> Result<(), OperationError> {
        if self.tail_lsn.is_some() {
            return Ok(());
        }

        self.loglet.seal().await
    }

    async fn read(&self, from: Lsn) -> Result<LogRecord<Lsn, Bytes>, OperationError> {
        if self.tail_lsn.is_some_and(|tail| from >= tail) {
            // We are trying to read past the the last record. Upper layers should fence against
            // this case, therefore, we throw an OperationError.
            return Err(OperationError::terminal(
                LogletWrapperError::OutOfBoundsRead {
                    attempt_lsn: from,
                    tail_lsn: self.tail_lsn.unwrap(),
                },
            ));
        }
        // convert LSN to loglet offset
        let offset = from.into_offset(self.base_lsn);
        self.loglet
            .read(offset)
            .await
            .map(|record| record.with_base_lsn(self.base_lsn))
    }

    async fn read_opt(
        &self,
        from: Self::Offset,
    ) -> Result<Option<LogRecord<Self::Offset, Bytes>>, OperationError> {
        if self.tail_lsn.is_some_and(|tail| from >= tail) {
            // We are trying to read past the the last record. Upper layers should fence against
            // this case, therefore, we throw an OperationError.
            return Err(OperationError::terminal(
                LogletWrapperError::OutOfBoundsRead {
                    attempt_lsn: from,
                    tail_lsn: self.tail_lsn.unwrap(),
                },
            ));
        }
        let offset = from.into_offset(self.base_lsn);
        self.loglet
            .read_opt(offset)
            .await
            .map(|maybe_record| maybe_record.map(|record| record.with_base_lsn(self.base_lsn)))
    }
}

/// Wraps loglet read streams with the base LSN of the segment
pub struct LogletReadStreamWrapper {
    pub(crate) base_lsn: Lsn,
    #[allow(dead_code)]
    loglet: LogletWrapper,
    inner_read_stream: SendableLogletReadStream<LogletOffset>,
}

impl LogletReadStreamWrapper {
    pub fn new(
        loglet: LogletWrapper,
        inner_read_stream: SendableLogletReadStream<LogletOffset>,
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
    #[allow(dead_code)]
    #[inline(always)]
    pub fn tail_lsn(&self) -> Option<Lsn> {
        self.loglet.tail_lsn
    }

    #[allow(dead_code)]
    pub fn set_tail_lsn(&mut self, tail_lsn: Lsn) {
        self.loglet.set_tail_lsn(tail_lsn)
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn loglet(&self) -> &LogletWrapper {
        &self.loglet
    }
}

impl Stream for LogletReadStreamWrapper {
    type Item = Result<LogRecord<Lsn, Bytes>, OperationError>;

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
