// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Add;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use async_trait::async_trait;

use bytes::Bytes;
use futures::Stream;
use restate_types::config::Configuration;
use restate_types::logs::metadata::{LogletParams, ProviderKind};
use restate_types::logs::{Lsn, Payload, SequenceNumber};

use crate::Result;
use crate::{LogRecord, LsnExt, ProviderError};

pub fn create_provider(kind: ProviderKind) -> Result<Arc<dyn LogletProvider>, ProviderError> {
    match kind {
        ProviderKind::Local => Ok(crate::loglets::local_loglet::LocalLogletProvider::new(
            &Configuration::current().load().bifrost.local,
            Configuration::mapped_updateable(|c| &c.bifrost.local.rocksdb),
        )?),
        ProviderKind::InMemory => Ok(crate::loglets::memory_loglet::MemoryLogletProvider::new()?),
    }
}

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

    fn next(self) -> Self {
        Self(self.0 + 1)
    }

    fn prev(self) -> Self {
        if self == Self::INVALID {
            Self::INVALID
        } else {
            Self(std::cmp::max(Self::OLDEST.0, self.0.saturating_sub(1)))
        }
    }
}

#[async_trait]
pub trait LogletProvider: Send + Sync {
    /// Create a loglet client for a given segment and configuration.
    async fn get_loglet(&self, params: &LogletParams) -> Result<Arc<dyn Loglet>>;

    // Hook for handling lazy initialization
    fn start(&self) -> Result<(), ProviderError>;

    // Hook for handling graceful shutdown
    async fn shutdown(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}

pub trait Loglet: LogletBase<Offset = LogletOffset> {}
impl<T> Loglet for T where T: LogletBase<Offset = LogletOffset> {}

/// Wraps loglets with the base LSN of the segment
#[derive(Clone, Debug)]
pub struct LogletWrapper {
    pub(crate) base_lsn: Lsn,
    loglet: Arc<dyn Loglet>,
}

impl LogletWrapper {
    pub fn new(base_lsn: Lsn, loglet: Arc<dyn Loglet>) -> Self {
        Self { base_lsn, loglet }
    }

    pub async fn create_wrapped_read_stream(self, after: Lsn) -> Result<LogletReadStreamWrapper> {
        // // convert LSN to loglet offset
        Ok(LogletReadStreamWrapper::new(
            self.loglet
                .create_read_stream(after.into_offset(self.base_lsn))
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

/// A loglet represents a logical log stream provided by a provider implementation.
///
/// Loglets are required to follow these rules:
/// - Loglet implementations must be Send + Sync (internal mutability is required)
/// - Loglets must strictly adhere to the consistency requirements as the interface calls
///   that is, if an append returns an offset, it **must** be durably committed.
/// - Loglets are allowed to buffer writes internally as long as the order of records
///   follows the order of append calls.
#[async_trait]
pub trait LogletBase: Send + Sync + std::fmt::Debug {
    type Offset: SequenceNumber;

    /// Append a record to the loglet.
    async fn create_read_stream(
        self: Arc<Self>,
        after: Self::Offset,
    ) -> Result<SendableLogletReadStream<Self::Offset>>;

    /// Append a record to the loglet.
    async fn append(&self, data: Bytes) -> Result<Self::Offset>;

    /// Append a batch of records to the loglet. The returned offset (on success) if the offset of
    /// the first record in the batch)
    async fn append_batch(&self, payloads: &[Bytes]) -> Result<Self::Offset>;

    /// Find the tail of the loglet. If the loglet is empty or have been trimmed, the loglet should
    /// return `None`.
    async fn find_tail(&self) -> Result<Option<Self::Offset>>;

    /// The offset of the slot **before** the first readable record (if it exists), or the offset
    /// before the next slot that will be written to.
    async fn get_trim_point(&self) -> Result<Self::Offset>;

    /// Read or wait for the record at `from` offset, or the next available record if `from` isn't
    /// defined for the loglet.
    async fn read_next_single(&self, after: Self::Offset)
        -> Result<LogRecord<Self::Offset, Bytes>>;

    /// Read the next record if it's been committed, otherwise, return None without waiting.
    async fn read_next_single_opt(
        &self,
        after: Self::Offset,
    ) -> Result<Option<LogRecord<Self::Offset, Bytes>>>;
}

/// A stream of log records from a single loglet. Loglet streams are _always_ tailing streams.
pub trait LogletReadStream<S: SequenceNumber>: Stream<Item = Result<LogRecord<S, Bytes>>> {}

pub type SendableLogletReadStream<S = Lsn> = Pin<Box<dyn LogletReadStream<S> + Send>>;

#[async_trait]
impl LogletBase for LogletWrapper {
    type Offset = Lsn;

    /// This should never be used directly. Instead, use `create_wrapped_read_stream()` instead.
    async fn create_read_stream(
        self: Arc<Self>,
        after: Self::Offset,
    ) -> Result<SendableLogletReadStream<Self::Offset>> {
        unreachable!("create_read_stream on LogletWrapper should never be used directly")
    }

    async fn append(&self, data: Bytes) -> Result<Lsn> {
        let offset = self.loglet.append(data).await?;
        // Return the LSN given the loglet offset.
        Ok(self.base_lsn.offset_by(offset))
    }

    async fn append_batch(&self, payloads: &[Bytes]) -> Result<Lsn> {
        let offset = self.loglet.append_batch(payloads).await?;
        Ok(self.base_lsn.offset_by(offset))
    }

    async fn find_tail(&self) -> Result<Option<Lsn>> {
        let offset = self.loglet.find_tail().await?;
        Ok(offset.map(|o| self.base_lsn.offset_by(o)))
    }

    async fn get_trim_point(&self) -> Result<Self::Offset> {
        let offset = self.loglet.get_trim_point().await?;
        Ok(self.base_lsn.offset_by(offset))
    }

    async fn read_next_single(&self, after: Lsn) -> Result<LogRecord<Lsn, Bytes>> {
        // convert LSN to loglet offset
        let offset = after.into_offset(self.base_lsn);
        self.loglet
            .read_next_single(offset)
            .await
            .map(|record| record.with_base_lsn(self.base_lsn))
    }

    async fn read_next_single_opt(
        &self,
        after: Self::Offset,
    ) -> Result<Option<LogRecord<Self::Offset, Bytes>>> {
        let offset = after.into_offset(self.base_lsn);
        self.loglet
            .read_next_single_opt(offset)
            .await
            .map(|maybe_record| maybe_record.map(|record| record.with_base_lsn(self.base_lsn)))
    }
}

/// Wraps loglet read streams with the base LSN of the segment
pub struct LogletReadStreamWrapper {
    pub(crate) base_lsn: Lsn,
    inner: SendableLogletReadStream<LogletOffset>,
}

impl LogletReadStreamWrapper {
    pub fn new(inner: SendableLogletReadStream<LogletOffset>, base_lsn: Lsn) -> Self {
        Self { inner, base_lsn }
    }
}

impl Stream for LogletReadStreamWrapper {
    type Item = Result<LogRecord<Lsn, Bytes>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
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
