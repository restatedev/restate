// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use enum_map::Enum;

use restate_types::logs::{Lsn, Payload, SequenceNumber};

use crate::metadata::LogletParams;
use crate::{Error, LogRecord, LsnExt, Options};

/// An enum with the list of supported loglet providers.
/// For each variant we must have a corresponding implementation of the
/// [`crate::loglet::Loglet`] trait
#[derive(
    Debug,
    Clone,
    Hash,
    Eq,
    PartialEq,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    Enum,
    strum_macros::EnumIter,
)]
pub enum ProviderKind {
    /// A file-backed loglet.
    File,
    #[cfg(test)]
    Memory,
}

pub fn provider_default_config(kind: ProviderKind) -> serde_json::Value {
    match kind {
        ProviderKind::File => crate::loglets::file_loglet::default_config(),
        #[cfg(test)]
        ProviderKind::Memory => crate::loglets::memory_loglet::default_config(),
    }
}

pub fn create_provider(kind: ProviderKind, options: &Options) -> Arc<dyn LogletProvider> {
    match kind {
        ProviderKind::File => crate::loglets::file_loglet::FileLogletProvider::new(options),
        #[cfg(test)]
        ProviderKind::Memory => crate::loglets::memory_loglet::MemoryLogletProvider::new(),
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
    async fn get_loglet(&self, params: &LogletParams) -> Result<Arc<dyn Loglet>, Error>;

    // Hook for handling lazy initialization
    async fn start(&self) -> Result<(), Error> {
        Ok(())
    }

    // Hook for handling graceful shutdown
    async fn shutdown(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub trait Loglet: LogletBase<Offset = LogletOffset> {}
impl<T> Loglet for T where T: LogletBase<Offset = LogletOffset> {}

/// Wraps loglets with the base LSN of the segment
#[derive(Clone)]
pub struct LogletWrapper {
    pub(crate) base_lsn: Lsn,
    loglet: Arc<dyn Loglet>,
}

impl LogletWrapper {
    pub fn new(base_lsn: Lsn, loglet: Arc<dyn Loglet>) -> Self {
        Self { base_lsn, loglet }
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
pub trait LogletBase: Send + Sync {
    type Offset: SequenceNumber;

    /// Append a record to the loglet.
    async fn append(&self, payload: Payload) -> Result<Self::Offset, Error>;

    /// Find the tail of the loglet. If the loglet is empty or have been trimmed, the loglet should
    /// return `None`.
    async fn find_tail(&self) -> Result<Option<Self::Offset>, Error>;

    /// The offset of the slot **before** the first readable record (if it exists), or the offset
    /// before the next slot that will be written to.
    async fn get_trim_point(&self) -> Result<Self::Offset, Error>;

    /// Read or wait for the record at `from` offset, or the next available record if `from` isn't
    /// defined for the loglet.
    async fn read_next_single(&self, after: Self::Offset)
        -> Result<LogRecord<Self::Offset>, Error>;

    /// Read the next record if it's been committed, otherwise, return None without waiting.
    async fn read_next_single_opt(
        &self,
        after: Self::Offset,
    ) -> Result<Option<LogRecord<Self::Offset>>, Error>;
}

#[async_trait]
impl LogletBase for LogletWrapper {
    type Offset = Lsn;

    async fn append(&self, payload: Payload) -> Result<Lsn, Error> {
        let offset = self.loglet.append(payload).await?;
        // Return the LSN given the loglet offset.
        Ok(self.base_lsn.offset_by(offset))
    }

    async fn find_tail(&self) -> Result<Option<Lsn>, Error> {
        let offset = self.loglet.find_tail().await?;
        Ok(offset.map(|o| self.base_lsn.offset_by(o)))
    }

    async fn get_trim_point(&self) -> Result<Self::Offset, Error> {
        let offset = self.loglet.get_trim_point().await?;
        Ok(self.base_lsn.offset_by(offset))
    }

    async fn read_next_single(&self, after: Lsn) -> Result<LogRecord<Lsn>, Error> {
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
    ) -> Result<Option<LogRecord<Self::Offset>>, Error> {
        let offset = after.into_offset(self.base_lsn);
        self.loglet
            .read_next_single_opt(offset)
            .await
            .map(|maybe_record| maybe_record.map(|record| record.with_base_lsn(self.base_lsn)))
    }
}

static_assertions::assert_impl_all!(LogletWrapper: Send, Sync, Clone);
