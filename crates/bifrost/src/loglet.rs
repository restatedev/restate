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

use crate::metadata::LogletParams;
use crate::{AppendAttributes, DataRecord, Error, Lsn, Options};

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
#[derive(Debug, Clone, derive_more::From, derive_more::Display)]
pub struct LogletOffset(u64);

impl LogletOffset {
    /// Converts a loglet offset into the virtual address (LSN). The base_lsn is
    /// the starting address of the segment.
    pub fn into_lsn(self, base_lsn: Lsn) -> Lsn {
        // This assumes that this will not overflow. That's not guaranteed to always be the
        // case but it should be extremely rare that it'd be okay to just wrap in this case.
        Lsn(base_lsn.0.wrapping_add(self.0))
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
    base_lsn: Lsn,
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
    type Offset;

    /// Append a record to the loglet.
    async fn append(
        &self,
        record: DataRecord,
        attributes: AppendAttributes,
    ) -> Result<Self::Offset, Error>;
}

#[async_trait]
impl LogletBase for LogletWrapper {
    type Offset = Lsn;

    async fn append(&self, record: DataRecord, attributes: AppendAttributes) -> Result<Lsn, Error> {
        let offset = self.loglet.append(record, attributes).await?;
        // Return the LSN given the loglet offset.
        Ok(offset.into_lsn(self.base_lsn))
    }
}

static_assertions::assert_impl_all!(LogletWrapper: Send, Sync, Clone);
