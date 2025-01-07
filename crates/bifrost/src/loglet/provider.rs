// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_types::logs::metadata::{
    Chain, LogletParams, ProviderConfiguration, ProviderKind, SegmentIndex,
};
use restate_types::logs::LogId;

use super::{Loglet, OperationError};
use crate::Result;

#[async_trait]
/// Factory for creating loglet providers.
pub trait LogletProviderFactory: Send + 'static {
    /// Factory creates providers of `kind`.
    fn kind(&self) -> ProviderKind;
    /// Initialize provider.
    async fn create(self: Box<Self>) -> Result<Arc<dyn LogletProvider + 'static>, OperationError>;
}

#[async_trait]
pub trait LogletProvider: Send + Sync {
    /// Create a loglet client for a given segment and configuration.
    async fn get_loglet(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
        params: &LogletParams,
    ) -> Result<Arc<dyn Loglet>>;

    /// Returns a proposed `LogletParams` given this log_id, chain, and defaults.
    ///
    /// This will not perform any updates, it just statically generates a valid
    /// configuration for a potentially new loglet.
    ///
    /// if `chain` is None, the provider should assume that no chain exists already
    /// for this log.
    fn propose_new_loglet_params(
        &self,
        log_id: LogId,
        chain: Option<&Chain>,
        defaults: &ProviderConfiguration,
    ) -> Result<LogletParams, OperationError>;

    /// A hook that's called after provider is started.
    async fn post_start(&self) {}

    /// Hook for handling graceful shutdown
    async fn shutdown(&self) -> Result<(), OperationError> {
        Ok(())
    }
}
