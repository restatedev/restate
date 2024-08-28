// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::sync::Arc;

use restate_core::ShutdownError;
use restate_types::errors::MaybeRetryableError;

use crate::metadata::LogStoreMarker;

pub type Result<T, E = LogStoreError> = std::result::Result<T, E>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum LogStoreError {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Other(Arc<dyn MaybeRetryableError + Send + Sync + 'static>),
}

pub trait LogStore {
    fn load_marker(&self) -> impl Future<Output = Result<Option<LogStoreMarker>>> + Send + '_;
    fn store_marker(&self, marker: LogStoreMarker) -> impl Future<Output = Result<()>> + Send;
}
