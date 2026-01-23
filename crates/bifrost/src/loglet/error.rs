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
use std::fmt::Debug;
use std::sync::Arc;

use restate_core::ShutdownError;
use restate_types::errors::{IntoMaybeRetryable, MaybeRetryableError};

#[derive(Debug, Clone, thiserror::Error)]
pub enum AppendError {
    #[error("Loglet has been sealed")]
    Sealed,
    #[error("Loglet needs reconfiguration; {0}")]
    ReconfigurationNeeded(Cow<'static, str>),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Other(Arc<dyn MaybeRetryableError + Send + Sync>),
}

impl AppendError {
    pub fn retryable<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Arc::new(error.into_retryable()))
    }

    pub fn terminal<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Arc::new(error.into_terminal()))
    }

    pub fn other<E: MaybeRetryableError + Send + Sync>(error: E) -> Self {
        Self::Other(Arc::new(error))
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum OperationError {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Other(Arc<dyn MaybeRetryableError + Send + Sync>),
}

impl OperationError {
    pub fn retryable<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Arc::new(error.into_retryable()))
    }

    pub fn terminal<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Arc::new(error.into_terminal()))
    }

    pub fn other<E: MaybeRetryableError + Send + Sync>(error: E) -> Self {
        Self::Other(Arc::new(error))
    }
}

impl From<OperationError> for AppendError {
    fn from(value: OperationError) -> Self {
        match value {
            OperationError::Shutdown(s) => AppendError::Shutdown(s),
            OperationError::Other(o) => AppendError::Other(o),
        }
    }
}

impl MaybeRetryableError for OperationError {
    fn retryable(&self) -> bool {
        match self {
            OperationError::Shutdown(_) => false,
            OperationError::Other(err) => err.retryable(),
        }
    }
}
