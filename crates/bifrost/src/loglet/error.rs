// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Debug, Display};
use std::sync::Arc;

use restate_core::ShutdownError;

#[derive(Debug, Clone, thiserror::Error)]
pub enum AppendError {
    #[error("Loglet is sealed")]
    Sealed,
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Other(Arc<dyn LogletError>),
}

impl AppendError {
    pub fn retryable<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Arc::new(RetryableError(error)))
    }

    pub fn terminal<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Arc::new(TerminalError(error)))
    }

    pub fn other<E: LogletError + Send + Sync>(error: E) -> Self {
        Self::Other(Arc::new(error))
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum OperationError {
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    Other(Arc<dyn LogletError>),
}

impl OperationError {
    pub fn retryable<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Arc::new(RetryableError(error)))
    }

    pub fn terminal<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(Arc::new(TerminalError(error)))
    }

    pub fn other<E: LogletError + Send + Sync>(error: E) -> Self {
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

// -- Helper Types --

/// Represents a type-erased error from the loglet provider.
pub trait LogletError: std::error::Error + Send + Sync + Debug + Display + 'static {
    /// Signal upper layers whether this erorr should be retried or not.
    fn retryable(&self) -> bool {
        false
    }
}

#[derive(Debug, thiserror::Error)]
struct RetryableError<T>(#[source] T);

impl<T> Display for RetryableError<T>
where
    T: Debug + Display + Send + Sync + std::error::Error + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[retryable] {}", self.0)
    }
}

impl<T> LogletError for RetryableError<T>
where
    T: Debug + Display + Send + Sync + std::error::Error + 'static,
{
    fn retryable(&self) -> bool {
        true
    }
}

#[derive(Debug, thiserror::Error)]
struct TerminalError<T>(#[source] T);

impl<T> LogletError for TerminalError<T>
where
    T: Debug + Display + Send + Sync + std::error::Error + 'static,
{
    fn retryable(&self) -> bool {
        false
    }
}

impl<T> Display for TerminalError<T>
where
    T: Debug + Display + Send + Sync + std::error::Error + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[terminal] {}", self.0)
    }
}
