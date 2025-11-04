// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

/// Error type which abstracts away the actual [`std::error::Error`] type. Use this type
/// if you don't know the actual error type or if it is not important.
pub type GenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type BoxedMaybeRetryableError = Box<dyn MaybeRetryableError + Send + Sync>;

/// Tells whether an error should be retried by upper layers or not.
pub trait MaybeRetryableError: std::error::Error + 'static {
    /// Signal upper layers whether this error should be retried or not.
    fn retryable(&self) -> bool {
        false
    }
}

static_assertions::assert_obj_safe!(MaybeRetryableError);

pub trait IntoMaybeRetryable: Sized {
    /// Marks the error marked as retryable
    fn into_retryable(self) -> RetryableError<Self> {
        RetryableError(self)
    }

    /// Marks the error marked as non-retryable
    fn into_terminal(self) -> TerminalError<Self> {
        TerminalError(self)
    }
}

impl<T> IntoMaybeRetryable for T where
    T: fmt::Debug + fmt::Display + Send + Sync + std::error::Error + 'static
{
}

/// Wraps any source error and marks it as retryable
#[derive(Debug, thiserror::Error, derive_more::Deref, derive_more::From)]
pub struct RetryableError<T>(#[source] T);

/// Wraps any source error and marks it as non-retryable
#[derive(Debug, thiserror::Error, derive_more::Deref, derive_more::From)]
pub struct TerminalError<T>(#[source] T);

impl<T> MaybeRetryableError for RetryableError<T>
where
    T: std::error::Error + 'static,
{
    fn retryable(&self) -> bool {
        true
    }
}

impl<T> MaybeRetryableError for TerminalError<T>
where
    T: std::error::Error + 'static,
{
    fn retryable(&self) -> bool {
        false
    }
}

impl<T> fmt::Display for RetryableError<T>
where
    T: fmt::Debug + fmt::Display + std::error::Error,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[retryable] {}", self.0)
    }
}

impl<T> fmt::Display for TerminalError<T>
where
    T: fmt::Debug + fmt::Display + std::error::Error + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[terminal] {}", self.0)
    }
}
