// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::borrow::Cow;
use std::convert::Into;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use tonic;
use tracing::Level;

use restate_encoding::BilrostNewType;

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
    T: std::fmt::Debug + std::fmt::Display + Send + Sync + std::error::Error + 'static
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

impl<T> std::fmt::Display for RetryableError<T>
where
    T: std::fmt::Debug + std::fmt::Display + std::error::Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[retryable] {}", self.0)
    }
}

impl<T> std::fmt::Display for TerminalError<T>
where
    T: std::fmt::Debug + std::fmt::Display + std::error::Error + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[terminal] {}", self.0)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, BilrostNewType)]
#[serde(transparent)]
pub struct InvocationErrorCode(u16);

impl InvocationErrorCode {
    pub const fn new(code: u16) -> Self {
        InvocationErrorCode(code)
    }
}

impl From<u16> for InvocationErrorCode {
    fn from(value: u16) -> Self {
        InvocationErrorCode(value)
    }
}

impl From<u32> for InvocationErrorCode {
    fn from(value: u32) -> Self {
        value
            .try_into()
            .map(InvocationErrorCode)
            .unwrap_or(codes::INTERNAL)
    }
}

impl From<InvocationErrorCode> for u16 {
    fn from(value: InvocationErrorCode) -> Self {
        value.0
    }
}

impl From<InvocationErrorCode> for u32 {
    fn from(value: InvocationErrorCode) -> Self {
        value.0 as u32
    }
}

impl fmt::Display for InvocationErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(display_str) = self.display_str() {
            write!(f, "{} {}", self.0, display_str)
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl fmt::Debug for InvocationErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

pub mod codes {
    use super::InvocationErrorCode;

    macro_rules! codes {
        ($($name:ident $num:literal $str_name:literal,)*) => {
            $(pub const $name: InvocationErrorCode = InvocationErrorCode($num);)*

            impl InvocationErrorCode {
                pub(super) fn display_str(&self) -> Option<&'static str> {
                    $(if self.0 == $name.0 { return Some($str_name) })*
                    None
                }
            }
        };
    }

    codes!(
        BAD_REQUEST 400 "Bad request",
        NOT_FOUND 404 "Not found",
        INTERNAL 500 "Internal",
        ABORTED 409 "Aborted",
        GONE 410 "Gone",
        UNSUPPORTED_MEDIA_TYPE 415 "Unsupported media type",
        JOURNAL_MISMATCH 570 "Journal mismatch",
        PROTOCOL_VIOLATION 571 "Protocol violation",
        CONFLICT 409 "Conflict",
        NOT_READY 470 "Not ready",
    );
}

/// This struct represents errors arisen when processing a service invocation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationError {
    code: InvocationErrorCode,
    message: Cow<'static, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stacktrace: Option<Cow<'static, str>>,
}

pub const UNKNOWN_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::INTERNAL, "unknown");

impl Default for InvocationError {
    fn default() -> Self {
        UNKNOWN_INVOCATION_ERROR
    }
}

impl fmt::Display for InvocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code(), self.message())?;
        if self.stacktrace.is_some() {
            write!(f, "\n{}", self.stacktrace().unwrap())?;
        }
        Ok(())
    }
}

impl std::error::Error for InvocationError {}

impl InvocationError {
    pub const fn new_static(code: InvocationErrorCode, message: &'static str) -> Self {
        Self {
            code,
            message: Cow::Borrowed(message),
            stacktrace: None,
        }
    }

    pub fn new(code: impl Into<InvocationErrorCode>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: Cow::Owned(message.into()),
            stacktrace: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: codes::INTERNAL,
            message: Cow::Owned(message.into()),
            stacktrace: None,
        }
    }

    pub fn service_not_found(service: impl fmt::Display) -> Self {
        Self {
            code: codes::NOT_FOUND,
            message: Cow::Owned(format!(
                "Service '{service}' not found. Check whether the deployment containing the service is registered."
            )),
            stacktrace: None,
        }
    }

    pub fn service_handler_not_found(
        service: impl fmt::Display,
        handler: impl fmt::Display,
    ) -> Self {
        Self {
            code: codes::NOT_FOUND,
            message: Cow::Owned(format!(
                "Service handler '{service}/{handler}' not found. Check whether you've registered the correct version of your service."
            )),
            stacktrace: None,
        }
    }

    pub fn with_static_message(mut self, message: &'static str) -> InvocationError {
        self.message = Cow::Borrowed(message);
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> InvocationError {
        self.message = Cow::Owned(message.into());
        self
    }

    pub fn with_stacktrace(mut self, stacktrace: impl Into<String>) -> InvocationError {
        self.stacktrace = Some(Cow::Owned(stacktrace.into()));
        self
    }

    pub fn code(&self) -> InvocationErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn stacktrace(&self) -> Option<&str> {
        self.stacktrace.as_deref()
    }

    pub fn into_stacktrace(self) -> Option<Cow<'static, str>> {
        self.stacktrace
    }
}

impl From<anyhow::Error> for InvocationError {
    fn from(error: anyhow::Error) -> Self {
        InvocationError::internal(error.to_string())
    }
}

// -- Some known errors

pub const KILLED_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::ABORTED, "killed");

// TODO: Once we want to distinguish server side cancellations from user code returning the
//  UserErrorCode::Cancelled, we need to add a new RestateErrorCode.
pub const CANCELED_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::ABORTED, "canceled");

pub const GONE_INVOCATION_ERROR: InvocationError = InvocationError::new_static(codes::GONE, "gone");

pub const NOT_FOUND_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::NOT_FOUND, "not found");

pub const ALREADY_COMPLETED_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::CONFLICT, "promise was already completed");

pub const WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::CONFLICT, "the workflow method was already invoked");

pub const NOT_READY_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::NOT_READY, "the response is not ready yet");

/// Error parsing/decoding a resource ID.
#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
pub enum IdDecodeError {
    #[error("bad length")]
    Length,
    #[error("base62 decode error")]
    Codec,
    #[error("bad format")]
    Format,
    #[error("unrecognized codec version")]
    Version,
    #[error("id doesn't match the expected type")]
    TypeMismatch,
    #[error("unrecognized resource type: {0}")]
    UnrecognizedType(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ThreadJoinError {
    #[error("thread panicked: {0:?}")]
    Panic(sync_wrapper::SyncWrapper<Box<dyn Any + Send + 'static>>),
    #[error("thread terminated unexpectedly")]
    UnexpectedTermination,
}

#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error("missing field '{0}'")]
    MissingField(&'static str),
    #[error("invalid data '{0}'")]
    InvalidData(&'static str),
}

impl ConversionError {
    pub fn missing_field(field: &'static str) -> Self {
        ConversionError::MissingField(field)
    }

    pub fn invalid_data(field: &'static str) -> Self {
        ConversionError::InvalidData(field)
    }
}

/// A simplified display wrapper around tonic::Status that only shows the status code and message,
/// omitting the details and metadata fields for cleaner output unless log level DEBUG or higher is
/// enabled.
#[derive(Clone, derive_more::Debug)]
#[debug("{}", _0)]
pub struct SimpleStatus(pub tonic::Status);

impl From<tonic::Status> for SimpleStatus {
    fn from(status: tonic::Status) -> Self {
        Self(status)
    }
}

impl AsRef<tonic::Status> for SimpleStatus {
    fn as_ref(&self) -> &tonic::Status {
        &self.0
    }
}

impl From<SimpleStatus> for tonic::Status {
    fn from(status: SimpleStatus) -> Self {
        status.0
    }
}

impl SimpleStatus {
    /// Returns the status code
    pub fn code(&self) -> tonic::Code {
        self.0.code()
    }

    /// Returns the status message
    pub fn message(&self) -> &str {
        self.0.message()
    }
}

impl Display for SimpleStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if tracing::enabled!(Level::DEBUG) {
            // display all details of status if DEBUG log level or higher is enabled
            Display::fmt(&self.0, f)
        } else {
            write!(f, "{}: {}", self.0.code(), self.0.message())
        }
    }
}

impl std::error::Error for SimpleStatus {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn test_simple_status_display() {
        let status = tonic::Status::new(Code::NotFound, "Resource not found");
        let simple_status = SimpleStatus(status);
        assert_eq!(
            simple_status.to_string(),
            "Some requested entity was not found: Resource not found"
        );
    }

    #[test]
    fn test_simple_status_from_tonic() {
        let tonic_status = tonic::Status::new(Code::Internal, "Internal error");
        let simple_status: SimpleStatus = tonic_status.clone().into();
        assert_eq!(simple_status.to_string(), "Internal error: Internal error");
    }

    #[test]
    fn test_simple_status_as_ref() {
        let tonic_status = tonic::Status::new(Code::InvalidArgument, "Invalid argument");
        let simple_status = SimpleStatus(tonic_status.clone());
        assert_eq!(simple_status.as_ref().code(), tonic_status.code());
        assert_eq!(simple_status.as_ref().message(), tonic_status.message());
    }

    #[test]
    fn test_simple_status_into_tonic() {
        let tonic_status = tonic::Status::new(Code::Unauthenticated, "Unauthenticated");
        let simple_status = SimpleStatus(tonic_status.clone());
        let converted: tonic::Status = simple_status.into();
        assert_eq!(converted.code(), tonic_status.code());
        assert_eq!(converted.message(), tonic_status.message());
    }

    #[test]
    fn test_simple_status_omits_details_and_metadata() {
        let mut status = tonic::Status::new(Code::Internal, "Error message");

        status
            .metadata_mut()
            .insert("key", "value".parse().unwrap());

        let simple_status = SimpleStatus(status);
        assert_eq!(simple_status.to_string(), "Internal error: Error message")
    }
}
