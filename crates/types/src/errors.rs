// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

/// Error type which abstracts away the actual [`std::error::Error`] type. Use this type
/// if you don't know the actual error type or if it is not important.
pub type GenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    derive_more::Debug,
    derive_more::Display,
    serde::Serialize,
    serde::Deserialize,
)]
#[debug("{}", _0)]
#[display("{}", _0)]
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

pub mod codes {
    use super::InvocationErrorCode;

    pub const BAD_REQUEST: InvocationErrorCode = InvocationErrorCode(400);
    pub const NOT_FOUND: InvocationErrorCode = InvocationErrorCode(404);
    pub const INTERNAL: InvocationErrorCode = InvocationErrorCode(500);
    pub const UNKNOWN: InvocationErrorCode = INTERNAL;
    pub const ABORTED: InvocationErrorCode = InvocationErrorCode(409);
    pub const KILLED: InvocationErrorCode = ABORTED;
    pub const GONE: InvocationErrorCode = InvocationErrorCode(410);
    pub const JOURNAL_MISMATCH: InvocationErrorCode = InvocationErrorCode(570);
    pub const PROTOCOL_VIOLATION: InvocationErrorCode = InvocationErrorCode(571);
    pub const CONFLICT: InvocationErrorCode = InvocationErrorCode(409);
}

/// This struct represents errors arisen when processing a service invocation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationError {
    code: InvocationErrorCode,
    message: Cow<'static, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<Cow<'static, str>>,
}

pub const UNKNOWN_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::UNKNOWN, "unknown");

impl Default for InvocationError {
    fn default() -> Self {
        UNKNOWN_INVOCATION_ERROR
    }
}

impl fmt::Display for InvocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{:?}] {}", self.code(), self.message())?;
        if self.description.is_some() {
            write!(f, ".\n{}", self.description().unwrap())?;
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
            description: None,
        }
    }

    pub fn new(code: impl Into<InvocationErrorCode>, message: impl fmt::Display) -> Self {
        Self {
            code: code.into(),
            message: Cow::Owned(message.to_string()),
            description: None,
        }
    }

    pub fn internal(message: impl fmt::Display) -> Self {
        Self {
            code: codes::INTERNAL,
            message: Cow::Owned(message.to_string()),
            description: None,
        }
    }

    pub fn service_not_found(service: impl fmt::Display) -> Self {
        Self {
            code: codes::NOT_FOUND,
            message: Cow::Owned(format!("Service '{}' not found. Check whether the deployment containing the service is registered.", service)),
            description: None,
        }
    }

    pub fn service_handler_not_found(
        service: impl fmt::Display,
        handler: impl fmt::Display,
    ) -> Self {
        Self {
            code: codes::NOT_FOUND,
            message: Cow::Owned(format!("Service handler '{}/{}' not found. Check whether you've registered the correct version of your service.", service, handler)),
            description: None,
        }
    }

    pub fn with_static_message(mut self, message: &'static str) -> InvocationError {
        self.message = Cow::Borrowed(message);
        self
    }

    pub fn with_message(mut self, message: impl fmt::Display) -> InvocationError {
        self.message = Cow::Owned(message.to_string());
        self
    }

    pub fn with_static_description(mut self, description: &'static str) -> InvocationError {
        self.description = Some(Cow::Borrowed(description));
        self
    }

    pub fn with_description(mut self, description: impl fmt::Display) -> InvocationError {
        self.description = Some(Cow::Owned(description.to_string()));
        self
    }

    pub fn code(&self) -> InvocationErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

impl From<anyhow::Error> for InvocationError {
    fn from(error: anyhow::Error) -> Self {
        InvocationError::internal(error)
    }
}

// -- Some known errors

pub const KILLED_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::KILLED, "killed");

// TODO: Once we want to distinguish server side cancellations from user code returning the
//  UserErrorCode::Cancelled, we need to add a new RestateErrorCode.
pub const CANCELED_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::ABORTED, "canceled");

pub const GONE_INVOCATION_ERROR: InvocationError = InvocationError::new_static(codes::GONE, "gone");

pub const NOT_FOUND_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::NOT_FOUND, "not found");

pub const ATTACH_NOT_SUPPORTED_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::BAD_REQUEST, "attach not supported for this invocation. You can attach only to invocations created with an idempotency key, or for workflow methods.");

pub const ALREADY_COMPLETED_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::CONFLICT, "promise was already completed");

pub const WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR: InvocationError =
    InvocationError::new_static(codes::CONFLICT, "the workflow method was already invoked");

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
