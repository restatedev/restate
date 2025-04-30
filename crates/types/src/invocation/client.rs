// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::errors::InvocationError;
use crate::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
use crate::invocation::{InvocationQuery, InvocationRequest, InvocationResponse, InvocationTarget};
use crate::journal_v2::Signal;
use crate::time::MillisSinceEpoch;
use bytes::Bytes;
use std::error::Error;
use std::fmt;

pub struct InvocationClientError {
    is_safe_to_retry: bool,
    inner: anyhow::Error,
}

impl InvocationClientError {
    pub fn new(inner: impl Into<anyhow::Error>, is_safe_to_retry: bool) -> Self {
        Self {
            is_safe_to_retry,
            inner: inner.into(),
        }
    }

    pub fn is_safe_to_retry(&self) -> bool {
        self.is_safe_to_retry
    }

    pub fn into_inner(self) -> anyhow::Error {
        self.inner
    }
}

impl fmt::Debug for InvocationClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl fmt::Display for InvocationClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl Error for InvocationClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.inner.source()
    }

    #[allow(deprecated)]
    fn description(&self) -> &str {
        self.inner.description()
    }

    #[allow(deprecated)]
    fn cause(&self) -> Option<&dyn Error> {
        self.inner.cause()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubmittedInvocationNotification {
    pub request_id: PartitionProcessorRpcRequestId,
    pub execution_time: Option<MillisSinceEpoch>,
    /// If true, this request_id created a "fresh invocation",
    /// otherwise the invocation was previously submitted.
    pub is_new_invocation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationOutput {
    pub request_id: PartitionProcessorRpcRequestId,
    pub invocation_id: Option<InvocationId>,
    pub completion_expiry_time: Option<MillisSinceEpoch>,
    pub response: InvocationOutputResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum InvocationOutputResponse {
    Success(InvocationTarget, Bytes),
    Failure(InvocationError),
}

#[derive(Debug, Clone)]
pub enum AttachInvocationResponse {
    NotFound,
    /// Returned when the invocation hasn't an idempotency key, nor it's a workflow run.
    NotSupported,
    Ready(InvocationOutput),
}

#[derive(Debug, Clone)]
pub enum GetInvocationOutputResponse {
    NotFound,
    /// The invocation was found, but it's still processing and a result is not ready yet.
    NotReady,
    /// Returned when the invocation hasn't an idempotency key, nor it's a workflow run.
    NotSupported,
    Ready(InvocationOutput),
}

/// This trait provides the functionalities to interact with Restate invocations.
pub trait InvocationClient {
    /// Append the invocation to the log, waiting for the submit notification emitted by the PartitionProcessor.
    fn append_invocation_and_wait_submit_notification(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: InvocationRequest,
    ) -> impl Future<Output = Result<SubmittedInvocationNotification, InvocationClientError>> + Send;

    /// Append the invocation and wait for its output.
    fn append_invocation_and_wait_output(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_request: InvocationRequest,
    ) -> impl Future<Output = Result<InvocationOutput, InvocationClientError>> + Send;

    fn attach_invocation(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> impl Future<Output = Result<AttachInvocationResponse, InvocationClientError>> + Send;

    fn get_invocation_output(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> impl Future<Output = Result<GetInvocationOutputResponse, InvocationClientError>> + Send;

    fn append_invocation_response(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_response: InvocationResponse,
    ) -> impl Future<Output = Result<(), InvocationClientError>> + Send;

    fn append_signal(
        &self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
        signal: Signal,
    ) -> impl Future<Output = Result<(), InvocationClientError>> + Send;
}
