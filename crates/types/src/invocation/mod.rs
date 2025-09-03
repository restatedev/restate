// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains all the core types representing a service invocation.

pub mod client;

use crate::errors::InvocationError;
use crate::identifiers::{
    DeploymentId, EntryIndex, IdempotencyId, InvocationId, PartitionKey,
    PartitionProcessorRpcRequestId, ServiceId, SubscriptionId, WithInvocationId, WithPartitionKey,
};
use crate::journal_v2::{CompletionId, GetInvocationOutputResult, Signal};
use crate::time::MillisSinceEpoch;
use crate::{GenerationalNodeId, RestateVersion};

use bytes::Bytes;
use bytestring::ByteString;
use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceState};
use serde_with::{DisplayFromStr, FromInto, serde_as};
use std::borrow::Cow;
use std::hash::Hash;
use std::ops::Deref;
use std::str::FromStr;
use std::time::Duration;
use std::{cmp, fmt};

// Re-exporting opentelemetry [`TraceId`] to avoid having to import opentelemetry in all crates.
pub use opentelemetry::trace::TraceId;

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ServiceType {
    Service,
    VirtualObject,
    Workflow,
}

impl ServiceType {
    pub fn is_keyed(&self) -> bool {
        matches!(self, ServiceType::VirtualObject | ServiceType::Workflow)
    }

    pub fn has_state(&self) -> bool {
        self.is_keyed()
    }
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(
    Eq, Hash, PartialEq, Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum VirtualObjectHandlerType {
    #[default]
    Exclusive,
    Shared,
}

impl fmt::Display for VirtualObjectHandlerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(
    Eq, Hash, PartialEq, Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum WorkflowHandlerType {
    #[default]
    Workflow,
    Shared,
}

impl fmt::Display for WorkflowHandlerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum InvocationTargetType {
    Service,
    VirtualObject(VirtualObjectHandlerType),
    Workflow(WorkflowHandlerType),
}

impl InvocationTargetType {
    pub fn is_keyed(&self) -> bool {
        matches!(
            self,
            InvocationTargetType::VirtualObject(_) | InvocationTargetType::Workflow(_)
        )
    }

    pub fn can_read_state(&self) -> bool {
        self.is_keyed()
    }

    pub fn can_write_state(&self) -> bool {
        matches!(
            self,
            InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
                | InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        )
    }
}

impl fmt::Display for InvocationTargetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<InvocationTargetType> for ServiceType {
    fn from(value: InvocationTargetType) -> Self {
        match value {
            InvocationTargetType::Service => ServiceType::Service,
            InvocationTargetType::VirtualObject(_) => ServiceType::VirtualObject,
            InvocationTargetType::Workflow(_) => ServiceType::Workflow,
        }
    }
}

#[derive(Debug, derive_more::Display)]
/// Short is used to create a short [`Display`] implementation
/// for InvocationTarget. it's mainly use for tracing purposes
pub enum Short<'a> {
    #[display("{name}/{{key}}/{handler}")]
    Keyed { name: &'a str, handler: &'a str },
    #[display("{name}/{handler}")]
    UnKeyed { name: &'a str, handler: &'a str },
}

#[derive(Eq, Hash, PartialEq, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum InvocationTarget {
    Service {
        name: ByteString,
        handler: ByteString,
    },
    VirtualObject {
        name: ByteString,
        key: ByteString,
        handler: ByteString,
        handler_ty: VirtualObjectHandlerType,
    },
    Workflow {
        name: ByteString,
        key: ByteString,
        handler: ByteString,
        handler_ty: WorkflowHandlerType,
    },
}

impl InvocationTarget {
    pub fn service(name: impl Into<ByteString>, handler: impl Into<ByteString>) -> Self {
        Self::Service {
            name: name.into(),
            handler: handler.into(),
        }
    }

    pub fn short(&self) -> Short<'_> {
        match self {
            Self::Service { name, handler } => Short::UnKeyed { name, handler },
            Self::VirtualObject { name, handler, .. } | Self::Workflow { name, handler, .. } => {
                Short::Keyed { name, handler }
            }
        }
    }

    pub fn virtual_object(
        name: impl Into<ByteString>,
        key: impl Into<ByteString>,
        handler: impl Into<ByteString>,
        handler_ty: VirtualObjectHandlerType,
    ) -> Self {
        Self::VirtualObject {
            name: name.into(),
            key: key.into(),
            handler: handler.into(),
            handler_ty,
        }
    }

    pub fn workflow(
        name: impl Into<ByteString>,
        key: impl Into<ByteString>,
        handler: impl Into<ByteString>,
        handler_ty: WorkflowHandlerType,
    ) -> Self {
        Self::Workflow {
            name: name.into(),
            key: key.into(),
            handler: handler.into(),
            handler_ty,
        }
    }

    pub fn service_name(&self) -> &ByteString {
        match self {
            InvocationTarget::Service { name, .. } => name,
            InvocationTarget::VirtualObject { name, .. } => name,
            InvocationTarget::Workflow { name, .. } => name,
        }
    }

    pub fn key(&self) -> Option<&ByteString> {
        match self {
            InvocationTarget::Service { .. } => None,
            InvocationTarget::VirtualObject { key, .. } => Some(key),
            InvocationTarget::Workflow { key, .. } => Some(key),
        }
    }

    pub fn handler_name(&self) -> &ByteString {
        match self {
            InvocationTarget::Service { handler, .. } => handler,
            InvocationTarget::VirtualObject { handler, .. } => handler,
            InvocationTarget::Workflow { handler, .. } => handler,
        }
    }

    pub fn as_keyed_service_id(&self) -> Option<ServiceId> {
        match self {
            InvocationTarget::Service { .. } => None,
            InvocationTarget::VirtualObject { name, key, .. } => {
                Some(ServiceId::new(name.clone(), key.clone()))
            }
            InvocationTarget::Workflow { name, key, .. } => {
                Some(ServiceId::new(name.clone(), key.clone()))
            }
        }
    }

    pub fn service_ty(&self) -> ServiceType {
        match self {
            InvocationTarget::Service { .. } => ServiceType::Service,
            InvocationTarget::VirtualObject { .. } => ServiceType::VirtualObject,
            InvocationTarget::Workflow { .. } => ServiceType::Workflow,
        }
    }

    pub fn invocation_target_ty(&self) -> InvocationTargetType {
        match self {
            InvocationTarget::Service { .. } => InvocationTargetType::Service,
            InvocationTarget::VirtualObject { handler_ty, .. } => {
                InvocationTargetType::VirtualObject(*handler_ty)
            }
            InvocationTarget::Workflow { handler_ty, .. } => {
                InvocationTargetType::Workflow(*handler_ty)
            }
        }
    }
}

impl fmt::Display for InvocationTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.service_name())?;
        if let Some(key) = self.key() {
            write!(f, "{key}/")?;
        }
        write!(f, "{}", self.handler_name())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationRetention {
    /// Retention duration of the completed status. If zero, the completed status is not retained, and invocation won't be deduplicated.
    pub completion_retention: Duration,
    /// Retention duration of the journal. If zero, the journal is not retained.
    pub journal_retention: Duration,
}

impl InvocationRetention {
    pub fn none() -> Self {
        Self {
            completion_retention: Duration::ZERO,
            journal_retention: Duration::ZERO,
        }
    }
}

impl Default for InvocationRetention {
    fn default() -> Self {
        Self::none()
    }
}

#[serde_as]
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationRequestHeader {
    pub id: InvocationId,
    pub target: InvocationTarget,
    pub headers: Vec<Header>,
    pub span_context: ServiceInvocationSpanContext,

    /// Key to use for idempotent request. If none, this request is not idempotent, or it's a workflow call. See [`InvocationRequestHeader::is_idempotent`].
    pub idempotency_key: Option<ByteString>,

    /// Time when the request should be executed. If none, it's executed immediately.
    pub execution_time: Option<MillisSinceEpoch>,

    /// Retention duration of the completed status.
    /// If zero, the completed status is not retained.
    #[serde(default)]
    #[serde_as(deserialize_as = "serde_with::DefaultOnNull")]
    completion_retention_duration: Duration,
    /// Retention duration of the journal.
    /// If zero, the journal is not retained.
    /// If `completion_retention_duration < journal_retention_duration`, then completion retention is used as journal retention.
    #[serde(default, skip_serializing_if = "Duration::is_zero")]
    journal_retention_duration: Duration,
}

impl InvocationRequestHeader {
    pub fn initialize(id: InvocationId, target: InvocationTarget) -> Self {
        Self {
            id,
            target,
            headers: vec![],
            span_context: ServiceInvocationSpanContext::empty(),
            idempotency_key: None,
            execution_time: None,
            completion_retention_duration: Duration::ZERO,
            journal_retention_duration: Duration::ZERO,
        }
    }

    pub fn with_related_span(&mut self, span_relation: SpanRelation) {
        self.span_context = ServiceInvocationSpanContext::start(&self.id, span_relation);
    }

    pub fn with_headers(&mut self, headers: Vec<Header>) {
        self.headers.extend(headers);
    }

    pub fn with_retention(&mut self, invocation_retention: InvocationRetention) {
        self.completion_retention_duration = invocation_retention.completion_retention;
        self.journal_retention_duration = invocation_retention.journal_retention;
    }

    /// Invocations are idempotent if they have an idempotency key specified or are of type workflow
    pub fn is_idempotent(&self) -> bool {
        self.idempotency_key.is_some() || matches!(self.target.service_ty(), ServiceType::Workflow)
    }

    pub fn completion_retention_duration(&self) -> Duration {
        self.completion_retention_duration
    }

    pub fn journal_retention_duration(&self) -> Duration {
        self.journal_retention_duration
    }
}

impl WithInvocationId for InvocationRequestHeader {
    fn invocation_id(&self) -> InvocationId {
        self.id
    }
}

/// Struct representing an invocation request.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationRequest {
    pub header: InvocationRequestHeader,
    pub body: Bytes,
}

impl InvocationRequest {
    pub fn new(header: InvocationRequestHeader, body: Bytes) -> Self {
        Self { header, body }
    }

    /// Invocations are idempotent if they have an idempotency key specified or are of type workflow
    pub fn is_idempotent(&self) -> bool {
        self.header.is_idempotent()
    }
}

impl WithInvocationId for InvocationRequest {
    fn invocation_id(&self) -> InvocationId {
        self.header.invocation_id()
    }
}

/// Struct representing an invocation to a service. This struct is processed by Restate to execute the invocation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    from = "serde_hacks::ServiceInvocation",
    into = "serde_hacks::ServiceInvocation"
)]
pub struct ServiceInvocation {
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub argument: Bytes,
    pub source: Source,
    pub span_context: ServiceInvocationSpanContext,
    pub headers: Vec<Header>,

    /// Time when the request should be executed
    pub execution_time: Option<MillisSinceEpoch>,

    /// Retention duration of the completed status. If zero, the completed status is not retained, and invocation won't be deduplicated.
    pub completion_retention_duration: Duration,
    /// Retention duration of the journal. If zero, the journal is not retained. This should be smaller than `completion_retention_duration`.
    pub journal_retention_duration: Duration,

    pub idempotency_key: Option<ByteString>,

    // Where to send the response, if any
    pub response_sink: Option<ServiceInvocationResponseSink>,
    /// Where to send the submit notification, if any.
    /// The submit notification is sent back both when this invocation request attached to an existing invocation,
    /// or when this request started a fresh invocation.
    pub submit_notification_sink: Option<SubmitNotificationSink>,

    /// Restate version at the moment of the invocation creation.
    pub restate_version: RestateVersion,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    from = "serde_hacks::SubmitNotificationSink",
    into = "serde_hacks::SubmitNotificationSink"
)]
pub enum SubmitNotificationSink {
    Ingress {
        request_id: PartitionProcessorRpcRequestId,
    },
}

impl ServiceInvocation {
    pub fn from_request(request: InvocationRequest, source: Source) -> Self {
        Self {
            invocation_id: request.header.id,
            invocation_target: request.header.target,
            argument: request.body,
            source,
            span_context: request.header.span_context,
            headers: request.header.headers,
            execution_time: request.header.execution_time,
            completion_retention_duration: request.header.completion_retention_duration,
            journal_retention_duration: cmp::min(
                request.header.journal_retention_duration,
                request.header.completion_retention_duration,
            ),
            idempotency_key: request.header.idempotency_key,
            response_sink: None,
            submit_notification_sink: None,
            restate_version: RestateVersion::current(),
        }
    }

    pub fn initialize(
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        source: Source,
    ) -> Self {
        Self {
            invocation_id,
            invocation_target,
            argument: Default::default(),
            source,
            response_sink: None,
            span_context: ServiceInvocationSpanContext::empty(),
            headers: vec![],
            execution_time: None,
            completion_retention_duration: Duration::ZERO,
            journal_retention_duration: Duration::ZERO,
            idempotency_key: None,
            submit_notification_sink: None,
            restate_version: RestateVersion::current(),
        }
    }

    pub fn with_related_span(&mut self, span_relation: SpanRelation) {
        self.span_context = ServiceInvocationSpanContext::start(&self.invocation_id, span_relation);
    }

    pub fn compute_idempotency_id(&self) -> Option<IdempotencyId> {
        self.idempotency_key
            .as_ref()
            .map(|k| IdempotencyId::combine(self.invocation_id, &self.invocation_target, k.clone()))
    }

    /// Invocations are idempotent if they have an idempotency key specified or are of type workflow
    pub fn is_idempotent(&self) -> bool {
        self.idempotency_key.is_some()
            || matches!(self.invocation_target.service_ty(), ServiceType::Workflow)
    }

    pub fn with_retention(&mut self, invocation_retention: InvocationRetention) {
        self.completion_retention_duration = invocation_retention.completion_retention;
        self.journal_retention_duration = invocation_retention.journal_retention;
    }
}

impl WithPartitionKey for ServiceInvocation {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_id.partition_key()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationInput {
    pub argument: Bytes,
    pub headers: Vec<Header>,
}

/// Target of a completion sent from another Partition Processor.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash, bilrost::Message,
)]
pub struct JournalCompletionTarget {
    #[bilrost(1)]
    pub caller_id: InvocationId,
    #[bilrost(2)]
    pub caller_completion_id: CompletionId,
    #[bilrost(3)]
    pub caller_invocation_epoch: InvocationEpoch,
}

impl JournalCompletionTarget {
    /// MUST be used only for v3 journals/invocations, which have no concept of epoch
    pub fn for_v3_completions(caller_id: InvocationId, caller_entry_index: EntryIndex) -> Self {
        Self {
            caller_id,
            caller_completion_id: caller_entry_index,
            caller_invocation_epoch: 0,
        }
    }

    pub const fn from_parts(
        caller_id: InvocationId,
        caller_completion_id: CompletionId,
        caller_invocation_epoch: InvocationEpoch,
    ) -> Self {
        Self {
            caller_id,
            caller_completion_id,
            caller_invocation_epoch,
        }
    }
}

impl WithInvocationId for JournalCompletionTarget {
    fn invocation_id(&self) -> InvocationId {
        self.caller_id
    }
}

/// Representing a response for a caller
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    from = "serde_hacks::InvocationResponse",
    into = "serde_hacks::InvocationResponse"
)]
pub struct InvocationResponse {
    pub target: JournalCompletionTarget,
    pub result: ResponseResult,
}

impl WithInvocationId for InvocationResponse {
    fn invocation_id(&self) -> InvocationId {
        self.target.invocation_id()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResponseResult {
    Success(Bytes),
    Failure(InvocationError),
}

impl From<Result<Bytes, InvocationError>> for ResponseResult {
    fn from(value: Result<Bytes, InvocationError>) -> Self {
        match value {
            Ok(v) => ResponseResult::Success(v),
            Err(e) => ResponseResult::Failure(e),
        }
    }
}

impl From<InvocationError> for ResponseResult {
    fn from(e: InvocationError) -> Self {
        ResponseResult::Failure(e)
    }
}

impl From<&InvocationError> for ResponseResult {
    fn from(e: &InvocationError) -> Self {
        ResponseResult::Failure(e.clone())
    }
}

// Represents a response to get invocation output
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
#[serde(
    from = "serde_hacks::GetInvocationOutputResponse",
    into = "serde_hacks::GetInvocationOutputResponse"
)]
pub struct GetInvocationOutputResponse {
    #[bilrost(1)]
    pub target: JournalCompletionTarget,
    #[bilrost(oneof(2, 3))]
    pub result: GetInvocationOutputResult,
}

impl WithInvocationId for GetInvocationOutputResponse {
    fn invocation_id(&self) -> InvocationId {
        self.target.invocation_id()
    }
}

/// Definition of the sink where to send the result of a service invocation.
#[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
#[serde(
    from = "serde_hacks::ServiceInvocationResponseSink",
    into = "serde_hacks::ServiceInvocationResponseSink"
)]
pub enum ServiceInvocationResponseSink {
    /// The invocation has been created by a partition processor and is expecting a response.
    PartitionProcessor(JournalCompletionTarget),
    /// The invocation has been generated by a request received at an ingress, and the client is expecting a response back.
    Ingress {
        request_id: PartitionProcessorRpcRequestId,
    },
}

impl ServiceInvocationResponseSink {
    pub fn partition_processor(
        caller_id: InvocationId,
        caller_completion_id: CompletionId,
        caller_invocation_epoch: InvocationEpoch,
    ) -> Self {
        Self::PartitionProcessor(JournalCompletionTarget {
            caller_id,
            caller_completion_id,
            caller_invocation_epoch,
        })
    }

    pub fn ingress(request_id: PartitionProcessorRpcRequestId) -> Self {
        Self::Ingress { request_id }
    }
}

/// Source of an invocation
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Source {
    Ingress(PartitionProcessorRpcRequestId),
    Subscription(SubscriptionId),
    Service(InvocationId, InvocationTarget),
    RestartAsNew(InvocationId),
    /// Internal calls for the non-deterministic built-in services
    Internal,
}

impl Source {
    pub fn ingress(request_id: PartitionProcessorRpcRequestId) -> Self {
        Self::Ingress(request_id)
    }
}

/// This struct contains the relevant span information for a [`ServiceInvocation`].
/// It can be used to create related spans, such as child spans,
/// using [`ServiceInvocationSpanContext::as_linked`] or [`ServiceInvocationSpanContext::as_parent`].
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceInvocationSpanContext {
    span_context: SpanContextDef,
    cause: Option<SpanRelationCause>,
}

impl ServiceInvocationSpanContext {
    pub fn new(span_context: SpanContextDef, cause: Option<SpanRelationCause>) -> Self {
        Self {
            span_context,
            cause,
        }
    }

    pub fn empty() -> Self {
        Self {
            span_context: SpanContextDef::empty_context(),
            cause: None,
        }
    }

    /// Create a [`SpanContext`] for this invocation, a [`Span`] which will be created
    /// when the invocation completes.
    ///
    /// This function is **deterministic**.
    pub fn start(
        invocation_id: &InvocationId,
        related_span: SpanRelation,
    ) -> ServiceInvocationSpanContext {
        if !related_span.is_sampled() {
            // don't waste any time or storage space on unsampled traces
            // sampling based on parent is default otel behaviour; we do the same for the
            // non-parent background invoke relationship
            return ServiceInvocationSpanContext::empty();
        }

        let (cause, new_span_context) = match related_span {
            SpanRelation::Linked(linked_span_context) => {
                // create a span context with a new trace that will be used for any actions as part of the background invocation
                // a span will be emitted using these details when its finished (so we know how long the invocation took)
                let new_span_context = SpanContextDef::new(
                    // use invocation id as the new trace id; this allows you to follow cause -> new trace in jaeger
                    // trace ids are 128 bits and 'worldwide unique'
                    invocation_id.invocation_uuid().into(),
                    // use part of the invocation id as the new span id; this is 64 bits and best-effort 'globally unique'
                    invocation_id.invocation_uuid().into(),
                    // use sampling decision of the causing trace; this is NOT default otel behaviour but
                    // is useful for users
                    linked_span_context.trace_flags(),
                    // this would never be set to true for a span created in this binary
                    false,
                    TraceStateDef::default(),
                );

                let cause = SpanRelationCause::Linked(
                    linked_span_context.trace_id(),
                    linked_span_context.span_id(),
                );

                (Some(cause), new_span_context)
            }
            SpanRelation::Parent(parent_span_context) => {
                // create a span context as part of the existing trace, which will be used for any actions
                // of the invocation. a span will be emitted with these details when its finished
                let cause = SpanRelationCause::Parent(parent_span_context.span_id());
                let new_span_context = SpanContextDef::new(
                    // use parent trace id
                    parent_span_context.trace_id(),
                    // use part of the invocation id as the new span id
                    invocation_id.invocation_uuid().into(),
                    // use sampling decision of parent trace; this is default otel behaviour
                    parent_span_context.trace_flags(),
                    false,
                    parent_span_context.trace_state,
                );
                (Some(cause), new_span_context)
            }
            SpanRelation::None => {
                // we would only expect this in tests as there should always be either another invocation
                // or an ingress task leading to the invocation

                // create a span context with a new trace
                let new_span_context = SpanContextDef::new(
                    // use invocation id as the new trace id and span id
                    invocation_id.invocation_uuid().into(),
                    invocation_id.invocation_uuid().into(),
                    // we don't have the means to actually sample here; just hardcode a sampled trace
                    // as this should only happen in tests anyway
                    TraceFlags::SAMPLED,
                    false,
                    TraceStateDef::default(),
                );
                (None, new_span_context)
            }
        };

        ServiceInvocationSpanContext {
            span_context: new_span_context,
            cause,
        }
    }

    pub fn causing_span_relation(&self) -> SpanRelation {
        match self.cause {
            None => SpanRelation::None,
            Some(SpanRelationCause::Parent(span_id)) => {
                SpanRelation::Parent(SpanContextDef::new(
                    // in invoke case, trace id of cause matches that of child
                    self.span_context.trace_id(),
                    // use stored span id
                    span_id,
                    // use child trace flags as the cause trace flags; when this is set as parent
                    // the flags will be set on the child
                    self.span_context.trace_flags(),
                    // this will be ignored; is_remote is not propagated
                    false,
                    // use child trace state as the cause trace state; when this is set as parent
                    // the state will be set on the child
                    self.span_context.trace_state().clone(),
                ))
            }
            Some(SpanRelationCause::Linked(trace_id, span_id)) => {
                SpanRelation::Linked(SpanContextDef::new(
                    // use stored trace id
                    trace_id,
                    // use stored span id
                    span_id,
                    // this will be ignored; trace flags are not propagated to links
                    self.span_context.trace_flags(),
                    // this will be ignored; is_remote is not propagated
                    false,
                    // this will be ignored; trace state is not propagated to links
                    TraceStateDef::default(),
                ))
            }
        }
    }

    pub fn span_context(&self) -> &SpanContextDef {
        &self.span_context
    }

    pub fn into_span_context_and_cause(self) -> (SpanContextDef, Option<SpanRelationCause>) {
        (self.span_context, self.cause)
    }

    pub fn span_cause(&self) -> Option<&SpanRelationCause> {
        self.cause.as_ref()
    }

    pub fn as_linked(&self) -> SpanRelation {
        SpanRelation::Linked(self.span_context.clone())
    }

    pub fn as_parent(&self) -> SpanRelation {
        SpanRelation::Parent(self.span_context.clone())
    }

    pub fn is_sampled(&self) -> bool {
        self.span_context.trace_flags.is_sampled()
    }

    pub fn trace_id(&self) -> TraceId {
        self.span_context.trace_id
    }
}

impl Default for ServiceInvocationSpanContext {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<ServiceInvocationSpanContext> for SpanContextDef {
    fn from(value: ServiceInvocationSpanContext) -> Self {
        value.span_context
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Header {
    pub name: ByteString,
    pub value: ByteString,
}

impl Header {
    pub fn new(name: impl Into<ByteString>, value: impl Into<ByteString>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.value)
    }
}

/// Span relation cause, used to propagate tracing contexts.
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub enum SpanRelationCause {
    Parent(#[serde_as(as = "FromInto<SpanIdDef>")] SpanId),
    Linked(
        #[serde_as(as = "FromInto<TraceIdDef>")] TraceId,
        #[serde_as(as = "FromInto<SpanIdDef>")] SpanId,
    ),
}

#[derive(Default, Debug, Clone)]
pub enum SpanRelation {
    #[default]
    None,
    Parent(SpanContextDef),
    Linked(SpanContextDef),
}

impl SpanRelation {
    pub fn parent(ctx: impl Into<SpanContextDef>) -> Self {
        Self::Parent(ctx.into())
    }

    pub fn linked(ctx: impl Into<SpanContextDef>) -> Self {
        Self::Linked(ctx.into())
    }

    fn is_sampled(&self) -> bool {
        match self {
            SpanRelation::None => false,
            SpanRelation::Parent(span_context) => span_context.is_sampled(),
            SpanRelation::Linked(span_context) => span_context.is_sampled(),
        }
    }
}

#[derive(
    Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize, bilrost::Message,
)]
pub struct IngressInvocationResponseSink {
    #[bilrost(1)]
    pub request_id: PartitionProcessorRpcRequestId,
}

/// Definition of the sink where to send the result of a termination, or of purge.
#[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
pub enum InvocationMutationResponseSink {
    /// The invocation has been generated by a request received at an ingress, and the client is expecting a response back.
    Ingress(IngressInvocationResponseSink),
}

/// Message to terminate an invocation.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InvocationTermination {
    pub invocation_id: InvocationId,
    pub flavor: TerminationFlavor,
    #[serde(default)]
    pub response_sink: Option<InvocationMutationResponseSink>,
}

/// Flavor of the termination. Can be kill (hard stop) or graceful cancel.
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize, bilrost::Enumeration,
)]
pub enum TerminationFlavor {
    /// hard termination, no clean up
    Kill = 0,
    /// graceful termination allowing the invocation to clean up
    Cancel = 1,
}

/// Message to purge an invocation.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PurgeInvocationRequest {
    pub invocation_id: InvocationId,
    #[serde(default)]
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl WithInvocationId for PurgeInvocationRequest {
    fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }
}

/// Message to resume an invocation.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ResumeInvocationRequest {
    pub invocation_id: InvocationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_pinned_deployment_id: Option<DeploymentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl WithInvocationId for ResumeInvocationRequest {
    fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }
}

// We use this struct instead of SpanContext as it is serialisable and allows us to use TraceStateDef
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct SpanContextDef {
    #[serde_as(as = "FromInto<TraceIdDef>")]
    trace_id: TraceId,
    #[serde_as(as = "FromInto<SpanIdDef>")]
    span_id: SpanId,
    #[serde_as(as = "FromInto<TraceFlagsDef>")]
    trace_flags: TraceFlags,
    is_remote: bool,
    #[serde_as(as = "DisplayFromStr")]
    trace_state: TraceStateDef,
}

impl SpanContextDef {
    pub fn new(
        trace_id: TraceId,
        span_id: SpanId,
        trace_flags: TraceFlags,
        is_remote: bool,
        trace_state: TraceStateDef,
    ) -> Self {
        Self {
            trace_id,
            span_id,
            trace_flags,
            is_remote,
            trace_state,
        }
    }

    const fn empty_context() -> Self {
        Self {
            trace_id: TraceId::INVALID,
            span_id: SpanId::INVALID,
            trace_flags: TraceFlags::NOT_SAMPLED,
            is_remote: false,
            trace_state: TraceStateDef::new_header(String::new()),
        }
    }
    pub fn trace_id(&self) -> TraceId {
        self.trace_id
    }

    pub fn span_id(&self) -> SpanId {
        self.span_id
    }

    pub fn trace_flags(&self) -> TraceFlags {
        self.trace_flags
    }

    pub fn is_valid(&self) -> bool {
        self.trace_id != TraceId::INVALID && self.span_id != SpanId::INVALID
    }

    pub fn is_remote(&self) -> bool {
        self.is_remote
    }

    pub fn trace_state(&self) -> &TraceStateDef {
        &self.trace_state
    }

    pub fn into_trace_state(self) -> TraceStateDef {
        self.trace_state
    }

    fn is_sampled(&self) -> bool {
        self.trace_flags().is_sampled()
    }
}

// Provide a conversion to construct the remote type.
impl From<SpanContextDef> for SpanContext {
    fn from(def: SpanContextDef) -> Self {
        SpanContext::new(
            def.trace_id,
            def.span_id,
            def.trace_flags,
            def.is_remote,
            def.trace_state.into_trace_state().unwrap_or_default(),
        )
    }
}

impl From<SpanContext> for SpanContextDef {
    fn from(ctx: SpanContext) -> Self {
        Self {
            trace_id: ctx.trace_id(),
            span_id: ctx.span_id(),
            trace_flags: ctx.trace_flags(),
            is_remote: ctx.is_remote(),
            trace_state: TraceStateDef::new_trace_state(ctx.trace_state().clone()),
        }
    }
}

impl From<&SpanContext> for SpanContextDef {
    fn from(ctx: &SpanContext) -> Self {
        Self {
            trace_id: ctx.trace_id(),
            span_id: ctx.span_id(),
            trace_flags: ctx.trace_flags(),
            is_remote: ctx.is_remote(),
            trace_state: TraceStateDef::new_trace_state(ctx.trace_state().clone()),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SpanIdDef([u8; 8]);

impl From<SpanIdDef> for SpanId {
    fn from(def: SpanIdDef) -> Self {
        SpanId::from_bytes(def.0)
    }
}

impl From<SpanId> for SpanIdDef {
    fn from(def: SpanId) -> Self {
        SpanIdDef(def.to_bytes())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TraceIdDef([u8; 16]);

impl From<TraceIdDef> for TraceId {
    fn from(def: TraceIdDef) -> Self {
        TraceId::from_bytes(def.0)
    }
}

impl From<TraceId> for TraceIdDef {
    fn from(def: TraceId) -> Self {
        TraceIdDef(def.to_bytes())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TraceFlagsDef(u8);

impl From<TraceFlagsDef> for TraceFlags {
    fn from(def: TraceFlagsDef) -> Self {
        TraceFlags::new(def.0)
    }
}

impl From<TraceFlags> for TraceFlagsDef {
    fn from(tf: TraceFlags) -> Self {
        TraceFlagsDef(tf.to_u8())
    }
}

// TraceStateDef allows us to accept either header strings (from storage) or parsed TraceStates (from the otel libraries) and pass them on without being forced to parse/format
// It uses oncelock to ensure that the fairly expensive parse/format steps are only done once.
#[derive(Debug, Clone)]
pub enum TraceStateDef {
    TraceState(TraceState),
    Header(String),
}

pub struct InvalidTraceState;

impl TraceStateDef {
    pub const fn new_header(state: String) -> Self {
        Self::Header(state)
    }

    pub const fn new_trace_state(ts: TraceState) -> Self {
        Self::TraceState(ts)
    }

    pub fn into_header(self) -> String {
        match self {
            TraceStateDef::TraceState(ts) => ts.header(),
            TraceStateDef::Header(header) => header,
        }
    }

    pub fn header<'a>(&'a self) -> Cow<'a, str> {
        match self {
            TraceStateDef::TraceState(ts) => Cow::Owned(ts.header()),
            TraceStateDef::Header(header) => Cow::Borrowed(header),
        }
    }

    pub fn into_trace_state(self) -> Result<TraceState, opentelemetry::trace::TraceError> {
        match self {
            TraceStateDef::TraceState(ts) => Ok(ts),
            TraceStateDef::Header(header) => TraceState::from_str(&header),
        }
    }

    pub fn trace_state(&self) -> Result<TraceState, opentelemetry::trace::TraceError> {
        match self {
            TraceStateDef::TraceState(ts) => Ok(ts.clone()),
            TraceStateDef::Header(header) => TraceState::from_str(header),
        }
    }
}

impl Default for TraceStateDef {
    fn default() -> Self {
        Self::Header(String::default())
    }
}

impl std::fmt::Display for TraceStateDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.header())
    }
}

impl FromStr for TraceStateDef {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new_header(s.to_owned()))
    }
}

impl PartialEq for TraceStateDef {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::TraceState(l), Self::TraceState(r)) => l == r,
            (Self::Header(l), Self::Header(r)) => l == r,
            (Self::TraceState(l), Self::Header(r)) => &l.header() == r,
            (Self::Header(l), Self::TraceState(r)) => l == &r.header(),
        }
    }
}

impl Eq for TraceStateDef {}

/// Defines how to query the invocation.
/// This is used in some commands, such as [AttachInvocationRequest], to uniquely address an existing invocation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum InvocationQuery {
    Invocation(InvocationId),
    /// Query a workflow handler invocation.
    /// There can be at most one running workflow method for the given ServiceId. This uniqueness constraint is guaranteed by the PP state machine.
    Workflow(ServiceId),
    /// Query an idempotency id.
    IdempotencyId(IdempotencyId),
}

impl InvocationQuery {
    pub fn to_invocation_id(&self) -> InvocationId {
        // The business logic of this function is based on the deterministic invocation id generation.
        // For more info, please look at InvocationUuid::generate where the magic stuff happens.
        match self {
            InvocationQuery::Invocation(iid) => *iid,
            InvocationQuery::Workflow(wfid) => InvocationId::generate(
                &InvocationTarget::Workflow {
                    name: wfid.service_name.clone(),
                    key: wfid.key.clone(),
                    // Doesn't matter
                    handler: Default::default(),
                    // Must be the workflow handler type
                    handler_ty: WorkflowHandlerType::Workflow,
                },
                None,
            ),
            InvocationQuery::IdempotencyId(IdempotencyId {
                service_name,
                service_key,
                service_handler,
                idempotency_key,
                ..
            }) => {
                let target = match service_key {
                    None => {
                        InvocationTarget::service(service_name.clone(), service_handler.clone())
                    }
                    Some(k) => InvocationTarget::virtual_object(
                        service_name.clone(),
                        k.clone(),
                        service_handler.clone(),
                        // Doesn't really matter
                        VirtualObjectHandlerType::Exclusive,
                    ),
                };
                InvocationId::generate(&target, Some(idempotency_key.deref()))
            }
        }
    }
}

impl WithPartitionKey for InvocationQuery {
    fn partition_key(&self) -> PartitionKey {
        match self {
            InvocationQuery::Invocation(iid) => iid.partition_key(),
            InvocationQuery::Workflow(sid) => sid.partition_key(),
            InvocationQuery::IdempotencyId(iid) => iid.partition_key(),
        }
    }
}

/// Represents an "attach request" to an existing invocation
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AttachInvocationRequest {
    pub invocation_query: InvocationQuery,
    /// If the invocation is still in-flight when the command is processed,
    /// this command will block if this flag is true.
    /// Otherwise, the failure [crate::errors::NOT_READY_INVOCATION_ERROR] is sent instead as soon as the command is processed.
    #[serde(default = "restate_serde_util::default::bool::<true>")]
    pub block_on_inflight: bool,
    pub response_sink: ServiceInvocationResponseSink,
}

impl WithPartitionKey for AttachInvocationRequest {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_query.partition_key()
    }
}

/// Represents a request to notify a signal to an invocation
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NotifySignalRequest {
    pub invocation_id: InvocationId,
    pub signal: Signal,
}

impl WithInvocationId for NotifySignalRequest {
    fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }
}

/// The invocation epoch represents the restarts count of the invocation, as seen from the Partition processor.
pub type InvocationEpoch = u32;

mod serde_hacks {
    //! Module where we hide all the hacks to make back-compat working!

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub(super) struct ServiceInvocation {
        pub invocation_id: InvocationId,
        pub invocation_target: InvocationTarget,
        pub argument: Bytes,
        pub source: Source,
        pub span_context: ServiceInvocationSpanContext,
        pub headers: Vec<Header>,
        pub execution_time: Option<MillisSinceEpoch>,
        pub completion_retention_duration: Option<Duration>,
        #[serde(default, skip_serializing_if = "Duration::is_zero")]
        pub journal_retention_duration: Duration,
        pub idempotency_key: Option<ByteString>,
        pub response_sink: Option<ServiceInvocationResponseSink>,
        pub submit_notification_sink: Option<SubmitNotificationSink>,

        #[serde(default = "RestateVersion::unknown")]
        pub restate_version: RestateVersion,

        // TODO(slinkydeveloper) this field is here because serde doesn't like much when I change the shape of an enum variant from empty to tuple/named fields
        pub source_ingress_rpc_id: Option<PartitionProcessorRpcRequestId>,
    }

    #[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
    pub(super) enum Source {
        Ingress,
        Subscription(SubscriptionId),
        Service(InvocationId, InvocationTarget),
        RestartAsNew(InvocationId),
        /// Internal calls for the non-deterministic built-in services
        Internal,
    }

    impl From<ServiceInvocation> for super::ServiceInvocation {
        fn from(
            ServiceInvocation {
                invocation_id,
                invocation_target,
                argument,
                source,
                span_context,
                headers,
                execution_time,
                completion_retention_duration,
                journal_retention_duration,
                idempotency_key,
                response_sink,
                submit_notification_sink,
                restate_version,
                source_ingress_rpc_id,
            }: ServiceInvocation,
        ) -> Self {
            Self {
                invocation_id,
                invocation_target,
                argument,
                span_context,
                headers,
                execution_time,
                completion_retention_duration: completion_retention_duration.unwrap_or_default(),
                journal_retention_duration,
                idempotency_key,
                response_sink: response_sink.map(Into::into),
                submit_notification_sink: submit_notification_sink.map(Into::into),
                source: match source {
                    Source::Ingress => {
                        super::Source::Ingress(source_ingress_rpc_id.unwrap_or_default())
                    }
                    Source::Subscription(sid) => super::Source::Subscription(sid),
                    Source::Service(id, target) => super::Source::Service(id, target),
                    Source::RestartAsNew(id) => super::Source::RestartAsNew(id),
                    Source::Internal => super::Source::Internal,
                },
                restate_version,
            }
        }
    }

    impl From<super::ServiceInvocation> for ServiceInvocation {
        fn from(
            super::ServiceInvocation {
                invocation_id,
                invocation_target,
                argument,
                source,
                span_context,
                headers,
                execution_time,
                completion_retention_duration,
                journal_retention_duration,
                idempotency_key,
                response_sink,
                submit_notification_sink,
                restate_version,
            }: super::ServiceInvocation,
        ) -> Self {
            let source_ingress_rpc_id = if let super::Source::Ingress(rpc_id) = &source {
                Some(*rpc_id)
            } else {
                None
            };

            Self {
                invocation_id,
                invocation_target,
                argument,
                span_context,
                headers,
                execution_time,
                completion_retention_duration: Some(completion_retention_duration),
                journal_retention_duration,
                idempotency_key,
                response_sink: response_sink.map(Into::into),
                submit_notification_sink: submit_notification_sink.map(Into::into),
                restate_version,
                source_ingress_rpc_id,
                source: match source {
                    super::Source::Ingress(_) => Source::Ingress,
                    super::Source::Subscription(subid) => Source::Subscription(subid),
                    super::Source::Service(id, target) => Source::Service(id, target),
                    super::Source::Internal => Source::Internal,
                    super::Source::RestartAsNew(id) => Source::RestartAsNew(id),
                },
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub(super) enum SubmitNotificationSink {
        Ingress {
            // TODO(slinkydeveloper) remove this field in 1.4, it's unused, it's here only to preserve back-compat
            #[serde(default, skip_serializing_if = "Option::is_none")]
            node_id: Option<GenerationalNodeId>,
            request_id: PartitionProcessorRpcRequestId,
        },
    }

    impl From<SubmitNotificationSink> for super::SubmitNotificationSink {
        fn from(value: SubmitNotificationSink) -> Self {
            match value {
                SubmitNotificationSink::Ingress { request_id, .. } => Self::Ingress { request_id },
            }
        }
    }

    impl From<super::SubmitNotificationSink> for SubmitNotificationSink {
        fn from(value: super::SubmitNotificationSink) -> Self {
            match value {
                super::SubmitNotificationSink::Ingress { request_id } => Self::Ingress {
                    // TODO(slinkydeveloper) stop writing this field in 1.3, it's unused, it's here only to preserve back-compat
                    node_id: Some(GenerationalNodeId::new(1, 1)),
                    request_id,
                },
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub(super) struct InvocationResponse {
        pub id: InvocationId,
        pub entry_index: EntryIndex,
        #[serde(default, skip_serializing_if = "num_traits::Zero::is_zero")]
        pub caller_invocation_epoch: InvocationEpoch,
        pub result: ResponseResult,
    }

    impl From<InvocationResponse> for super::InvocationResponse {
        fn from(value: InvocationResponse) -> Self {
            Self {
                target: JournalCompletionTarget {
                    caller_id: value.id,
                    caller_completion_id: value.entry_index,
                    caller_invocation_epoch: value.caller_invocation_epoch,
                },
                result: value.result,
            }
        }
    }

    impl From<super::InvocationResponse> for InvocationResponse {
        fn from(value: super::InvocationResponse) -> Self {
            Self {
                id: value.target.caller_id,
                entry_index: value.target.caller_completion_id,
                caller_invocation_epoch: value.target.caller_invocation_epoch,
                result: value.result,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    pub(super) struct GetInvocationOutputResponse {
        pub caller_id: InvocationId,
        pub completion_id: CompletionId,
        #[serde(default, skip_serializing_if = "num_traits::Zero::is_zero")]
        pub caller_invocation_epoch: InvocationEpoch,
        pub result: GetInvocationOutputResult,
    }

    impl From<GetInvocationOutputResponse> for super::GetInvocationOutputResponse {
        fn from(value: GetInvocationOutputResponse) -> Self {
            Self {
                target: JournalCompletionTarget {
                    caller_id: value.caller_id,
                    caller_completion_id: value.completion_id,
                    caller_invocation_epoch: value.caller_invocation_epoch,
                },
                result: value.result,
            }
        }
    }

    impl From<super::GetInvocationOutputResponse> for GetInvocationOutputResponse {
        fn from(value: super::GetInvocationOutputResponse) -> Self {
            Self {
                caller_id: value.target.caller_id,
                completion_id: value.target.caller_completion_id,
                caller_invocation_epoch: value.target.caller_invocation_epoch,
                result: value.result,
            }
        }
    }

    #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
    pub(super) enum ServiceInvocationResponseSink {
        /// The invocation has been created by a partition processor and is expecting a response.
        PartitionProcessor {
            caller: InvocationId,
            entry_index: EntryIndex,
            #[serde(default, skip_serializing_if = "num_traits::Zero::is_zero")]
            caller_invocation_epoch: InvocationEpoch,
        },
        /// The invocation has been generated by a request received at an ingress, and the client is expecting a response back.
        Ingress {
            // TODO(slinkydeveloper) remove this field in 1.4, it's unused, it's here only to preserve back-compat
            #[serde(default, skip_serializing_if = "Option::is_none")]
            node_id: Option<GenerationalNodeId>,
            request_id: PartitionProcessorRpcRequestId,
        },
    }

    impl From<ServiceInvocationResponseSink> for super::ServiceInvocationResponseSink {
        fn from(value: ServiceInvocationResponseSink) -> Self {
            match value {
                ServiceInvocationResponseSink::Ingress { request_id, .. } => {
                    Self::Ingress { request_id }
                }
                ServiceInvocationResponseSink::PartitionProcessor {
                    entry_index,
                    caller,
                    caller_invocation_epoch,
                } => Self::PartitionProcessor(JournalCompletionTarget {
                    caller_id: caller,
                    caller_completion_id: entry_index,
                    caller_invocation_epoch,
                }),
            }
        }
    }

    impl From<super::ServiceInvocationResponseSink> for ServiceInvocationResponseSink {
        fn from(value: super::ServiceInvocationResponseSink) -> Self {
            match value {
                super::ServiceInvocationResponseSink::Ingress { request_id } => Self::Ingress {
                    // TODO(slinkydeveloper) stop writing this field in 1.3, it's unused, it's here only to preserve back-compat
                    node_id: Some(GenerationalNodeId::new(1, 1)),
                    request_id,
                },
                super::ServiceInvocationResponseSink::PartitionProcessor(
                    JournalCompletionTarget {
                        caller_id,
                        caller_completion_id,
                        caller_invocation_epoch,
                    },
                ) => Self::PartitionProcessor {
                    caller: caller_id,
                    entry_index: caller_completion_id,
                    caller_invocation_epoch,
                },
            }
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
mod mocks {
    use super::*;

    use rand::distr::{Alphanumeric, SampleString};

    fn generate_string() -> ByteString {
        Alphanumeric.sample_string(&mut rand::rng(), 8).into()
    }

    impl InvocationTarget {
        pub fn mock_service() -> Self {
            InvocationTarget::service(generate_string(), generate_string())
        }

        pub fn mock_virtual_object() -> Self {
            InvocationTarget::virtual_object(
                generate_string(),
                generate_string(),
                generate_string(),
                VirtualObjectHandlerType::Exclusive,
            )
        }

        pub fn mock_workflow() -> Self {
            InvocationTarget::workflow(
                generate_string(),
                generate_string(),
                generate_string(),
                WorkflowHandlerType::Workflow,
            )
        }

        pub fn mock_from_service_id(service_id: ServiceId) -> Self {
            InvocationTarget::virtual_object(
                service_id.service_name,
                service_id.key,
                "MyMethod",
                VirtualObjectHandlerType::Exclusive,
            )
        }
    }

    impl ServiceInvocation {
        pub fn mock() -> Self {
            Self {
                invocation_id: InvocationId::mock_random(),
                invocation_target: InvocationTarget::mock_service(),
                argument: Default::default(),
                source: Source::Service(
                    InvocationId::mock_random(),
                    InvocationTarget::mock_service(),
                ),
                response_sink: None,
                span_context: Default::default(),
                headers: vec![],
                execution_time: None,
                completion_retention_duration: Duration::ZERO,
                journal_retention_duration: Duration::ZERO,
                idempotency_key: None,
                submit_notification_sink: None,
                restate_version: RestateVersion::current(),
            }
        }
    }

    impl InvocationRequest {
        pub fn mock() -> Self {
            Self {
                header: InvocationRequestHeader::mock(),
                body: Default::default(),
            }
        }
    }

    impl InvocationRequestHeader {
        pub fn mock() -> Self {
            Self {
                id: InvocationId::mock_random(),
                target: InvocationTarget::mock_service(),
                headers: vec![],
                span_context: Default::default(),
                idempotency_key: None,
                execution_time: None,
                completion_retention_duration: Default::default(),
                journal_retention_duration: Default::default(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod restate_version {
        use super::*;

        #[test]
        fn unknown_version_is_valid_semver() {
            semver::Version::parse(RestateVersion::unknown().as_str()).unwrap();
        }
    }

    mod invocation_response {
        use super::*;

        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        pub struct InvocationResponse_1_2 {
            pub id: InvocationId,
            pub entry_index: EntryIndex,
            pub result: ResponseResult,
        }

        #[test]
        fn backward_compatibility() {
            let caller_id = InvocationId::mock_random();
            let caller_completion_id = 10;
            let result = ResponseResult::Success(Bytes::from_static(b"123"));

            let old_response = InvocationResponse_1_2 {
                id: caller_id,
                entry_index: caller_completion_id,
                result: result.clone(),
            };

            let new_response: InvocationResponse =
                serde_json::from_str(&serde_json::to_string(&old_response).unwrap()).unwrap();

            assert_eq!(new_response.target.caller_id, caller_id);
            assert_eq!(
                new_response.target.caller_completion_id,
                caller_completion_id
            );
            assert_eq!(new_response.target.caller_invocation_epoch, 0);
            assert_eq!(new_response.result, result);
        }

        #[test]
        fn forward_compatibility() {
            let caller_id = InvocationId::mock_random();
            let caller_completion_id = 10;
            let result = ResponseResult::Success(Bytes::from_static(b"123"));

            let new_response = InvocationResponse {
                target: JournalCompletionTarget {
                    caller_id,
                    caller_completion_id,
                    caller_invocation_epoch: 0,
                },
                result: result.clone(),
            };

            let old_response: InvocationResponse_1_2 =
                serde_json::from_str(&serde_json::to_string(&new_response).unwrap()).unwrap();

            assert_eq!(old_response.id, caller_id);
            assert_eq!(old_response.entry_index, caller_completion_id);
            assert_eq!(old_response.result, result);
        }
    }

    mod span_context {
        use std::str::FromStr;

        use opentelemetry::trace::{SpanId, TraceFlags, TraceId, TraceState};

        use crate::invocation::{
            ServiceInvocationSpanContext, SpanContextDef, SpanRelationCause, TraceStateDef,
        };

        #[test]
        fn roundtrip_invocation_span_context() {
            let ctx = ServiceInvocationSpanContext::new(
                SpanContextDef::new(
                    TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                    SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]),
                    TraceFlags::SAMPLED,
                    false,
                    TraceStateDef::new_trace_state(
                        TraceState::from_str("vendorname1=opaqueValue1,vendorname2=opaqueValue2")
                            .unwrap(),
                    ),
                ),
                Some(SpanRelationCause::Linked(
                    TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                    SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]),
                )),
            );

            let encoded_ctx = serde_json::to_string(&ctx).unwrap();

            assert_eq!(
                encoded_ctx,
                r#"{"span_context":{"trace_id":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],"span_id":[1,2,3,4,5,6,7,8],"trace_flags":1,"is_remote":false,"trace_state":"vendorname1=opaqueValue1,vendorname2=opaqueValue2"},"cause":{"Linked":[[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],[1,2,3,4,5,6,7,8]]}}"#
            );

            let decoded_ctx = serde_json::from_str(&encoded_ctx).unwrap();

            assert_eq!(ctx, decoded_ctx)
        }

        #[test]
        fn trace_state_def_eq() {
            assert_eq!(
                TraceStateDef::new_trace_state(
                    TraceState::from_str("vendorname1=opaqueValue1,vendorname2=opaqueValue2")
                        .unwrap(),
                ),
                TraceStateDef::Header("vendorname1=opaqueValue1,vendorname2=opaqueValue2".into())
            );

            assert_eq!(
                TraceStateDef::new_trace_state(TraceState::from_str("").unwrap(),),
                TraceStateDef::Header("".into())
            );
        }
    }
}
