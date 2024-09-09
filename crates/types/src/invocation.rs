// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains all the core types representing a service invocation.

use crate::errors::InvocationError;
use crate::identifiers::{
    EntryIndex, IdempotencyId, IngressRequestId, InvocationId, PartitionKey, ServiceId,
    WithPartitionKey,
};
use crate::time::MillisSinceEpoch;
use crate::GenerationalNodeId;
use bytes::Bytes;
use bytestring::ByteString;
use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceState};
use serde_with::{serde_as, FromInto};
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;
use std::time::Duration;

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

    pub fn short(&self) -> Short {
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
            write!(f, "{}/", key)?;
        }
        write!(f, "{}", self.handler_name())?;
        Ok(())
    }
}

/// Struct representing an invocation to a service. This struct is processed by Restate to execute the invocation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ServiceInvocation {
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub argument: Bytes,
    pub source: Source,
    pub span_context: ServiceInvocationSpanContext,
    pub headers: Vec<Header>,
    /// Time when the request should be executed
    pub execution_time: Option<MillisSinceEpoch>,
    pub completion_retention_duration: Option<Duration>,
    pub idempotency_key: Option<ByteString>,

    // Where to send the response, if any
    pub response_sink: Option<ServiceInvocationResponseSink>,
    // Where to send the submit notification, if any.
    //  The submit notification is sent back both when this invocation request attached to an existing invocation,
    //  or when this request started a fresh invocation.
    pub submit_notification_sink: Option<SubmitNotificationSink>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubmitNotificationSink {
    Ingress {
        node_id: GenerationalNodeId,
        request_id: IngressRequestId,
    },
}

impl ServiceInvocation {
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
            completion_retention_duration: None,
            idempotency_key: None,
            submit_notification_sink: None,
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

/// Representing a response for a caller
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InvocationResponse {
    pub id: InvocationId,
    pub entry_index: EntryIndex,
    pub result: ResponseResult,
}

impl WithPartitionKey for InvocationResponse {
    fn partition_key(&self) -> PartitionKey {
        self.id.partition_key()
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

impl From<ResponseResult> for Result<Bytes, InvocationError> {
    fn from(value: ResponseResult) -> Self {
        match value {
            ResponseResult::Success(bytes) => Ok(bytes),
            ResponseResult::Failure(e) => Err(e),
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

/// Definition of the sink where to send the result of a service invocation.
#[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
pub enum ServiceInvocationResponseSink {
    /// The invocation has been created by a partition processor and is expecting a response.
    PartitionProcessor {
        caller: InvocationId,
        entry_index: EntryIndex,
    },
    /// The invocation has been generated by a request received at an ingress, and the client is expecting a response back.
    Ingress {
        node_id: GenerationalNodeId,
        request_id: IngressRequestId,
    },
}

impl ServiceInvocationResponseSink {
    pub fn partition_processor(caller: InvocationId, entry_index: EntryIndex) -> Self {
        Self::PartitionProcessor {
            caller,
            entry_index,
        }
    }

    pub fn ingress(ingress: GenerationalNodeId, request_id: IngressRequestId) -> Self {
        Self::Ingress {
            node_id: ingress,
            request_id,
        }
    }
}

/// Source of an invocation
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Source {
    Ingress,
    Service(InvocationId, InvocationTarget),
    /// Internal calls for the non-deterministic built-in services
    Internal,
}

/// This struct contains the relevant span information for a [`ServiceInvocation`].
/// It can be used to create related spans, such as child spans,
/// using [`ServiceInvocationSpanContext::as_linked`] or [`ServiceInvocationSpanContext::as_parent`].
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceInvocationSpanContext {
    #[serde_as(as = "FromInto<SpanContextDef>")]
    span_context: SpanContext,
    cause: Option<SpanRelationCause>,
}

impl ServiceInvocationSpanContext {
    pub fn new(span_context: SpanContext, cause: Option<SpanRelationCause>) -> Self {
        Self {
            span_context,
            cause,
        }
    }

    pub fn empty() -> Self {
        Self {
            span_context: SpanContext::empty_context(),
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

        let (cause, new_span_context) = match &related_span {
            SpanRelation::Linked(linked_span_context) => {
                // create a span context with a new trace that will be used for any actions as part of the background invocation
                // a span will be emitted using these details when its finished (so we know how long the invocation took)
                let new_span_context = SpanContext::new(
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
                    TraceState::default(),
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
                let new_span_context = SpanContext::new(
                    // use parent trace id
                    parent_span_context.trace_id(),
                    // use part of the invocation id as the new span id
                    invocation_id.invocation_uuid().into(),
                    // use sampling decision of parent trace; this is default otel behaviour
                    parent_span_context.trace_flags(),
                    false,
                    parent_span_context.trace_state().clone(),
                );
                let cause = SpanRelationCause::Parent(parent_span_context.span_id());
                (Some(cause), new_span_context)
            }
            SpanRelation::None => {
                // we would only expect this in tests as there should always be either another invocation
                // or an ingress task leading to the invocation

                // create a span context with a new trace
                let new_span_context = SpanContext::new(
                    // use invocation id as the new trace id and span id
                    invocation_id.invocation_uuid().into(),
                    invocation_id.invocation_uuid().into(),
                    // we don't have the means to actually sample here; just hardcode a sampled trace
                    // as this should only happen in tests anyway
                    TraceFlags::SAMPLED,
                    false,
                    TraceState::default(),
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
                SpanRelation::Parent(SpanContext::new(
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
                SpanRelation::Linked(SpanContext::new(
                    // use stored trace id
                    trace_id,
                    // use stored span id
                    span_id,
                    // this will be ignored; trace flags are not propagated to links
                    self.span_context.trace_flags(),
                    // this will be ignored; is_remote is not propagated
                    false,
                    // this will be ignored; trace state is not propagated to links
                    TraceState::default(),
                ))
            }
        }
    }

    pub fn span_context(&self) -> &SpanContext {
        &self.span_context
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
        self.span_context.trace_flags().is_sampled()
    }

    pub fn trace_id(&self) -> TraceId {
        self.span_context.trace_id()
    }
}

impl Default for ServiceInvocationSpanContext {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<ServiceInvocationSpanContext> for SpanContext {
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
    Parent(SpanContext),
    Linked(SpanContext),
}

impl SpanRelation {
    fn is_sampled(&self) -> bool {
        match self {
            SpanRelation::None => false,
            SpanRelation::Parent(span_context) => span_context.is_sampled(),
            SpanRelation::Linked(span_context) => span_context.is_sampled(),
        }
    }
}

/// Message to terminate an invocation.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InvocationTermination {
    pub invocation_id: InvocationId,
    pub flavor: TerminationFlavor,
}

impl InvocationTermination {
    pub const fn kill(invocation_id: InvocationId) -> Self {
        Self {
            invocation_id,
            flavor: TerminationFlavor::Kill,
        }
    }

    pub const fn cancel(invocation_id: InvocationId) -> Self {
        Self {
            invocation_id,
            flavor: TerminationFlavor::Cancel,
        }
    }
}

/// Flavor of the termination. Can be kill (hard stop) or graceful cancel.
#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TerminationFlavor {
    /// hard termination, no clean up
    Kill,
    /// graceful termination allowing the invocation to clean up
    Cancel,
}

/// Message to purge an invocation.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PurgeInvocationRequest {
    pub invocation_id: InvocationId,
}

// A hack to allow spancontext to be serialized.
// Details in https://github.com/open-telemetry/opentelemetry-rust/issues/576#issuecomment-1253396100
#[derive(serde::Serialize, serde::Deserialize)]
struct SpanContextDef {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    trace_flags: u8,
    is_remote: bool,
    trace_state: String,
}

// Provide a conversion to construct the remote type.
impl From<SpanContextDef> for SpanContext {
    fn from(def: SpanContextDef) -> Self {
        SpanContext::new(
            TraceId::from_bytes(def.trace_id),
            SpanId::from_bytes(def.span_id),
            TraceFlags::new(def.trace_flags),
            def.is_remote,
            TraceState::from_str(&def.trace_state).unwrap(),
        )
    }
}

impl From<SpanContext> for SpanContextDef {
    fn from(ctx: SpanContext) -> Self {
        Self {
            trace_id: ctx.trace_id().to_bytes(),
            span_id: ctx.span_id().to_bytes(),
            trace_flags: ctx.trace_flags().to_u8(),
            is_remote: ctx.is_remote(),
            trace_state: ctx.trace_state().header(),
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
    pub response_sink: ServiceInvocationResponseSink,
}

impl WithPartitionKey for AttachInvocationRequest {
    fn partition_key(&self) -> PartitionKey {
        self.invocation_query.partition_key()
    }
}

#[cfg(any(test, feature = "test-util"))]
mod mocks {
    use super::*;

    use rand::distributions::{Alphanumeric, DistString};

    fn generate_string() -> ByteString {
        Alphanumeric
            .sample_string(&mut rand::thread_rng(), 8)
            .into()
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
                completion_retention_duration: None,
                idempotency_key: None,
                submit_notification_sink: None,
            }
        }
    }
}
