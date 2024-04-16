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
use crate::identifiers::{EntryIndex, InvocationId, PartitionKey, ServiceId, WithPartitionKey};
use crate::time::MillisSinceEpoch;
use crate::GenerationalNodeId;
use bytes::Bytes;
use bytestring::ByteString;
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceState};
use opentelemetry::Context;
use serde_with::{serde_as, FromInto};
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;
use std::time::Duration;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

// Re-exporting opentelemetry [`TraceId`] to avoid having to import opentelemetry in all crates.
pub use opentelemetry::trace::TraceId;

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ComponentType {
    Service,
    VirtualObject,
}

impl ComponentType {
    pub fn is_keyed(&self) -> bool {
        matches!(self, ComponentType::VirtualObject)
    }

    pub fn has_state(&self) -> bool {
        self.is_keyed()
    }
}

impl fmt::Display for ComponentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum HandlerType {
    Exclusive,
    Shared,
}

impl HandlerType {
    pub fn default_for_component_type(component_type: ComponentType) -> Self {
        match component_type {
            ComponentType::Service => HandlerType::Shared,
            ComponentType::VirtualObject => HandlerType::Exclusive,
        }
    }
}

impl fmt::Display for HandlerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
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
        handler_ty: HandlerType,
    },
}

impl InvocationTarget {
    pub fn service(name: impl Into<ByteString>, handler: impl Into<ByteString>) -> Self {
        Self::Service {
            name: name.into(),
            handler: handler.into(),
        }
    }

    pub fn virtual_object(
        name: impl Into<ByteString>,
        key: impl Into<ByteString>,
        handler: impl Into<ByteString>,
        handler_ty: HandlerType,
    ) -> Self {
        Self::VirtualObject {
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
        }
    }

    pub fn key(&self) -> Option<&ByteString> {
        match self {
            InvocationTarget::Service { .. } => None,
            InvocationTarget::VirtualObject { key, .. } => Some(key),
        }
    }

    pub fn handler_name(&self) -> &ByteString {
        match self {
            InvocationTarget::Service { handler, .. } => handler,
            InvocationTarget::VirtualObject { handler, .. } => handler,
        }
    }

    pub fn as_keyed_service_id(&self) -> Option<ServiceId> {
        match self {
            InvocationTarget::Service { .. } => None,
            InvocationTarget::VirtualObject { name, key, .. } => {
                Some(ServiceId::new(name.clone(), key.as_bytes().clone()))
            }
        }
    }

    pub fn service_ty(&self) -> ComponentType {
        match self {
            InvocationTarget::Service { .. } => ComponentType::Service,
            InvocationTarget::VirtualObject { .. } => ComponentType::VirtualObject,
        }
    }

    pub fn handler_ty(&self) -> Option<HandlerType> {
        match self {
            InvocationTarget::Service { .. } => None,
            InvocationTarget::VirtualObject { handler_ty, .. } => Some(*handler_ty),
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
    pub response_sink: Option<ServiceInvocationResponseSink>,
    pub span_context: ServiceInvocationSpanContext,
    pub headers: Vec<Header>,
    /// Time when the request should be executed
    pub execution_time: Option<MillisSinceEpoch>,
    pub idempotency: Option<Idempotency>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Idempotency {
    pub key: ByteString,
    pub retention: Duration,
}

impl ServiceInvocation {
    /// Create a new [`ServiceInvocation`].
    ///
    /// This method returns the [`Span`] associated to the created [`ServiceInvocation`].
    /// It is not required to keep this [`Span`] around for the whole lifecycle of the invocation.
    /// On the contrary, it is encouraged to drop it as soon as possible,
    /// to let the exporter commit this span to jaeger/zipkin to visualize intermediate results of the invocation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        argument: impl Into<Bytes>,
        source: Source,
        response_sink: Option<ServiceInvocationResponseSink>,
        related_span: SpanRelation,
        headers: Vec<Header>,
        execution_time: Option<MillisSinceEpoch>,
        idempotency: Option<Idempotency>,
    ) -> Self {
        let span_context = ServiceInvocationSpanContext::start(&invocation_id, related_span);
        Self {
            invocation_id,
            invocation_target,
            argument: argument.into(),
            source,
            response_sink,
            span_context,
            headers,
            execution_time,
            idempotency,
        }
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
    /// The result needs to be used as input argument of a new invocation
    NewInvocation {
        id: InvocationId,
        target: InvocationTarget,
        caller_context: Bytes,
    },
    /// The invocation has been generated by a request received at an ingress, and the client is expecting a response back.
    Ingress(GenerationalNodeId),
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
                // use part of the invocation id as the span id of the new trace root
                let span_id: SpanId = invocation_id.invocation_uuid().into();

                // use its reverse as the span id of the background_invoke 'pointer' span in the previous trace
                // as we cannot use the same span id for both spans
                let mut pointer_span_id = span_id.to_bytes();
                pointer_span_id.reverse();

                // create a span context with a new trace that will be used for any actions as part of the background invocation
                // a span will be emitted using these details when its finished (so we know how long the invocation took)
                let new_span_context = SpanContext::new(
                    // use invocation id as the new trace id; this allows you to follow cause -> new trace in jaeger
                    // trace ids are 128 bits and 'worldwide unique'
                    invocation_id.invocation_uuid().into(),
                    // use part of the invocation id as the new span id; this is 64 bits and best-effort 'globally unique'
                    span_id,
                    // use sampling decision of the causing trace; this is NOT default otel behaviour but
                    // is useful for users
                    linked_span_context.trace_flags(),
                    // this would never be set to true for a span created in this binary
                    false,
                    TraceState::default(),
                );
                let cause = SpanRelationCause::Linked(
                    linked_span_context.trace_id(),
                    SpanId::from_bytes(pointer_span_id),
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
    /// Attach this [`SpanRelation`] to the given [`Span`]
    pub fn attach_to_span(&self, span: &Span) {
        match self {
            SpanRelation::Parent(span_context) => {
                span.set_parent(Context::new().with_remote_span_context(span_context.clone()))
            }
            SpanRelation::Linked(span_context) => span.add_link(span_context.clone()),
            SpanRelation::None => (),
        };
    }

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
                HandlerType::Exclusive,
            )
        }

        pub fn mock_from_service_id(service_id: ServiceId) -> Self {
            InvocationTarget::virtual_object(
                service_id.service_name,
                ByteString::try_from(service_id.key.clone()).unwrap(),
                "MyMethod",
                HandlerType::Exclusive,
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
                idempotency: None,
            }
        }
    }
}
