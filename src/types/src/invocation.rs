//! This module contains all the core types representing a service invocation.

use crate::errors::UserErrorCode;
use crate::identifiers::{EntryIndex, IngressId, ServiceInvocationId};
use bytes::Bytes;
use bytestring::ByteString;
use opentelemetry_api::trace::{
    SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
};
use opentelemetry_api::Context;
use tracing::{info_span, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Struct representing an invocation to a service. This struct is processed by Restate to execute the invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceInvocation {
    pub id: ServiceInvocationId,
    pub method_name: ByteString,
    pub argument: Bytes,
    pub response_sink: Option<ServiceInvocationResponseSink>,
    pub span_context: ServiceInvocationSpanContext,
}

impl ServiceInvocation {
    /// Create a new [`ServiceInvocation`].
    ///
    /// This method returns the [`Span`] associated to the created [`ServiceInvocation`].
    /// It is not required to keep this [`Span`] around for the whole lifecycle of the invocation.
    /// On the contrary, it is encouraged to drop it as soon as possible,
    /// to let the exporter commit this span to jaeger/zipkin to visualize intermediate results of the invocation.
    pub fn new(
        id: ServiceInvocationId,
        method_name: ByteString,
        argument: Bytes,
        response_sink: Option<ServiceInvocationResponseSink>,
        related_span: SpanRelation,
    ) -> Self {
        let span_context = ServiceInvocationSpanContext::start(&id, &method_name, related_span);
        Self {
            id,
            method_name,
            argument,
            response_sink,
            span_context,
        }
    }
}

/// Representing a response for a caller
#[derive(Debug, Clone, PartialEq)]
pub struct InvocationResponse {
    pub id: ServiceInvocationId,
    pub entry_index: EntryIndex,
    pub result: ResponseResult,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseResult {
    Success(Bytes),
    Failure(UserErrorCode, ByteString),
}

/// Definition of the sink where to send the result of a service invocation.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ServiceInvocationResponseSink {
    /// The invocation has been created by a partition processor and is expecting a response.
    PartitionProcessor {
        caller: ServiceInvocationId,
        entry_index: EntryIndex,
    },
    /// The invocation has been generated by a request received at an ingress, and the client is expecting a response back.
    Ingress(IngressId),
}

/// This struct contains the relevant span information for a [`ServiceInvocation`].
/// It can be used to create related spans, such as child spans,
/// using [`ServiceInvocationSpanContext::as_background_invoke`] or [`ServiceInvocationSpanContext::as_invoke`].
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ServiceInvocationSpanContext {
    span_context: SpanContext,
    span_relation: Option<SpanRelationType>,
}

impl ServiceInvocationSpanContext {
    pub fn new(span_context: SpanContext, span_relation: Option<SpanRelationType>) -> Self {
        Self {
            span_context,
            span_relation,
        }
    }

    pub fn empty() -> Self {
        Self {
            span_context: SpanContext::empty_context(),
            span_relation: None,
        }
    }

    /// Create a [`SpanContext`] for this invocation, a [`Span`] for which will be created
    /// when the invocation completes. In the background invoke case, a 'pointer' [`Span`]
    /// with no duration is created for the purpose of linking back to in the background trace.
    pub fn start(
        service_invocation_id: &ServiceInvocationId,
        method_name: &str,
        related_span_context: SpanRelation,
    ) -> ServiceInvocationSpanContext {
        let (span_relation, span_context) = match &related_span_context {
            SpanRelation::Cause(span_relation, cause) => {
                if !cause.is_sampled() {
                    // don't waste any time or storage space on unsampled traces
                    // sampling based on parent is default otel behaviour; we do the same for the
                    // non-parent background invoke relationship
                    return ServiceInvocationSpanContext {
                        span_context: SpanContext::empty_context(),
                        span_relation: None,
                    };
                }
                match span_relation {
                    SpanRelationType::BackgroundInvoke(trace_id, _) => {
                        // create an instantaneous 'pointer span' which lives in the calling trace at the
                        // time of background call, and exists only to be linked to by the new trace that
                        // will be created for the background invocation
                        let pointer_span = info_span!(
                            "background_invoke",
                            otel.name = format!("background_invoke {method_name}"),
                            rpc.system = "restate",
                            rpc.service = %service_invocation_id.service_id.service_name,
                            rpc.method = method_name,
                            restate.invocation.sid = %service_invocation_id,
                        );

                        // set parent so that this goes in the calling trace
                        pointer_span
                            .set_parent(Context::new().with_remote_span_context(cause.clone()));
                        // instantaneous span
                        let _ = pointer_span.enter();

                        // create a span context with a new trace that will be used for any actions as part of the background invocation
                        // a span will be emitted using these details when its finished (so we know how long the invocation took)
                        let span_context = SpanContext::new(
                            // use invocation id as the new trace id; this allows you to follow cause -> new trace in jaeger
                            // trace ids are 128 bits and must be globally unique
                            TraceId::from_bytes(*service_invocation_id.invocation_id.as_bytes()),
                            // use part of the invocation id as the new span id; this just needs to be 64 bits
                            // and unique within the trace
                            SpanId::from_bytes({
                                let (_, _, _, span_id) =
                                    service_invocation_id.invocation_id.as_fields();
                                *span_id
                            }),
                            // use sampling decision of the causing trace; this is NOT default otel behaviour but
                            // is useful for users
                            cause.trace_flags(),
                            // this would never be set to true for a span created in this binary
                            false,
                            TraceState::default(),
                        );
                        let span_relation = SpanRelationType::BackgroundInvoke(
                            *trace_id,
                            pointer_span.context().span().span_context().span_id(),
                        );
                        (Some(span_relation), span_context)
                    }
                    SpanRelationType::Invoke(span_id) => {
                        // create a span context as part of the existing trace, which will be used for any actions
                        // of the invocation. a span will be emitted with these details when its finished
                        let span_context = SpanContext::new(
                            // use parent trace id
                            cause.trace_id(),
                            SpanId::from_bytes({
                                let (_, _, _, span_id) =
                                    service_invocation_id.invocation_id.as_fields();
                                *span_id
                            }),
                            // use sampling decision of parent trace; this is default otel behaviour
                            cause.trace_flags(),
                            false,
                            cause.trace_state().clone(),
                        );
                        let span_relation = SpanRelationType::Invoke(*span_id);
                        (Some(span_relation), span_context)
                    }
                }
            }
            SpanRelation::None => {
                // we would only expect this in tests as there should always be either another invocation
                // or an ingress task leading to the invocation

                // create a span context with a new trace
                let span_context = SpanContext::new(
                    // use invocation id as the new trace id
                    TraceId::from_bytes(*service_invocation_id.invocation_id.as_bytes()),
                    SpanId::from_bytes({
                        let (_, _, _, span_id) = service_invocation_id.invocation_id.as_fields();
                        *span_id
                    }),
                    // we don't have the means to actually sample here; just hardcode a sampled trace
                    // as this should only happen in tests anyway
                    TraceFlags::SAMPLED,
                    false,
                    TraceState::default(),
                );
                (None, span_context)
            }
        };

        ServiceInvocationSpanContext {
            span_context,
            span_relation,
        }
    }

    pub fn cause(self) -> SpanRelation {
        match self.span_relation {
            None => SpanRelation::None,
            Some(SpanRelationType::Invoke(span_id)) => {
                SpanRelation::Cause(
                    SpanRelationType::Invoke(span_id),
                    SpanContext::new(
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
                    ),
                )
            }
            Some(SpanRelationType::BackgroundInvoke(trace_id, span_id)) => {
                SpanRelation::Cause(
                    SpanRelationType::BackgroundInvoke(trace_id, span_id),
                    SpanContext::new(
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
                    ),
                )
            }
        }
    }

    pub fn span_context(&self) -> &SpanContext {
        &self.span_context
    }

    pub fn span_relation(&self) -> Option<&SpanRelationType> {
        self.span_relation.as_ref()
    }

    pub fn as_background_invoke(&self) -> SpanRelation {
        SpanRelation::Cause(
            SpanRelationType::BackgroundInvoke(
                self.span_context.trace_id(),
                self.span_context.span_id(),
            ),
            self.span_context.clone(),
        )
    }

    pub fn as_invoke(&self) -> SpanRelation {
        SpanRelation::Cause(
            SpanRelationType::Invoke(self.span_context.span_id()),
            self.span_context.clone(),
        )
    }

    pub fn is_sampled(&self) -> bool {
        self.span_context.trace_flags().is_sampled()
    }
}

impl From<ServiceInvocationSpanContext> for SpanContext {
    fn from(value: ServiceInvocationSpanContext) -> Self {
        value.span_context
    }
}

/// Span relation, used to propagate tracing contexts.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SpanRelationType {
    Invoke(SpanId),
    BackgroundInvoke(TraceId, SpanId),
}

pub enum SpanRelation {
    None,
    Cause(SpanRelationType, SpanContext),
}

impl From<SpanRelation> for Option<SpanRelationType> {
    fn from(value: SpanRelation) -> Self {
        match value {
            SpanRelation::None => None,
            SpanRelation::Cause(relation, _) => Some(relation),
        }
    }
}

impl SpanRelation {
    /// Attach this [`SpanRelation`] to the given [`Span`]
    pub fn attach_to_span(self, span: &Span) {
        let (span_relation, span_context) = match self {
            Self::Cause(span_relation, span_context) => (span_relation, span_context),
            Self::None => return,
        };

        match span_relation {
            SpanRelationType::Invoke(_) => {
                span.set_parent(Context::new().with_remote_span_context(span_context))
            }
            SpanRelationType::BackgroundInvoke(_, _) => span.add_link(span_context),
        };
    }
}
