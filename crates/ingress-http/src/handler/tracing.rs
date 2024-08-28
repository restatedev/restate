// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ConnectInfo;

use http::Request;
use opentelemetry::trace::{SpanContext, TraceContextExt};
use restate_tracing_instrumentation as instrumentation;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{InvocationTarget, SpanExt, SpanRelation};
use tracing::{Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) fn prepare_tracing_span<B>(
    invocation_id: &InvocationId,
    invocation_target: &InvocationTarget,
    req: &Request<B>,
) -> (Span, SpanContext) {
    let connect_info: &ConnectInfo = req
        .extensions()
        .get()
        .expect("Should have been injected by the previous layer");
    let (client_addr, client_port) = (connect_info.address(), connect_info.port());

    // Extract tracing context if any
    let tracing_context: &opentelemetry::Context = req
        .extensions()
        .get()
        .expect("Should have been injected by the previous layer");

    // Create the ingress span and attach it to the next async block.
    // This span is committed once the async block terminates, recording the execution time of the invocation.
    // Another span is created later by the ServiceInvocationFactory, for the ServiceInvocation itself,
    // which is used by the Restate services to correctly link to a single parent span
    // to commit intermediate results of the processing.

    let ingress_span = instrumentation::invocation_span!(
        level = Level::INFO,
        prefix = "ingress",
        id = invocation_id,
        target = invocation_target,
        client.socket.port = client_port as i64,
        client.socket.address = client_addr.to_string()
    );
    ingress_span.set_relation(span_relation(tracing_context.span().span_context()));

    // We need the context to link it to the service invocation span
    let ingress_span_context = ingress_span.context().span().span_context().clone();

    (ingress_span, ingress_span_context)
}

fn span_relation(request_span: &SpanContext) -> SpanRelation {
    if request_span.is_valid() {
        SpanRelation::Parent(request_span.clone())
    } else {
        SpanRelation::None
    }
}
