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
use restate_types::identifiers::FullInvocationId;
use restate_types::invocation::SpanRelation;
use tracing::{info_span, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) fn prepare_tracing_span<B>(
    fid: &FullInvocationId,
    handler_name: &str,
    req: &Request<B>,
) -> (Span, SpanContext) {
    let connect_info: &ConnectInfo = req
        .extensions()
        .get()
        .expect("Should have been injected by the previous layer");
    let (client_addr, client_port) = (connect_info.address(), connect_info.port());

    // Create the ingress span and attach it to the next async block.
    // This span is committed once the async block terminates, recording the execution time of the invocation.
    // Another span is created later by the ServiceInvocationFactory, for the ServiceInvocation itself,
    // which is used by the Restate components to correctly link to a single parent span
    // to commit intermediate results of the processing.
    let ingress_span = info_span!(
        "ingress_invoke",
        otel.name = format!("ingress_invoke {}/{}", fid.service_id.service_name, handler_name),
        rpc.system = "restate",
        rpc.service = %fid.service_id.service_name,
        rpc.method = %handler_name,
        client.socket.address = %client_addr,
        client.socket.port = %client_port,
    );

    // Extract tracing context if any
    let tracing_context: &opentelemetry::Context = req
        .extensions()
        .get()
        .expect("Should have been injected by the previous layer");

    // Attach this ingress_span to the parent parsed from the headers, if any.
    span_relation(tracing_context.span().span_context()).attach_to_span(&ingress_span);

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
