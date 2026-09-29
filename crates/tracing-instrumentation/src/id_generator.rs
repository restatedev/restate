// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::Cell;

use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator};

thread_local! {
    static PRESET_TRACE_ID: Cell<Option<TraceId>> = const { Cell::new(None) };
    static PRESET_SPAN_ID: Cell<Option<SpanId>> = const { Cell::new(None) };
}

/// [`IdGenerator`] returning the ids set by [`with_preset_ids`], and random ids otherwise.
///
/// Since OpenTelemetry 0.32, the `SpanBuilder` no longer allows setting the trace and span ids
/// of a span. We need this to emit spans whose ids are derived deterministically from the
/// invocation id (see `ServiceInvocationSpanContext::start`).
#[derive(Debug, Default)]
pub(crate) struct PresetIdGenerator(RandomIdGenerator);

impl IdGenerator for PresetIdGenerator {
    fn new_trace_id(&self) -> TraceId {
        PRESET_TRACE_ID
            .get()
            .unwrap_or_else(|| self.0.new_trace_id())
    }

    fn new_span_id(&self) -> SpanId {
        // Consumed so that no two spans can end up with the same preset span id.
        PRESET_SPAN_ID
            .take()
            .unwrap_or_else(|| self.0.new_span_id())
    }
}

/// Runs `f` with the [`PresetIdGenerator`] returning the given ids on the current thread.
///
/// Only the first span started within `f` gets the preset span id; any further span gets a
/// random one. The SDK only asks for a new trace id if the span has no parent.
pub(crate) fn with_preset_ids<R>(trace_id: TraceId, span_id: SpanId, f: impl FnOnce() -> R) -> R {
    struct Reset(Option<TraceId>, Option<SpanId>);
    impl Drop for Reset {
        fn drop(&mut self) {
            PRESET_TRACE_ID.set(self.0);
            PRESET_SPAN_ID.set(self.1);
        }
    }

    let _reset = Reset(
        PRESET_TRACE_ID.replace(Some(trace_id)),
        PRESET_SPAN_ID.replace(Some(span_id)),
    );
    f()
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{Span, TraceContextExt, Tracer, TracerProvider};
    use opentelemetry::{Context, trace::SpanContext, trace::TraceFlags, trace::TraceState};
    use opentelemetry_sdk::trace::SdkTracerProvider;

    use super::*;

    #[test]
    fn preset_ids() {
        let provider = SdkTracerProvider::builder()
            .with_id_generator(PresetIdGenerator::default())
            .build();
        let tracer = provider.tracer("test");
        let trace_id = TraceId::from(42);
        let span_id = SpanId::from(7);

        // root span takes both preset ids, a second span only the trace id
        let (span, second) = with_preset_ids(trace_id, span_id, || {
            (tracer.start("root"), tracer.start("second"))
        });
        assert_eq!(span.span_context().trace_id(), trace_id);
        assert_eq!(span.span_context().span_id(), span_id);
        assert_eq!(second.span_context().trace_id(), trace_id);
        assert_ne!(second.span_context().span_id(), span_id);

        // child span keeps the parent's trace id and takes the preset span id
        let parent = SpanContext::new(
            TraceId::from(1),
            SpanId::from(2),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let parent_cx = Context::new().with_remote_span_context(parent);
        let span = with_preset_ids(trace_id, span_id, || {
            tracer.start_with_context("child", &parent_cx)
        });
        assert_eq!(span.span_context().trace_id(), TraceId::from(1));
        assert_eq!(span.span_context().span_id(), span_id);

        // presets are reset afterwards
        let span = tracer.start("random");
        assert_ne!(span.span_context().trace_id(), trace_id);
        assert_ne!(span.span_context().span_id(), span_id);
        assert_eq!(PRESET_TRACE_ID.get(), None);
        assert_eq!(PRESET_SPAN_ID.get(), None);
    }
}
