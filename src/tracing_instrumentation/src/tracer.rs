use opentelemetry::trace::{OrderMap, TraceId};
use opentelemetry::trace::{SpanBuilder, SpanId};
use opentelemetry::{Context, Key, Value};
use std::collections::HashSet;
use std::ops::Add;

use once_cell::sync::Lazy;
use std::time::{Duration, SystemTime};

use tracing_opentelemetry::{OtelData, PreSampledTracer};

/// `TRACE_ID` is used to override `trace_id` on the `SpanBuilder`. It must be a hex string
const TRACE_ID: Key = Key::from_static_str("restate.internal.trace_id");
/// `SPAN_ID` is used to override `span_id` on the `SpanBuilder`. It must be a hex string
const SPAN_ID: Key = Key::from_static_str("restate.internal.span_id");
/// `START_TIME` is used to override `start_time` on the `SpanBuilder`. It must be the number of
/// millis since epoch written as a string
const START_TIME: Key = Key::from_static_str("restate.internal.start_time");
/// `START_TIME` is used to override `start_time` on the `SpanBuilder`. It must be the number of
/// millis since epoch written as a string
const END_TIME: Key = Key::from_static_str("restate.internal.end_time");

static KEYS: Lazy<HashSet<Key>> = Lazy::new(|| {
    vec![TRACE_ID, SPAN_ID, START_TIME, END_TIME]
        .into_iter()
        .collect()
});

// Tracer wraps the opentelemetry Tracer struct, in order to allow certain span fields to be overridden
// using attribute fields when spans are created.
#[derive(Debug)]
pub(crate) struct SpanModifyingTracer {
    inner: opentelemetry::sdk::trace::Tracer,
}

impl SpanModifyingTracer {
    pub(crate) fn new(inner: opentelemetry::sdk::trace::Tracer) -> Self {
        Self { inner }
    }
}

impl opentelemetry::trace::Tracer for SpanModifyingTracer {
    type Span = opentelemetry::sdk::trace::Span;

    fn build_with_context(&self, mut builder: SpanBuilder, parent_cx: &Context) -> Self::Span {
        let attributes = if let Some(attributes) = &mut builder.attributes {
            attributes
        } else {
            return self.inner.build_with_context(builder, parent_cx);
        };

        builder.trace_id = attributes
            .get(&TRACE_ID)
            .map(|trace_id| {
                TraceId::from_hex(trace_id.as_str().as_ref())
                    .expect("restate.internal.trace_id must be a valid hex string")
            })
            .or(builder.trace_id);

        builder.span_id = attributes
            .get(&SPAN_ID)
            .map(|span_id| {
                SpanId::from_hex(span_id.as_str().as_ref())
                    .expect("restate.internal.span_id must be a valid hex string")
            })
            .or(builder.span_id);

        fn time(key: Key, attributes: &mut OrderMap<Key, Value>) -> Option<SystemTime> {
            attributes.get(&key).map(|value| -> SystemTime {
                match value {
                    Value::I64(value) => {
                        SystemTime::UNIX_EPOCH.add(Duration::from_millis(*value as u64))
                    }
                    other => panic!(
                        "expected {} to be an i64, instead found {:?}",
                        key.as_str(),
                        other
                    ),
                }
            })
        }

        builder.start_time = time(START_TIME, attributes).or(builder.start_time);
        builder.end_time = time(END_TIME, attributes).or(builder.end_time);

        // now that we no longer hold references to the values, we can remove all the keys we used from attributes
        // by using retain, we can do this in a single O(n) scan, which is better than calling delete 1-4 times,
        // as a delete is also O(n)
        attributes.retain(|k, _| !KEYS.contains(k));

        self.inner.build_with_context(builder, parent_cx)
    }
}

impl PreSampledTracer for SpanModifyingTracer {
    fn sampled_context(&self, data: &mut OtelData) -> Context {
        self.inner.sampled_context(data)
    }

    fn new_trace_id(&self) -> TraceId {
        self.inner.new_trace_id()
    }

    fn new_span_id(&self) -> SpanId {
        self.inner.new_span_id()
    }
}
