use opentelemetry::trace::TraceId;
use opentelemetry::trace::{SpanBuilder, SpanId};
use opentelemetry::{Context, Key, Value};
use std::collections::HashSet;
use std::ops::Add;

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

#[derive(Debug, thiserror::Error)]
enum SpanModifyingTracerError {
    #[error("{0} must be valid hex: {1}")]
    Hex(Key, std::num::ParseIntError),
    #[error("{0} must be an integer")]
    Integer(Key),
}

impl opentelemetry::trace::Tracer for SpanModifyingTracer {
    type Span = opentelemetry::sdk::trace::Span;

    fn build_with_context(&self, mut builder: SpanBuilder, parent_cx: &Context) -> Self::Span {
        let attributes = if let Some(attributes) = &mut builder.attributes {
            attributes
        } else {
            return self.inner.build_with_context(builder, parent_cx);
        };

        let trace_id = attributes.get_key_value(&TRACE_ID);
        let span_id = attributes.get_key_value(&SPAN_ID);
        let start_time = attributes.get_key_value(&START_TIME);
        let end_time = attributes.get_key_value(&END_TIME);

        // store a set of which keys were actually present
        let keys: HashSet<Key> = trace_id
            .iter()
            .chain(span_id.iter())
            .chain(start_time.iter())
            .chain(end_time.iter())
            .map(|&(k, _)| k.clone())
            .collect();

        if keys.is_empty() {
            // nothing to do
            return self.inner.build_with_context(builder, parent_cx);
        }

        builder.trace_id = trace_id
            .map(|(_, trace_id)| {
                TraceId::from_hex(trace_id.as_str().as_ref())
                    .map_err(|err| SpanModifyingTracerError::Hex(TRACE_ID, err))
                    .unwrap()
            })
            .or(builder.trace_id);

        builder.span_id = span_id
            .map(|(_, span_id)| {
                SpanId::from_hex(span_id.as_str().as_ref())
                    .map_err(|err| SpanModifyingTracerError::Hex(SPAN_ID, err))
                    .unwrap()
            })
            .or(builder.span_id);

        let time = |kv: Option<(&Key, &Value)>| {
            kv.map(
                |(key, value)| -> Result<SystemTime, SpanModifyingTracerError> {
                    let duration = match value {
                        Value::I64(value) => Duration::from_millis(*value as u64),
                        _ => return Err(SpanModifyingTracerError::Integer(key.clone())),
                    };
                    Ok(SystemTime::UNIX_EPOCH.add(duration))
                },
            )
            .transpose()
        };

        builder.start_time = time(start_time).unwrap().or(builder.start_time);
        builder.end_time = time(end_time).unwrap().or(builder.end_time);

        // now that we no longer hold references to the values, we can remove all the keys we used from attributes
        // by using retain, we can do this in a single O(n) scan, which is better than calling delete 1-4 times,
        // as a delete is also O(n)
        attributes.retain(|k, _| !keys.contains(k));

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
