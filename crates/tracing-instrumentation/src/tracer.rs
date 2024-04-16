// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opentelemetry::trace::TraceId;
use opentelemetry::trace::{SpanBuilder, SpanId};
use opentelemetry::{Context, Key, Value};
use std::cmp;
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
    inner: opentelemetry_sdk::trace::Tracer,
}

impl SpanModifyingTracer {
    pub(crate) fn new(inner: opentelemetry_sdk::trace::Tracer) -> Self {
        Self { inner }
    }
}

impl opentelemetry::trace::Tracer for SpanModifyingTracer {
    type Span = opentelemetry_sdk::trace::Span;

    fn build_with_context(&self, mut builder: SpanBuilder, parent_cx: &Context) -> Self::Span {
        let attributes = if let Some(attributes) = &mut builder.attributes {
            attributes
        } else {
            return self.inner.build_with_context(builder, parent_cx);
        };

        let mut trace_id: Option<&Value> = None;
        let mut span_id: Option<&Value> = None;
        let mut start_time: Option<&Value> = None;
        let mut end_time: Option<&Value> = None;

        for attr in attributes.iter() {
            if trace_id.is_none() && attr.key == TRACE_ID {
                trace_id = Some(&attr.value);
            } else if span_id.is_none() && attr.key == SPAN_ID {
                span_id = Some(&attr.value);
            } else if start_time.is_none() && attr.key == START_TIME {
                start_time = Some(&attr.value);
            } else if end_time.is_none() && attr.key == END_TIME {
                end_time = Some(&attr.value);
            } else if trace_id.is_some()
                && span_id.is_some()
                && start_time.is_some()
                && end_time.is_some()
            {
                break;
            }
        }

        builder.trace_id = trace_id
            .map(|trace_id| {
                TraceId::from_hex(trace_id.as_str().as_ref())
                    .expect("restate.internal.trace_id must be a valid hex string")
            })
            .or(builder.trace_id);

        builder.span_id = span_id
            .map(|span_id| {
                SpanId::from_hex(span_id.as_str().as_ref())
                    .expect("restate.internal.span_id must be a valid hex string")
            })
            .or(builder.span_id);

        fn time(key: Key, attribute: Option<&Value>) -> Option<SystemTime> {
            attribute.map(|value| -> SystemTime {
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

        builder.start_time = time(START_TIME, start_time).or(builder.start_time);
        // Because we might be messing up with the start/end time due to the fact that we set some of these manually in the attributes,
        // and not enforce them through the API, we use this additional check to make sure that we don't generate spans with negative duration.
        builder.end_time = cmp::max(
            time(END_TIME, end_time).or(builder.end_time),
            builder.start_time,
        );

        // now that we no longer hold references to the values, we can remove all the keys we used from attributes
        // by using retain, we can do this in a single O(n) scan, which is better than calling delete 1-4 times,
        // as a delete is also O(n)
        attributes.retain(|kv| !KEYS.contains(&kv.key));

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
