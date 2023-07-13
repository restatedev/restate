use opentelemetry::sdk::export::trace::SpanData;
use opentelemetry::sdk::trace::Span;
use opentelemetry::sdk::Resource;
use opentelemetry::trace::TraceResult;
use opentelemetry::{Context, Key, KeyValue};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use std::borrow::Cow;

/// `RPC_SERVICE` is used to override `service.name` on the `SpanBuilder`
const RPC_SERVICE: Key = Key::from_static_str("rpc.service");

/// `ResourceModifyingSpanProcessor` wraps a `opentelemetry::sdk::trace::SpanProcessor` in order to allow mutating
/// the service name which is within the resource field. As this field is set just before export,
/// we are forced to intercept the actual export step (on_end).
#[derive(Debug)]
pub(crate) struct ResourceModifyingSpanProcessor<T> {
    inner: T,
}

impl<T> ResourceModifyingSpanProcessor<T> {
    pub(crate) fn new(inner: T) -> Self {
        ResourceModifyingSpanProcessor { inner }
    }
}

impl<T: opentelemetry::sdk::trace::SpanProcessor> opentelemetry::sdk::trace::SpanProcessor
    for ResourceModifyingSpanProcessor<T>
{
    fn on_start(&self, span: &mut Span, cx: &Context) {
        self.inner.on_start(span, cx)
    }

    fn on_end(&self, data: SpanData) {
        let mut data = data;

        if let Some(service_name) = data.attributes.get(&RPC_SERVICE) {
            data.resource = Cow::Owned(data.resource.merge(&Resource::new(std::iter::once(
                KeyValue::new(SERVICE_NAME, service_name.clone()),
            ))));
        };

        self.inner.on_end(data)
    }

    fn force_flush(&self) -> TraceResult<()> {
        self.inner.force_flush()
    }

    fn shutdown(&mut self) -> TraceResult<()> {
        self.inner.shutdown()
    }
}
