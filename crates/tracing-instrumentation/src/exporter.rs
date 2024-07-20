// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::future::BoxFuture;
use opentelemetry::{Key, KeyValue, StringValue, Value};
use opentelemetry_sdk::export::trace::SpanData;
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use std::collections::HashMap;
use std::sync::Arc;

/// `RPC_SERVICE` is used to override `service.name` on the `SpanBuilder`
const RPC_SERVICE: Key = Key::from_static_str("rpc.service");

/// `ResourceModifyingSpanExporter` wraps a `opentelemetry::sdk::trace::SpanExporter` in order to allow mutating
/// the service name which is within the resource field. As this field is set during export,
/// we are forced to intercept the export
#[derive(Debug)]
pub(crate) struct ResourceModifyingSpanExporter<T> {
    inner: Arc<ResourceModifyingSpanExporterInner<T>>,
}

#[derive(Debug)]
struct ResourceModifyingSpanExporterInner<T> {
    exporter: T,
    resource: Resource,
}

impl<T> ResourceModifyingSpanExporter<T> {
    pub(crate) fn new(inner: T) -> Self {
        ResourceModifyingSpanExporter {
            inner: Arc::new(ResourceModifyingSpanExporterInner {
                exporter: inner,
                resource: Resource::empty(),
            }),
        }
    }
}

impl<T: opentelemetry_sdk::export::trace::SpanExporter + 'static>
    opentelemetry_sdk::export::trace::SpanExporter for ResourceModifyingSpanExporter<T>
{
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        let mut spans_by_service =
            HashMap::<Option<StringValue>, Vec<SpanData>>::with_capacity(batch.len());

        for span in batch {
            if let Some(KeyValue {
                value: Value::String(string_value),
                ..
            }) = span.attributes.iter().find(|kv| kv.key == RPC_SERVICE)
            {
                spans_by_service
                    .entry(Some(string_value.clone()))
                    .or_default()
                    .push(span);
            } else {
                spans_by_service.entry(None).or_default().push(span);
            }
        }

        let mut inner = self.inner.clone();

        Box::pin(async move {
            let inner = Arc::get_mut(&mut inner).unwrap();
            for (service_name, batch) in spans_by_service.into_iter() {
                match service_name {
                    None => inner.exporter.set_resource(&inner.resource),
                    Some(service_name) => {
                        inner
                            .exporter
                            .set_resource(&inner.resource.merge(&Resource::new(std::iter::once(
                                KeyValue::new(SERVICE_NAME, service_name),
                            ))))
                    }
                }
                inner.exporter.export(batch).await?
            }
            Ok(())
        })
    }

    fn shutdown(&mut self) {
        let inner = Arc::get_mut(&mut self.inner).unwrap();
        inner.exporter.shutdown()
    }

    fn force_flush(
        &mut self,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        let inner = Arc::get_mut(&mut self.inner).unwrap();
        inner.exporter.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        let inner = Arc::get_mut(&mut self.inner).unwrap();
        inner.resource = resource.clone();
    }
}
