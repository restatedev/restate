// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arc_swap::ArcSwap;
use futures::future::BoxFuture;
use opentelemetry::trace::TraceError;
use opentelemetry::{Key, KeyValue, StringValue, Value};
use opentelemetry_sdk::export::trace::SpanData;
use opentelemetry_sdk::runtime::Runtime;
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// `RPC_SERVICE` is used to override `service.name` on the `SpanBuilder`
const RPC_SERVICE: Key = Key::from_static_str("rpc.service");

/// `ResourceModifyingSpanExporter` wraps a `opentelemetry::sdk::trace::SpanExporter` in order to allow mutating
/// the service name which is within the resource field. As this field is set during export,
/// we are forced to intercept the export
#[derive(Debug)]
pub(crate) struct ResourceModifyingSpanExporter<T, R> {
    exporter: Option<Arc<Mutex<T>>>,
    resource: ArcSwap<Resource>,
    runtime: R,
}

impl<T, R> ResourceModifyingSpanExporter<T, R> {
    pub(crate) fn new(inner: T, runtime: R) -> Self {
        ResourceModifyingSpanExporter {
            exporter: Some(Arc::new(Mutex::new(inner))),
            resource: ArcSwap::from_pointee(Resource::empty()),
            runtime,
        }
    }
}

impl<T: opentelemetry_sdk::export::trace::SpanExporter + 'static, R: Runtime + std::fmt::Debug>
    opentelemetry_sdk::export::trace::SpanExporter for ResourceModifyingSpanExporter<T, R>
{
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        let exporter = match &self.exporter {
            Some(exporter) => exporter.clone(),
            None => {
                return Box::pin(std::future::ready(Err(TraceError::Other(
                    "exporter is already shut down".into(),
                ))))
            }
        };

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

        let resource = self.resource.load();

        Box::pin(async move {
            let mut exporter = match exporter.try_lock() {
                Ok(exporter) => exporter,
                Err(_) => {
                    return Err(TraceError::Other("export on SpanExporter has been called concurrently in violation of its contract".into()))
                }
            };
            for (service_name, batch) in spans_by_service.into_iter() {
                match service_name {
                    None => exporter.set_resource(&resource),
                    Some(service_name) => exporter.set_resource(&resource.merge(&Resource::new(
                        std::iter::once(KeyValue::new(SERVICE_NAME, service_name)),
                    ))),
                }
                exporter.export(batch).await?;
            }
            Ok(())
        })
    }

    fn shutdown(&mut self) {
        if let Some(exporter) = self.exporter.take() {
            if let Ok(mut exporter) = exporter.try_lock() {
                // this is the case we expect; that shutdown is only called when all export calls have been awaited
                return exporter.shutdown();
            }
            // but for completeness, cover the contested case too
            // wait for existing export() to complete, then shutdown underlying exporter
            _ = self.runtime.spawn(Box::pin(async move {
                let mut e = exporter.lock().await;
                e.shutdown()
            }));
        }
    }

    fn force_flush(
        &mut self,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        let exporter = match &self.exporter {
            Some(exporter) => exporter.clone(),
            None => {
                return Box::pin(std::future::ready(Err(TraceError::Other(
                    "exporter is already shut down".into(),
                ))))
            }
        };
        Box::pin(async move {
            // wait for any in-flight export to finish
            let mut exporter = exporter.lock().await;
            // wait for flush on the underlying exporter
            exporter.force_flush().await
        })
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource.store(Arc::new(resource.clone()))
    }
}
