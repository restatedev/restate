// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use futures::future::BoxFuture;
use opentelemetry::trace::TraceError;
use opentelemetry::{Key, KeyValue, StringValue, Value};
use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tokio::sync::OnceCell;

use restate_types::GenerationalNodeId;

/// `RPC_SERVICE` is used to override `service.name` on the `SpanBuilder`
const RPC_SERVICE: Key = Key::from_static_str("rpc.service");

static GLOBAL_NODE_ID: OnceCell<GenerationalNodeId> = OnceCell::const_new();

pub fn set_global_node_id(node_id: GenerationalNodeId) {
    GLOBAL_NODE_ID
        .set(node_id)
        .expect("Global NodeId is not set")
}
/// `ResourceModifyingSpanExporter` wraps a `opentelemetry::sdk::trace::SpanExporter` in order to allow mutating
/// the service name which is within the resource field. As this field is set during export,
/// we are forced to intercept the export
#[derive(Debug)]
pub(crate) struct UserServiceModifierSpanExporter<T> {
    exporter: Option<Arc<Mutex<T>>>,
    resource: ArcSwap<Resource>,
}

impl<T> UserServiceModifierSpanExporter<T> {
    pub(crate) fn new(inner: T) -> Self {
        UserServiceModifierSpanExporter {
            exporter: Some(Arc::new(Mutex::new(inner))),
            resource: ArcSwap::from_pointee(Resource::empty()),
        }
    }
}

impl<T: SpanExporter + 'static> SpanExporter for UserServiceModifierSpanExporter<T> {
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
            for (service_name, batch) in spans_by_service.into_iter() {
                {
                    let mut exporter_guard = match exporter.lock() {
                        Ok(exporter) => exporter,
                        Err(_) => {
                            return Err(TraceError::Other("exporter mutex is poisoned".into()))
                        }
                    };
                    match service_name {
                        None => exporter_guard.set_resource(&resource),
                        Some(service_name) => {
                            exporter_guard.set_resource(&resource.merge(&Resource::new(
                                std::iter::once(KeyValue::new(SERVICE_NAME, service_name)),
                            )))
                        }
                    }
                    exporter_guard.export(batch)
                }
                .await?;
            }
            Ok(())
        })
    }

    fn shutdown(&mut self) {
        if let Some(exporter) = self.exporter.take() {
            // wait for any in-flight export to finish
            if let Ok(mut exporter) = exporter.lock() {
                exporter.shutdown()
            }
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
        // wait for any in-flight export to finish
        let mut exporter_guard = match exporter.lock() {
            Ok(exporter_guard) => exporter_guard,
            Err(_) => {
                return Box::pin(std::future::ready(Err(TraceError::Other(
                    "exporter mutex is poisoned".into(),
                ))))
            }
        };
        let fut = exporter_guard.force_flush();
        Box::pin(fut)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource.store(Arc::new(resource.clone()))
    }
}

#[derive(Debug)]
pub(crate) struct RuntimeModifierSpanExporter<E>
where
    E: SpanExporter,
{
    inner: E,
    resources: Resource,
    injected: bool,
}

impl<E> RuntimeModifierSpanExporter<E>
where
    E: SpanExporter,
{
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            resources: Resource::empty(),
            injected: false,
        }
    }
}

impl<E> SpanExporter for RuntimeModifierSpanExporter<E>
where
    E: SpanExporter,
{
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        if !self.injected && GLOBAL_NODE_ID.initialized() {
            self.injected = true;
            let resources = self.resources.clone();
            let resources = resources.merge(&Resource::new(vec![KeyValue::new(
                SERVICE_INSTANCE_ID,
                GLOBAL_NODE_ID
                    .get()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "UNKNOWN".to_owned()),
            )]));

            self.inner.set_resource(&resources);
        }

        self.inner.export(batch)
    }

    fn force_flush(
        &mut self,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resources = resource.clone();
        self.inner.set_resource(resource);
    }

    fn shutdown(&mut self) {
        self.inner.shutdown();
    }
}
