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
use opentelemetry::trace::{Link, SpanContext, SpanId, TraceError, TraceId};
use opentelemetry::{Key, KeyValue, StringValue, Value};
use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// `RPC_SERVICE` is used to override `service.name` on the `SpanBuilder`
const RPC_SERVICE: Key = Key::from_static_str("rpc.service");
/// a marker we use to tell user services related spans apart from runtime tracing spans
const USER_SERVICE_KEY: Key = Key::from_static_str("restate.user.service");

/// `ResourceModifyingSpanExporter` wraps a `opentelemetry::sdk::trace::SpanExporter` in order to allow mutating
/// the service name which is within the resource field. As this field is set during export,
/// we are forced to intercept the export
#[derive(Debug)]
pub(crate) struct ResourceModifyingSpanExporter<T> {
    exporter: Option<Arc<Mutex<T>>>,
    resource: ArcSwap<Resource>,
}

impl<T> ResourceModifyingSpanExporter<T> {
    pub(crate) fn new(inner: T) -> Self {
        ResourceModifyingSpanExporter {
            exporter: Some(Arc::new(Mutex::new(inner))),
            resource: ArcSwap::from_pointee(Resource::empty()),
        }
    }
}

impl<T: SpanExporter + 'static> SpanExporter for ResourceModifyingSpanExporter<T> {
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

/// rotates a given ID around midpoint
/// it's used to generate unique trace ids and span ids
/// that are deterministic
macro_rules! rotate_half {
    ($type:tt, $id:expr) => {{
        let mut bytes = $id.to_bytes();
        let mid = bytes.len() / 2;
        bytes.rotate_left(mid);
        $type::from_bytes(bytes)
    }};
}

#[derive(Debug)]
/// this is an exporter that can classify and separate spans that related
/// to runtime tracing from services spans.
///
/// After classification, **all** spans (including services spans/traces) are sent to the inner runtime exporter
/// and the services spans are then handed over to the inner services exporter (if set)
///
/// To make it possible to use the same exporter for both runtime and services spans. The services traces/spans ID
/// are deterministically rotated from the original ids. This maintains the parent child relationship of services
/// spans
pub struct RuntimeServicesExporter<A, B> {
    runtime: Arc<Mutex<A>>,
    services: Option<Arc<Mutex<B>>>,
}

impl<A, B> RuntimeServicesExporter<A, B> {
    pub fn new(runtime: A, services: Option<B>) -> Self {
        Self {
            runtime: Arc::new(Mutex::new(runtime)),
            services: services.map(|s| Arc::new(Mutex::new(s))),
        }
    }
}

impl<A, B> SpanExporter for RuntimeServicesExporter<A, B>
where
    A: SpanExporter + 'static,
    B: SpanExporter + 'static,
{
    fn export(
        &mut self,
        mut batch: Vec<SpanData>,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        let runtime = Arc::clone(&self.runtime);
        let services = self.services.clone();

        let mut services_batch = Vec::default();
        // only build services traces if services exporter is set
        if self.services.is_some() {
            for span in batch.iter_mut() {
                if span.attributes.iter().any(|kv| kv.key == USER_SERVICE_KEY) {
                    span.attributes.retain(|e| e.key != USER_SERVICE_KEY);

                    let mut service_span = span.clone();
                    let ctx = service_span.span_context;
                    service_span.span_context = SpanContext::new(
                        rotate_half!(TraceId, ctx.trace_id()),
                        rotate_half!(SpanId, ctx.span_id()),
                        ctx.trace_flags(),
                        ctx.is_remote(),
                        ctx.trace_state().clone(),
                    );

                    // link this span to original runtime span
                    service_span.links.links.push(Link::new(ctx, vec![], 0));
                    service_span.parent_span_id = rotate_half!(SpanId, service_span.parent_span_id);

                    services_batch.push(service_span);
                }
            }
        }

        Box::pin(async move {
            {
                let mut a = match runtime.lock() {
                    Ok(a) => a,
                    Err(_) => return Err(TraceError::Other("exporter mutex is poisoned".into())),
                };
                a.export(batch)
            }
            .await?;

            if services_batch.is_empty() {
                return Ok(());
            }

            {
                if let Some(services) = services {
                    let mut services = match services.lock() {
                        Ok(b) => b,
                        Err(_) => {
                            return Err(TraceError::Other("exporter mutex is poisoned".into()))
                        }
                    };
                    services.export(services_batch)
                } else {
                    Box::pin(async { Ok(()) })
                }
            }
            .await
        })
    }

    fn force_flush(
        &mut self,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        let runtime = Arc::clone(&self.runtime);
        let services = self.services.clone();
        Box::pin(async move {
            {
                let mut runtime = match runtime.lock() {
                    Ok(runtime) => runtime,
                    Err(_) => return Err(TraceError::Other("exporter mutex is poisoned".into())),
                };
                runtime.force_flush()
            }
            .await?;

            {
                if let Some(services) = services {
                    let mut services = match services.lock() {
                        Ok(services) => services,
                        Err(_) => {
                            return Err(TraceError::Other("exporter mutex is poisoned".into()))
                        }
                    };
                    services.force_flush()
                } else {
                    Box::pin(async { Ok(()) })
                }
            }
            .await
        })
    }

    fn set_resource(&mut self, resource: &Resource) {
        if let Ok(mut a) = self.runtime.lock() {
            a.set_resource(resource);
        }

        if let Some(ref services) = self.services {
            if let Ok(mut b) = services.lock() {
                b.set_resource(resource)
            }
        }
    }

    fn shutdown(&mut self) {
        if let Ok(mut a) = self.runtime.lock() {
            a.shutdown()
        }

        if let Some(ref services) = self.services {
            if let Ok(mut b) = services.lock() {
                b.shutdown()
            }
        }
    }
}
