// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// mod multi_service_tracer;
mod exporter;
mod pretty;

use crate::exporter::ResourceModifyingSpanExporter;
use crate::pretty::PrettyFields;
use opentelemetry::trace::{TraceError, TracerProvider};
use opentelemetry::KeyValue;
use opentelemetry_contrib::trace::exporter::jaeger_json::JaegerJsonExporter;
use opentelemetry_otlp::{SpanExporterBuilder, WithExportConfig};
use opentelemetry_sdk::trace::BatchSpanProcessor;
use pretty::Pretty;
use restate_types::config::{CommonOptions, LogFormat};
use std::collections::HashMap;
use std::env;
use std::fmt::Display;
use tonic::codegen::http::HeaderMap;
use tonic::metadata::MetadataMap;
use tracing::{info, warn, Level};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::filter::{Filtered, ParseError};
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::reload::Handle;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

#[derive(Debug, thiserror::Error)]
#[error("could not initialize tracing {trace_error}")]
pub enum Error {
    #[error("could not initialize tracing: you must specify at least `tracing_endpoint` or `tracing_json_path`")]
    InvalidTracingConfiguration,
    #[error("could not initialize tracing: {0}")]
    Tracing(#[from] TraceError),
    #[error(
        "cannot parse log configuration {} environment variable: {0}",
        EnvFilter::DEFAULT_ENV
    )]
    LogDirectiveParseError(#[from] ParseError),
}

/// creates and register a global opentelemetry tracer provider. The global
/// provider is exclusively used by the [`invocation_span!`] macro
fn build_services_tracing(common_opts: &CommonOptions) -> Result<(), Error> {
    let opts = &common_opts.tracing;

    let endpoint = match &opts
        .tracing_services_endpoint
        .as_ref()
        .or(opts.tracing_endpoint.as_ref())
    {
        Some(endpoint) => *endpoint,
        None => return Ok(()),
    };

    let resource = opentelemetry_sdk::Resource::new(vec![
        KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            "services",
        ),
        KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAMESPACE,
            "Restate",
        ),
        KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID,
            format!("{}/{}", common_opts.cluster_name(), common_opts.node_name()),
        ),
        KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
            env!("CARGO_PKG_VERSION"),
        ),
    ]);

    let header_map = HeaderMap::from_iter(HashMap::from(opts.tracing_headers.clone()));

    let exporter = SpanExporterBuilder::from(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(endpoint)
            .with_metadata(MetadataMap::from_headers(header_map.clone())),
    )
    .build_span_exporter()?;

    let exporter = ResourceModifyingSpanExporter::new(exporter);

    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_config(opentelemetry_sdk::trace::Config::default().with_resource(resource))
        .with_span_processor(
            BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio).build(),
        )
        .build();

    opentelemetry::global::set_tracer_provider(provider);

    Ok(())
}

#[allow(clippy::type_complexity)]
fn build_runtime_tracing_layer<S>(
    common_opts: &CommonOptions,
    service_name: String,
) -> Result<
    Option<Filtered<OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>, EnvFilter, S>>,
    Error,
>
where
    S: tracing::Subscriber
        + for<'span> tracing_subscriber::registry::LookupSpan<'span>
        + Send
        + Sync,
{
    let opts = &common_opts.tracing;

    let endpoint = opts
        .tracing_runtime_endpoint
        .as_ref()
        .or(opts.tracing_endpoint.as_ref());

    // only enable tracing if endpoint or json file is set.
    if endpoint.is_none() && common_opts.tracing.tracing_json_path.is_none() {
        return Ok(None);
    }

    let resource = opentelemetry_sdk::Resource::new(vec![
        KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            service_name.clone(),
        ),
        KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAMESPACE,
            "Restate",
        ),
        KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID,
            format!("{}/{}", common_opts.cluster_name(), common_opts.node_name()),
        ),
        KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_VERSION,
            env!("CARGO_PKG_VERSION"),
        ),
    ]);

    let mut tracer_provider_builder = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_config(opentelemetry_sdk::trace::Config::default().with_resource(resource));

    if let Some(endpoint) = endpoint {
        let header_map =
            HeaderMap::from_iter(HashMap::from(common_opts.tracing.tracing_headers.clone()));

        let exporter = SpanExporterBuilder::from(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint)
                .with_metadata(MetadataMap::from_headers(header_map)),
        )
        .build_span_exporter()?;

        tracer_provider_builder = tracer_provider_builder.with_span_processor(
            BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio).build(),
        );
    }

    if let Some(path) = &common_opts.tracing.tracing_json_path {
        let exporter = JaegerJsonExporter::new(
            path.into(),
            "trace".to_string(),
            service_name,
            opentelemetry_sdk::runtime::Tokio,
        );
        let exporter = ResourceModifyingSpanExporter::new(exporter);

        tracer_provider_builder = tracer_provider_builder.with_span_processor(
            BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio).build(),
        );
    }

    let provider = tracer_provider_builder.build();

    let tracer = provider
        .tracer_builder("opentelemetry-otlp")
        .with_version(env!("CARGO_PKG_VERSION"))
        .build();

    Ok(Some(
        tracing_opentelemetry::layer()
            .with_location(false)
            .with_threads(false)
            .with_tracked_inactivity(false)
            .with_tracer(tracer)
            .with_filter(EnvFilter::try_new(&opts.tracing_filter)?),
    ))
}

#[allow(clippy::type_complexity)]
fn build_logging_layer<S>(
    common_opts: &CommonOptions,
) -> Result<Box<dyn Layer<S> + Send + Sync>, Error>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    let k = match common_opts.log_format {
        LogFormat::Pretty => tracing_subscriber::fmt::layer()
            .event_format::<Pretty<SystemTime>>(Pretty::default())
            .fmt_fields(PrettyFields)
            .with_writer(
                // Write WARN and ERR to stderr, everything else to stdout
                std::io::stderr
                    .with_max_level(Level::WARN)
                    .or_else(std::io::stdout),
            )
            .with_ansi(!common_opts.log_disable_ansi_codes)
            .boxed(),
        LogFormat::Compact => tracing_subscriber::fmt::layer()
            .compact()
            .with_ansi(!common_opts.log_disable_ansi_codes)
            .boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_ansi(!common_opts.log_disable_ansi_codes)
            .boxed(),
    };
    Ok(k)
}

/// Instruments the process with logging and tracing. The method returns [`TracingGuard`] which
/// unregisters the tracing when being shut down or dropped.
///
/// # Panics
/// This method will panic if there is already a global subscriber configured. Moreover, it will
/// panic if it is executed outside of a Tokio runtime.
pub fn init_tracing_and_logging(
    common_opts: &CommonOptions,
    service_name: impl Display,
) -> Result<TracingGuard, Error> {
    let layers = tracing_subscriber::registry();

    let filter = EnvFilter::try_new(&common_opts.log_filter)?;
    let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(filter);
    // Logging layer
    let layers = layers.with(build_logging_layer(common_opts)?.with_filter(filter));
    // Enables auto extraction of selected span labels in emitted metrics.
    // allowed labels are defined in restate_node_ctrl::metrics::ALLOWED_LABELS.
    //
    // This is temporarily disabled due to its performance cost. This will be re-enabled when it
    // gets benchmarked and optimized.
    //.with(MetricsLayer::new());

    // Console subscriber layer
    #[cfg(feature = "console-subscriber")]
    let layers = layers.with(console_subscriber::spawn());

    build_services_tracing(common_opts)?;

    // Tracing layer
    let layers = layers.with(build_runtime_tracing_layer(
        common_opts,
        service_name.to_string(),
    )?);

    layers.init();

    Ok(TracingGuard {
        is_dropped: false,
        reload_handle,
    })
}

#[derive(Debug)]
pub struct TracingGuard {
    is_dropped: bool,
    reload_handle: Handle<EnvFilter, Registry>,
}

impl TracingGuard {
    /// Shuts down the tracing instrumentation.
    ///
    /// IMPORTANT: This operation is blocking and should not be run from a Tokio thread when
    /// using the multi thread runtime because it can block tasks that are required for the shut
    /// down to complete.
    pub fn shutdown(mut self) {
        opentelemetry::global::shutdown_tracer_provider();
        self.is_dropped = true;
    }

    pub fn reload_log_filter(&self, common_opts: &CommonOptions) {
        info!("Setting log filter to '{}'", common_opts.log_filter);
        let _ = &self.reload_handle.modify(|f| {
            let new_filter = EnvFilter::try_new(&common_opts.log_filter);
            match new_filter {
                Ok(new_filter) => {
                    *f = new_filter;
                }
                // don't use logging here, tracing will panic!
                Err(e) => eprintln!("Failed to reload log filter: '{}'", e),
            }
        });
    }

    /// Shuts down the tracing instrumentation by running [`shutdown`] on a blocking Tokio thread.
    #[cfg(feature = "rt-tokio")]
    pub async fn async_shutdown(self) {
        tokio::task::spawn_blocking(|| self.shutdown());
    }
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if !self.is_dropped {
            warn!(
                "Shutting down the tracer provider from the drop implementation. \
            This is a blocking operation and should not be executed from a Tokio thread, \
            because it can block tasks that are required for the shut down to complete!"
            );

            #[cfg(feature = "rt-tokio")]
            {
                if tokio::runtime::Handle::try_current().is_ok() {
                    // we are running within the Tokio runtime, try to unblock other tasks
                    tokio::task::block_in_place(|| {
                        opentelemetry::global::shutdown_tracer_provider()
                    });
                } else {
                    opentelemetry::global::shutdown_tracer_provider();
                }
            }

            #[cfg(not(feature = "rt-tokio"))]
            opentelemetry::global::shutdown_tracer_provider();
        }
    }
}

/// invocation_span macro create a span given invocation_id and invocation_target. The created span will show up
/// mainly in services tracing. It will also show up in runtime traces but in relation to other runtime spans
/// that are created normally with tracing::span!
///
/// level: [`Level`]
/// prefix: static span name
/// id: ref to an instance of [`InvocationId`]
/// target: ref to an instance of [`InvocationTarget`]
/// tags: is a list of any extra tags that need to be associated with this span for example `tags = (client.ip = "10.20.30.40")`
/// fields [optional]: is a list of extra custom span builder fields that can be used to override the default ones for example `fields = (with_span_id = 10)`
#[macro_export]
macro_rules! invocation_span {
    (level= $lvl:expr, relation = $relation:expr, prefix= $prefix:expr, id= $id:expr, target= $target:expr, tags=($($($key:ident).+ = $value:expr),*), fields=($($field:ident = $field_value:expr),*)) => {
        {
            use opentelemetry::KeyValue;

            let attributes = vec![
                KeyValue::new("rpc.service", $target.service_name().to_string()),
                KeyValue::new("rpc.method", $target.handler_name().to_string()),
                KeyValue::new("restate.invocation.id", $id.to_string()),
                KeyValue::new("restate.invocation.target", $target.to_string()),
                $(KeyValue::new(stringify!($($key).+), $value),)*
            ];

            $crate::invocation_span!(
                level = $lvl,
                relation = $relation,
                prefix = $prefix,
                name = format!("{} {}", $prefix, $target.short()),
                attributes=attributes,
                fields=($($field = $field_value),*)
            )
        }
    };
    (level= $lvl:expr, relation = $relation:expr, prefix= $prefix:expr, id= $id:expr, name= $name:expr, tags=($($($key:ident).+ = $value:expr),*), fields=($($field:ident = $field_value:expr),*)) => {
        {
            use opentelemetry::KeyValue;

            let attributes = vec![
                KeyValue::new("restate.invocation.id", $id.to_string()),
                $(KeyValue::new(stringify!($($key).+), $value),)*
            ];

            $crate::invocation_span!(
                level = $lvl,
                relation = $relation,
                prefix = $prefix,
                name = format!("{} {}", $prefix, $name),
                attributes=attributes,
                fields=($($field = $field_value),*)
            )
        }
    };
    (level= $lvl:expr, relation=$relation:expr, prefix= $prefix:expr, name= $name:expr, attributes=$attributes:ident, fields=($($field:ident = $field_value:expr),*)) => {
        {
            use opentelemetry::{KeyValue, Context, trace::{Tracer, Link, TracerProvider, TraceContextExt}};
            use restate_types::invocation::SpanRelation;
            use tracing_opentelemetry::OpenTelemetrySpanExt;

            let tracer = opentelemetry::global::tracer_provider()
                .tracer_builder("services")
                .build();

            let builder = tracer
                .span_builder($name)
                $(.$field($field_value))*
                .with_attributes($attributes);

            let mut links = vec![Link::with_context(
                ::tracing::Span::current()
                    .context()
                    .span()
                    .span_context()
                    .clone(),
            )];

            if let SpanRelation::Linked(ref ctx) = $relation {
                links.push(Link::new(ctx.clone(), vec![KeyValue::new("restate.runtime", true)], 0));
            }

            let builder = builder.with_links(links);

            let span = match $relation {
                SpanRelation::None | SpanRelation::Linked(_) => builder.start(&tracer),
                SpanRelation::Parent(ctx) => builder.start_with_context(&tracer,&Context::new().with_remote_span_context(ctx)),
            };

            span
        }
    }
}

/// info_invocation_span is a shortcut for [`invocation_span!`] with `Info` level
#[macro_export]
macro_rules! info_invocation_span {
    (relation=$relation:expr, prefix= $prefix:expr, id= $id:expr, target=$target:expr, tags=($($($key:ident).+ = $value:expr),*), fields=($($field:ident = $field_value:expr),*)) => {
        $crate::invocation_span!(
            level = ::tracing::Level::INFO,
            relation = $relation,
            prefix = $prefix,
            id = $id,
            target = $target,
            tags=($($($key).+ = $value),*),
            fields=($($field = $field_value),*)
        )
    };
    (relation=$relation:expr, prefix= $prefix:expr, id= $id:expr, target= $target:expr, tags=($($($key:ident).+ = $value:expr),*)) => {
        $crate::invocation_span!(
            level = ::tracing::Level::INFO,
            relation = $relation,
            prefix = $prefix,
            id = $id,
            target = $target,
            tags = ($($($key).+ = $value),*),
            fields = ()
        )
    };
    (relation=$relation:expr, prefix= $prefix:expr, id= $id:expr, name=$name:expr, tags=($($($key:ident).+ = $value:expr),*), fields=($($field:ident = $field_value:expr),*)) => {
        $crate::invocation_span!(
            level = ::tracing::Level::INFO,
            relation = $relation,
            prefix = $prefix,
            id = $id,
            name = $name,
            tags=($($($key).+ = $value),*),
            fields=($($field = $field_value),*)
        )
    };
    (relation=$relation:expr, prefix= $prefix:expr, id= $id:expr, name= $name:expr, tags=($($($key:ident).+ = $value:expr),*)) => {
        $crate::invocation_span!(
            level = ::tracing::Level::INFO,
            relation = $relation,
            prefix = $prefix,
            id = $id,
            name = $name,
            tags = ($($($key).+ = $value),*),
            fields = ()
        )
    };
}

#[cfg(test)]
mod test {

    use opentelemetry::trace::SpanId;
    use restate_types::invocation::InvocationTarget;

    #[test]
    fn test_macro() {
        let target = InvocationTarget::mock_virtual_object();

        // verification of macro call syntax
        let _span = super::info_invocation_span!(
            relation = SpanRelation::None,
            prefix = "aaa",
            id = "hello",
            target = target,
            tags = (hello.world = 10, error = true),
            fields = (with_span_id = SpanId::from(10))
        );
    }
}
