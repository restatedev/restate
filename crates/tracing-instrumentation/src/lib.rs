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
mod pretty;
mod processor;
mod tracer;

use crate::pretty::PrettyFields;
use crate::processor::ResourceModifyingSpanProcessor;
use crate::tracer::SpanModifyingTracer;
use metrics_tracing_context::MetricsLayer;
use opentelemetry::trace::{TraceError, TracerProvider};
use opentelemetry::KeyValue;
use opentelemetry_contrib::trace::exporter::jaeger_json::JaegerJsonExporter;
use opentelemetry_otlp::{SpanExporterBuilder, WithExportConfig};
use opentelemetry_sdk::trace::BatchSpanProcessor;
use pretty::Pretty;
use restate_types::config::{CommonOptions, LogFormat};
use std::env;
use std::fmt::Display;
use tracing::{info, warn, Level};
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

#[allow(clippy::type_complexity)]
fn build_tracing_layer<S>(
    common_opts: &CommonOptions,
    service_name: String,
) -> Result<
    Option<
        Filtered<tracing_opentelemetry::OpenTelemetryLayer<S, SpanModifyingTracer>, EnvFilter, S>,
    >,
    Error,
>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    // only enable tracing if endpoint or json file is set.
    if common_opts.tracing_endpoint.is_none() && common_opts.tracing_json_path.is_none() {
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

    // the following logic is based on `opentelemetry_otlp::span::build_batch_with_exporter`
    // but also injecting ResourceModifyingSpanProcessor around the BatchSpanProcessor

    let mut tracer_provider_builder = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_config(opentelemetry_sdk::trace::config().with_resource(resource));

    if let Some(endpoint) = &common_opts.tracing_endpoint {
        let exporter = SpanExporterBuilder::from(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint),
        )
        .build_span_exporter()?;
        tracer_provider_builder =
            tracer_provider_builder.with_span_processor(ResourceModifyingSpanProcessor::new(
                BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio).build(),
            ));
    }

    if let Some(path) = &common_opts.tracing_json_path {
        tracer_provider_builder =
            tracer_provider_builder.with_span_processor(ResourceModifyingSpanProcessor::new(
                BatchSpanProcessor::builder(
                    JaegerJsonExporter::new(
                        path.into(),
                        "trace".to_string(),
                        service_name,
                        opentelemetry_sdk::runtime::Tokio,
                    ),
                    opentelemetry_sdk::runtime::Tokio,
                )
                .build(),
            ));
    }

    let provider = tracer_provider_builder.build();

    let tracer = SpanModifyingTracer::new(provider.versioned_tracer(
        "opentelemetry-otlp",
        Some(env!("CARGO_PKG_VERSION")),
        None::<&'static str>,
        None,
    ));
    let _ = opentelemetry::global::set_tracer_provider(provider);

    Ok(Some(
        tracing_opentelemetry::layer()
            .with_location(false)
            .with_threads(false)
            .with_tracked_inactivity(false)
            .with_tracer(tracer)
            .with_filter(EnvFilter::try_new(&common_opts.tracing_filter)?),
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
    let restate_service_name = format!("Restate service: {service_name}");

    let layers = tracing_subscriber::registry();

    let filter = EnvFilter::try_new(&common_opts.log_filter)?;
    let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(filter);
    // Logging layer
    let layers = layers
        .with(build_logging_layer(common_opts)?.with_filter(filter))
        // Enables auto extraction of selected span labels in emitted metrics.
        // allowed labels are defined in restate_node_ctrl::metrics::ALLOWED_LABELS.
        .with(MetricsLayer::new());

    // Console subscriber layer
    #[cfg(feature = "console-subscriber")]
    let layers = layers.with(console_subscriber::spawn());

    // Tracing layer
    let layers = layers.with(build_tracing_layer(
        common_opts,
        restate_service_name.clone(),
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
