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
use opentelemetry::sdk::trace::BatchSpanProcessor;
use opentelemetry::trace::{TraceError, TracerProvider};
use opentelemetry_contrib::trace::exporter::jaeger_json::JaegerJsonExporter;
use opentelemetry_otlp::{SpanExporterBuilder, WithExportConfig};
use pretty::Pretty;
use std::fmt::Display;
use tracing::{warn, Level};
use tracing_subscriber::filter::{Filtered, ParseError};
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[derive(Debug, thiserror::Error)]
#[error("could not initialize tracing {trace_error}")]
pub enum Error {
    #[error("could not initialize tracing: you must specify at least `endpoint` or `json_file_export_path`")]
    InvalidTracingConfiguration,
    #[error("could not initialize tracing: {0}")]
    Tracing(#[from] TraceError),
    #[error(
        "cannot parse log configuration {} environment variable: {0}",
        EnvFilter::DEFAULT_ENV
    )]
    LogDirectiveParseError(#[from] ParseError),
}

fn default_filter() -> String {
    "info".to_string()
}

/// # Tracing Options
///
/// Configuration for the [OTLP exporter](https://opentelemetry.io/docs/specs/otel/protocol/exporter/) which can export to all OTLP compatible systems (e.g. Jaeger).
///
/// At least `endpoint` or `json_file_export_path` must be configured.
///
/// To configure the sampling, please refer to the [opentelemetry autoconfigure docs](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#sampler).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
pub struct TracingOptions {
    /// # Endpoint
    ///
    /// Specify the tracing endpoint to send traces to.
    /// Traces will be exported using [OTLP gRPC](https://opentelemetry.io/docs/specs/otlp/#otlpgrpc)
    /// through [opentelemetry_otlp](https://docs.rs/opentelemetry-otlp/0.12.0/opentelemetry_otlp/).
    endpoint: Option<String>,

    /// # JSON File export path
    ///
    /// If set, an exporter will be configured to write traces to files using the Jaeger JSON format.
    /// Each trace file will start with the `trace` prefix.
    ///
    /// If unset, no traces will be written to file.
    ///
    /// It can be used to export traces in a structured format without configuring a Jaeger agent.
    ///
    /// To inspect the traces, open the Jaeger UI and use the Upload JSON feature to load and inspect them.
    json_file_export_path: Option<String>,

    /// # Filter
    ///
    /// Exporter filter configuration.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    #[serde(default = "default_filter")]
    #[builder(default = "default_filter()")]
    filter: String,
}

impl TracingOptions {
    pub(crate) fn build_layer<S>(
        &self,
        service_name: String,
        instance_id: impl Display,
    ) -> Result<
        Filtered<tracing_opentelemetry::OpenTelemetryLayer<S, SpanModifyingTracer>, EnvFilter, S>,
        Error,
    >
    where
        S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        if self.endpoint.is_none() && self.json_file_export_path.is_none() {
            return Err(Error::InvalidTracingConfiguration);
        }

        let resource = opentelemetry::sdk::Resource::new(vec![
            opentelemetry_semantic_conventions::resource::SERVICE_NAME.string(service_name.clone()),
            opentelemetry_semantic_conventions::resource::SERVICE_NAMESPACE.string("Restate"),
            opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID
                .string(instance_id.to_string()),
            opentelemetry_semantic_conventions::resource::SERVICE_VERSION
                .string(env!("CARGO_PKG_VERSION")),
        ]);

        // the following logic is based on `opentelemetry_otlp::span::build_batch_with_exporter`
        // but also injecting ResourceModifyingSpanProcessor around the BatchSpanProcessor

        let mut tracer_provider_builder = opentelemetry::sdk::trace::TracerProvider::builder()
            .with_config(opentelemetry::sdk::trace::config().with_resource(resource));

        if let Some(endpoint) = &self.endpoint {
            let exporter = SpanExporterBuilder::from(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(endpoint),
            )
            .build_span_exporter()?;
            tracer_provider_builder =
                tracer_provider_builder.with_span_processor(ResourceModifyingSpanProcessor::new(
                    BatchSpanProcessor::builder(exporter, opentelemetry::runtime::Tokio).build(),
                ));
        }

        if let Some(path) = &self.json_file_export_path {
            tracer_provider_builder =
                tracer_provider_builder.with_span_processor(ResourceModifyingSpanProcessor::new(
                    BatchSpanProcessor::builder(
                        JaegerJsonExporter::new(
                            path.into(),
                            "trace".to_string(),
                            service_name,
                            opentelemetry::runtime::Tokio,
                        ),
                        opentelemetry::runtime::Tokio,
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

        Ok(tracing_opentelemetry::layer()
            .with_location(false)
            .with_threads(false)
            .with_tracked_inactivity(false)
            .with_tracer(tracer)
            .with_filter(EnvFilter::try_new(&self.filter)?))
    }
}

/// # Log format
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
pub enum LogFormat {
    /// # Pretty
    ///
    /// Enables verbose logging. Not recommended in production.
    #[default]
    Pretty,
    /// # Compact
    ///
    /// Enables compact logging.
    Compact,
    /// # Json
    ///
    /// Enables json logging. You can use a json log collector to ingest these logs and further process them.
    Json,
}

/// # Log options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[builder(default)]
pub struct LogOptions {
    /// # Filter
    ///
    /// Log filter configuration. Can be overridden by the `RUST_LOG` environment variable.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "LogOptions::default_filter")
    )]
    filter: String,

    /// # Log format
    ///
    /// Format to use when logging.
    #[cfg_attr(feature = "options_schema", schemars(default))]
    format: LogFormat,

    /// # Disable ANSI log
    ///
    /// Disable ANSI terminal codes for logs. This is useful when the log collector doesn't support processing ANSI terminal codes.
    #[cfg_attr(feature = "options_schema", schemars(default))]
    disable_ansi_codes: bool,
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            filter: LogOptions::default_filter(),
            format: Default::default(),
            disable_ansi_codes: false,
        }
    }
}

impl LogOptions {
    fn default_filter() -> String {
        "warn,restate=info".to_string()
    }

    #[allow(clippy::type_complexity)]
    fn build_layer<S>(
        &self,
    ) -> Result<Filtered<Box<dyn Layer<S> + Send + Sync>, EnvFilter, S>, Error>
    where
        S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        let filter = EnvFilter::try_new(&self.filter)?;
        Ok(match self.format {
            LogFormat::Pretty => tracing_subscriber::fmt::layer()
                .event_format::<Pretty<SystemTime>>(Pretty::default())
                .fmt_fields(PrettyFields)
                .with_writer(
                    // Write WARN and ERR to stderr, everything else to stdout
                    std::io::stderr
                        .with_max_level(Level::WARN)
                        .or_else(std::io::stdout),
                )
                .with_ansi(!self.disable_ansi_codes)
                .boxed()
                .with_filter(filter),
            LogFormat::Compact => tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(!self.disable_ansi_codes)
                .boxed()
                .with_filter(filter),
            LogFormat::Json => tracing_subscriber::fmt::layer()
                .json()
                .with_ansi(!self.disable_ansi_codes)
                .boxed()
                .with_filter(filter),
        })
    }
}

/// # Observability options
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "ObservabilityOptions"))]
pub struct Options {
    /// # Tracing options
    #[builder(default)]
    tracing: Option<TracingOptions>,

    /// # Logging options
    #[cfg_attr(feature = "options_schema", schemars(default))]
    #[builder(default)]
    log: LogOptions,
}

impl Options {
    /// Instruments the process with logging and tracing. The method returns [`TracingGuard`] which
    /// unregisters the tracing when being shut down or dropped.
    ///
    /// # Panics
    /// This method will panic if there is already a global subscriber configured. Moreover, it will
    /// panic if it is executed outside of a Tokio runtime.
    pub fn init(
        &self,
        service_name: impl Display,
        instance_id: impl Display,
    ) -> Result<TracingGuard, Error> {
        let restate_service_name = format!("Restate service: {service_name}");

        let layers = tracing_subscriber::registry();

        // Logging layer
        let layers = layers.with(self.log.build_layer()?);

        // Console subscriber layer
        #[cfg(feature = "console-subscriber")]
        let layers = layers.with(console_subscriber::spawn());

        // Tracing layer
        let layers = layers.with(
            self.tracing
                .as_ref()
                .map(|tracing_options| {
                    tracing_options.build_layer(restate_service_name.clone(), instance_id)
                })
                .transpose()?,
        );

        layers.init();

        Ok(TracingGuard::default())
    }
}

#[derive(Debug, Default)]
pub struct TracingGuard {
    is_dropped: bool,
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
