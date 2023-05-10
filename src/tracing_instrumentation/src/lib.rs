mod pretty;

use crate::pretty::PrettyFields;
use opentelemetry::trace::TraceError;
use opentelemetry_contrib::trace::exporter::jaeger_json::JaegerJsonExporter;
use pretty::Pretty;
use std::fmt::Display;
use tracing::Level;
use tracing_subscriber::filter::{Filtered, ParseError};
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[derive(Debug, thiserror::Error)]
#[error("could not initialize tracing {trace_error}")]
pub enum Error {
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

/// # Jaeger Options
///
/// Configuration for the [Jaeger Agent exporter](https://www.jaegertracing.io/docs/1.6/deployment/#agent).
///
/// To configure the sampling, please refer to the [opentelemetry autoconfigure docs](https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk-extensions/autoconfigure/README.md#sampler).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
struct JaegerOptions {
    /// # Endpoint
    ///
    /// Specify the Jaeger endpoint to use to send traces.
    /// Traces will be exported using the [Jaeger Agent UDP protocol](https://www.jaegertracing.io/docs/1.6/deployment/#agent)
    /// through [opentelemetry_jaeger](https://docs.rs/opentelemetry-jaeger/latest/opentelemetry_jaeger/config/agent/struct.AgentPipeline.html).
    endpoint: String,
    /// # Filter
    ///
    /// Exporter filter configuration.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    #[serde(default = "default_filter")]
    filter: String,
}

impl JaegerOptions {
    pub(crate) fn build_layer<S>(
        &self,
        service_name: String,
        instance_id: impl Display,
    ) -> Result<
        Filtered<
            tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry::sdk::trace::Tracer>,
            EnvFilter,
            S,
        >,
        Error,
    >
    where
        S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        let resource = opentelemetry::sdk::Resource::new(
            vec![
                opentelemetry_semantic_conventions::resource::SERVICE_NAME.string(service_name),
                opentelemetry_semantic_conventions::resource::SERVICE_NAMESPACE.string("Restate"),
                opentelemetry_semantic_conventions::resource::SERVICE_INSTANCE_ID
                    .string(instance_id.to_string()),
                opentelemetry_semantic_conventions::resource::SERVICE_VERSION
                    .string(env!("CARGO_PKG_VERSION")),
            ]
            .into_iter(),
        );

        let mut jaeger_pipeline = opentelemetry_jaeger::new_agent_pipeline()
            .with_trace_config(opentelemetry::sdk::trace::config().with_resource(resource));

        jaeger_pipeline = jaeger_pipeline
            .with_endpoint(&self.endpoint)
            .with_auto_split_batch(true);

        let jaeger_tracer = jaeger_pipeline.install_batch(opentelemetry::runtime::Tokio)?;

        Ok(tracing_opentelemetry::layer()
            .with_location(false)
            .with_threads(false)
            .with_tracked_inactivity(false)
            .with_tracer(jaeger_tracer)
            .with_filter(EnvFilter::try_new(&self.filter)?))
    }
}

/// # Jaeger File Options
///
/// Configuration for the Jaeger file exporter. This exporter writes traces as JSON in the Jaeger format.
///
/// It can be used to export traces in a structured format without configuring a Jaeger agent.
/// To inspect the traces, open the Jaeger UI and use the Upload JSON feature to load and inspect them.
///
/// All spans will be sampled.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
struct JaegerFileOptions {
    /// # Path
    ///
    /// Specify the path where all the traces will be exported. Each trace file will start with the `trace` prefix.
    path: String,
    /// # Filter
    ///
    /// Exporter filter configuration.
    /// Check the [`RUST_LOG` documentation](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) for more details how to configure it.
    #[serde(default = "default_filter")]
    filter: String,
}

impl JaegerFileOptions {
    fn build_layer<S>(
        &self,
        restate_service_name: String,
    ) -> Result<
        Filtered<
            tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry::sdk::trace::Tracer>,
            EnvFilter,
            S,
        >,
        Error,
    >
    where
        S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        Ok(tracing_opentelemetry::layer()
            .with_tracer(
                JaegerJsonExporter::new(
                    self.path.clone().into(),
                    "trace".to_string(),
                    restate_service_name,
                    opentelemetry::runtime::Tokio,
                )
                .install_batch(),
            )
            .with_filter(EnvFilter::try_new(&self.filter)?))
    }
}

/// # Log format
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
struct LogOptions {
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
                .fmt_fields(PrettyFields::default())
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
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "ObservabilityOptions"))]
pub struct Options {
    /// # Jaeger options
    jaeger: Option<JaegerOptions>,

    /// # Jaeger file exporter options
    jaeger_file: Option<JaegerFileOptions>,

    /// # Logging options
    log: LogOptions,
}

impl Options {
    /// Instruments the process with logging and tracing.
    ///
    /// The opentelemetry tracer provider is automatically shut down when this struct is being dropped.
    ///
    /// # Panics
    /// This method will panic if there is already a global subscriber configured. Moreover, it will
    /// panic if it is executed outside of a Tokio runtime.
    pub fn init(&self, service_name: impl Display, instance_id: impl Display) -> Result<(), Error> {
        let restate_service_name = format!("Restate service: {service_name}");

        let layers = tracing_subscriber::registry();

        // Logging layer
        let layers = layers.with(self.log.build_layer()?);

        // Console subscriber layer
        #[cfg(feature = "console-subscriber")]
        let layers = layers.with(console_subscriber::spawn());

        // Jaeger layer
        let layers = layers.with(
            self.jaeger
                .as_ref()
                .map(|jaeger| jaeger.build_layer(restate_service_name.clone(), instance_id))
                .transpose()?,
        );

        // Jaeger file layer
        let layers = layers.with(
            self.jaeger_file
                .as_ref()
                .map(|jaeger_file| jaeger_file.build_layer(restate_service_name))
                .transpose()?,
        );

        layers.init();

        Ok(())
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}
