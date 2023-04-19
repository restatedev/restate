mod pretty;

use crate::pretty::PrettyFields;
use opentelemetry::trace::TraceError;
use pretty::Pretty;
use std::fmt::Display;
use tracing::Level;
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[derive(Debug, thiserror::Error)]
#[error("could not initialize tracing {trace_error}")]
pub struct Error {
    #[from]
    trace_error: TraceError,
}

pub type TracingResult<T> = Result<T, Error>;

/// # Tracing options
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "TracingOptions"))]
pub struct Options {
    /// # Jaeger endpoint
    ///
    /// Specify the Jaeger endpoint to use to send traces. Traces will be exported using the [Jaeger Agent UDP protocol](https://www.jaegertracing.io/docs/1.6/deployment/#agent) through [opentelemetry_jaeger](https://docs.rs/opentelemetry-jaeger/latest/opentelemetry_jaeger/config/agent/struct.AgentPipeline.html).
    jaeger_endpoint: Option<String>,

    /// # Disable ANSI log
    ///
    /// Disable ANSI terminal codes for logs. This is useful when the log collector doesn't support processing ANSI terminal codes.
    #[cfg_attr(feature = "options_schema", schemars(default))]
    disable_ansi_log: bool,
}

impl Options {
    /// Instruments the process with logging and tracing.
    ///
    /// The opentelemetry tracer provider is automatically shut down when this struct is being dropped.
    ///
    /// # Panics
    /// This method will panic if there is already a global subscriber configured. Moreover, it will
    /// panic if it is executed outside of a Tokio runtime.
    pub fn init(&self, service_name: impl Display, instance_id: impl Display) -> TracingResult<()> {
        let layers = tracing_subscriber::registry()
            .with(EnvFilter::from_default_env())
            .with(
                tracing_subscriber::fmt::layer()
                    .event_format::<Pretty<SystemTime>>(Pretty::default())
                    .fmt_fields(PrettyFields::default())
                    .with_writer(
                        // Write WARN and ERR to stderr, everything else to stdout
                        std::io::stderr
                            .with_max_level(Level::WARN)
                            .or_else(std::io::stdout),
                    )
                    .with_ansi(!self.disable_ansi_log),
            );
        #[cfg(feature = "console-subscriber")]
        let layers = layers.with(console_subscriber::spawn());

        if let Some(jaeger_endpoint) = &self.jaeger_endpoint {
            layers
                .with(self.try_init_jaeger_tracing(jaeger_endpoint, service_name, instance_id)?)
                .init();
        } else {
            layers.init();
        }

        Ok(())
    }

    fn try_init_jaeger_tracing<S>(
        &self,
        jaeger_endpoint: impl AsRef<str>,
        service_name: impl Display,
        instance_id: impl Display,
    ) -> TracingResult<
        tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry::sdk::trace::Tracer>,
    >
    where
        S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    {
        let resource = opentelemetry::sdk::Resource::new(
            vec![
                opentelemetry_semantic_conventions::resource::SERVICE_NAME
                    .string(format!("Restate service: {service_name}")),
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
            .with_endpoint(jaeger_endpoint.as_ref())
            .with_auto_split_batch(true);

        let jaeger_tracer = jaeger_pipeline.install_batch(opentelemetry::runtime::Tokio)?;

        Ok(tracing_opentelemetry::layer()
            .with_location(false)
            .with_threads(false)
            .with_tracked_inactivity(false)
            .with_tracer(jaeger_tracer))
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}
