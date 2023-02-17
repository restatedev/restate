use opentelemetry::trace::TraceError;
use std::fmt::Display;
use tracing::Level;
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

#[derive(Debug, clap::Parser, Default)]
pub struct Options {
    /// Specify to expose OTEL spans to Jaeger
    #[arg(long = "tracing-jaeger-endpoint", env = "TRACING_JAEGER_ENDPOINT")]
    jaeger_endpoint: Option<String>,

    /// Disable ANSI colors for formatted output
    #[arg(long = "tracing-disable-ansi-log", env = "TRACING_DISABLE_ANSI_LOG")]
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
    ///
    /// Example:
    /// ```
    /// use clap::Parser;
    ///
    /// #[derive(Debug, clap::Parser)]
    /// #[group(skip)]
    /// struct Options {
    ///     #[clap(flatten)]
    ///     tracing: tracing_instrumentation::Options,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    /// let options = Options::parse();
    ///     options
    ///         .tracing
    ///         .init("service", "instance")
    ///         .unwrap()
    /// }
    /// ```
    pub fn init(&self, service_name: impl Display, instance_id: impl Display) -> TracingResult<()> {
        let layers = tracing_subscriber::registry()
            .with(EnvFilter::from_default_env())
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(
                        // Write WARN and ERR to stderr, everything else to stdout
                        std::io::stderr
                            .with_max_level(Level::WARN)
                            .or_else(std::io::stdout),
                    )
                    .with_ansi(!self.disable_ansi_log),
            );

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
