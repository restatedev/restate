use clap::Parser;
use tracing::{info, warn};

mod app;
mod rt;
mod signal;

#[derive(Debug, clap::Parser)]
#[group(skip)]
struct Options {
    #[arg(long, env = "SHUTDOWN_GRACE_PERIOD", default_value = "1 min")]
    shutdown_grace_period: humantime::Duration,

    #[command(flatten)]
    tracing: tracing_instrumentation::Options,

    #[command(flatten)]
    application: app::Options,
}

fn main() {
    let options = Options::parse();

    let runtime = rt::build_runtime().expect("failed to build Tokio runtime!");

    runtime.block_on(async move {
        options
            .tracing
            .init("Restate binary", std::process::id())
            .expect("failed to instrument logging and tracing!");

        info!(?options, "Running Restate.");

        let app = options.application.build();

        let drain_signal = app.run();

        tokio::select! {
            () = signal::shutdown() => {
                info!("Received shutdown signal.")
            }
        }

        if tokio::time::timeout(options.shutdown_grace_period.into(), drain_signal.drain())
            .await
            .is_err()
        {
            warn!("Could not gracefully shut down Restate, terminating now.");
        } else {
            info!("Restate has been gracefully shut down.");
        }
    });
}
