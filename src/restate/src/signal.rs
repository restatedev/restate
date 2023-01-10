use tokio::signal::unix::{signal, SignalKind};
use tracing::info;

pub(super) async fn shutdown() {
    let signal = tokio::select! {
        () = await_signal(SignalKind::interrupt()) => "SIGINT",
        () = await_signal(SignalKind::terminate()) => "SIGTERM"
    };

    info!(%signal, "Received signal, starting shutdown.")
}

async fn await_signal(kind: SignalKind) {
    signal(kind)
        .expect("failed to register signal handler")
        .recv()
        .await;
}
