use std::{
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
};

use tokio::signal::unix::SignalKind;
use tracing::info;

pub mod cluster;
pub mod node;

pub fn shutdown() -> impl Future<Output = &'static str> {
    let mut interrupt = tokio::signal::unix::signal(SignalKind::interrupt())
        .expect("failed to register signal handler");
    let mut terminate = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to register signal handler");

    async move {
        let signal = tokio::select! {
            _ = interrupt.recv() => "SIGINT",
            _ = terminate.recv() => "SIGTERM",
        };

        info!(%signal, "Received signal, starting cluster shutdown.");
        signal
    }
}

const RANDOM_SOCKET_ADDRESS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

pub fn random_socket_address() -> io::Result<(SocketAddr, TcpListener)> {
    let inner = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;

    #[cfg(not(windows))]
    inner.set_reuse_address(true)?;

    inner.set_cloexec(true)?;
    inner.bind(&RANDOM_SOCKET_ADDRESS.into())?;
    inner.listen(128)?;

    let listener = TcpListener::from(inner);
    let socket_addr = listener.local_addr()?;

    Ok((socket_addr, listener))
}
