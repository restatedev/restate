use std::{
    env, fs,
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::PathBuf,
};

use rand::Rng;
use tokio::signal::unix::SignalKind;
use tracing::info;

pub mod cluster;
pub mod node;

/// Used to store marker files of "used" ports to avoid confilcts
///
/// Please make sure the path `$TMP_DIR/restate_test_ports` is deleted
/// before starting tests
const DEFAULTS_PORTS_POOL: &str = "restate_test_ports";
const MAX_ALLOCATION_ATTEMPTS: u32 = 20;
const RESTATE_TEST_PORTS_POOL_DIR: &str = "RESTATE_TEST_PORTS_POOL";

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

pub fn random_socket_address() -> io::Result<SocketAddr> {
    let base_path = env::var_os(RESTATE_TEST_PORTS_POOL_DIR)
        .map(PathBuf::from)
        .unwrap_or_else(|| env::temp_dir().join(DEFAULTS_PORTS_POOL));

    // this can happen repeatedly but it's a test so it's okay
    fs::create_dir_all(&base_path)?;
    let mut rng = rand::thread_rng();
    let mut attempts = 0;
    loop {
        attempts += 1;
        if attempts > MAX_ALLOCATION_ATTEMPTS {
            return Err(io::Error::other("Max allocation attempts exahusted"));
        }

        let port = rng.gen_range(1025..u16::MAX);
        let port_file = base_path.join(port.to_string());

        match fs::OpenOptions::new()
            .create(true)
            .create_new(true)
            .truncate(true)
            .write(true)
            .open(port_file)
        {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                // port already reserved!
                // try another port
                continue;
            }
            Err(err) => return Err(err),
        }

        // just make sure this port is actually free
        match TcpListener::bind((IpAddr::V4(Ipv4Addr::LOCALHOST), port)) {
            Ok(listener) => return listener.local_addr(),
            Err(err) if err.kind() == io::ErrorKind::AddrInUse => {
                // try again !
                continue;
            }
            Err(err) => return Err(err),
        }
    }
}
