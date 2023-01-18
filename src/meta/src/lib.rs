use axum::response::IntoResponse;
use axum::routing::{get, IntoMakeService};
use axum::Router;
use hyper::server::conn::AddrIncoming;
use hyper::Server;
use std::net::SocketAddr;
use tracing::debug;

#[derive(Debug, clap::Parser)]
#[group(skip)]
pub struct Options {
    /// Address of the REST endpoint
    #[arg(
        long = "meta-rest-addr",
        env = "META_REST_ADDRESS",
        default_value = "0.0.0.0:8081"
    )]
    rest_addr: SocketAddr,
}

pub struct Meta {
    server: Server<AddrIncoming, IntoMakeService<Router>>,
}

impl Options {
    pub fn build(self) -> Meta {
        let meta_api = Router::new().route("/", get(index));
        let server = Server::bind(&self.rest_addr).serve(meta_api.into_make_service());

        Meta { server }
    }
}

impl Meta {
    pub async fn run(self, drain: drain::Watch) -> Result<(), anyhow::Error> {
        debug!(rest_addr = ?self.server.local_addr(), "Starting the meta component.");
        let shutdown = drain.signaled();

        tokio::select! {
            result = self.server => {
                result.map_err(Into::into)
            },
            _ = shutdown => {
                debug!("Shutting meta down.");
                Ok(())
            }
        }
    }
}

async fn index() -> impl IntoResponse {
    "Welcome to Restate :-)"
}
