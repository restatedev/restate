mod rest_api;
mod service;
mod storage;

use crate::storage::InMemoryMetaStorage;
use rest_api::MetaRestEndpoint;
use service::MetaService;
use std::net::SocketAddr;
use tokio::join;
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

impl Options {
    pub fn build(self) -> Meta {
        Meta {
            rest_endpoint: MetaRestEndpoint::new(self.rest_addr),
            service: MetaService::new(),
        }
    }
}

pub struct Meta {
    rest_endpoint: MetaRestEndpoint,
    service: MetaService<InMemoryMetaStorage>,
}

impl Meta {
    pub fn service_invocation_factory() {
        unimplemented!("Return the service invocation factory");
    }

    pub fn method_descriptor_registry() {
        unimplemented!("Return the method descriptor registry");
    }

    pub async fn run(self, drain: drain::Watch) {
        let (shutdown_signal, shutdown_watch) = drain::channel();

        let meta_handle = self.service.meta_handle();

        let service_fut = self.service.run(shutdown_watch.clone());
        let rest_endpoint_fut = self.rest_endpoint.run(meta_handle, shutdown_watch);
        tokio::pin!(service_fut, rest_endpoint_fut);

        let shutdown = drain.signaled();

        tokio::select! {
            _ = shutdown => {
                debug!("Initiating shutdown of meta");

                shutdown_signal.drain().await;

                // ignored because we are shutting down
                let _ = join!(service_fut, rest_endpoint_fut);

                debug!("Completed shutdown of meta");
            },
            _ = &mut rest_endpoint_fut => {
                panic!("Rest endpoint stopped running");
            },
            _ = &mut service_fut => {
                panic!("Service stopped running");
            },
        }
    }
}
