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
        let mut service_handle = tokio::spawn(self.service.run(shutdown_watch.clone()));
        let mut rest_endpoint_handle =
            tokio::spawn(self.rest_endpoint.run(meta_handle, shutdown_watch));

        let shutdown = drain.signaled();

        tokio::select! {
            _ = shutdown => {
                debug!("Initiating shutdown of meta");

                shutdown_signal.drain().await;

                // ignored because we are shutting down
                let _ = join!(service_handle, rest_endpoint_handle);

                debug!("Completed shutdown of meta");
            },
            rest_endpoint_result = &mut rest_endpoint_handle => {
                panic!("Rest endpoint stopped running: {rest_endpoint_result:?}");
            },
            service_result = &mut service_handle => {
                panic!("Service stopped running: {service_result:?}");
            },
        }
    }
}
