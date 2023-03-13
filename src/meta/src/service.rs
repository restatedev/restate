// --- Handle

use std::collections::HashMap;

use futures_util::command::{Command, UnboundedCommandReceiver, UnboundedCommandSender};
use hyper::Uri;
use ingress_grpc::InMemoryMethodDescriptorRegistry;
use service_key_extractor::KeyExtractorsRegistry;
use service_protocol::discovery::ServiceDiscovery;
use tokio::sync::mpsc;
use tracing::debug;
use common::retry_policy::RetryPolicy;

use crate::storage::MetaStorage;

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("discovery error")]
    DiscoveryError,
    #[error("meta closed")]
    MetaClosed,
}

#[derive(Clone)]
pub struct MetaHandle(UnboundedCommandSender<MetaHandleRequest, MetaHandleResponse>);

enum MetaHandleRequest {
    DiscoverEndpoint {
        uri: Uri,
        additional_headers: HashMap<String, String>,
    },
}

enum MetaHandleResponse {
    DiscoverEndpoint(Result<Vec<String>, MetaError>),
}

impl MetaHandle {
    pub async fn register(
        &self,
        uri: Uri,
        additional_headers: HashMap<String, String>,
    ) -> Result<Vec<String>, MetaError> {
        let (cmd, response_tx) = Command::prepare(MetaHandleRequest::DiscoverEndpoint {
            uri,
            additional_headers,
        });
        self.0.send(cmd).map_err(|_e| MetaError::MetaClosed)?;
        response_tx
            .await
            .map(|res| match res {
                MetaHandleResponse::DiscoverEndpoint(res) => res,
                #[allow(unreachable_patterns)]
                _ => panic!("Unexpected response message, this is a bug"),
            })
            .map_err(|_e| MetaError::MetaClosed)?
    }
}

// -- Service implementation

pub struct MetaService<Storage> {
    key_extractors_registry: KeyExtractorsRegistry,
    method_descriptors_registry: InMemoryMethodDescriptorRegistry,
    service_discovery: ServiceDiscovery,

    storage: Storage,

    handle: MetaHandle,
    api_cmd_rx: UnboundedCommandReceiver<MetaHandleRequest, MetaHandleResponse>,
}

impl<Storage> MetaService<Storage>
where
    Storage: MetaStorage,
{
    pub fn new(
        key_extractors_registry: KeyExtractorsRegistry,
        method_descriptors_registry: InMemoryMethodDescriptorRegistry,
        storage: Storage,
        service_discovery_retry_policy: RetryPolicy,
    ) -> Self {
        let (api_cmd_tx, api_cmd_rx) = mpsc::unbounded_channel();

        Self {
            key_extractors_registry,
            method_descriptors_registry,
            service_discovery: ServiceDiscovery::new(service_discovery_retry_policy),
            storage,
            handle: MetaHandle(api_cmd_tx),
            api_cmd_rx,
        }
    }

    pub fn meta_handle(&self) -> MetaHandle {
        self.handle.clone()
    }

    pub async fn run(mut self, drain: drain::Watch) {
        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                cmd = self.api_cmd_rx.recv() => {
                    let (req, replier) = cmd.expect("This channel should never be closed").into_inner();

                    // If error, the client went away, so it's fine to ignore it
                    let _ = replier.send(match req {
                        MetaHandleRequest::DiscoverEndpoint { uri, additional_headers } => MetaHandleResponse::DiscoverEndpoint(
                            self.discover_endpoint(uri, additional_headers).await
                        )
                    });
                },
                _ = shutdown.as_mut() => {
                    debug!("Shutdown meta");
                    return;
                },
            }
        }
    }

    async fn discover_endpoint(
        &mut self,
        uri: Uri,
        additional_headers: HashMap<String, String>,
    ) -> Result<Vec<String>, MetaError> {
        todo!()
    }
}
