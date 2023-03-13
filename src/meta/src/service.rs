#![allow(dead_code)]

// --- Handle

use futures_util::command::{Command, UnboundedCommandSender};
use hyper::Uri;
use std::collections::HashMap;

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
    // TODO Add maps of service key extractions and descriptors
    _storage: Storage,

    _handle: MetaHandle,
}

impl<Storage> MetaService<Storage>
where
    Storage: MetaStorage,
{
    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn meta_handle(&self) -> MetaHandle {
        unimplemented!()
    }

    pub async fn run(self, _drain: drain::Watch) {
        todo!("Implement meta service loop processing commands from handlers, mutating internal state and updating the local maps for key extraction and descriptors")
    }
}
