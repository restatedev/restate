// --- Handle

use std::collections::HashMap;

use crate::storage::MetaStorage;
use channel_interface_derive::request_response_channel_interface;
use futures_util::channel_interface::ClosedError;
use hyper::Uri;

#[derive(Debug, thiserror::Error)]
pub enum MetaServiceError {
    #[error("meta closed")]
    Closed(#[from] ClosedError),
}

request_response_channel_interface!(
    pub struct MetaHandle {
        async fn register(
            &self,
            endpoint_uri: Uri,
            endpoint_additional_headers: HashMap<String, String>
        ) -> Result<Vec<String>, MetaServiceError>;
    }
);

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
