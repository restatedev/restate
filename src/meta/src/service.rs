// --- Handle

use crate::storage::MetaStorage;

#[derive(Clone)]
pub struct MetaHandle {}

impl MetaHandle {
    pub async fn register(&self) {
        todo!("Implement command to send message to meta service loop and wait for response")
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
