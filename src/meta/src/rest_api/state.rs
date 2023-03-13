use crate::service::MetaHandle;

/// Handlers share this state
#[derive(Clone)]
pub struct RestEndpointState {
    meta_handle: MetaHandle,
}

impl RestEndpointState {
    pub fn new(meta_handle: MetaHandle) -> Self {
        Self { meta_handle }
    }

    pub fn meta_handle(&self) -> &MetaHandle {
        &self.meta_handle
    }
}
