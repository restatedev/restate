use crate::service::MetaHandle;

/// Handlers share this state
#[derive(Clone)]
pub struct RestEndpointState<S, W> {
    meta_handle: MetaHandle,
    schemas: S,
    worker_handle: W,
}

impl<S, W> RestEndpointState<S, W> {
    pub fn new(meta_handle: MetaHandle, schemas: S, worker_handle: W) -> Self {
        Self {
            meta_handle,
            schemas,
            worker_handle,
        }
    }

    pub fn meta_handle(&self) -> &MetaHandle {
        &self.meta_handle
    }

    pub fn schemas(&self) -> &S {
        &self.schemas
    }

    pub fn worker_handle(&self) -> &W {
        &self.worker_handle
    }
}
