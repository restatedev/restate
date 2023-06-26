use crate::service::MetaHandle;

/// Handlers share this state
#[derive(Clone)]
pub struct RestEndpointState<S, M, K, W> {
    meta_handle: MetaHandle,
    service_endpoint_registry: S,
    method_descriptor_registry: M,
    key_converter: K,
    worker_handle: W,
}

impl<S, M, K, W> RestEndpointState<S, M, K, W> {
    pub fn new(
        meta_handle: MetaHandle,
        service_endpoint_registry: S,
        method_descriptor_registry: M,
        key_converter: K,
        worker_handle: W,
    ) -> Self {
        Self {
            meta_handle,
            service_endpoint_registry,
            method_descriptor_registry,
            key_converter,
            worker_handle,
        }
    }

    pub fn meta_handle(&self) -> &MetaHandle {
        &self.meta_handle
    }

    pub fn service_endpoint_registry(&self) -> &S {
        &self.service_endpoint_registry
    }

    pub fn method_descriptor_registry(&self) -> &M {
        &self.method_descriptor_registry
    }

    pub fn key_converter(&self) -> &K {
        &self.key_converter
    }

    pub fn worker_handle(&self) -> &W {
        &self.worker_handle
    }
}
