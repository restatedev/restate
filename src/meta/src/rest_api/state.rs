use crate::service::MetaHandle;

/// Handlers share this state
#[derive(Clone)]
pub struct RestEndpointState<S, M> {
    meta_handle: MetaHandle,
    service_endpoint_registry: S,
    method_descriptor_registry: M,
}

impl<S, M> RestEndpointState<S, M> {
    pub fn new(
        meta_handle: MetaHandle,
        service_endpoint_registry: S,
        method_descriptor_registry: M,
    ) -> Self {
        Self {
            meta_handle,
            service_endpoint_registry,
            method_descriptor_registry,
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
}
