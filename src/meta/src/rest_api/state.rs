use crate::service::MetaHandle;
use restate_common::worker_command::WorkerCommandSender;

/// Handlers share this state
#[derive(Clone)]
pub struct RestEndpointState<S, M, K> {
    meta_handle: MetaHandle,
    service_endpoint_registry: S,
    method_descriptor_registry: M,
    key_converter: K,
    worker_command_tx: WorkerCommandSender,
}

impl<S, M, K> RestEndpointState<S, M, K> {
    pub fn new(
        meta_handle: MetaHandle,
        service_endpoint_registry: S,
        method_descriptor_registry: M,
        key_converter: K,
        worker_command_tx: WorkerCommandSender,
    ) -> Self {
        Self {
            meta_handle,
            service_endpoint_registry,
            method_descriptor_registry,
            key_converter,
            worker_command_tx,
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

    pub fn worker_command_tx(&self) -> &WorkerCommandSender {
        &self.worker_command_tx
    }
}
