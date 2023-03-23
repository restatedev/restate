use futures::future::BoxFuture;
use futures::FutureExt;
use prost_reflect::DescriptorPool;
use service_metadata::{EndpointMetadata, ServiceMetadata};

#[derive(Debug, thiserror::Error)]
#[error("meta storage error")]
pub struct MetaStorageError;

pub trait MetaStorage {
    // TODO: Replace with async trait or proper future
    fn register_endpoint(
        &self,
        endpoint_metadata: &EndpointMetadata,
        exposed_services: &[ServiceMetadata],
        descriptor_pool: DescriptorPool,
    ) -> BoxFuture<Result<(), MetaStorageError>>;
}

// No-op in memory storage implementation, useful for testing environments

#[derive(Debug, Default)]
pub struct InMemoryMetaStorage {}

impl MetaStorage for InMemoryMetaStorage {
    fn register_endpoint(
        &self,
        _service_metadata: &EndpointMetadata,
        _exposed_services: &[ServiceMetadata],
        _descriptor_pool: DescriptorPool,
    ) -> BoxFuture<Result<(), MetaStorageError>> {
        async { Ok(()) }.boxed()
    }
}
