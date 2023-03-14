use futures::future::BoxFuture;
use futures::FutureExt;
use prost_reflect::ServiceDescriptor;
use service_key_extractor::ServiceInstanceType;

#[derive(Debug, thiserror::Error)]
#[error("meta storage error")]
pub struct MetaStorageError;

pub trait MetaStorage {
    // TODO: Replace with async trait or proper future
    fn register_service(
        &self,
        service_name: String,
        service_instance_type: ServiceInstanceType,
        service_descriptor: ServiceDescriptor,
    ) -> BoxFuture<Result<(), MetaStorageError>>;
}

#[derive(Debug, Default)]
pub struct InMemoryMetaStorage {}

impl MetaStorage for InMemoryMetaStorage {
    fn register_service(
        &self,
        _service_name: String,
        _service_instance_type: ServiceInstanceType,
        _service_descriptor: ServiceDescriptor,
    ) -> BoxFuture<Result<(), MetaStorageError>> {
        async { Ok(()) }.boxed()
    }
}
