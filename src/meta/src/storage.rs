use futures::future::BoxFuture;

#[derive(Debug, thiserror::Error)]
#[error("meta storage error")]
pub struct MetaStorageError;

pub trait MetaStorage {
    // TODO: Replace with async trait or proper future
    fn register(&self) -> BoxFuture<Result<(), MetaStorageError>>;
}

#[derive(Debug, Default)]
pub struct InMemoryMetaStorage {}

impl MetaStorage for InMemoryMetaStorage {
    fn register(&self) -> BoxFuture<Result<(), MetaStorageError>> {
        todo!()
    }
}
