use std::path::PathBuf;
use base64::Engine;
use futures::future::BoxFuture;
use futures::FutureExt;
use hyper::Uri;
use prost_reflect::DescriptorPool;
use serde::{Deserialize, Serialize};
use tokio::io;
use tracing::warn;
use service_metadata::{EndpointMetadata, ServiceMetadata};

#[derive(Debug, thiserror::Error)]
pub enum MetaStorageError {
    #[error("generic io error: {0}")]
    Io(#[from] io::Error),
    #[error("generic serde error: {0}. This is probably a runtime bug.")]
    Serde(#[from] serde_json::Error)
}

pub trait MetaStorage {
    // TODO: Replace with async trait or proper future
    fn register_endpoint(
        &self,
        endpoint_metadata: &EndpointMetadata,
        exposed_services: &[ServiceMetadata],
        descriptor_pool: DescriptorPool,
    ) -> BoxFuture<Result<(), MetaStorageError>>;
}

// --- No-op in memory storage implementation, useful for testing environments

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

// --- File based implementation, using serde

#[derive(Debug)]
pub struct FileMetaStorage {
    root_path: PathBuf
}

impl FileMetaStorage {
    pub fn new(root_path: PathBuf) -> Self {
        Self {
            root_path,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct MetadataFile {
    endpoint_metadata: EndpointMetadata,
    exposed_services: Vec<ServiceMetadata>
}

impl MetaStorage for FileMetaStorage {
    fn register_endpoint(&self, endpoint_metadata: &EndpointMetadata, exposed_services: &[ServiceMetadata], descriptor_pool: DescriptorPool) -> BoxFuture<Result<(), MetaStorageError>> {
        let endpoint_id = endpoint_id(endpoint_metadata.address());
        let metadata_file_path = self.root_path.join(format!("{}.json", endpoint_id));
        let descriptor_file_path = self.root_path.join(format!("{}.desc", endpoint_id));

        // TODO to avoid these clones, we could use Cow and ZeroVec
        //  https://github.com/restatedev/restate/issues/230
        let metadata_file_struct = MetadataFile {
            endpoint_metadata: endpoint_metadata.clone(),
            exposed_services: Vec::from(exposed_services),
        };

        async move {
            remove_if_exists(&metadata_file_path).await?;
            remove_if_exists(&descriptor_file_path).await?;

            tokio::fs::write(metadata_file_path, serde_json::to_vec_pretty(&metadata_file_struct)?).await?;
            tokio::fs::write(descriptor_file_path, descriptor_pool.encode_to_vec()).await?;

            Ok(())
        }.boxed()
    }
}

fn endpoint_id(uri: &Uri) -> String {
    // We use only authority and path, as those uniquely identify the endpoint.
    let authority_and_path = format!("{}{}", uri.authority().expect("Must have authority"), uri.path());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(authority_and_path.as_bytes())
}

async fn remove_if_exists(path: &PathBuf) -> io::Result<()> {
    if tokio::fs::metadata(path).await.is_ok() {
        warn!("Replacing file {}", path.display());
        tokio::fs::remove_file(path).await?;
    }
    Ok(())
}
