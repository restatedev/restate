use std::path::PathBuf;
use std::time::SystemTime;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{stream, FutureExt, StreamExt};
use prost_reflect::DescriptorPool;
use restate_service_key_extractor::ServiceInstanceType;
use restate_types::service_endpoint::EndpointMetadata;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io;
use tokio::io::AsyncWriteExt;
use tracing::trace;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceMetadata {
    name: String,
    instance_type: ServiceInstanceType,
}

impl ServiceMetadata {
    pub fn new(name: String, instance_type: ServiceInstanceType) -> Self {
        Self {
            name,
            instance_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn instance_type(&self) -> &ServiceInstanceType {
        &self.instance_type
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MetaStorageError {
    #[error("generic io error: {0}")]
    Io(#[from] io::Error),
    #[error("generic serde error: {0}. This is probably a runtime bug.")]
    Serde(#[from] serde_json::Error),
    #[error("generic descriptor error: {0}. This is probably a runtime bug.")]
    Descriptor(#[from] prost_reflect::DescriptorError),
}

pub trait MetaStorage {
    // TODO: Replace with async trait or proper future
    fn register_endpoint(
        &self,
        endpoint_metadata: &EndpointMetadata,
        exposed_services: &[ServiceMetadata],
        descriptor_pool: DescriptorPool,
    ) -> BoxFuture<Result<(), MetaStorageError>>;

    // TODO: Replace with async trait or proper future
    //  This type could also be simplified simply returning the stream,
    //  and eventually failing the first stream element in case the stream cannot be "created".
    #[allow(clippy::type_complexity)]
    fn reload(
        &self,
    ) -> BoxFuture<
        'static,
        Result<
            BoxStream<
                'static,
                Result<(EndpointMetadata, Vec<ServiceMetadata>, DescriptorPool), MetaStorageError>,
            >,
            MetaStorageError,
        >,
    >;
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

    fn reload(
        &self,
    ) -> BoxFuture<
        'static,
        Result<
            BoxStream<
                'static,
                Result<(EndpointMetadata, Vec<ServiceMetadata>, DescriptorPool), MetaStorageError>,
            >,
            MetaStorageError,
        >,
    > {
        async { Ok(stream::empty().boxed()) }.boxed()
    }
}

// --- File based implementation, using serde

const JSON_EXTENSION: &str = "json";
const DESC_EXTENSION: &str = "desc";

#[derive(Debug)]
pub struct FileMetaStorage {
    root_path: PathBuf,
}

impl FileMetaStorage {
    pub fn new(root_path: PathBuf) -> Self {
        Self { root_path }
    }
}

#[derive(Serialize, Deserialize)]
struct MetadataFile {
    endpoint_metadata: EndpointMetadata,
    exposed_services: Vec<ServiceMetadata>,
}

impl MetaStorage for FileMetaStorage {
    fn register_endpoint(
        &self,
        endpoint_metadata: &EndpointMetadata,
        exposed_services: &[ServiceMetadata],
        descriptor_pool: DescriptorPool,
    ) -> BoxFuture<Result<(), MetaStorageError>> {
        let endpoint_id = endpoint_metadata.id();
        let metadata_file_path = self
            .root_path
            .join(format!("{}.{}", endpoint_id, JSON_EXTENSION));
        let descriptor_file_path = self
            .root_path
            .join(format!("{}.{}", endpoint_id, DESC_EXTENSION));

        // TODO to avoid these clones, we could use Cow and ZeroVec
        //  https://github.com/restatedev/restate/issues/230
        let metadata_file_struct = MetadataFile {
            endpoint_metadata: endpoint_metadata.clone(),
            exposed_services: Vec::from(exposed_services),
        };

        async move {
            restate_fs_util::remove_file_if_exists(&metadata_file_path).await?;
            restate_fs_util::remove_file_if_exists(&descriptor_file_path).await?;

            let metadata_buf = serde_json::to_vec_pretty(&metadata_file_struct)?;
            let descriptor_pool_buf = descriptor_pool.encode_to_vec();

            let mut metadata_file = File::create(metadata_file_path).await?;
            let mut descriptor_file = File::create(descriptor_file_path).await?;

            futures::try_join!(
                metadata_file.write_all(&metadata_buf),
                descriptor_file.write_all(&descriptor_pool_buf)
            )?;
            futures::try_join!(metadata_file.sync_all(), descriptor_file.sync_all())?;

            Ok(())
        }
        .boxed()
    }

    fn reload(
        &self,
    ) -> BoxFuture<
        'static,
        Result<
            BoxStream<
                'static,
                Result<(EndpointMetadata, Vec<ServiceMetadata>, DescriptorPool), MetaStorageError>,
            >,
            MetaStorageError,
        >,
    > {
        let root_path = self.root_path.clone();
        FutureExt::boxed(async move {
            // Try to create a dir, in case it doesn't exist
            restate_fs_util::create_dir_all_if_doesnt_exists(&root_path).await?;

            // Find all the metadata files in the root path directory, then sort them by modified date
            let mut read_dir = tokio::fs::read_dir(root_path).await?;
            let mut metadata_files = vec![];
            while let Some(dir_entry) = read_dir.next_entry().await? {
                if dir_entry
                    .path()
                    .extension()
                    .and_then(|os_str| os_str.to_str())
                    == Some(JSON_EXTENSION)
                {
                    metadata_files.push((dir_entry.path(), dir_entry.metadata().await?.modified()?))
                }
            }
            metadata_files.sort_by(|a, b| SystemTime::cmp(&a.1, &b.1));

            let result_stream =
                stream::iter(metadata_files).then(|(metadata_file_path, _)| async move {
                    // Metadata_file_path is the json metadata descriptor
                    trace!("Reloading metadata file {}", metadata_file_path.display());
                    let metadata_bytes = tokio::fs::read(&metadata_file_path).await?;
                    let metadata_file: MetadataFile = serde_json::from_slice(&metadata_bytes)?;

                    // Now load the descriptor pool
                    let mut descriptor_pool_file = metadata_file_path.clone();
                    descriptor_pool_file.set_extension(DESC_EXTENSION);
                    trace!(
                        restate.service_endpoint.url = %metadata_file.endpoint_metadata.address(),
                        "Reloading descriptor file {}",
                        descriptor_pool_file.display()
                    );
                    let mut descriptor_pool = DescriptorPool::new();
                    descriptor_pool.decode_file_descriptor_set(
                        tokio::fs::read(&descriptor_pool_file).await?.as_ref(),
                    )?;

                    Ok((
                        metadata_file.endpoint_metadata,
                        metadata_file.exposed_services,
                        descriptor_pool,
                    ))
                });

            Ok(StreamExt::boxed(result_stream))
        })
    }
}
