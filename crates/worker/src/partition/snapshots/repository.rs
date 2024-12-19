// Copyright (c) 2024 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use bytes::BytesMut;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::{MultipartUpload, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tempfile::TempDir;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;
use tracing::{debug, info, instrument, warn};
use url::Url;

use restate_partition_store::snapshots::{
    LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotFormatVersion,
};
use restate_types::config::SnapshotsOptions;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::Lsn;

/// Provides read and write access to the long-term partition snapshot storage destination.
///
/// The repository wraps access to an object store "bucket" that contains snapshot metadata and data
/// optimized for efficient retrieval. The bucket layout is split into two top-level prefixes for
/// snapshot metadata and data respectively. While full snapshot archives contain all relevant
/// metadata, this split layout allows for efficient retrieval of only the metadata upfront. It also
/// enables us to evolve the data storage layout independently in the future.
///
/// A single top-level `latest.json` file is the only key which is repeatedly overwritten; all other
/// data is immutable until the pruning policy allows for deletion.
///
/// - `[<prefix>/]<partition_id>/latest.json` - latest snapshot metadata for the partition
/// - `[<prefix>/]<partition_id>/{lsn}_{snapshot_id}/metadata.json` - snapshot descriptor
/// - `[<prefix>/]<partition_id>/{lsn}_{snapshot_id}/*.sst` - data files (explicitly named in `metadata.json`)
#[derive(Clone)]
pub struct SnapshotRepository {
    object_store: Arc<dyn ObjectStore>,
    destination: Url,
    prefix: String,
    /// Ingested snapshots staging location.
    staging_dir: PathBuf,
    /// Expected cluster name for the snapshots in this repository.
    cluster_name: String,
}

/// S3 and other stores require a certain minimum size for the parts of a multipart upload. It is an
/// API error to attempt a multipart put below this size, apart from the final segment.
const MULTIPART_UPLOAD_CHUNK_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// Maximum number of concurrent downloads when getting snapshots from the repository.
const DOWNLOAD_CONCURRENCY_LIMIT: usize = 8;

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatestSnapshot {
    pub version: SnapshotFormatVersion,

    pub partition_id: PartitionId,

    /// Restate cluster name which produced the snapshot.
    pub cluster_name: String,

    /// Node that produced this snapshot.
    pub node_name: String,

    /// Local node time when the snapshot was created.
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub created_at: humantime::Timestamp,

    /// Unique snapshot id.
    pub snapshot_id: SnapshotId,

    /// The minimum LSN guaranteed to be applied in this snapshot. The actual
    /// LSN may be >= [minimum_lsn].
    pub min_applied_lsn: Lsn,

    /// The relative path within the snapshot repository where the snapshot data is stored.
    pub path: String,
}

impl LatestSnapshot {
    /// `relative_path` is the path to the snapshot data relative to the partition's snapshots prefix.
    pub fn from_snapshot(
        snapshot: &PartitionSnapshotMetadata,
        unique_snapshot_path: String,
    ) -> Self {
        LatestSnapshot {
            version: snapshot.version,
            cluster_name: snapshot.cluster_name.clone(),
            node_name: snapshot.node_name.clone(),
            partition_id: snapshot.partition_id,
            snapshot_id: snapshot.snapshot_id,
            created_at: snapshot.created_at.clone(),
            min_applied_lsn: snapshot.min_applied_lsn,
            path: unique_snapshot_path,
        }
    }
}

impl SnapshotRepository {
    /// Creates an instance of the repository if a snapshots destination is configured.
    pub async fn create_if_configured(
        snapshots_options: &SnapshotsOptions,
        staging_dir: PathBuf,
        cluster_name: String,
    ) -> anyhow::Result<Option<SnapshotRepository>> {
        let mut destination = if let Some(ref destination) = snapshots_options.destination {
            Url::parse(destination).context("Failed parsing snapshot repository URL")?
        } else {
            return Ok(None);
        };
        // Prevent passing configuration options to object_store via the destination URL.
        destination
            .query()
            .inspect(|params| info!("Snapshot destination parameters ignored: {params}"));
        destination.set_query(None);

        let object_store = create_object_store_client(destination.clone()).await?;

        // The prefix must be stripped of any leading slash and, unless it is empty, must end in a
        // single "/" character.
        let prefix: String = destination.path().into();
        let prefix = match prefix.as_str() {
            "" | "/" => "".to_string(),
            prefix => format!("{}/", prefix.trim_start_matches('/').trim_end_matches('/')),
        };

        Ok(Some(SnapshotRepository {
            object_store,
            destination,
            prefix,
            staging_dir,
            cluster_name,
        }))
    }

    /// Write a partition snapshot to the snapshot repository.
    #[instrument(
        level = "debug",
        skip_all,
        err,
        fields(
            snapshot_id = ?snapshot.snapshot_id,
            partition_id = ?snapshot.partition_id,
        )
    )]
    pub(crate) async fn put(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        local_snapshot_path: PathBuf,
    ) -> anyhow::Result<()> {
        debug!("Publishing partition snapshot to: {}", self.destination);

        let put_result = self
            .put_snapshot_inner(snapshot, local_snapshot_path.as_path())
            .await;

        // We only log the error here since (a) it's relatively unlikely for rmdir to fail, and (b)
        // if we've uploaded the snapshot, we should get the response back to the caller. Logging at
        // WARN level as repeated failures could compromise the cluster.
        let _ = tokio::fs::remove_dir_all(local_snapshot_path.as_path())
            .await
            .inspect_err(|e| warn!("Failed to delete local snapshot files: {}", e));

        match put_result {
            Ok(_) => Ok(()),
            Err(put_error) => {
                for filename in put_error.uploaded_files {
                    let path = object_store::path::Path::from(format!(
                        "{}{}",
                        put_error.full_snapshot_path, filename
                    ));

                    // We disregard errors at this point; the snapshot repository pruning mechanism
                    // should catch these eventually.
                    let _ = self.object_store.delete(&path).await.inspect_err(|e| {
                        info!(
                            "Failed to delete file from partially uploaded snapshot: {}",
                            e
                        )
                    });
                }
                Err(put_error.error)
            }
        }
    }

    // It is the outer put method's responsibility to clean up partial progress.
    async fn put_snapshot_inner(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        local_snapshot_path: &Path,
    ) -> Result<(), PutSnapshotError> {
        let snapshot_path = self.get_snapshot_complete_prefix(snapshot);
        debug!(
            "Uploading snapshot from {:?} to {}",
            local_snapshot_path, snapshot_path
        );

        let mut progress = SnapshotUploadProgress::with_snapshot_path(snapshot_path.clone());
        let mut buf = BytesMut::new();
        for file in &snapshot.files {
            let filename = file.name.trim_start_matches("/");
            let key = object_store::path::Path::from(
                self.get_snapshot_file_full_path(snapshot, filename),
            );

            let put_result = put_snapshot_object(
                local_snapshot_path.join(filename).as_path(),
                &key,
                &self.object_store,
                &mut buf,
            )
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;

            debug!(
                etag = put_result.e_tag.unwrap_or_default(),
                ?key,
                "Put snapshot data file completed",
            );
            progress.push(file.name.clone());
        }

        let metadata_key = object_store::path::Path::from(
            self.get_snapshot_file_full_path(snapshot, "metadata.json"),
        );
        let metadata_json_payload = PutPayload::from(
            serde_json::to_string_pretty(snapshot).expect("Can always serialize JSON"),
        );

        let put_result = self
            .object_store
            .put(&metadata_key, metadata_json_payload)
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;
        progress.push("/metadata.json".to_owned());

        debug!(
            etag = put_result.e_tag.unwrap_or_default(),
            key = ?metadata_key,
            "Successfully published snapshot metadata",
        );

        let latest_path = object_store::path::Path::from(
            self.get_partition_latest_snapshot_pointer_path(snapshot.partition_id),
        );

        // By performing a CAS on the latest snapshot pointer, we can ensure strictly monotonic updates.
        let maybe_stored = self
            .get_latest_snapshot_metadata_for_update(snapshot, &latest_path)
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;
        if maybe_stored.as_ref().is_some_and(|(latest_stored, _)| {
            latest_stored.min_applied_lsn >= snapshot.min_applied_lsn
        }) {
            let repository_latest_lsn = maybe_stored.expect("is some").0.min_applied_lsn;
            info!(
                ?repository_latest_lsn,
                new_snapshot_lsn = ?snapshot.min_applied_lsn,
                "The newly uploaded snapshot is no newer than the already-stored latest snapshot, will not update latest pointer"
            );
            return Ok(());
        }

        let latest =
            LatestSnapshot::from_snapshot(snapshot, Self::get_snapshot_id_component(snapshot));
        let latest = PutPayload::from(
            serde_json::to_string_pretty(&latest)
                .map_err(|e| PutSnapshotError::from(e, progress.clone()))?,
        );

        // The object_store file provider supports create-if-not-exists but not update-version on put
        let use_conditional_update = !matches!(self.destination.scheme(), "file");
        let conditions = maybe_stored
            .map(|(_, version)| PutOptions {
                mode: match use_conditional_update {
                    true => PutMode::Update(version),
                    false => PutMode::Overwrite,
                },
                ..PutOptions::default()
            })
            .unwrap_or_else(|| PutOptions::from(PutMode::Create));

        // Note: this call may return an error on concurrent modification. Since we don't expect any
        // contention (here), and this doesn't cause any correctness issues, we don't bother with
        // retrying the entire put_snapshot attempt on object_store::Error::Precondition.
        let put_result = self
            .object_store
            .put_opts(&latest_path, latest, conditions)
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;

        debug!(
            etag = put_result.e_tag.unwrap_or_default(),
            key = ?latest_path,
            "Successfully updated latest snapshot pointer",
        );

        Ok(())
    }

    /// Discover and download the latest snapshot available. It is the caller's responsibility
    /// to delete the snapshot directory when it is no longer needed.
    #[instrument(
        level = "debug",
        skip_all,
        err,
        fields(%partition_id),
    )]
    pub(crate) async fn get_latest(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Option<LocalPartitionSnapshot>> {
        let latest_path = object_store::path::Path::from(
            self.get_partition_latest_snapshot_pointer_path(partition_id),
        );

        let latest = self.object_store.get(&latest_path).await;

        let latest = match latest {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                debug!("Latest snapshot data not found in repository");
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        debug!("Latest snapshot metadata: {:?}", latest);

        let snapshot_metadata_path = object_store::path::Path::from(format!(
            "{prefix}{partition_id}/{snapshot_path}/metadata.json",
            prefix = self.prefix,
            partition_id = partition_id,
            snapshot_path = latest.path,
        ));
        let snapshot_metadata = self.object_store.get(&snapshot_metadata_path).await;

        let snapshot_metadata = match snapshot_metadata {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                // todo(pavel): revisit whether we shouldn't just panic at this point - this is a bad sign!
                warn!("Latest snapshot points to a snapshot that was not found in the repository!");
                return Ok(None); // arguably this could also be an error
            }
            Err(e) => return Err(e.into()),
        };

        let mut snapshot_metadata: PartitionSnapshotMetadata =
            serde_json::from_slice(&snapshot_metadata.bytes().await?)?;
        if snapshot_metadata.version != SnapshotFormatVersion::V1 {
            return Err(anyhow!(
                "Unsupported snapshot format version: {:?}",
                snapshot_metadata.version
            ));
        }

        if snapshot_metadata.cluster_name != self.cluster_name {
            // todo(pavel): revisit whether we shouldn't just panic at this point - this is a bad sign!
            warn!("Snapshot does not match the cluster name of latest snapshot at destination in snapshot id {}! Expected: cluster name=\"{}\", found: \"{}\"",
                   snapshot_metadata.snapshot_id,
                   self.cluster_name,
                   snapshot_metadata.cluster_name);
            return Ok(None); // perhaps this needs to be a configuration error
        }

        // The snapshot ingest directory should be on the same filesystem as the partition store
        // to minimize IO and disk space usage during import.
        let snapshot_dir = TempDir::with_prefix_in(
            format!("{}-", snapshot_metadata.snapshot_id),
            &self.staging_dir,
        )?;
        debug!(
            snapshot_id = %snapshot_metadata.snapshot_id,
            path = ?snapshot_dir.path(),
            "Getting snapshot data",
        );

        let directory = snapshot_dir.path().to_string_lossy().to_string();
        let concurrency_limiter = Arc::new(Semaphore::new(DOWNLOAD_CONCURRENCY_LIMIT));
        let mut downloads = JoinSet::new();
        let mut task_handles = HashMap::with_capacity(snapshot_metadata.files.len());
        for file in &mut snapshot_metadata.files {
            let filename = file.name.trim_start_matches("/");
            let expected_size = file.size;
            let key = object_store::path::Path::from(format!(
                "{prefix}{partition_id}/{snapshot_path}/{filename}",
                prefix = self.prefix,
                partition_id = partition_id,
                snapshot_path = latest.path,
                filename = filename,
            ));
            let file_path = snapshot_dir.path().join(filename);
            let concurrency_limiter = Arc::clone(&concurrency_limiter);
            let object_store = Arc::clone(&self.object_store);

            let handle = downloads.build_task().name(filename).spawn(async move {
                let _permit = concurrency_limiter.acquire().await?;
                let mut file_data = StreamReader::new(
                    object_store
                        .get(&key)
                        .await
                        .map_err(|e| anyhow!("Failed to download snapshot file {:?}: {}", key, e))?
                        .into_stream(),
                );
                let mut snapshot_file =
                    tokio::fs::File::create_new(&file_path).await.map_err(|e| {
                        anyhow!("Failed to create snapshot file {:?}: {}", file_path, e)
                    })?;
                let size = io::copy(&mut file_data, &mut snapshot_file)
                    .await
                    .map_err(|e| anyhow!("Failed to download snapshot file {:?}: {}", key, e))?;
                if size != expected_size as u64 {
                    return Err(anyhow!(
                        "Downloaded snapshot file {:?} has unexpected size: expected {}, got {}",
                        key,
                        expected_size,
                        size
                    ));
                }
                debug!(%key, ?size, "Downloaded snapshot data file to {:?}", file_path);
                anyhow::Ok(())
            })?;
            task_handles.insert(handle.id(), filename.to_string());
            // patch the directory path to reflect the actual location on the restoring node
            file.directory = directory.clone();
        }

        loop {
            match downloads.join_next().await {
                None => {
                    debug!("All download tasks completed");
                    break;
                }
                Some(Err(join_error)) => {
                    let failed = task_handles.get(&join_error.id());
                    abort_tasks(downloads).await;
                    return Err(anyhow!(
                        "Failed to download snapshot file {:?}: {}",
                        failed,
                        join_error
                    ));
                }
                Some(Ok(Err(error))) => {
                    abort_tasks(downloads).await;
                    return Err(error);
                }
                Some(Ok(Ok(_))) => {}
            }
        }

        info!(
            snapshot_id = %snapshot_metadata.snapshot_id,
            path = ?snapshot_dir.path(),
            "Downloaded partition snapshot",
        );
        Ok(Some(LocalPartitionSnapshot {
            base_dir: snapshot_dir.into_path(),
            min_applied_lsn: snapshot_metadata.min_applied_lsn,
            db_comparator_name: snapshot_metadata.db_comparator_name,
            files: snapshot_metadata.files,
            key_range: snapshot_metadata.key_range.clone(),
        }))
    }

    async fn get_latest_snapshot_metadata_for_update(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        path: &object_store::path::Path,
    ) -> anyhow::Result<Option<(LatestSnapshot, UpdateVersion)>> {
        match self.object_store.get(path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let latest: LatestSnapshot = serde_json::from_slice(
                    &result.bytes().await?,
                )
                .inspect_err(|e| {
                    debug!(
                        repository_latest_lsn = "unknown",
                        new_snapshot_lsn = ?snapshot.min_applied_lsn,
                        "Failed to parse stored latest snapshot pointer, refusing to overwrite: {}",
                        e
                    )
                })
                .map_err(|e| anyhow!("Failed to parse latest snapshot metadata: {}", e))?;
                if snapshot.cluster_name != latest.cluster_name {
                    // This indicates a serious misconfiguration and we should complain loudly
                    bail!("Snapshot does not match the cluster name of latest snapshot at destination!");
                }
                Ok(Some((latest, version)))
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!(
                    repository_latest_lsn = "none",
                    new_snapshot_lsn = ?snapshot.min_applied_lsn,
                    "No latest snapshot pointer found, will create one"
                );
                Ok(None)
            }
            Err(e) => {
                bail!("Failed to get latest snapshot pointer: {}", e);
            }
        }
    }

    /// Construct the complete snapshot prefix from the base of the repository bucket.
    fn get_snapshot_complete_prefix(
        &self,
        snapshot_metadata: &PartitionSnapshotMetadata,
    ) -> String {
        format!(
            "{partition_prefix}/{snapshot_path}",
            partition_prefix = self.get_partition_snapshots_prefix(snapshot_metadata.partition_id),
            snapshot_path = Self::get_snapshot_id_component(snapshot_metadata)
        )
    }

    /// Construct the full path to a specific file that is a part of a given snapshot.
    fn get_snapshot_file_full_path(
        &self,
        snapshot_metadata: &PartitionSnapshotMetadata,
        filename: &str,
    ) -> String {
        format!(
            "{snapshot_complete_prefix}/{filename}",
            snapshot_complete_prefix = self.get_snapshot_complete_prefix(snapshot_metadata),
        )
    }

    /// Construct the full path to the latest snapshot pointer for a given partition.
    fn get_partition_latest_snapshot_pointer_path(&self, partition_id: PartitionId) -> String {
        format!(
            "{partition_prefix}/latest.json",
            partition_prefix = self.get_partition_snapshots_prefix(partition_id)
        )
    }

    /// Construct the snapshots prefix relative to the repository bucket for a given partition's snapshots.
    fn get_partition_snapshots_prefix(&self, partition_id: PartitionId) -> String {
        format!(
            "{prefix}{partition_id}",
            prefix = self.prefix,
            partition_id = partition_id,
        )
    }

    /// Construct the unique path component for a snapshot, e.g. `lsn_00001234-snap_abc123`.
    fn get_snapshot_id_component(snapshot: &PartitionSnapshotMetadata) -> String {
        // We zero-pad the LSN to ensure correct lexicographic sorting
        format!(
            "lsn_{lsn:020}-{snapshot_id}",
            lsn = snapshot.min_applied_lsn,
            snapshot_id = snapshot.snapshot_id
        )
    }
}

async fn create_object_store_client(destination: Url) -> anyhow::Result<Arc<dyn ObjectStore>> {
    // We use the AWS SDK configuration and credentials provider so that the conventional AWS
    // environment variables and config files work as expected. The object_store crate has its
    // own configuration mechanism which doesn't support many of the AWS conventions. This
    // differs quite a lot from the Lambda invoker which uses the AWS SDK, and that would be a
    // very surprising inconsistency for customers. This mechanism allows us to infer the region
    // and securely obtain session credentials without any hard-coded configuration.
    let object_store: Arc<dyn ObjectStore> = if destination.scheme() == "s3" {
        debug!("Using AWS SDK credentials provider");
        let aws_region = aws_config::load_defaults(BehaviorVersion::v2024_03_28())
            .await
            .region()
            .context("Unable to determine AWS region to use with S3")?
            .clone();

        let store = AmazonS3Builder::new()
            .with_url(destination)
            .with_region(aws_region.to_string())
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_credentials(Arc::new(AwsSdkCredentialsProvider {
                credentials_provider: DefaultCredentialsChain::builder().build().await,
            }))
            .with_retry(object_store::RetryConfig {
                max_retries: 8,
                retry_timeout: Duration::from_secs(60),
                backoff: object_store::BackoffConfig {
                    init_backoff: Duration::from_millis(100),
                    max_backoff: Duration::from_secs(5),
                    base: 2.,
                },
            })
            .build()?;

        Arc::new(store)
    } else {
        object_store::parse_url(&destination)?.0.into()
    };
    Ok(object_store)
}

#[derive(Clone, Debug)]
struct SnapshotUploadProgress {
    pub snapshot_complete_path: String,
    pub uploaded_files: Vec<String>,
}

impl SnapshotUploadProgress {
    fn with_snapshot_path(snapshot_complete_path: String) -> Self {
        SnapshotUploadProgress {
            snapshot_complete_path,
            uploaded_files: vec![],
        }
    }

    fn push(&mut self, filename: String) {
        self.uploaded_files.push(filename);
    }
}

struct PutSnapshotError {
    pub full_snapshot_path: String,
    pub uploaded_files: Vec<String>,
    pub error: anyhow::Error,
}

impl PutSnapshotError {
    fn from<E>(error: E, progress: SnapshotUploadProgress) -> Self
    where
        E: Into<anyhow::Error>,
    {
        PutSnapshotError {
            error: error.into(),
            full_snapshot_path: progress.snapshot_complete_path,
            uploaded_files: progress.uploaded_files,
        }
    }
}

// The object_store `put_multipart` method does not currently support PutMode, so we don't pass this
// at all; however since we upload snapshots to a unique path on every attempt, we don't expect any
// conflicts to arise.
async fn put_snapshot_object(
    file_path: &Path,
    key: &object_store::path::Path,
    object_store: &Arc<dyn ObjectStore>,
    buf: &mut BytesMut,
) -> anyhow::Result<object_store::PutResult> {
    debug!(path = ?file_path, "Putting snapshot object from local file");
    let mut snapshot = tokio::fs::File::open(file_path).await?;

    if snapshot.metadata().await?.len() < MULTIPART_UPLOAD_CHUNK_SIZE_BYTES as u64 {
        let payload = PutPayload::from(tokio::fs::read(file_path).await?);
        return object_store.put(key, payload).await.map_err(|e| e.into());
    }

    debug!("Performing multipart upload for {key}");
    let mut upload = object_store.put_multipart(key).await?;

    let result: anyhow::Result<_> = async {
        loop {
            let mut len = 0;
            buf.reserve(MULTIPART_UPLOAD_CHUNK_SIZE_BYTES);

            // Ensure full buffer unless at EOF
            while buf.len() < MULTIPART_UPLOAD_CHUNK_SIZE_BYTES {
                len = snapshot.read_buf(buf).await?;
                if len == 0 {
                    break;
                }
            }

            if !buf.is_empty() {
                upload
                    .put_part(PutPayload::from_bytes(buf.split().freeze()))
                    .await?;
            }

            if len == 0 {
                break;
            }
        }
        upload.complete().await.map_err(|e| anyhow!(e))
    }
    .await;

    match result {
        Ok(r) => Ok(r),
        Err(e) => {
            debug!("Aborting failed multipart upload");
            upload.abort().await?;
            Err(e)
        }
    }
}

async fn abort_tasks<T: 'static>(mut join_set: JoinSet<T>) {
    join_set.abort_all();
    while join_set.join_next().await.is_some() {}
}

#[derive(Debug)]
struct AwsSdkCredentialsProvider {
    credentials_provider: DefaultCredentialsChain,
}

#[async_trait]
impl object_store::CredentialProvider for AwsSdkCredentialsProvider {
    type Credential = object_store::aws::AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds = self
            .credentials_provider
            .provide_credentials()
            .await
            .map_err(|e| {
                // object_store's error detail rendering is not great but aws_config logs the
                // detailed underlying cause at WARN level so we don't need to do it again here
                object_store::Error::Unauthenticated {
                    path: "<n/a>".to_string(),
                    source: e.into(),
                }
            })?;

        Ok(Arc::new(object_store::aws::AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(|t| t.to_string()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use tracing::info;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{fmt, EnvFilter};
    use url::Url;

    use super::{LatestSnapshot, SnapshotRepository};
    use restate_partition_store::snapshots::{PartitionSnapshotMetadata, SnapshotFormatVersion};
    use restate_types::config::SnapshotsOptions;
    use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId};
    use restate_types::logs::{Lsn, SequenceNumber};

    #[tokio::test]
    async fn test_overwrite_unparsable_latest() -> anyhow::Result<()> {
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .init();

        let snapshot_source = TempDir::new()?;
        let source_dir = snapshot_source.path().to_path_buf();

        let data = b"snapshot-data";
        let mut data_file = tokio::fs::File::create(source_dir.join("data.sst")).await?;
        data_file.write_all(data).await?;

        let snapshot = mock_snapshot_metadata(
            "/data.sst".to_owned(),
            source_dir.to_string_lossy().to_string(),
            data.len(),
        );

        let snapshots_destination: TempDir = TempDir::new()?;
        let destination_dir = snapshots_destination.path().to_owned();
        let opts = SnapshotsOptions {
            destination: Some(
                Url::from_file_path(snapshots_destination.path())
                    .unwrap()
                    .to_string(),
            ),
            ..SnapshotsOptions::default()
        };
        let repository = SnapshotRepository::create_if_configured(
            &opts,
            TempDir::new().unwrap().into_path(),
            "cluster".to_owned(),
        )
        .await?
        .unwrap();

        // Write invalid JSON to latest.json
        let latest_path = destination_dir.join(format!("{}/latest.json", PartitionId::MIN));
        tokio::fs::create_dir_all(latest_path.parent().unwrap()).await?;
        info!("Creating file: {:?}", latest_path);
        let mut latest = tokio::fs::File::create(&latest_path).await?;
        latest.write_all(b"not valid json").await?;

        assert!(repository.put(&snapshot, source_dir).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_put_snapshot_local_filesystem() -> anyhow::Result<()> {
        let snapshots_destination = TempDir::new()?;
        test_put_snapshot(
            Url::from_file_path(snapshots_destination.path())
                .unwrap()
                .to_string(),
        )
        .await
    }

    /// For this test to run, set RESTATE_S3_INTEGRATION_TEST_BUCKET_NAME to a writable S3 bucket name
    #[tokio::test]
    async fn test_put_snapshot_s3() -> anyhow::Result<()> {
        let Ok(bucket_name) = std::env::var("RESTATE_S3_INTEGRATION_TEST_BUCKET_NAME") else {
            return Ok(());
        };
        test_put_snapshot(format!("s3://{bucket_name}/integration-test")).await
    }

    async fn test_put_snapshot(destination: String) -> anyhow::Result<()> {
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .init();

        let snapshot_source = TempDir::new()?;
        let source_dir = snapshot_source.path().to_path_buf();

        let destination_url = Url::parse(destination.as_str())?;
        let path = destination_url.path().to_string();
        let object_store = super::create_object_store_client(destination_url).await?;

        let latest = object_store
            .get(&Path::from(format!(
                "{}/{}/latest.json",
                path,
                PartitionId::MIN,
            )))
            .await;
        assert!(matches!(latest, Err(object_store::Error::NotFound { .. })));

        let data = b"snapshot-data";
        let mut data_file = tokio::fs::File::create(source_dir.join("data.sst")).await?;
        data_file.write_all(data).await?;

        let mut snapshot1 = mock_snapshot_metadata(
            "/data.sst".to_owned(),
            source_dir.to_string_lossy().to_string(),
            data.len(),
        );
        snapshot1.min_applied_lsn = Lsn::new(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_millis() as u64,
        );

        let opts = SnapshotsOptions {
            destination: Some(destination),
            ..SnapshotsOptions::default()
        };

        let repository = SnapshotRepository::create_if_configured(
            &opts,
            TempDir::new().unwrap().into_path(),
            "cluster".to_owned(),
        )
        .await?
        .unwrap();

        repository.put(&snapshot1, source_dir.clone()).await?;

        let snapshot_prefix = SnapshotRepository::get_snapshot_id_component(&snapshot1);
        let data = object_store
            .get(&Path::from(format!(
                "{}/{}/{}/data.sst",
                path, snapshot1.partition_id, snapshot_prefix,
            )))
            .await?;
        assert_eq!(data.bytes().await?, Bytes::from_static(b"snapshot-data"));

        let metadata = object_store
            .get(&Path::from(format!(
                "{}/{}/{}/metadata.json",
                path, snapshot1.partition_id, snapshot_prefix,
            )))
            .await?;
        let metadata: PartitionSnapshotMetadata = serde_json::from_slice(&metadata.bytes().await?)?;
        assert_eq!(snapshot1.snapshot_id, metadata.snapshot_id);

        let latest = object_store
            .get(&Path::from(format!(
                "{}/{}/latest.json",
                path, snapshot1.partition_id,
            )))
            .await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        assert_eq!(
            LatestSnapshot::from_snapshot(&snapshot1, snapshot_prefix),
            latest
        );

        let snapshot_source = TempDir::new()?;
        let source_dir = snapshot_source.path().to_path_buf();

        let data = b"snapshot-data";
        let mut data_file = tokio::fs::File::create(source_dir.join("data.sst")).await?;
        data_file.write_all(data).await?;

        let mut snapshot2 = mock_snapshot_metadata(
            "/data.sst".to_owned(),
            source_dir.to_string_lossy().to_string(),
            data.len(),
        );
        snapshot2.min_applied_lsn = snapshot1.min_applied_lsn.next();

        repository.put(&snapshot2, source_dir).await?;

        let latest = object_store
            .get(&Path::from(format!(
                "{}/{}/latest.json",
                path, snapshot2.partition_id,
            )))
            .await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        assert_eq!(
            LatestSnapshot::from_snapshot(
                &snapshot2,
                SnapshotRepository::get_snapshot_id_component(&snapshot2)
            ),
            latest
        );

        let latest = repository.get_latest(PartitionId::MIN).await?.unwrap();
        assert_eq!(latest.min_applied_lsn, snapshot2.min_applied_lsn);
        let local_path = latest.base_dir.as_path().to_string_lossy().to_string();
        drop(latest);

        let local_dir_exists = tokio::fs::try_exists(&local_path).await?;
        assert!(local_dir_exists);
        tokio::fs::remove_dir_all(&local_path).await?;

        Ok(())
    }

    fn mock_snapshot_metadata(
        file_name: String,
        directory: String,
        size: usize,
    ) -> PartitionSnapshotMetadata {
        PartitionSnapshotMetadata {
            version: SnapshotFormatVersion::V1,
            cluster_name: "cluster".to_string(),
            node_name: "node".to_string(),
            partition_id: PartitionId::MIN,
            created_at: humantime::Timestamp::from(SystemTime::now()),
            snapshot_id: SnapshotId::new(),
            key_range: PartitionKey::MIN..=PartitionKey::MAX,
            min_applied_lsn: Lsn::new(1),
            db_comparator_name: "leveldb.BytewiseComparator".to_string(),
            // this is totally bogus, but it doesn't matter since we won't be importing it into RocksDB
            files: vec![rocksdb::LiveFile {
                column_family_name: "data-0".to_owned(),
                name: file_name,
                directory,
                size,
                level: 0,
                start_key: Some(vec![0]),
                end_key: Some(vec![0xff, 0xff]),
                num_entries: 0,
                num_deletions: 0,
                smallest_seqno: 0,
                largest_seqno: 0,
            }],
        }
    }
}
