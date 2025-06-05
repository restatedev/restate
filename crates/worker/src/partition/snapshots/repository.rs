// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use anyhow::{Context, anyhow, bail};
use bytes::BytesMut;
use object_store::path::Path as ObjectPath;
use object_store::{MultipartUpload, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tempfile::TempDir;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;
use tracing::{debug, info, instrument, warn};
use url::Url;

use restate_object_store_util::create_object_store_client;
use restate_partition_store::snapshots::{
    LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotFormatVersion,
};
use restate_types::config::SnapshotsOptions;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::{Lsn, SequenceNumber};

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
    prefix: ObjectPath,
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
    pub fn from_snapshot(snapshot: &PartitionSnapshotMetadata) -> Self {
        LatestSnapshot {
            version: snapshot.version,
            cluster_name: snapshot.cluster_name.clone(),
            node_name: snapshot.node_name.clone(),
            partition_id: snapshot.partition_id,
            snapshot_id: snapshot.snapshot_id,
            created_at: snapshot.created_at,
            min_applied_lsn: snapshot.min_applied_lsn,
            path: UniqueSnapshotKey::from_metadata(snapshot).padded_key(),
        }
    }
}

struct UniqueSnapshotKey {
    lsn: Lsn,
    snapshot_id: SnapshotId,
}

impl UniqueSnapshotKey {
    fn from_metadata(snapshot: &PartitionSnapshotMetadata) -> Self {
        UniqueSnapshotKey {
            lsn: snapshot.min_applied_lsn,
            snapshot_id: snapshot.snapshot_id,
        }
    }

    /// Construct the unique path component for a snapshot, e.g. `lsn_00001234-snap_abc123`.
    /// The LSN is zero-padded for correct lexicographical sorting in object stores.
    fn padded_key(&self) -> String {
        format!(
            "lsn_{lsn:020}-{snapshot_id}",
            lsn = self.lsn,
            snapshot_id = self.snapshot_id
        )
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

        let prefix = destination.path().to_string();
        let object_store = create_object_store_client(
            destination.clone(),
            &snapshots_options.object_store,
            &snapshots_options.object_store_retry_policy,
        )
        .await?;

        Ok(Some(SnapshotRepository {
            object_store,
            destination,
            prefix: ObjectPath::from(prefix),
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
                    let path = put_error.full_snapshot_path.child(filename);

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
        let snapshot_prefix = self.get_base_prefix(snapshot);
        debug!(
            "Uploading snapshot from {:?} to {}",
            local_snapshot_path, snapshot_prefix
        );

        let mut progress = SnapshotUploadProgress::with_snapshot_path(snapshot_prefix);
        let mut buf = BytesMut::new();
        for file in &snapshot.files {
            let filename = file.name.trim_start_matches("/");
            let key = self.get_snapshot_file(snapshot, filename);

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

        let metadata_key = self.get_snapshot_file(snapshot, "metadata.json");
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

        let latest_path = self.get_latest_snapshot_pointer(snapshot.partition_id);

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

        let latest = LatestSnapshot::from_snapshot(snapshot);
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
        let latest_path = self.get_latest_snapshot_pointer(partition_id);

        let latest = match self.object_store.get(&latest_path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                debug!("Latest snapshot data not found in repository");
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        debug!("Latest snapshot metadata: {:?}", latest);

        let snapshot_metadata_path = self
            .prefix
            .child(partition_id.to_string())
            .child(latest.path.as_str())
            .child("metadata.json");
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
            warn!(
                "Snapshot does not match the cluster name of latest snapshot at destination in snapshot id {}! Expected: cluster name=\"{}\", found: \"{}\"",
                snapshot_metadata.snapshot_id, self.cluster_name, snapshot_metadata.cluster_name
            );
            return Ok(None); // perhaps this needs to be a configuration error
        }

        if !self.staging_dir.exists() {
            std::fs::create_dir_all(&self.staging_dir)?;
        }

        // The snapshot ingest directory should be on the same filesystem as the partition store
        // to minimize IO and disk space usage during import.
        let snapshot_dir = TempDir::with_prefix_in(
            format!("{}-", snapshot_metadata.snapshot_id),
            &self.staging_dir,
        )?;
        debug!(
            snapshot_id = %snapshot_metadata.snapshot_id,
            path = %snapshot_dir.path().display(),
            "Getting snapshot data files",
        );

        let directory = snapshot_dir.path().to_string_lossy().to_string();
        let concurrency_limiter = Arc::new(Semaphore::new(DOWNLOAD_CONCURRENCY_LIMIT));
        let mut downloads = JoinSet::new();
        let mut task_handles = HashMap::with_capacity(snapshot_metadata.files.len());
        for file in &mut snapshot_metadata.files {
            let filename = file.name.trim_start_matches("/");
            let expected_size = file.size;
            let key = self
                .prefix
                .child(partition_id.to_string())
                .child(latest.path.as_str())
                .child(filename);
            let file_path = snapshot_dir.path().join(filename);
            let concurrency_limiter = Arc::clone(&concurrency_limiter);
            let object_store = Arc::clone(&self.object_store);

            let handle = downloads.build_task().name(filename).spawn(async move {
                let _permit = concurrency_limiter.acquire().await?;
                debug!(%key, "Downloading snapshot file");
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
                snapshot_file.shutdown().await?;

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
            path = %snapshot_dir.path().display(),
            "Downloaded partition snapshot",
        );
        Ok(Some(LocalPartitionSnapshot {
            base_dir: snapshot_dir.into_path(),
            log_id: snapshot_metadata.get_log_id(),
            min_applied_lsn: snapshot_metadata.min_applied_lsn,
            db_comparator_name: snapshot_metadata.db_comparator_name,
            files: snapshot_metadata.files,
            key_range: snapshot_metadata.key_range.clone(),
        }))
    }

    /// Retrieve the latest known LSN to be archived to the snapshot repository.
    /// Response of `Ok(Lsn::INVALID)` indicates no existing snapshot for the partition.
    #[instrument(
        level = "debug",
        skip_all,
        err,
        fields(%partition_id),
    )]
    pub(crate) async fn get_latest_archived_lsn(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Lsn> {
        let latest_path = self.get_latest_snapshot_pointer(partition_id);

        let latest = match self.object_store.get(&latest_path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                debug!("Latest snapshot data not found in repository");
                return Ok(Lsn::INVALID);
            }
            Err(e) => return Err(e.into()),
        };

        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        debug!("Latest snapshot metadata: {:?}", latest);

        Ok(latest.min_applied_lsn)
    }

    async fn get_latest_snapshot_metadata_for_update(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        path: &ObjectPath,
    ) -> anyhow::Result<Option<(LatestSnapshot, UpdateVersion)>> {
        debug!(%path, "Getting latest snapshot pointer from repository");
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
                    bail!(
                        "Snapshot does not match the cluster name of latest snapshot at destination!"
                    );
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

    /// Construct the full object path to the latest snapshot pointer for a given partition.
    fn get_latest_snapshot_pointer(&self, partition_id: PartitionId) -> ObjectPath {
        self.get_partition_snapshots_prefix(partition_id)
            .child("latest.json")
    }

    /// Construct the prefix relative to the destination root for a given partition's snapshots.
    fn get_partition_snapshots_prefix(&self, partition_id: PartitionId) -> ObjectPath {
        self.prefix.child(partition_id.to_string())
    }

    /// Construct the complete snapshot prefix from the base of the object store destination.
    fn get_base_prefix(&self, snapshot_metadata: &PartitionSnapshotMetadata) -> ObjectPath {
        self.get_partition_snapshots_prefix(snapshot_metadata.partition_id)
            .child(UniqueSnapshotKey::from_metadata(snapshot_metadata).padded_key())
    }

    /// Construct the full object path for a specific file from the given snapshot.
    fn get_snapshot_file(
        &self,
        snapshot_metadata: &PartitionSnapshotMetadata,
        filename: &str,
    ) -> ObjectPath {
        self.get_base_prefix(snapshot_metadata).child(filename)
    }
}

#[derive(Clone, Debug)]
struct SnapshotUploadProgress {
    pub snapshot_complete_path: ObjectPath,
    pub uploaded_files: Vec<String>,
}

impl SnapshotUploadProgress {
    fn with_snapshot_path(snapshot_complete_path: ObjectPath) -> Self {
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
    pub full_snapshot_path: ObjectPath,
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
    key: &ObjectPath,
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use object_store::ObjectStore;
    use restate_object_store_util::create_object_store_client;
    use restate_types::retries::RetryPolicy;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use tracing::info;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{EnvFilter, fmt};
    use url::Url;

    use super::{LatestSnapshot, SnapshotRepository, UniqueSnapshotKey};
    use restate_partition_store::snapshots::{PartitionSnapshotMetadata, SnapshotFormatVersion};
    use restate_types::config::{ObjectStoreOptions, SnapshotsOptions};
    use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId};
    use restate_types::logs::{LogId, Lsn, SequenceNumber};

    use crate::partition::snapshots::repository::ObjectPath;

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
        data_file.shutdown().await?;

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
        let latest_path = destination_dir
            .join(PartitionId::MIN.to_string())
            .join("latest.json");
        tokio::fs::create_dir_all(latest_path.parent().unwrap()).await?;
        info!("Creating file: {:?}", latest_path);
        let mut latest = tokio::fs::File::create(&latest_path).await?;
        latest.write_all(b"not valid json").await?;
        latest.shutdown().await?;

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
        let latest_path = ObjectPath::from(destination_url.path().to_string())
            .child(PartitionId::MIN.to_string())
            .child("latest.json");
        let object_store = create_object_store_client(
            destination_url.clone(),
            &ObjectStoreOptions::default(),
            &RetryPolicy::None,
        )
        .await?;

        let latest = object_store.get(&latest_path).await;
        assert!(matches!(latest, Err(object_store::Error::NotFound { .. })));

        let data = b"snapshot-data";
        let mut data_file = tokio::fs::File::create(source_dir.join("data.sst")).await?;
        data_file.write_all(data).await?;
        data_file.shutdown().await?;

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

        let partition_prefix =
            ObjectPath::from(destination_url.path()).child(snapshot1.partition_id.to_string());

        let snapshot_1_prefix = partition_prefix.child(
            UniqueSnapshotKey::from_metadata(&snapshot1)
                .padded_key()
                .as_str(),
        );

        let data = object_store
            .get(&snapshot_1_prefix.child("data.sst"))
            .await?;
        assert_eq!(data.bytes().await?, Bytes::from_static(b"snapshot-data"));

        let metadata = object_store
            .get(&snapshot_1_prefix.child("metadata.json"))
            .await?;
        let metadata: PartitionSnapshotMetadata = serde_json::from_slice(&metadata.bytes().await?)?;
        assert_eq!(snapshot1.snapshot_id, metadata.snapshot_id);

        let latest = object_store
            .get(&partition_prefix.child("latest.json"))
            .await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        assert_eq!(LatestSnapshot::from_snapshot(&snapshot1), latest);

        let snapshot_source = TempDir::new()?;
        let source_dir = snapshot_source.path().to_path_buf();

        let data = b"snapshot-data";
        let mut data_file = tokio::fs::File::create(source_dir.join("data.sst")).await?;
        data_file.write_all(data).await?;
        data_file.shutdown().await?;

        let mut snapshot2 = mock_snapshot_metadata(
            "/data.sst".to_owned(),
            source_dir.to_string_lossy().to_string(),
            data.len(),
        );
        snapshot2.min_applied_lsn = snapshot1.min_applied_lsn.next();

        repository.put(&snapshot2, source_dir).await?;

        let latest = object_store
            .get(&partition_prefix.child("latest.json"))
            .await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        assert_eq!(LatestSnapshot::from_snapshot(&snapshot2,), latest);

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
            log_id: Some(LogId::from(PartitionId::MIN)),
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
