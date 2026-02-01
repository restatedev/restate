// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use anyhow::{Context, anyhow, bail};
use bytes::BytesMut;
use object_store::path::Path as ObjectPath;
use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tempfile::TempDir;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;
use tracing::{Instrument, Span, debug, info, instrument, warn};
use url::Url;

use restate_clock::WallClock;
use restate_core::Metadata;
use restate_metadata_server::MetadataStoreClient;
use restate_object_store_util::create_object_store_client;
use restate_types::config::SnapshotsOptions;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::{LogId, Lsn};
use restate_types::nodes_config::ClusterFingerprint;
use restate_types::time::MillisSinceEpoch;
use restate_types::{RESTATE_VERSION_1_7_0, SemanticRestateVersion};

#[cfg(any(test, feature = "test-util"))]
use super::leases::NoOpLeaseManager;
use super::leases::{LeaseError, SnapshotLeaseGuard, SnapshotLeaseManager};
use super::{LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotFormatVersion};

#[derive(Clone)]
pub enum LeaseProvider {
    Real(SnapshotLeaseManager),
    #[cfg(any(test, feature = "test-util"))]
    NoOp(NoOpLeaseManager),
}

impl LeaseProvider {
    pub async fn acquire(
        &self,
        partition_id: PartitionId,
    ) -> Result<SnapshotLeaseGuard, super::leases::LeaseError> {
        match self {
            Self::Real(m) => m.acquire(partition_id).await,
            #[cfg(any(test, feature = "test-util"))]
            Self::NoOp(m) => m.acquire(partition_id).await,
        }
    }
}

/// XXH3-128 content hash for SST files. Produces a 128-bit digest (32 hex characters).
#[derive(Clone, Copy)]
struct ContentHash(u128);

impl std::fmt::Display for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

async fn compute_content_hash(path: &Path) -> io::Result<ContentHash> {
    use xxhash_rust::xxh3::Xxh3;

    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Xxh3::new();
    let mut buf = vec![0u8; 256 * 1024]; // 256KB chunks

    loop {
        let len = file.read(&mut buf).await?;
        if len == 0 {
            break;
        }
        hasher.update(&buf[..len]);
    }

    Ok(ContentHash(hasher.digest128()))
}

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
/// Bucket layout:
/// - `[<prefix>/]<partition_id>/latest.json` - latest snapshot metadata for the partition
/// - `[<prefix>/]<partition_id>/{lsn}_{snapshot_id}/metadata.json` - snapshot descriptor
/// - `[<prefix>/]<partition_id>/{lsn}_{snapshot_id}/*.sst` - data files for full snapshots
/// - `[<prefix>/]<partition_id>/ssts/{hash}.sst` - shared SST files for incremental snapshots
///
/// Incremental snapshots use content-addressed SST naming where `{hash}` is the xxh3-128 hash
/// of the file contents. This enables deduplication across snapshots since files with identical
/// content will have the same key regardless of which snapshot created them.
#[derive(Clone)]
pub struct SnapshotRepository {
    object_store: Arc<dyn ObjectStore>,
    destination: Url,
    prefix: ObjectPath,
    staging_dir: PathBuf,
    num_retained: Option<std::num::NonZeroU8>,
    snapshot_type: restate_types::config::SnapshotType,
    lease_provider: Option<LeaseProvider>,
    #[cfg(any(test, feature = "test-util"))]
    enable_cleanup: bool,
}

/// S3 and other stores require a certain minimum size for the parts of a multipart upload. It is an
/// API error to attempt a multipart put below this size, apart from the final segment.
const MULTIPART_UPLOAD_CHUNK_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// Maximum number of concurrent downloads when getting snapshots from the repository.
const DOWNLOAD_CONCURRENCY_LIMIT: usize = 8;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum LatestSnapshotVersion {
    #[default]
    V1,
    /// V2 adds support for retained snapshots. Introduced in v1.6.
    V2,
}

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LatestSnapshot {
    pub version: LatestSnapshotVersion,

    pub partition_id: PartitionId,
    pub log_id: Option<LogId>, // mandatory in LatestSnapshotVersion::V2

    /// Restate cluster name which produced the snapshot.
    pub cluster_name: String,

    /// a unique fingerprint for this cluster.
    #[serde(default)]
    pub cluster_fingerprint: Option<ClusterFingerprint>,

    /// Node that produced this snapshot.
    pub node_name: String,

    /// Local node time when the snapshot was created.
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub created_at: jiff::Timestamp,

    /// Unique snapshot id.
    pub snapshot_id: SnapshotId,

    /// The minimum LSN guaranteed to be applied in this snapshot. The actual
    /// LSN may be >= [minimum_lsn].
    pub min_applied_lsn: Lsn,

    /// The relative path within the snapshot repository where the snapshot data is stored.
    pub path: String,

    /// Retained snapshots ordered by descending applied LSN (newest snapshot first). Pruning
    /// depends on correct ordering. Introduced in V2.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained_snapshots: Vec<SnapshotReference>,
}

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotReference {
    pub snapshot_id: SnapshotId,
    pub min_applied_lsn: Lsn,
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub created_at: jiff::Timestamp,
    pub path: String,
}

impl SnapshotReference {
    fn from_metadata(snapshot: &PartitionSnapshotMetadata) -> Self {
        SnapshotReference {
            snapshot_id: snapshot.snapshot_id,
            min_applied_lsn: snapshot.min_applied_lsn,
            created_at: snapshot.created_at,
            path: UniqueSnapshotKey::from_metadata(snapshot).padded_key(),
        }
    }
}

impl LatestSnapshot {
    pub fn from_snapshot(snapshot: &PartitionSnapshotMetadata) -> Self {
        LatestSnapshot {
            version: LatestSnapshotVersion::V1,
            cluster_name: snapshot.cluster_name.clone(),
            cluster_fingerprint: snapshot.cluster_fingerprint,
            node_name: snapshot.node_name.clone(),
            partition_id: snapshot.partition_id,
            log_id: Some(snapshot.log_id),
            snapshot_id: snapshot.snapshot_id,
            created_at: snapshot.created_at,
            min_applied_lsn: snapshot.min_applied_lsn,
            path: UniqueSnapshotKey::from_metadata(snapshot).padded_key(),
            retained_snapshots: vec![],
        }
    }

    /// We ensure that retained snapshots is in descending order of archived LSN, most recent snapshot first.
    fn effective_retained_snapshots(&self) -> Vec<SnapshotReference> {
        if self.retained_snapshots.is_empty() {
            // Upgrade path from V1 - implicitly the "latest" snapshot is always retained.
            if self.snapshot_id == SnapshotId::INVALID {
                // this is a placeholder (no snapshot exists in repo) - skip
                vec![]
            } else {
                vec![SnapshotReference {
                    snapshot_id: self.snapshot_id,
                    min_applied_lsn: self.min_applied_lsn,
                    created_at: self.created_at,
                    path: self.path.clone(),
                }]
            }
        } else {
            self.retained_snapshots.clone()
        }
    }

    pub fn validate(
        &self,
        cluster_name: &str,
        cluster_fingerprint: Option<ClusterFingerprint>,
    ) -> anyhow::Result<()> {
        if cluster_name != self.cluster_name {
            anyhow::bail!(
                "snapshot does not match the cluster name of this cluster, \
                 expected: '{cluster_name}' got: '{}'",
                self.cluster_name
            );
        }

        // Snapshots from earlier Restate versions might not have the fingerprint set. Hence, only
        // compare the fingerprints if both the snapshot and the cluster have fingerprints.
        if let (Some(incoming_fingerprint), Some(expected_fingerprint)) =
            (self.cluster_fingerprint, cluster_fingerprint)
            && expected_fingerprint != incoming_fingerprint
        {
            bail!(
                "cluster fingerprint mismatch, \
                 expected:'{expected_fingerprint}' {expected_fingerprint:?} got:'{incoming_fingerprint}' {incoming_fingerprint:?}. \
                 This often happens if this cluster is reusing a snapshot repository path from a different cluster"
            );
        }

        Ok(())
    }
}

/// Point-in-time representation of a given partition's known snapshot status
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, derive_more::Display)]
#[display("{}", latest_snapshot_id)]
pub struct PartitionSnapshotStatus {
    // Field ordering is intentional to naturally order items by LSN
    /// Safe to trim LSN for the partition
    pub archived_lsn: Lsn,
    pub latest_snapshot_lsn: Lsn,
    pub latest_snapshot_id: SnapshotId,
    pub log_id: LogId,
    pub latest_snapshot_created_at: MillisSinceEpoch,
}

impl PartitionSnapshotStatus {
    /// Creates a sentinel status indicating no snapshot exists for this partition
    pub fn none(log_id: LogId) -> Self {
        use restate_types::logs::SequenceNumber;
        Self {
            archived_lsn: Lsn::INVALID,
            latest_snapshot_lsn: Lsn::INVALID,
            latest_snapshot_id: SnapshotId::INVALID,
            log_id,
            latest_snapshot_created_at: WallClock::recent_ms(),
        }
    }
}

impl TryFrom<&LatestSnapshot> for PartitionSnapshotStatus {
    type Error = anyhow::Error;

    fn try_from(latest: &LatestSnapshot) -> Result<Self, Self::Error> {
        let log_id = match (latest.version, latest.log_id) {
            (_, Some(log_id)) => log_id,
            (LatestSnapshotVersion::V1, None) => {
                // V1 didn't store log_id, fall back to partition table lookup
                Metadata::with_current(|m| m.partition_table_ref())
                    .get(&latest.partition_id)
                    .map(|p| p.log_id())
                    .unwrap_or_else(|| LogId::default_for_partition(latest.partition_id))
            }
            (LatestSnapshotVersion::V2, None) => {
                return Err(anyhow!(
                    "LatestSnapshot V2 for partition {} (snapshot {}) missing required log_id",
                    latest.partition_id,
                    latest.snapshot_id
                ));
            }
        };

        let (archived_lsn, latest_snapshot_created_at) = match latest.version {
            LatestSnapshotVersion::V1 => (latest.min_applied_lsn, latest.created_at.into()),
            LatestSnapshotVersion::V2 => {
                // Report the earliest as the archived LSN, enables restoring from any of them.
                // We deliberately iterate instead of relying on the descending sort order
                // invariant as a defensive measure against e.g. manually edited metadata.
                let archived_lsn = latest
                    .retained_snapshots
                    .iter()
                    .min_by_key(|s| s.min_applied_lsn)
                    .map(|s| s.min_applied_lsn)
                    .unwrap_or(latest.min_applied_lsn);

                let latest_snapshot_created_at = latest
                    .retained_snapshots
                    .iter()
                    .max_by_key(|s| s.min_applied_lsn)
                    .map(|s| s.created_at.into())
                    .unwrap_or_else(|| latest.created_at.into());

                (archived_lsn, latest_snapshot_created_at)
            }
        };

        Ok(PartitionSnapshotStatus {
            archived_lsn,
            latest_snapshot_lsn: latest.min_applied_lsn,
            latest_snapshot_created_at,
            latest_snapshot_id: latest.snapshot_id,
            log_id,
        })
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
    /// Creates a writable repository with a default metadata-backed lease manager
    pub async fn new_from_config(
        snapshots_options: &SnapshotsOptions,
        staging_dir: PathBuf,
        metadata_store_client: MetadataStoreClient,
    ) -> anyhow::Result<Option<SnapshotRepository>> {
        Self::new_internal(snapshots_options, staging_dir, Some(metadata_store_client)).await
    }

    /// Creates a repository without a lease manager; can not be used to upload snapshots
    pub async fn new_read_only_from_config(
        snapshots_options: &SnapshotsOptions,
        staging_dir: PathBuf,
    ) -> anyhow::Result<Option<SnapshotRepository>> {
        Self::new_internal(snapshots_options, staging_dir, None).await
    }

    async fn new_internal(
        snapshots_options: &SnapshotsOptions,
        staging_dir: PathBuf,
        metadata_store_client: Option<MetadataStoreClient>,
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

        let lease_provider = metadata_store_client
            .map(|client| LeaseProvider::Real(SnapshotLeaseManager::new(client)));

        Ok(Some(SnapshotRepository {
            object_store,
            destination,
            prefix: ObjectPath::from(prefix),
            staging_dir,
            num_retained: snapshots_options.experimental_num_retained,
            snapshot_type: snapshots_options.experimental_snapshot_type,
            #[cfg(any(test, feature = "test-util"))]
            enable_cleanup: snapshots_options.enable_cleanup,
            lease_provider,
        }))
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn new_from_config_with_stub_leases(
        snapshots_options: &SnapshotsOptions,
        staging_dir: PathBuf,
    ) -> anyhow::Result<Option<SnapshotRepository>> {
        let mut destination = if let Some(ref destination) = snapshots_options.destination {
            Url::parse(destination).context("Failed parsing snapshot repository URL")?
        } else {
            return Ok(None);
        };
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
            num_retained: snapshots_options.experimental_num_retained,
            snapshot_type: snapshots_options.experimental_snapshot_type,
            enable_cleanup: snapshots_options.enable_cleanup,
            lease_provider: Some(LeaseProvider::NoOp(NoOpLeaseManager::new())),
        }))
    }

    /// Acquire a lease for snapshot operations on this partition
    ///
    /// On success, returns a guard with background renewal already started. The guard should be
    /// held for the duration of snapshot operations and passed to `put`.
    ///
    /// Returns `LeaseError::ReadOnly` if the repository was created without a lease provider.
    pub async fn acquire_lease(
        &self,
        partition_id: PartitionId,
    ) -> Result<Arc<SnapshotLeaseGuard>, LeaseError> {
        let provider = self
            .lease_provider
            .as_ref()
            .ok_or(LeaseError::Unavailable)?;
        let guard = provider.acquire(partition_id).await?;
        let guard = Arc::new(guard);
        guard.start_renewal_task()?;
        Ok(guard)
    }

    /// Write a partition snapshot to the snapshot repository
    ///
    /// Returns the latest snapshot status on successful upload. Depending on retention settings,
    /// the archived LSN may be earlier than that of the snapshot which was just uploaded. This
    /// operation requires a valid lease obtained by calling `acquire_lease`.
    #[instrument(
        level = "error",
        err,
        skip_all,
        fields(local_path = %local_snapshot_path.display())
    )]
    pub(crate) async fn put(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        local_snapshot_path: PathBuf,
        lease_guard: Arc<SnapshotLeaseGuard>,
    ) -> anyhow::Result<PartitionSnapshotStatus> {
        use crate::metric_definitions::{
            SNAPSHOT_UPLOAD_DURATION, SNAPSHOT_UPLOAD_FAILED, SNAPSHOT_UPLOAD_SUCCESS,
        };

        debug!("Publishing partition snapshot to: {}", self.destination);

        let start = tokio::time::Instant::now();
        let put_result = self
            .put_snapshot_inner(snapshot, local_snapshot_path.as_path(), lease_guard)
            .await;

        // We only log the error here since (a) it's relatively unlikely for rmdir to fail, and (b)
        // if we've uploaded the snapshot, we should get the response back to the caller. Logging at
        // WARN level as repeated failures could compromise the cluster.
        if let Err(err) = tokio::fs::remove_dir_all(local_snapshot_path.as_path()).await {
            warn!(%err, "Failed to delete local snapshot files");
        }

        metrics::histogram!(SNAPSHOT_UPLOAD_DURATION).record(start.elapsed());

        match put_result {
            Ok(status) => {
                metrics::counter!(SNAPSHOT_UPLOAD_SUCCESS).increment(1);
                Ok(status)
            }
            Err(put_error) => {
                metrics::counter!(SNAPSHOT_UPLOAD_FAILED).increment(1);
                for filename in put_error.uploaded_files {
                    let path = put_error.full_snapshot_path.child(filename);

                    // We disregard errors at this point; the snapshot repository pruning mechanism
                    // should catch these eventually.
                    if let Err(err) = self.object_store.delete(&path).await {
                        info!(%err, "Failed to delete file from partially uploaded snapshot");
                    }
                }
                Err(put_error.error)
            }
        }
    }

    /// If snapshot type is incremental, we will skip existing existing SST objects in the store
    /// based on their content hash. Returns a LiveFile SST name to object key mapping, if SSTs are
    /// uploaded to a path different from their original name (i.e. when part of content-addressed
    /// incremental snapshots).
    async fn upload_snapshot_with_dedup(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        local_snapshot_path: &Path,
        buf: &mut BytesMut,
        progress: &mut SnapshotUploadProgress,
    ) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
        use restate_types::config::SnapshotType;

        let mut file_keys = std::collections::BTreeMap::new();
        let mut total_size = 0usize;
        let mut uploaded_size = 0usize;
        let mut files_uploaded = 0usize;
        let mut files_skipped = 0usize;

        for file in &snapshot.files {
            let filename = strip_leading_slash(&file.name);
            let local_path = local_snapshot_path.join(filename);
            total_size += file.size;

            let (repository_key, relative_key, must_upload) = match self.snapshot_type {
                SnapshotType::Full => {
                    let key = self.snapshot_file_path(snapshot, filename);
                    (key, None, true)
                }
                SnapshotType::Incremental => {
                    // content-addressed SSTs live in shared ../ssts/ prefix
                    // key format: {hash}.sst - content-addressed, existence implies identical content
                    let content_hash = compute_content_hash(&local_path).await?;
                    let sst_name = format!("{content_hash}.sst");
                    let sst_key = self
                        .partition_snapshots_prefix(snapshot.partition_id)
                        .child("ssts")
                        .child(sst_name.as_str());

                    let must_upload = match self.object_store.head(&sst_key).await {
                        Ok(_) => {
                            debug!(
                                sst = %filename,
                                hash = %content_hash,
                                size = file.size,
                                "SST already exists in repository (content-addressed), skipping"
                            );
                            files_skipped += 1;
                            false
                        }
                        Err(object_store::Error::NotFound { .. }) => true,
                        Err(e) => {
                            warn!(
                                sst = %filename,
                                error = %e,
                                "Failed to check if SST exists, uploading to be safe"
                            );
                            true
                        }
                    };

                    let relative_key = format!("ssts/{}", sst_name);
                    (sst_key, Some(relative_key), must_upload)
                }
            };

            if must_upload {
                put_snapshot_object(&local_path, &repository_key, &self.object_store, buf).await?;
                debug!(
                    sst = %filename,
                    size = file.size,
                    repository_key = %repository_key,
                    "Uploaded SST to repository"
                );

                files_uploaded += 1;
                uploaded_size += file.size;
                progress.push(file.name.clone());
            }

            if let Some(key) = relative_key {
                file_keys.insert(file.name.clone(), key);
            }
        }

        if matches!(self.snapshot_type, SnapshotType::Incremental) && files_skipped > 0 {
            let dedup_rate = if total_size > 0 {
                ((total_size - uploaded_size) as f64 / total_size as f64) * 100.0
            } else {
                0.0
            };

            info!(
                partition_id = %snapshot.partition_id,
                snapshot_id = %snapshot.snapshot_id,
                files_uploaded = files_uploaded,
                files_skipped = files_skipped,
                bytes_uploaded = uploaded_size,
                bytes_saved = total_size - uploaded_size,
                dedup_rate = format!("{:.1}%", dedup_rate),
                "Snapshot SST upload completed with deduplication"
            );
        }

        Ok(file_keys)
    }

    // It is the outer put method's responsibility to clean up partial progress.
    async fn put_snapshot_inner(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        local_snapshot_path: &Path,
        lease_guard: Arc<SnapshotLeaseGuard>,
    ) -> Result<PartitionSnapshotStatus, PutSnapshotError> {
        let snapshot_prefix = self.base_prefix(snapshot);
        debug!(
            "Uploading snapshot from {:?} to {}",
            local_snapshot_path, snapshot_prefix
        );

        let mut progress = SnapshotUploadProgress::with_snapshot_path(snapshot_prefix);
        let mut buf = BytesMut::new();

        let file_keys = self
            .upload_snapshot_with_dedup(snapshot, local_snapshot_path, &mut buf, &mut progress)
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;

        let mut snapshot_with_keys = snapshot.clone();
        snapshot_with_keys.file_keys = file_keys;

        let metadata_key = self.snapshot_file_path(snapshot, "metadata.json");
        let metadata_json_payload = PutPayload::from(
            serde_json::to_string_pretty(&snapshot_with_keys).expect("Can always serialize JSON"),
        );

        let put_result = self
            .object_store
            .put(&metadata_key, metadata_json_payload)
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;
        progress.push("/metadata.json".to_owned());

        debug!(
            key = %metadata_key,
            etag = %put_result.e_tag.unwrap_or_default(),
            "Successfully published snapshot metadata",
        );

        let latest_path = self.latest_snapshot_pointer_path(snapshot.partition_id);
        let maybe_stored = self
            .get_latest_snapshot_metadata_for_update(&latest_path)
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;

        let format_version = self.determine_format_version(maybe_stored.as_ref().map(|(l, _)| l));
        let (new_latest, evicted_snapshots) = match format_version {
            LatestSnapshotVersion::V1 => (self.build_latest_v1(snapshot), vec![]),
            LatestSnapshotVersion::V2 => self
                .build_latest_v2(snapshot, maybe_stored.as_ref().map(|(l, _)| l))
                .map_err(|e| PutSnapshotError::from(e, progress.clone()))?,
        };

        let latest_payload = PutPayload::from(
            serde_json::to_string_pretty(&new_latest)
                .map_err(|e| PutSnapshotError::from(e, progress.clone()))?,
        );

        let conditions = self.conditional_put_options(maybe_stored.map(|(_, v)| v));
        let put_result = self
            .object_store
            .put_opts(&latest_path, latest_payload, conditions)
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;

        debug!(
            key = ?latest_path,
            etag = %put_result.e_tag.unwrap_or_default(),
            "Successfully updated latest snapshot pointer",
        );

        #[cfg(not(any(test, feature = "test-util")))]
        let enable_cleanup = true;
        #[cfg(any(test, feature = "test-util"))]
        let enable_cleanup = self.enable_cleanup;

        if !evicted_snapshots.is_empty() && enable_cleanup {
            self.spawn_cleanup_task(snapshot.partition_id, evicted_snapshots, lease_guard);
        }

        PartitionSnapshotStatus::try_from(&new_latest)
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))
    }

    fn determine_format_version(&self, current: Option<&LatestSnapshot>) -> LatestSnapshotVersion {
        // V2 is the default from 1.7.0, including pre-releases
        if SemanticRestateVersion::current().is_equal_or_newer_than(&RESTATE_VERSION_1_7_0)
            || self.num_retained.is_some()
        {
            if let Some(latest) = current
                && latest.version == LatestSnapshotVersion::V1
            {
                debug!("Upgrading latest snapshot format from V1 to V2");
            }
            LatestSnapshotVersion::V2
        } else {
            current
                .map(|l| l.version)
                .unwrap_or(LatestSnapshotVersion::V1)
        }
    }

    fn build_latest_v1(&self, snapshot: &PartitionSnapshotMetadata) -> LatestSnapshot {
        LatestSnapshot::from_snapshot(snapshot)
    }

    /// Builds V2 latest snapshot metadata.
    ///
    /// Returns `(LatestSnapshot, Vec<SnapshotReference>)` where the second element contains the
    /// snapshots that can be evicted from the store following the metadata update.
    fn build_latest_v2(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        current: Option<&LatestSnapshot>,
    ) -> anyhow::Result<(LatestSnapshot, Vec<SnapshotReference>)> {
        let new_snapshot_ref = SnapshotReference::from_metadata(snapshot);

        let (retained_snapshots, evicted_snapshots) = match self.num_retained {
            None => (vec![], vec![]), // tracking is only enabled if num-retained is set
            Some(num_retained) => {
                let mut retained_snapshots = current
                    .map(|l| l.effective_retained_snapshots())
                    .unwrap_or_default();

                // List will be in correct descending order if we insert the newest snapshot first
                retained_snapshots.insert(0, new_snapshot_ref.clone());

                let evicted_snapshots = retained_snapshots
                    .split_off((num_retained.get() as usize).min(retained_snapshots.len()));

                (retained_snapshots, evicted_snapshots)
            }
        };

        let latest = LatestSnapshot {
            version: LatestSnapshotVersion::V2,
            partition_id: snapshot.partition_id,
            log_id: Some(snapshot.log_id),
            cluster_name: snapshot.cluster_name.clone(),
            cluster_fingerprint: snapshot.cluster_fingerprint,
            node_name: snapshot.node_name.clone(),
            created_at: snapshot.created_at,
            snapshot_id: snapshot.snapshot_id,
            min_applied_lsn: snapshot.min_applied_lsn,
            path: new_snapshot_ref.path.clone(),
            retained_snapshots,
        };

        Ok((latest, evicted_snapshots))
    }

    fn conditional_put_options(&self, version: Option<UpdateVersion>) -> PutOptions {
        // The object_store file provider supports create-if-not-exists but not update-version on
        // put. The file:// protocol is only be enabled in test because of this.
        let use_conditional_update = !matches!(self.destination.scheme(), "file");

        let mode = match (use_conditional_update, version) {
            (true, Some(v)) if v.e_tag.is_some() || v.version.is_some() => PutMode::Update(v),
            (false, _) => PutMode::Overwrite,
            _ => PutMode::Create,
        };

        PutOptions {
            mode,
            ..PutOptions::default()
        }
    }

    fn spawn_cleanup_task(
        &self,
        partition_id: PartitionId,
        cleanup_snapshots: Vec<SnapshotReference>,
        lease_guard: Arc<SnapshotLeaseGuard>,
    ) {
        let repository = self.clone();
        let task_name = format!("snapshot-cleanup-{}", partition_id);

        let _ = restate_core::TaskCenter::spawn_unmanaged_child(
            restate_core::TaskKind::Disposable,
            task_name,
            async move {
                repository
                    .cleanup_evicted_snapshots(partition_id, cleanup_snapshots, lease_guard)
                    .await;
                Ok::<(), anyhow::Error>(())
            },
        );
    }

    #[instrument(level = "debug", skip_all, fields(%partition_id))]
    async fn cleanup_evicted_snapshots(
        &self,
        partition_id: PartitionId,
        evicted_snapshots: Vec<SnapshotReference>,
        lease_guard: Arc<SnapshotLeaseGuard>,
    ) {
        if !lease_guard.is_valid() {
            debug!("Lease expired before cleanup, aborting");
            return;
        }

        let result = lease_guard
            .run_under_lease(self.cleanup_evicted_snapshots_inner(
                partition_id,
                evicted_snapshots,
                &lease_guard,
            ))
            .await;

        match result {
            Some(Ok(())) => {
                debug!("Cleanup completed successfully");
            }
            Some(Err(e)) => {
                debug!(error = %e, "Cleanup failed");
            }
            None => {
                warn!("Cleanup aborted due to lease loss");
            }
        }
        // lease_guard dropped
    }

    async fn cleanup_evicted_snapshots_inner(
        &self,
        partition_id: PartitionId,
        evicted_snapshots: Vec<SnapshotReference>,
        lease_guard: &Arc<SnapshotLeaseGuard>,
    ) -> anyhow::Result<()> {
        if !lease_guard.is_valid() {
            anyhow::bail!("Lease expired before cleanup could start");
        }

        // Get currently retained snapshots to know which SSTs are still in use.
        // Abort cleanup if we cannot determine this to avoid deleting shared SSTs.
        let retained_snapshots = self
            .get_current_retained_snapshots(partition_id)
            .await
            .context("Cannot proceed with cleanup; aborting to prevent potential data loss")?;

        for snapshot_ref in &evicted_snapshots {
            if !lease_guard.is_valid() {
                debug!(
                    %partition_id,
                    "Lease approaching deadline, aborting remaining cleanup"
                );
                break;
            }

            // Errors are logged inside delete_snapshot_files; if cleanup fails,
            // these snapshots become orphans to be cleaned by future scan-sweep.
            self.delete_snapshot_files(partition_id, snapshot_ref, &retained_snapshots)
                .await;
        }

        Ok(())
    }

    /// Get retained snapshots for cleanup coordination.
    ///
    /// Returns an error if latest.json exists but cannot be read or parsed, since we cannot
    /// safely determine which SSTs are still referenced by retained snapshots. Cleanup must
    /// abort in this case to avoid deleting shared SSTs that may still be in use.
    async fn get_current_retained_snapshots(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Vec<SnapshotReference>> {
        let latest_path = self.latest_snapshot_pointer_path(partition_id);
        match self.object_store.get(&latest_path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|e| {
                    warn!(
                        %partition_id,
                        error = %e,
                        "Failed to read latest.json bytes; cannot determine retained snapshots"
                    );
                    anyhow!("Failed to read latest.json bytes: {}", e)
                })?;
                let latest: LatestSnapshot = serde_json::from_slice(&bytes).map_err(|e| {
                    warn!(
                        %partition_id,
                        error = %e,
                        "Failed to parse latest.json; cannot determine retained snapshots"
                    );
                    anyhow!("Failed to parse latest.json: {}", e)
                })?;
                Ok(latest.effective_retained_snapshots())
            }
            Err(object_store::Error::NotFound { .. }) => {
                // No latest.json means no snapshots exist yet, so no SSTs to protect
                debug!(%partition_id, "No latest.json found; no retained snapshots to protect");
                Ok(vec![])
            }
            Err(e) => {
                warn!(
                    %partition_id,
                    error = %e,
                    "Failed to fetch latest.json; cannot determine retained snapshots"
                );
                Err(anyhow!("Failed to fetch latest.json: {}", e))
            }
        }
    }

    /// Best-effort deletion of snapshot files. Errors are logged but not propagated.
    #[instrument(level = "warn", skip(self), fields(%partition_id, snapshot_id = %snapshot_ref.snapshot_id))]
    async fn delete_snapshot_files(
        &self,
        partition_id: PartitionId,
        snapshot_ref: &SnapshotReference,
        retained_snapshots: &[SnapshotReference],
    ) {
        let metadata_path = self
            .prefix
            .child(partition_id.to_string())
            .child(snapshot_ref.path.as_str())
            .child("metadata.json");

        let metadata = match self.object_store.get(&metadata_path).await {
            Ok(data) => {
                let bytes = match data.bytes().await {
                    Ok(b) => b,
                    Err(err) => {
                        warn!(%err, "Failed to read snapshot metadata bytes during cleanup");
                        return;
                    }
                };
                match serde_json::from_slice::<PartitionSnapshotMetadata>(&bytes) {
                    Ok(m) => m,
                    Err(err) => {
                        warn!(%err, "Failed to parse snapshot metadata during cleanup");
                        return;
                    }
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                // already deleted, this is fine
                return;
            }
            Err(err) => {
                warn!(%err, "Failed to fetch snapshot metadata during cleanup");
                return;
            }
        };

        // Build set of SST keys that are still referenced by retained snapshots
        let mut referenced_sst_keys = HashSet::new();
        for retained_ref in retained_snapshots {
            let retained_metadata_path = self
                .prefix
                .child(partition_id.to_string())
                .child(retained_ref.path.as_str())
                .child("metadata.json");

            if let Ok(data) = self.object_store.get(&retained_metadata_path).await
                && let Ok(bytes) = data.bytes().await
                && let Ok(retained_metadata) =
                    serde_json::from_slice::<PartitionSnapshotMetadata>(&bytes)
            {
                for file in &retained_metadata.files {
                    let filename = strip_leading_slash(&file.name);
                    let sst_key =
                        if let Some(relative_key) = retained_metadata.file_keys.get(&file.name) {
                            let parts: Vec<&str> = relative_key.split('/').collect();
                            if parts.len() == 2 {
                                self.prefix
                                    .child(partition_id.to_string())
                                    .child(parts[0])
                                    .child(parts[1])
                            } else {
                                self.prefix
                                    .child(partition_id.to_string())
                                    .child(relative_key.as_str())
                            }
                        } else {
                            self.prefix
                                .child(partition_id.to_string())
                                .child(retained_ref.path.as_str())
                                .child(filename)
                        };
                    referenced_sst_keys.insert(sst_key);
                }
            }
        }

        let mut any_failed = false;
        for file in &metadata.files {
            let filename = strip_leading_slash(&file.name);
            let path = if let Some(relative_key) = metadata.file_keys.get(&file.name) {
                // Incremental snapshot: SST is in shared ssts/ directory
                // Format: "ssts/{hash}.sst" (content-addressed) or legacy "ssts/{node_id}_{filename}"
                let parts: Vec<&str> = relative_key.split('/').collect();
                if parts.len() == 2 {
                    self.prefix
                        .child(partition_id.to_string())
                        .child(parts[0])
                        .child(parts[1])
                } else {
                    self.prefix
                        .child(partition_id.to_string())
                        .child(relative_key.as_str())
                }
            } else {
                // Legacy full snapshot: SST is in snapshot-specific directory
                self.snapshot_file_path(&metadata, filename)
            };

            if referenced_sst_keys.contains(&path) {
                debug!(%path, "Skipping deletion of SST file still referenced by retained snapshots");
                continue;
            }

            if let Err(err) = self.object_store.delete(&path).await
                && !matches!(err, object_store::Error::NotFound { .. })
            {
                warn!(%path, %err, "Failed to delete snapshot object");
                any_failed = true;
            }
        }

        // Only delete metadata.json after all SST deletions succeed.
        // This ensures retries can still identify which SSTs need to be deleted.
        // If we deleted metadata while SSTs remain, subsequent retries would see
        // NotFound for metadata.json and return Ok(()), leaving orphaned SSTs.
        if !any_failed
            && let Err(err) = self.object_store.delete(&metadata_path).await
            && !matches!(err, object_store::Error::NotFound { .. })
        {
            warn!(%metadata_path, %err, "Failed to delete snapshot metadata");
            any_failed = true;
        }

        if any_failed {
            warn!(
                "Failed to clean up old snapshot; repeated failures may lead to increased object store usage"
            );
        }
    }

    /// Discover and download the latest snapshot available. It is the caller's responsibility
    /// to delete the snapshot directory when it is no longer needed.
    #[instrument(
        name = "get-latest-snapshot",
        level = "error",
        skip_all,
        fields(%partition_id, snapshot_id = tracing::field::Empty),
    )]
    pub async fn get_latest(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Option<LocalPartitionSnapshot>> {
        use crate::metric_definitions::{SNAPSHOT_DOWNLOAD_DURATION, SNAPSHOT_DOWNLOAD_FAILED};

        let start = tokio::time::Instant::now();
        let result = self.get_latest_inner(partition_id).await;

        if result.is_err() {
            metrics::counter!(SNAPSHOT_DOWNLOAD_FAILED).increment(1);
        }
        metrics::histogram!(SNAPSHOT_DOWNLOAD_DURATION).record(start.elapsed());

        result
    }

    async fn get_latest_inner(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Option<LocalPartitionSnapshot>> {
        let latest_path = self.latest_snapshot_pointer_path(partition_id);

        let latest = match self.object_store.get(&latest_path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                debug!("Latest snapshot data not found in repository");
                return Ok(None);
            }
            Err(err) => return Err(err.into()),
        };

        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        tracing::Span::current().record("snapshot_id", tracing::field::display(latest.snapshot_id));
        debug!("Latest snapshot metadata: {latest:?}");
        Metadata::with_current(|m| {
            let nodes_config = m.nodes_config_ref();

            latest.validate(
                nodes_config.cluster_name(),
                nodes_config.cluster_fingerprint(),
            )?;
            anyhow::Ok(())
        })
        .with_context(|| format!("'{latest_path}' has validation errors"))?;

        let snapshot_metadata_path = self
            .prefix
            .child(partition_id.to_string())
            .child(latest.path.as_str())
            .child("metadata.json");
        let snapshot_metadata = self.object_store.get(&snapshot_metadata_path).await;

        let snapshot_metadata = match snapshot_metadata {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                bail!(
                    "Latest snapshot points to '{snapshot_metadata_path}' that was not found in the repository!"
                );
            }
            Err(err) => return Err(err.into()),
        };

        let mut snapshot_metadata: PartitionSnapshotMetadata =
            serde_json::from_slice(&snapshot_metadata.bytes().await?)?;
        if !matches!(snapshot_metadata.version, SnapshotFormatVersion::V1) {
            bail!(
                "Unsupported snapshot format version: {:?}",
                snapshot_metadata.version
            );
        }

        Metadata::with_current(|m| {
            let nodes_config = m.nodes_config_ref();

            snapshot_metadata.validate(
                nodes_config.cluster_name(),
                nodes_config.cluster_fingerprint(),
            )?;
            anyhow::Ok(())
        })
        .with_context(|| {
            format!(
                "failed validating metadata of snapshot {}",
                snapshot_metadata.snapshot_id
            )
        })?;

        if !self.staging_dir.exists() {
            std::fs::create_dir_all(&self.staging_dir)?;
        }

        // The snapshot ingest directory should be on the same filesystem as the partition store
        // to minimize IO and disk space usage during import.
        let snapshot_dir = TempDir::with_prefix_in(
            format!("{}-", snapshot_metadata.snapshot_id),
            &self.staging_dir,
        )?;
        debug!(path = %snapshot_dir.path().display(), "Downloading snapshot");

        let directory = snapshot_dir.path().to_string_lossy().to_string();
        let concurrency_limiter = Arc::new(Semaphore::new(DOWNLOAD_CONCURRENCY_LIMIT));
        let mut downloads = JoinSet::new();
        let mut task_handles = HashMap::with_capacity(snapshot_metadata.files.len());
        for file in &mut snapshot_metadata.files {
            let filename = strip_leading_slash(&file.name);
            let expected_size = file.size;
            let key = if let Some(relative_key) = snapshot_metadata.file_keys.get(&file.name) {
                // Incremental snapshot: parse the relative key and build proper path
                // Format: "ssts/{hash}.sst" (content-addressed) or legacy "ssts/{node_id}_{filename}"
                let parts: Vec<&str> = relative_key.split('/').collect();
                if parts.len() == 2 {
                    self.prefix
                        .child(partition_id.to_string())
                        .child(parts[0]) // "ssts"
                        .child(parts[1]) // "{node_id}_{filename}"
                } else {
                    // Fallback if format is unexpected
                    self.prefix
                        .child(partition_id.to_string())
                        .child(relative_key.as_str())
                }
            } else {
                // Full/legacy snapshot: use snapshot-specific path
                self.prefix
                    .child(partition_id.to_string())
                    .child(latest.path.as_str())
                    .child(filename)
            };
            let local_path = snapshot_dir.path().join(filename);
            let concurrency_limiter = Arc::clone(&concurrency_limiter);
            let object_store = Arc::clone(&self.object_store);
            let snapshot_id = snapshot_metadata.snapshot_id;
            let snapshot_filename = filename.to_owned();

            let handle = downloads.build_task().name(filename).spawn(async move {
                let _permit = concurrency_limiter.acquire().await?;
                debug!(%key, "Downloading snapshot object");
                let mut file_data = StreamReader::new(
                    object_store
                        .get(&key)
                        .await
                        .map_err(|e| anyhow!("Failed to download partition {partition_id} snapshot {snapshot_id} file {key:?}: {e}"))?
                        .into_stream(),
                );

                let mut snapshot_file =
                    tokio::fs::File::create_new(&local_path).await.map_err(|e| {
                        anyhow!("Failed to create local partition {partition_id} snapshot file {local_path:?}: {e}")
                    })?;
                let size = io::copy(&mut file_data, &mut snapshot_file)
                    .await
                    .map_err(|e| anyhow!("Failed to download snapshot object {:?}: {}", key, e))?;
                snapshot_file.shutdown().await?;

                if size != expected_size as u64 {
                    return Err(anyhow!("Downloaded partition {partition_id} snapshot {snapshot_id} component file {:?} has unexpected size: expected: {}, actual: {}", snapshot_filename, expected_size, size));
                }
                debug!(
                    %key,
                    ?size,
                    "Downloaded snapshot object {}",
                    local_path.display(),
                );
                anyhow::Ok(())
            }.instrument(Span::current()))?;
            task_handles.insert(handle.id(), filename.to_string());
            // patch the directory path to reflect the actual location on the restoring node
            file.directory = directory.clone();
        }

        loop {
            match downloads.join_next().await {
                None => {
                    debug!(snapshot_id = %snapshot_metadata.snapshot_id, "All download tasks completed");
                    break;
                }
                Some(Err(join_error)) => {
                    let failed_id = task_handles.get(&join_error.id());
                    abort_tasks(downloads).await;
                    return Err(anyhow!(
                        "Failed to download snapshot object {:?}: {}",
                        failed_id,
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
            base_dir: snapshot_dir.keep(),
            log_id: snapshot_metadata.log_id,
            min_applied_lsn: snapshot_metadata.min_applied_lsn,
            db_comparator_name: snapshot_metadata.db_comparator_name,
            files: snapshot_metadata.files,
            key_range: snapshot_metadata.key_range.clone(),
        }))
    }

    /// Retrieve the latest snapshot metadata from the snapshot repository
    ///
    /// If there are multiple retained snapshots, the archived LSN will be that of the earliest
    /// snapshot's LSN. This allows restoring any of the retained snapshots.
    pub async fn get_latest_partition_snapshot_status(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Option<PartitionSnapshotStatus>> {
        let latest_path = self.latest_snapshot_pointer_path(partition_id);

        let latest = match self.object_store.get(&latest_path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                debug!("Latest snapshot data not found in repository");
                return Ok(None);
            }
            Err(err) => {
                return Err(anyhow::Error::new(err).context(format!(
                    "couldn't fetch '{latest_path}' from snapshot repository"
                )));
            }
        };

        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        debug!(partition_id = %partition_id, snapshot_id = %latest.snapshot_id, "Latest snapshot metadata: {:?}", latest);

        Ok(Some(PartitionSnapshotStatus::try_from(&latest)?))
    }

    async fn get_latest_snapshot_metadata_for_update(
        &self,
        path: &ObjectPath,
    ) -> anyhow::Result<Option<(LatestSnapshot, UpdateVersion)>> {
        debug!(%path, "Getting latest snapshot pointer for update");
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
                        "Failed to parse stored latest snapshot pointer, refusing to overwrite: {}",
                        e
                    )
                    })
                    .map_err(|e| anyhow!("Failed to parse latest snapshot metadata: {}", e))?;

                Metadata::with_current(|m| {
                    let nodes_config = m.nodes_config_ref();
                    let fingerprint = nodes_config.cluster_fingerprint();
                    let cluster_name = nodes_config.cluster_name();
                    latest.validate(cluster_name, fingerprint)?;
                    anyhow::Ok(())
                })?;

                Ok(Some((latest, version)))
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("No latest snapshot pointer found, will create one");
                Ok(None)
            }
            Err(err) => {
                bail!("Failed to get latest snapshot pointer: {}", err);
            }
        }
    }

    fn latest_snapshot_pointer_path(&self, partition_id: PartitionId) -> ObjectPath {
        self.partition_snapshots_prefix(partition_id)
            .child("latest.json")
    }

    fn partition_snapshots_prefix(&self, partition_id: PartitionId) -> ObjectPath {
        self.prefix.child(partition_id.to_string())
    }

    fn base_prefix(&self, snapshot_metadata: &PartitionSnapshotMetadata) -> ObjectPath {
        self.partition_snapshots_prefix(snapshot_metadata.partition_id)
            .child(UniqueSnapshotKey::from_metadata(snapshot_metadata).padded_key())
    }

    fn snapshot_file_path(
        &self,
        snapshot_metadata: &PartitionSnapshotMetadata,
        filename: &str,
    ) -> ObjectPath {
        self.base_prefix(snapshot_metadata).child(filename)
    }
}

// Strip the leading "/" character from RocksDB LiveFile names
fn strip_leading_slash(name: &str) -> &str {
    name.trim_start_matches('/')
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
        Err(err) => {
            debug!("Aborting failed multipart upload");
            upload.abort().await?;
            Err(err)
        }
    }
}

async fn abort_tasks<T: 'static>(mut join_set: JoinSet<T>) {
    join_set.abort_all();
    while join_set.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use ahash::HashSet;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use jiff::Timestamp;
    use object_store::ObjectStoreExt;
    use object_store::path::Path as ObjectPath;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use tracing::info;
    use url::Url;

    use restate_clock::time::MillisSinceEpoch;
    use restate_core::{Metadata, TestCoreEnv};
    use restate_object_store_util::create_object_store_client;
    use restate_types::config::{ObjectStoreOptions, SnapshotsOptions};
    use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId};
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::retries::RetryPolicy;

    use crate::snapshots::SnapshotLeaseGuard;
    use crate::snapshots::repository::LatestSnapshotVersion;

    use super::{LatestSnapshot, SnapshotReference, SnapshotRepository, UniqueSnapshotKey};
    use super::{PartitionSnapshotMetadata, SnapshotFormatVersion};

    #[restate_core::test]
    async fn test_overwrite_unparsable_latest() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

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
        let repository = SnapshotRepository::new_from_config(
            &opts,
            TempDir::new().unwrap().keep(),
            env.metadata_store_client.clone(),
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

        assert!(
            repository
                .put(&snapshot, source_dir, Arc::new(SnapshotLeaseGuard::noop()))
                .await
                .is_err()
        );

        Ok(())
    }

    #[restate_core::test]
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
    #[restate_core::test]
    async fn test_put_snapshot_s3() -> anyhow::Result<()> {
        let Ok(bucket_name) = std::env::var("RESTATE_S3_INTEGRATION_TEST_BUCKET_NAME") else {
            return Ok(());
        };
        test_put_snapshot(format!("s3://{bucket_name}/integration-test")).await
    }

    async fn test_put_snapshot(destination: String) -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

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

        let repository = SnapshotRepository::new_from_config(
            &opts,
            TempDir::new().unwrap().keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

        repository
            .put(
                &snapshot1,
                source_dir.clone(),
                Arc::new(SnapshotLeaseGuard::noop()),
            )
            .await?;

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

        repository
            .put(&snapshot2, source_dir, Arc::new(SnapshotLeaseGuard::noop()))
            .await?;

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

    async fn mock_snapshot(
        data: &[u8],
        lsn: Lsn,
    ) -> anyhow::Result<(PartitionSnapshotMetadata, PathBuf)> {
        let snapshot_dir = TempDir::new()?.keep();

        let mut data_file = tokio::fs::File::create(snapshot_dir.join("data.sst")).await?;
        data_file.write_all(data).await?;
        data_file.shutdown().await?;

        let mut snapshot = mock_snapshot_metadata(
            "/data.sst".to_owned(),
            snapshot_dir.to_string_lossy().to_string(),
            data.len(),
        );
        snapshot.min_applied_lsn = lsn;

        Ok((snapshot, snapshot_dir))
    }

    fn mock_snapshot_metadata(
        file_name: String,
        directory: String,
        size: usize,
    ) -> PartitionSnapshotMetadata {
        PartitionSnapshotMetadata {
            version: SnapshotFormatVersion::V1,
            cluster_name: Metadata::with_current(|m| {
                m.nodes_config_ref().cluster_name().to_string()
            }),
            cluster_fingerprint: Metadata::with_current(|m| {
                m.nodes_config_ref().cluster_fingerprint()
            }),
            node_name: "node".to_string(),
            partition_id: PartitionId::MIN,
            created_at: jiff::Timestamp::now(),
            snapshot_id: SnapshotId::new(),
            key_range: PartitionKey::MIN..=PartitionKey::MAX,
            log_id: LogId::MIN,
            min_applied_lsn: Lsn::new(1),
            db_comparator_name: "leveldb.BytewiseComparator".to_string(),
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
            file_keys: std::collections::BTreeMap::new(),
        }
    }

    #[restate_core::test]
    async fn test_snapshot_retention_v2() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            experimental_num_retained: Some(std::num::NonZeroU8::new(3).unwrap()),
            ..SnapshotsOptions::default()
        };

        let repository = SnapshotRepository::new_from_config(
            &opts,
            TempDir::new().unwrap().keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

        let mut snapshots = Vec::new();
        for i in 1..=4 {
            let snapshot_source = TempDir::new()?;
            let source_dir = snapshot_source.path().to_path_buf();

            let data = format!("snapshot-data-{}", i);
            let mut data_file = tokio::fs::File::create(source_dir.join("data.sst")).await?;
            data_file.write_all(data.as_bytes()).await?;
            data_file.shutdown().await?;

            let mut snapshot = mock_snapshot_metadata(
                "/data.sst".to_owned(),
                source_dir.to_string_lossy().to_string(),
                data.len(),
            );
            snapshot.min_applied_lsn = Lsn::new(i * 1000);

            repository
                .put(&snapshot, source_dir, Arc::new(SnapshotLeaseGuard::noop()))
                .await?;
            snapshots.push(snapshot);
        }

        let latest_path = ObjectPath::from(Url::parse(&destination)?.path().to_string())
            .child(PartitionId::MIN.to_string())
            .child("latest.json");

        let object_store = create_object_store_client(
            Url::parse(&destination)?,
            &ObjectStoreOptions::default(),
            &RetryPolicy::None,
        )
        .await?;

        let latest_data = object_store.get(&latest_path).await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest_data.bytes().await?)?;

        assert_eq!(latest.version, LatestSnapshotVersion::V2);
        assert_eq!(latest.retained_snapshots.len(), 3);
        assert_eq!(latest.retained_snapshots[0].min_applied_lsn, Lsn::new(4000));
        assert_eq!(latest.retained_snapshots[1].min_applied_lsn, Lsn::new(3000));
        assert_eq!(latest.retained_snapshots[2].min_applied_lsn, Lsn::new(2000));

        Ok(())
    }

    #[restate_core::test]
    async fn test_v1_to_v2_migration() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts_v1 = SnapshotsOptions {
            destination: Some(destination.clone()),
            experimental_num_retained: None,
            ..SnapshotsOptions::default()
        };

        let repository_v1 = SnapshotRepository::new_from_config(
            &opts_v1,
            TempDir::new().unwrap().keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

        let snapshot_source = TempDir::new()?;
        let source_dir = snapshot_source.path().to_path_buf();
        let data = b"snapshot-data-v1";
        let mut data_file = tokio::fs::File::create(source_dir.join("data.sst")).await?;
        data_file.write_all(data).await?;
        data_file.shutdown().await?;

        let mut snapshot_v1 = mock_snapshot_metadata(
            "/data.sst".to_owned(),
            source_dir.to_string_lossy().to_string(),
            data.len(),
        );
        snapshot_v1.min_applied_lsn = Lsn::new(1000);

        repository_v1
            .put(
                &snapshot_v1,
                source_dir.clone(),
                Arc::new(SnapshotLeaseGuard::noop()),
            )
            .await?;

        let object_store = create_object_store_client(
            Url::parse(&destination)?,
            &ObjectStoreOptions::default(),
            &RetryPolicy::None,
        )
        .await?;

        let latest_path = ObjectPath::from(Url::parse(&destination)?.path().to_string())
            .child(PartitionId::MIN.to_string())
            .child("latest.json");

        let latest_data = object_store.get(&latest_path).await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest_data.bytes().await?)?;
        assert_eq!(latest.version, LatestSnapshotVersion::V1);

        let opts_v2 = SnapshotsOptions {
            destination: Some(destination.clone()),
            experimental_num_retained: Some(std::num::NonZeroU8::new(2).unwrap()),
            ..SnapshotsOptions::default()
        };

        let repository_v2 = SnapshotRepository::new_from_config(
            &opts_v2,
            TempDir::new().unwrap().keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

        let snapshot_source_2 = TempDir::new()?;
        let source_dir_2 = snapshot_source_2.path().to_path_buf();
        let data2 = b"snapshot-data-v2";
        let mut data_file_2 = tokio::fs::File::create(source_dir_2.join("data.sst")).await?;
        data_file_2.write_all(data2).await?;
        data_file_2.shutdown().await?;

        let mut snapshot_v2 = mock_snapshot_metadata(
            "/data.sst".to_owned(),
            source_dir_2.to_string_lossy().to_string(),
            data2.len(),
        );
        snapshot_v2.min_applied_lsn = Lsn::new(2000);

        repository_v2
            .put(
                &snapshot_v2,
                source_dir_2,
                Arc::new(SnapshotLeaseGuard::noop()),
            )
            .await?;

        let latest_data = object_store.get(&latest_path).await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest_data.bytes().await?)?;
        assert_eq!(latest.version, LatestSnapshotVersion::V2);
        assert_eq!(latest.retained_snapshots.len(), 2);

        Ok(())
    }

    #[restate_core::test]
    async fn test_archived_lsn_v2() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            experimental_num_retained: Some(std::num::NonZeroU8::new(3).unwrap()),
            ..SnapshotsOptions::default()
        };

        let repository = SnapshotRepository::new_from_config(
            &opts,
            TempDir::new().unwrap().keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

        for i in 1..=3 {
            let snapshot_source = TempDir::new()?;
            let source_dir = snapshot_source.path().to_path_buf();

            let data = format!("snapshot-data-{}", i);
            let mut data_file = tokio::fs::File::create(source_dir.join("data.sst")).await?;
            data_file.write_all(data.as_bytes()).await?;
            data_file.shutdown().await?;

            let mut snapshot = mock_snapshot_metadata(
                "/data.sst".to_owned(),
                source_dir.to_string_lossy().to_string(),
                data.len(),
            );
            snapshot.min_applied_lsn = Lsn::new(i * 1000);

            repository
                .put(&snapshot, source_dir, Arc::new(SnapshotLeaseGuard::noop()))
                .await?;
        }

        let status = repository
            .get_latest_partition_snapshot_status(PartitionId::MIN)
            .await?;
        assert_eq!(
            status.unwrap().archived_lsn,
            Lsn::new(1000),
            "archived LSN = earliest retained snapshot as the safe to trim LSN"
        );

        Ok(())
    }

    #[restate_core::test]
    async fn test_cleanup() -> anyhow::Result<()> {
        // Required for mock_snapshot's use of Metadata::with_current
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            experimental_num_retained: Some(std::num::NonZeroU8::new(3).unwrap()),
            enable_cleanup: true,
            ..SnapshotsOptions::default()
        };

        let repository = SnapshotRepository::new_from_config_with_stub_leases(
            &opts,
            TempDir::new().unwrap().keep(),
        )
        .await?
        .unwrap();

        let mut all_snapshot_paths = Vec::new();
        for i in 1..=5 {
            let (snapshot, source_dir) =
                mock_snapshot(format!("data-{}", i).as_bytes(), Lsn::new(100 * i)).await?;
            all_snapshot_paths.push(SnapshotReference::from_metadata(&snapshot).path);
            repository
                .put(&snapshot, source_dir, Arc::new(SnapshotLeaseGuard::noop()))
                .await?;
        }

        let latest_path = repository.latest_snapshot_pointer_path(PartitionId::MIN);
        let partition_prefix = repository.partition_snapshots_prefix(PartitionId::MIN);
        let result = repository.object_store.get(&latest_path).await?;
        let latest: LatestSnapshot = serde_json::from_slice(&result.bytes().await?)?;
        assert_eq!(latest.retained_snapshots.len(), 3);
        assert_eq!(
            latest
                .retained_snapshots
                .iter()
                .map(|s| s.min_applied_lsn)
                .collect::<Vec<_>>(),
            vec![Lsn::new(500), Lsn::new(400), Lsn::new(300)]
        );

        let retained_snapshot_paths: HashSet<_> = latest
            .retained_snapshots
            .iter()
            .map(|s| s.path.to_string())
            .collect();

        // wait for cleanup
        let mut repository_snapshot_paths: HashSet<_> = HashSet::default();
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let repository_listing: Vec<_> = repository
                .object_store
                .list(Some(&partition_prefix))
                .try_collect()
                .await?;
            repository_snapshot_paths = repository_listing
                .iter()
                .filter_map(|obj| {
                    let path_str = obj.location.as_ref();
                    path_str
                        .split('/')
                        .find(|s| s.starts_with("lsn_"))
                        .map(|s| s.to_owned())
                })
                .collect();

            if repository_snapshot_paths == retained_snapshot_paths {
                break;
            }
        }

        // only retained snapshots should exist in the object store, ignoring order (HashSet comparison)
        assert_eq!(
            repository_snapshot_paths,
            retained_snapshot_paths,
            "only retained snapshots should exist in object store - uploaded: {:?}, expected to be deleted: {:?}",
            all_snapshot_paths,
            all_snapshot_paths
                .iter()
                .filter(|p| !retained_snapshot_paths.contains(p.as_str()))
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[restate_core::test]
    async fn test_archived_lsn_reports_earliest_retained_v2() -> anyhow::Result<()> {
        use super::PartitionSnapshotStatus;

        // Create a V2 latest snapshot with multiple retained snapshots.
        // The retained_snapshots list is intentionally out of order to verify that the
        // conversion code does NOT rely on the descending LSN sort invariant. This tests
        // defensive behavior against corrupted or manually edited metadata.
        let latest = LatestSnapshot {
            version: LatestSnapshotVersion::V2,
            partition_id: PartitionId::MIN,
            log_id: Some(LogId::MIN),
            cluster_name: "test".to_string(),
            cluster_fingerprint: None,
            node_name: "node1".to_string(),
            created_at: Timestamp::from_second(2).unwrap(),
            snapshot_id: SnapshotId::new(),
            min_applied_lsn: Lsn::new(3484), // This is the newest snapshot
            path: "newest".to_string(),
            retained_snapshots: vec![
                SnapshotReference {
                    snapshot_id: SnapshotId::new(),
                    min_applied_lsn: Lsn::new(3484), // Newest (index 0)
                    created_at: Timestamp::from_second(2).unwrap(),
                    path: "snap1".to_string(),
                },
                SnapshotReference {
                    snapshot_id: SnapshotId::new(),
                    min_applied_lsn: Lsn::new(1342), // Oldest by LSN but at index 1
                    created_at: Timestamp::from_second(0).unwrap(),
                    path: "snap3".to_string(),
                },
                SnapshotReference {
                    snapshot_id: SnapshotId::new(),
                    min_applied_lsn: Lsn::new(2839), // Middle LSN at index 2
                    created_at: Timestamp::from_second(1).unwrap(),
                    path: "snap2".to_string(),
                },
            ],
        };

        let status = PartitionSnapshotStatus::try_from(&latest)?;

        assert_eq!(
            status.archived_lsn,
            Lsn::new(1342),
            "reports the earliest snapshot LSN as archived"
        );

        assert_eq!(
            status.latest_snapshot_created_at,
            MillisSinceEpoch::UNIX_EPOCH + Duration::from_secs(2),
            "reports the latest retained snapshot's created-at time"
        );

        Ok(())
    }
}
