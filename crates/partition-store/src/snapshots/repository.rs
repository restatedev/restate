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
use std::time::Duration;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use anyhow::{Context, anyhow, bail};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
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
use tracing::{Instrument, Span, debug, info, instrument, trace, warn};
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

#[cfg(any(test, feature = "test-util"))]
use super::leases::NoOpLeaseManager;
use super::leases::{LeaseError, SnapshotLeaseGuard, SnapshotLeaseManager};
use super::{
    LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotDir, SnapshotFormatVersion,
};

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
///
/// XXH3 is a non-cryptographic hash. We rely on a 128-bit digest's collision space being
/// large enough that random RocksDB SSTs do not collide in practice; the input is trusted
/// data produced by this process (not attacker-controlled), so chosen-prefix collision
/// resistance is not required. If snapshot inputs ever become reachable from untrusted
/// sources, this hash must be replaced with a cryptographic one.
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
    num_retained: std::num::NonZeroU8,
    snapshot_type: restate_types::config::SnapshotType,
    orphan_cleanup: bool,
    lease_provider: Option<LeaseProvider>,
    #[cfg(any(test, feature = "test-util"))]
    enable_cleanup: bool,
}

/// S3 and other stores require a certain minimum size for the parts of a multipart upload. It is an
/// API error to attempt a multipart put below this size, apart from the final segment.
const MULTIPART_UPLOAD_CHUNK_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// Maximum number of concurrent downloads when getting snapshots from the repository.
const DOWNLOAD_CONCURRENCY_LIMIT: usize = 8;

/// Minimum age for files to be considered orphan candidates during repository scan.
const ORPHAN_MIN_AGE: Duration = Duration::from_hours(24);

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum LatestSnapshotVersion {
    V1,
    /// V2 adds support for retained snapshots. Introduced in v1.6.
    #[default]
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

#[derive(Debug, Default)]
struct SnapshotInventory {
    /// SST files found in the ssts/ directory with their last modified time.
    shared_sst_files: HashMap<ObjectPath, DateTime<Utc>>,
    /// Snapshot prefixes found: (prefix -> (has_metadata, newest_file_modified)).
    /// We track the newest file to avoid deleting in-flight/partially replicated uploads.
    snapshot_prefixes: HashMap<String, (bool, DateTime<Utc>)>,
}

#[derive(Debug, Default)]
struct OrphanedPaths {
    /// SST files not referenced by any retained snapshot.
    ssts: Vec<ObjectPath>,
    /// Snapshot prefixes not in the retained snapshots list.
    snapshots: Vec<String>,
}

impl LatestSnapshot {
    /// Returns retained snapshots in descending order of archived LSN, most recent first.
    /// This relies on the write-time invariant maintained by the snapshot put / eviction logic;
    /// callers should not assume re-sorting here.
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

        // Best-effort cleanup of leftover snapshot staging directories from a previous run
        // (e.g. downloads interrupted by a hard crash mid-import, which the per-download RAII
        // guard cannot clean up). Safe here because no downloads are in flight at startup.
        // See https://github.com/restatedev/restate/issues/4838.
        Self::sweep_staging_dir(&staging_dir).await;

        let lease_provider = metadata_store_client
            .map(|client| LeaseProvider::Real(SnapshotLeaseManager::new(client)));

        Ok(Some(SnapshotRepository {
            object_store,
            destination,
            prefix: ObjectPath::from(prefix),
            staging_dir,
            num_retained: snapshots_options.num_retained,
            snapshot_type: snapshots_options.experimental_snapshot_type,
            orphan_cleanup: snapshots_options
                .experimental_orphan_cleanup
                .unwrap_or(false),
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
            num_retained: snapshots_options.num_retained,
            snapshot_type: snapshots_options.experimental_snapshot_type,
            orphan_cleanup: snapshots_options
                .experimental_orphan_cleanup
                .unwrap_or(false),
            enable_cleanup: snapshots_options.enable_cleanup,
            lease_provider: Some(LeaseProvider::NoOp(NoOpLeaseManager::new())),
        }))
    }

    /// Acquire a lease for snapshot operations on this partition
    ///
    /// On success, returns a guard with background renewal already started. The guard should be
    /// held for the duration of snapshot operations and passed to `put`.
    ///
    /// Returns `LeaseError::Unavailable` if the repository was created without a lease provider.
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

    /// Removes any entries left over in the snapshot staging directory by a previous run.
    ///
    /// This is best-effort: failures are logged and ignored. It must only be called at
    /// startup, before any snapshot download can be in flight.
    async fn sweep_staging_dir(staging_dir: &Path) {
        // Remove the whole staging directory; `get_latest_inner` recreates it on demand before
        // the next download. No download can be in flight at startup, so this is safe.
        match tokio::fs::remove_dir_all(staging_dir).await {
            Ok(()) => debug!(
                path = %staging_dir.display(),
                "Cleared snapshot staging directory left over from a previous run",
            ),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => warn!(
                %err,
                path = %staging_dir.display(),
                "Failed to clear snapshot staging directory",
            ),
        }
    }

    /// Write a partition snapshot to the snapshot repository
    ///
    /// Returns the latest snapshot status on successful upload. Depending on retention settings,
    /// the archived LSN may be earlier than that of the snapshot which was just uploaded. This
    /// operation requires a valid lease obtained by calling `acquire_lease`.
    ///
    /// Takes ownership of the local snapshot directory via [`SnapshotDir`] and removes it once
    /// the upload completes (success or failure), so callers cannot leak it.
    #[instrument(
        level = "error",
        err,
        skip_all,
        fields(local_path = %local_snapshot.path().display())
    )]
    pub(crate) async fn put(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        local_snapshot: SnapshotDir,
        lease_guard: Arc<SnapshotLeaseGuard>,
    ) -> anyhow::Result<PartitionSnapshotStatus> {
        use crate::metric_definitions::{
            SNAPSHOT_UPLOAD_DURATION, SNAPSHOT_UPLOAD_FAILED, SNAPSHOT_UPLOAD_SUCCESS,
        };

        debug!("Publishing partition snapshot to: {}", self.destination);

        let start = tokio::time::Instant::now();
        let put_result = self
            .put_snapshot_inner(snapshot, local_snapshot.path(), lease_guard)
            .await;

        // We own the local snapshot directory; remove it asynchronously (it may hold large SST
        // files) regardless of the upload outcome. Failure is only logged: if the snapshot was
        // uploaded we still want to return the result to the caller.
        local_snapshot.remove().await;

        metrics::histogram!(SNAPSHOT_UPLOAD_DURATION).record(start.elapsed());

        match put_result {
            Ok(status) => {
                metrics::counter!(SNAPSHOT_UPLOAD_SUCCESS).increment(1);
                Ok(status)
            }
            Err(put_error) => {
                metrics::counter!(SNAPSHOT_UPLOAD_FAILED).increment(1);
                for filename in put_error.uploaded_files {
                    let path = put_error.full_snapshot_path.clone().join(filename);

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
                        .join("ssts")
                        .join(sst_name.as_str());

                    // Content-addressed dedup: if an object already exists at this key, we
                    // trust that its content matches the local file (xxh3-128 collisions are
                    // astronomically unlikely on random RocksDB content). We still verify the
                    // stored object's size matches, which catches truncated/partial uploads
                    // from a prior crashed put.
                    let must_upload = match self.object_store.head(&sst_key).await {
                        Ok(meta) if meta.size as usize != file.size => {
                            warn!(
                                sst = %filename,
                                hash = %content_hash,
                                expected_size = file.size,
                                actual_size = meta.size,
                                "Existing object at content-addressed key has unexpected size; \
                                re-uploading to overwrite the corrupt object"
                            );
                            true
                        }
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

                    let relative_key = format!("ssts/{sst_name}");
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

        if matches!(self.snapshot_type, SnapshotType::Incremental) {
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
                "Snapshot SST upload completed"
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

        self.upload_snapshot(snapshot, local_snapshot_path, &mut progress)
            .await?;

        let latest_path = self.latest_snapshot_pointer_path(snapshot.partition_id);
        let maybe_stored = self
            .get_latest_snapshot_metadata_for_update(&latest_path)
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;

        let (new_latest, evicted_snapshots) = self
            .build_latest_v2(snapshot, maybe_stored.as_ref().map(|(l, _)| l))
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;

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

    async fn upload_snapshot(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        local_snapshot_path: &Path,
        progress: &mut SnapshotUploadProgress,
    ) -> Result<(), PutSnapshotError> {
        let mut buf = BytesMut::new();

        let file_keys = self
            .upload_snapshot_with_dedup(snapshot, local_snapshot_path, &mut buf, progress)
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

        Ok(())
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

        let mut retained_snapshots = current
            .map(|l| l.effective_retained_snapshots())
            .unwrap_or_default();

        // List will be in correct descending order if we insert the newest snapshot first
        retained_snapshots.insert(0, new_snapshot_ref.clone());

        let evicted_snapshots = retained_snapshots
            .split_off((self.num_retained.get() as usize).min(retained_snapshots.len()));

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
            path: new_snapshot_ref.path,
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

        let referenced_sst_keys = self
            .build_referenced_sst_keys(partition_id, &retained_snapshots)
            .await?;

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
            self.delete_snapshot_files(partition_id, snapshot_ref, &referenced_sst_keys)
                .await;
        }

        if self.orphan_cleanup && lease_guard.is_valid() {
            use crate::metric_definitions::{
                SNAPSHOT_ORPHAN_FILES_DELETED, SNAPSHOT_ORPHAN_SCAN_FAILED,
                SNAPSHOT_ORPHAN_SCAN_TOTAL,
            };

            debug!(%partition_id, "Running orphan scan");

            metrics::counter!(SNAPSHOT_ORPHAN_SCAN_TOTAL).increment(1);

            match self
                .find_orphaned_files(
                    partition_id,
                    &retained_snapshots,
                    ORPHAN_MIN_AGE,
                    &referenced_sst_keys,
                )
                .await
            {
                Ok(orphans) if !orphans.ssts.is_empty() || !orphans.snapshots.is_empty() => {
                    let (deleted_ssts, deleted_dirs) = self
                        .cleanup_orphaned_files(partition_id, orphans, lease_guard)
                        .await;
                    metrics::counter!(SNAPSHOT_ORPHAN_FILES_DELETED)
                        .increment((deleted_ssts + deleted_dirs) as u64);
                }
                Ok(_) => {
                    debug!(%partition_id, "No orphaned files found during scan");
                }
                Err(err) => {
                    warn!(%partition_id, %err, "Failed to scan for orphaned files");
                    metrics::counter!(SNAPSHOT_ORPHAN_SCAN_FAILED).increment(1);
                }
            }
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
    #[instrument(
        level = "warn",
        skip(self, referenced_sst_keys),
        fields(%partition_id, snapshot_id = %snapshot_ref.snapshot_id)
    )]
    async fn delete_snapshot_files(
        &self,
        partition_id: PartitionId,
        snapshot_ref: &SnapshotReference,
        referenced_sst_keys: &HashSet<ObjectPath>,
    ) {
        let metadata_path = self
            .prefix
            .clone()
            .join(partition_id.to_string())
            .join(snapshot_ref.path.as_str())
            .join("metadata.json");

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

        let mut any_failed = false;
        for file in &metadata.files {
            let filename = strip_leading_slash(&file.name);
            let path = self.resolve_file_path(
                partition_id,
                metadata.file_keys.get(&file.name).map(|s| s.as_str()),
                &snapshot_ref.path,
                filename,
            );

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
        let candidates = self.get_snapshot_candidates(partition_id).await?;
        let Some(latest) = candidates.first() else {
            return Ok(None);
        };
        tracing::Span::current().record("snapshot_id", tracing::field::display(latest.snapshot_id));
        self.get_snapshot(partition_id, latest).await.map(Some)
    }

    /// Returns the partition's retained snapshot references, most-recent first (descending archived
    /// LSN), or an empty vec if the repository holds no snapshot for the partition. Selecting a
    /// snapshot suitable for a given target LSN is the caller's responsibility.
    pub(crate) async fn get_snapshot_candidates(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Vec<SnapshotReference>> {
        let latest_path = self.latest_snapshot_pointer_path(partition_id);

        let latest = match self.object_store.get(&latest_path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                debug!("Latest snapshot data not found in repository");
                return Ok(vec![]);
            }
            Err(err) => return Err(err.into()),
        };

        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        debug!(snapshot_id = %latest.snapshot_id, "Latest snapshot metadata: {latest:?}");

        Metadata::with_current(|m| {
            let nodes_config = m.nodes_config_ref();
            latest.validate(
                nodes_config.cluster_name(),
                nodes_config.cluster_fingerprint(),
            )?;
            anyhow::Ok(())
        })
        .with_context(|| format!("'{latest_path}' has validation errors"))?;

        Ok(latest.effective_retained_snapshots())
    }

    #[instrument(
        level = "error",
        skip_all,
        fields(%partition_id, snapshot_id = %snapshot_ref.snapshot_id),
    )]
    pub(crate) async fn get_snapshot(
        &self,
        partition_id: PartitionId,
        snapshot_ref: &SnapshotReference,
    ) -> anyhow::Result<LocalPartitionSnapshot> {
        let snapshot_metadata_path = self
            .prefix
            .clone()
            .join(partition_id.to_string())
            .join(snapshot_ref.path.as_str())
            .join("metadata.json");

        let snapshot_metadata = self
            .object_store
            .get(&snapshot_metadata_path)
            .await
            .with_context(|| {
                format!("Snapshot metadata at '{snapshot_metadata_path}' not found in repository")
            })?;

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
            let key = self.resolve_file_path(
                partition_id,
                snapshot_metadata
                    .file_keys
                    .get(&file.name)
                    .map(|s| s.as_str()),
                &snapshot_ref.path,
                filename,
            );
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
            file.directory.clone_from(&directory);
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
        // Transfer ownership of the staging directory from the `TempDir` (which auto-cleans on
        // an early return mid-download) to the snapshot's `SnapshotDir`, which removes it on drop
        // whether the subsequent import succeeds or fails (see #4838).
        Ok(LocalPartitionSnapshot {
            base_dir: SnapshotDir::new(snapshot_dir.keep()),
            log_id: snapshot_metadata.log_id,
            min_applied_lsn: snapshot_metadata.min_applied_lsn,
            db_comparator_name: snapshot_metadata.db_comparator_name,
            files: snapshot_metadata.files,
            key_range: snapshot_metadata.key_range,
        })
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
            .join("latest.json")
    }

    fn partition_snapshots_prefix(&self, partition_id: PartitionId) -> ObjectPath {
        self.prefix.clone().join(partition_id.to_string())
    }

    fn base_prefix(&self, snapshot_metadata: &PartitionSnapshotMetadata) -> ObjectPath {
        self.partition_snapshots_prefix(snapshot_metadata.partition_id)
            .join(UniqueSnapshotKey::from_metadata(snapshot_metadata).padded_key())
    }

    fn snapshot_file_path(
        &self,
        snapshot_metadata: &PartitionSnapshotMetadata,
        filename: &str,
    ) -> ObjectPath {
        self.base_prefix(snapshot_metadata).join(filename)
    }

    /// Resolves a file reference to its full object path.
    ///
    /// Handles two storage formats:
    /// - Incremental snapshots: `relative_key` contains "ssts/{hash}.sst" (content-addressed)
    /// - Legacy full snapshots: file is stored at `{snapshot_path}/{filename}`
    fn resolve_file_path(
        &self,
        partition_id: PartitionId,
        relative_key: Option<&str>,
        snapshot_path: &str,
        filename: &str,
    ) -> ObjectPath {
        if let Some(key) = relative_key {
            // Incremental snapshot: SST is in shared ssts/ directory.
            // Format: "ssts/{hash}.sst" (two segments). Any other shape is treated as opaque
            // and joined as-is for forward compatibility, but the upload path only produces
            // the two-segment form.
            let parts: Vec<&str> = key.split('/').collect();
            if parts.len() == 2 {
                self.prefix
                    .clone()
                    .join(partition_id.to_string())
                    .join(parts[0])
                    .join(parts[1])
            } else {
                warn!(
                    %filename,
                    relative_key = %key,
                    "Unexpected file_keys entry shape; treating as opaque path"
                );
                self.prefix.clone().join(partition_id.to_string()).join(key)
            }
        } else {
            // Legacy full snapshot: SST is in snapshot-specific directory
            self.prefix
                .clone()
                .join(partition_id.to_string())
                .join(snapshot_path)
                .join(filename)
        }
    }

    /// Scans the repository to enumerate files for orphan detection.
    ///
    /// # Scanned patterns (IMPORTANT: keep in sync with bucket layout docs)
    ///
    /// Only these specific patterns are considered orphan candidates:
    ///
    /// 1. **SST files**: `{partition_id}/ssts/*.sst`
    ///    - Match: `path.contains("/ssts/") && path.ends_with(".sst")`
    ///    - Used by incremental snapshots for content-addressed SST storage
    ///
    /// 2. **Snapshot directories**: `{partition_id}/lsn_*-*/`
    ///    - Match: path segment `starts_with("lsn_") && contains('-')`
    ///    - Standard snapshot directory format: `lsn_{padded_lsn}-{snapshot_id}`
    ///
    /// # Explicitly NOT scanned (safe for future use)
    ///
    /// - `{partition_id}/latest.json` - the pointer file
    /// - `{partition_id}/*.json` - any other root-level JSON files
    /// - `{partition_id}/{other_prefix}_*/` - directories not starting with `lsn_`
    /// - `{partition_id}/{other_dir}/` - e.g., `indices/`, `metadata/`, etc.
    ///
    /// When adding new storage patterns, avoid `ssts/` and `lsn_*` prefixes to
    /// ensure orphan cleanup doesn't interfere.
    async fn scan_partition_files(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<SnapshotInventory> {
        let prefix = self.partition_snapshots_prefix(partition_id);
        let stream = self.object_store.list(Some(&prefix));

        let mut scan = SnapshotInventory::default();

        futures::pin_mut!(stream);
        while let Some(meta) = stream.try_next().await? {
            let path_str = meta.location.as_ref();

            // SST files in ssts/ directory (incremental snapshot storage)
            if path_str.contains("/ssts/") && path_str.ends_with(".sst") {
                scan.shared_sst_files
                    .insert(meta.location, meta.last_modified);
                continue;
            }

            // Snapshot directories matching lsn_*-* pattern
            if let Some(dir_name) = Self::extract_snapshot_prefix_from_path(path_str) {
                let is_metadata = path_str.ends_with("/metadata.json");
                scan.snapshot_prefixes
                    .entry(dir_name)
                    .and_modify(|(has_meta, newest)| {
                        *has_meta = *has_meta || is_metadata;
                        // Track the newest file to protect in-flight uploads
                        if meta.last_modified > *newest {
                            *newest = meta.last_modified;
                        }
                    })
                    .or_insert((is_metadata, meta.last_modified));
            }
        }

        trace!(
            %partition_id,
            sst_count = scan.shared_sst_files.len(),
            dir_count = scan.snapshot_prefixes.len(),
            "Scanned partition files"
        );

        Ok(scan)
    }

    fn extract_snapshot_prefix_from_path(path: &str) -> Option<String> {
        // Path format: {prefix}/{partition_id}/{lsn}_{snap_id}/...
        // We want to extract the {lsn}_{snap_id} part
        for segment in path.split('/') {
            // Snapshot directories start with "lsn_" (padded format)
            // Legacy format may use different patterns, but lsn_ is the current standard
            if segment.starts_with("lsn_") && segment.contains('-') {
                return Some(segment.to_string());
            }
        }
        None
    }

    /// Builds the set of SST keys referenced by the given retained snapshots.
    ///
    /// Returns an error if any retained snapshot's metadata cannot be read or parsed
    /// to prevent orphan cleanup from deleting SSTs that are still referenced.
    async fn build_referenced_sst_keys(
        &self,
        partition_id: PartitionId,
        retained_snapshots: &[SnapshotReference],
    ) -> anyhow::Result<HashSet<ObjectPath>> {
        let mut referenced_sst_keys = HashSet::new();

        for retained_ref in retained_snapshots {
            let retained_metadata_path = self
                .prefix
                .clone()
                .join(partition_id.to_string())
                .join(retained_ref.path.as_str())
                .join("metadata.json");

            let data = match self.object_store.get(&retained_metadata_path).await {
                Ok(data) => data,
                Err(object_store::Error::NotFound { .. }) => {
                    // Metadata missing - could be race condition or transient failure.
                    // Bail to prevent potential data loss as we can't know which SSTs are retained.
                    warn!(
                        path = %retained_ref.path,
                        "Retained snapshot metadata not found - aborting to prevent data loss"
                    );
                    return Err(anyhow::anyhow!(
                        "Cannot verify retained snapshot {}: metadata not found",
                        retained_ref.path
                    ));
                }
                Err(err) => {
                    return Err(anyhow::anyhow!(
                        "Failed to read metadata for retained snapshot {}: {}",
                        retained_ref.path,
                        err
                    ));
                }
            };

            let bytes = data.bytes().await.context(format!(
                "Failed to read metadata bytes for retained snapshot {}",
                retained_ref.path
            ))?;

            let retained_metadata: PartitionSnapshotMetadata = serde_json::from_slice(&bytes)
                .context(format!(
                    "Failed to parse metadata for retained snapshot {}",
                    retained_ref.path
                ))?;

            for file in &retained_metadata.files {
                let filename = strip_leading_slash(&file.name);
                let sst_key = self.resolve_file_path(
                    partition_id,
                    retained_metadata
                        .file_keys
                        .get(&file.name)
                        .map(|s| s.as_str()),
                    &retained_ref.path,
                    filename,
                );
                referenced_sst_keys.insert(sst_key);
            }
        }

        Ok(referenced_sst_keys)
    }

    /// Finds orphaned files in the repository by scanning all files and comparing
    /// against the retained snapshots list.
    ///
    /// Files are only considered orphaned if they are older than `min_age` to avoid
    /// deleting files from in-flight uploads or CRR replication.
    async fn find_orphaned_files(
        &self,
        partition_id: PartitionId,
        retained_snapshots: &[SnapshotReference],
        min_age: Duration,
        referenced_ssts: &HashSet<ObjectPath>,
    ) -> anyhow::Result<OrphanedPaths> {
        let scan = self.scan_partition_files(partition_id).await?;
        let now = Utc::now();
        let age_threshold = now - chrono::Duration::from_std(min_age)?;

        let retained_prefixes: HashSet<&str> =
            retained_snapshots.iter().map(|r| r.path.as_str()).collect();

        let orphaned_ssts: Vec<ObjectPath> = scan
            .shared_sst_files
            .into_iter()
            .filter(|(path, modified)| !referenced_ssts.contains(path) && *modified < age_threshold)
            .map(|(path, _)| path)
            .collect();

        let orphaned_snapshots: Vec<String> = scan
            .snapshot_prefixes
            .into_iter()
            .filter(|(prefix, (_has_metadata, newest_modified))| {
                !retained_prefixes.contains(prefix.as_str()) && *newest_modified < age_threshold
            })
            .map(|(prefix, _)| prefix)
            .collect();

        if !orphaned_ssts.is_empty() || !orphaned_snapshots.is_empty() {
            info!(
                %partition_id,
                orphaned_sst_count = orphaned_ssts.len(),
                orphaned_snapshot_count = orphaned_snapshots.len(),
                "Found orphaned files during repository scan"
            );
        }

        Ok(OrphanedPaths {
            ssts: orphaned_ssts,
            snapshots: orphaned_snapshots,
        })
    }

    /// Cleans up orphaned files discovered during a repository scan.
    /// Should be called with an active lease to prevent races.
    async fn cleanup_orphaned_files(
        &self,
        partition_id: PartitionId,
        orphans: OrphanedPaths,
        lease_guard: &Arc<SnapshotLeaseGuard>,
    ) -> (usize, usize) {
        let mut deleted_ssts = 0;
        let mut deleted_snapshots = 0;

        for sst_path in orphans.ssts {
            if !lease_guard.is_valid() {
                debug!(%partition_id, "Lease expired during orphan cleanup, aborting");
                break;
            }

            if let Err(err) = self.object_store.delete(&sst_path).await {
                if !matches!(err, object_store::Error::NotFound { .. }) {
                    warn!(%sst_path, %err, "Failed to delete orphaned SST");
                }
            } else {
                debug!(%sst_path, "Deleted orphaned SST");
                deleted_ssts += 1;
            }
        }

        for snapshot_prefix in orphans.snapshots {
            if !lease_guard.is_valid() {
                debug!(%partition_id, "Lease expired during orphan cleanup, aborting");
                break;
            }

            let snapshot_path = self
                .prefix
                .clone()
                .join(partition_id.to_string())
                .join(snapshot_prefix.as_str());

            let stream = self.object_store.list(Some(&snapshot_path));
            futures::pin_mut!(stream);

            let mut prefix_deleted = true;
            loop {
                match stream.try_next().await {
                    Ok(Some(meta)) => {
                        if let Err(err) = self.object_store.delete(&meta.location).await
                            && !matches!(err, object_store::Error::NotFound { .. })
                        {
                            warn!(path = %meta.location, %err, "Failed to delete file in orphaned snapshot prefix");
                            prefix_deleted = false;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!(%snapshot_prefix, %err, "Failed to list files in orphaned snapshot prefix");
                        prefix_deleted = false;
                        break;
                    }
                }
            }

            if prefix_deleted {
                debug!(%snapshot_prefix, "Deleted orphaned snapshot directory");
                deleted_snapshots += 1;
            }
        }

        if deleted_ssts > 0 || deleted_snapshots > 0 {
            info!(
                %partition_id,
                deleted_ssts,
                deleted_snapshots,
                "Cleaned up orphaned files"
            );
        }

        (deleted_ssts, deleted_snapshots)
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

#[derive(Debug, thiserror::Error)]
#[error("failed to upload snapshot to {full_snapshot_path}: {error}")]
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

// The object_store `put_multipart` method does not currently support PutMode, so we cannot pass
// a CAS condition here. Snapshots write to per-snapshot-id paths, so collisions are not expected.
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
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStoreExt, PutPayload};
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use tracing::info;
    use url::Url;

    use restate_clock::time::MillisSinceEpoch;
    use restate_core::{Metadata, TestCoreEnv};
    use restate_object_store_util::create_object_store_client;
    use restate_types::config::{ObjectStoreOptions, SnapshotsOptions};
    use restate_types::identifiers::{PartitionId, SnapshotId};
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::retries::RetryPolicy;
    use restate_types::sharding::KeyRange;

    use crate::snapshots::SnapshotLeaseGuard;
    use crate::snapshots::repository::{LatestSnapshotVersion, SnapshotUploadProgress};

    use super::{LatestSnapshot, SnapshotReference, SnapshotRepository, UniqueSnapshotKey};
    use super::{PartitionSnapshotMetadata, SnapshotDir, SnapshotFormatVersion};

    #[restate_core::test]
    async fn overwrite_unparsable_latest() -> anyhow::Result<()> {
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
                .put(
                    &snapshot,
                    SnapshotDir::new(source_dir),
                    Arc::new(SnapshotLeaseGuard::noop()),
                )
                .await
                .is_err()
        );

        Ok(())
    }

    #[restate_core::test]
    async fn put_snapshot_local_filesystem() -> anyhow::Result<()> {
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
    async fn put_snapshot_s3() -> anyhow::Result<()> {
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
            .join(PartitionId::MIN.to_string())
            .join("latest.json");
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
                SnapshotDir::new(source_dir.clone()),
                Arc::new(SnapshotLeaseGuard::noop()),
            )
            .await?;

        let partition_prefix =
            ObjectPath::from(destination_url.path()).join(snapshot1.partition_id.to_string());

        let snapshot_1_prefix = partition_prefix.clone().join(
            UniqueSnapshotKey::from_metadata(&snapshot1)
                .padded_key()
                .as_str(),
        );

        let data = object_store
            .get(&snapshot_1_prefix.clone().join("data.sst"))
            .await?;
        assert_eq!(data.bytes().await?, Bytes::from_static(b"snapshot-data"));

        let metadata = object_store
            .get(&snapshot_1_prefix.join("metadata.json"))
            .await?;
        let metadata: PartitionSnapshotMetadata = serde_json::from_slice(&metadata.bytes().await?)?;
        assert_eq!(snapshot1.snapshot_id, metadata.snapshot_id);

        let latest = object_store
            .get(&partition_prefix.clone().join("latest.json"))
            .await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        let (expected_latest, _) = repository.build_latest_v2(&snapshot1, None)?;
        assert_eq!(expected_latest, latest);

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
            .put(
                &snapshot2,
                SnapshotDir::new(source_dir),
                Arc::new(SnapshotLeaseGuard::noop()),
            )
            .await?;

        let latest = object_store
            .get(&partition_prefix.join("latest.json"))
            .await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        let (expected_latest2, _) = repository.build_latest_v2(&snapshot2, None)?;
        assert_eq!(expected_latest2, latest);

        let latest = repository.get_latest(PartitionId::MIN).await?.unwrap();
        assert_eq!(latest.min_applied_lsn, snapshot2.min_applied_lsn);
        let local_path = latest.base_dir.path().to_string_lossy().to_string();

        // The staging directory exists while the snapshot is held...
        assert!(tokio::fs::try_exists(&local_path).await?);

        // ...and is removed by its owning SnapshotDir once the snapshot is dropped, so failed
        // restores no longer leak downloads (#4838).
        drop(latest);
        assert!(!tokio::fs::try_exists(&local_path).await?);

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
            key_range: KeyRange::FULL,
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
    async fn snapshot_retention_v2() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            num_retained: std::num::NonZeroU8::new(3).unwrap(),
            ..SnapshotsOptions::default()
        };

        let repository = SnapshotRepository::new_from_config(
            &opts,
            TempDir::new().unwrap().keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

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
                .put(
                    &snapshot,
                    SnapshotDir::new(source_dir),
                    Arc::new(SnapshotLeaseGuard::noop()),
                )
                .await?;
        }

        let latest_path = ObjectPath::from(Url::parse(&destination)?.path().to_string())
            .join(PartitionId::MIN.to_string())
            .join("latest.json");

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
    async fn get_snapshot_candidates_returns_retained_descending() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();
        let opts = SnapshotsOptions {
            destination: Some(destination),
            num_retained: std::num::NonZeroU8::new(3).unwrap(),
            ..SnapshotsOptions::default()
        };
        let repository = SnapshotRepository::new_from_config(
            &opts,
            TempDir::new().unwrap().keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

        // No snapshots yet -> no candidates.
        assert!(
            repository
                .get_snapshot_candidates(PartitionId::MIN)
                .await?
                .is_empty()
        );

        // Put four snapshots; with num_retained = 3 the oldest (1000) is evicted.
        for i in 1..=4 {
            let (snapshot, source_dir) =
                mock_snapshot(format!("snapshot-data-{i}").as_bytes(), Lsn::new(i * 1000)).await?;
            repository
                .put(
                    &snapshot,
                    SnapshotDir::new(source_dir),
                    Arc::new(SnapshotLeaseGuard::noop()),
                )
                .await?;
        }

        // Retained candidates are returned most-recent-first.
        let candidates = repository.get_snapshot_candidates(PartitionId::MIN).await?;
        assert_eq!(
            candidates
                .iter()
                .map(|c| c.min_applied_lsn)
                .collect::<Vec<_>>(),
            vec![Lsn::new(4000), Lsn::new(3000), Lsn::new(2000)],
        );

        Ok(())
    }

    #[restate_core::test]
    async fn download_snapshot_falls_back_to_older_when_latest_fails() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();
        let opts = SnapshotsOptions {
            destination: Some(destination),
            num_retained: std::num::NonZeroU8::new(3).unwrap(),
            ..SnapshotsOptions::default()
        };
        let repository = SnapshotRepository::new_from_config(
            &opts,
            TempDir::new().unwrap().keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

        // `SnapshotRepository` is cheaply cloneable and shares the underlying object store, so the
        // `Snapshots` wrapper observes snapshots we `put` through `repository` below.
        let snapshots = crate::snapshots::Snapshots {
            repository: Some(repository.clone()),
            concurrency_limit: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        };

        // An empty repository is "no snapshot present", not an error.
        assert!(
            snapshots
                .download_snapshot(PartitionId::MIN, None)
                .await?
                .is_none()
        );

        // Older snapshot A (LSN 2000) then latest B (LSN 3000).
        let (snapshot_a, dir_a) = mock_snapshot(b"snapshot-A", Lsn::new(2000)).await?;
        repository
            .put(
                &snapshot_a,
                SnapshotDir::new(dir_a),
                Arc::new(SnapshotLeaseGuard::noop()),
            )
            .await?;
        let (snapshot_b, dir_b) = mock_snapshot(b"snapshot-B", Lsn::new(3000)).await?;
        repository
            .put(
                &snapshot_b,
                SnapshotDir::new(dir_b),
                Arc::new(SnapshotLeaseGuard::noop()),
            )
            .await?;

        // Happy path: the latest snapshot is restored.
        let restored = snapshots
            .download_snapshot(PartitionId::MIN, None)
            .await?
            .expect("a snapshot is available");
        assert_eq!(restored.min_applied_lsn, Lsn::new(3000));

        // A target LSN selects only snapshots at or above it: 2500 still allows the latest (3000).
        let restored = snapshots
            .download_snapshot(PartitionId::MIN, Some(Lsn::new(2500)))
            .await?
            .expect("latest snapshot satisfies the target LSN");
        assert_eq!(restored.min_applied_lsn, Lsn::new(3000));

        // A target above every retained snapshot yields no suitable candidate (Ok(None)).
        assert!(
            snapshots
                .download_snapshot(PartitionId::MIN, Some(Lsn::new(5000)))
                .await?
                .is_none()
        );

        // Corrupt the latest by removing its data file; the download must fall back to A.
        let sst_path = |snapshot: &PartitionSnapshotMetadata| {
            snapshots_destination
                .path()
                .join(PartitionId::MIN.to_string())
                .join(UniqueSnapshotKey::from_metadata(snapshot).padded_key())
                .join("data.sst")
        };
        std::fs::remove_file(sst_path(&snapshot_b))?;
        let restored = snapshots
            .download_snapshot(PartitionId::MIN, None)
            .await?
            .expect("falls back to the older snapshot");
        assert_eq!(restored.min_applied_lsn, Lsn::new(2000));

        // Remove A's data too: every candidate now fails -> error, not a silent "no snapshot".
        std::fs::remove_file(sst_path(&snapshot_a))?;
        assert!(
            snapshots
                .download_snapshot(PartitionId::MIN, None)
                .await
                .is_err()
        );

        Ok(())
    }

    #[restate_core::test]
    async fn v1_to_v2_migration() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let snapshot_source = TempDir::new()?;
        let source_dir = snapshot_source.path().to_path_buf();
        let data = b"snapshot-data-v1";
        let mut data_file = tokio::fs::File::create(source_dir.join("data.sst")).await?;
        data_file.write_all(data).await?;
        data_file.shutdown().await?;

        let mut snapshot = mock_snapshot_metadata(
            "/data.sst".to_owned(),
            source_dir.to_string_lossy().to_string(),
            data.len(),
        );
        snapshot.min_applied_lsn = Lsn::new(1000);

        let object_store = create_object_store_client(
            Url::parse(&destination)?,
            &ObjectStoreOptions::default(),
            &RetryPolicy::None,
        )
        .await?;

        let opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            num_retained: std::num::NonZeroU8::new(2).unwrap(),
            ..SnapshotsOptions::default()
        };

        let repository = SnapshotRepository::new_from_config(
            &opts,
            TempDir::new().unwrap().keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

        let mut progress =
            SnapshotUploadProgress::with_snapshot_path(repository.base_prefix(&snapshot));
        // upload the snapshot data
        repository
            .upload_snapshot(&snapshot, source_dir.as_path(), &mut progress)
            .await?;

        // store v1 latest snapshot pointer
        let latest_snapshot_v1 = LatestSnapshot {
            version: LatestSnapshotVersion::V1,
            cluster_name: snapshot.cluster_name.clone(),
            cluster_fingerprint: snapshot.cluster_fingerprint,
            node_name: snapshot.node_name.clone(),
            partition_id: snapshot.partition_id,
            log_id: Some(snapshot.log_id),
            snapshot_id: snapshot.snapshot_id,
            created_at: snapshot.created_at,
            min_applied_lsn: snapshot.min_applied_lsn,
            path: UniqueSnapshotKey::from_metadata(&snapshot).padded_key(),
            retained_snapshots: vec![],
        };

        let latest_path = repository.latest_snapshot_pointer_path(snapshot.partition_id);
        object_store
            .put(
                &latest_path,
                PutPayload::from(serde_json::to_string(&latest_snapshot_v1)?),
            )
            .await?;

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

        repository
            .put(
                &snapshot_v2,
                SnapshotDir::new(source_dir_2),
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
    async fn archived_lsn_v2() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            num_retained: std::num::NonZeroU8::new(3).unwrap(),
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
                .put(
                    &snapshot,
                    SnapshotDir::new(source_dir),
                    Arc::new(SnapshotLeaseGuard::noop()),
                )
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
    async fn cleanup() -> anyhow::Result<()> {
        // Required for mock_snapshot's use of Metadata::with_current
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            num_retained: std::num::NonZeroU8::new(3).unwrap(),
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
                .put(
                    &snapshot,
                    SnapshotDir::new(source_dir),
                    Arc::new(SnapshotLeaseGuard::noop()),
                )
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
    async fn archived_lsn_reports_earliest_retained_v2() -> anyhow::Result<()> {
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

    #[test]
    fn extract_snapshot_prefix_from_path() {
        assert_eq!(
            SnapshotRepository::extract_snapshot_prefix_from_path(
                "prefix/0/lsn_00000000000000001234-abc123/metadata.json"
            ),
            Some("lsn_00000000000000001234-abc123".to_string())
        );
        assert_eq!(
            SnapshotRepository::extract_snapshot_prefix_from_path(
                "0/lsn_00000000000000000100-snap1/data.sst"
            ),
            Some("lsn_00000000000000000100-snap1".to_string())
        );

        assert_eq!(
            SnapshotRepository::extract_snapshot_prefix_from_path("0/ssts/abc123.sst"),
            None
        );

        assert_eq!(
            SnapshotRepository::extract_snapshot_prefix_from_path("0/latest.json"),
            None
        );

        assert_eq!(
            SnapshotRepository::extract_snapshot_prefix_from_path("random/path/file.txt"),
            None
        );
    }

    #[restate_core::test]
    async fn scan_partition_files() -> anyhow::Result<()> {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination_url = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination_url.clone()),
            ..SnapshotsOptions::default()
        };
        let repository = SnapshotRepository::new_from_config_with_stub_leases(
            &opts,
            TempDir::new().unwrap().keep(),
        )
        .await?
        .unwrap();

        let partition_id = PartitionId::MIN;

        let ssts_path = snapshots_destination
            .path()
            .join(partition_id.to_string())
            .join("ssts");
        tokio::fs::create_dir_all(&ssts_path).await?;
        tokio::fs::write(ssts_path.join("abc123.sst"), b"sst1").await?;
        tokio::fs::write(ssts_path.join("def456.sst"), b"sst2").await?;

        let snapshot_prefix = snapshots_destination
            .path()
            .join(partition_id.to_string())
            .join("lsn_00000000000000000100-snap1");
        tokio::fs::create_dir_all(&snapshot_prefix).await?;
        tokio::fs::write(snapshot_prefix.join("metadata.json"), b"{}").await?;
        tokio::fs::write(snapshot_prefix.join("data.sst"), b"data").await?;

        let incomplete_snapshot_path = snapshots_destination
            .path()
            .join(partition_id.to_string())
            .join("lsn_00000000000000000200-snap2");
        tokio::fs::create_dir_all(&incomplete_snapshot_path).await?;
        tokio::fs::write(incomplete_snapshot_path.join("data.sst"), b"data").await?;

        // Scan the partition
        let scan = repository.scan_partition_files(partition_id).await?;

        assert_eq!(scan.shared_sst_files.len(), 2, "should find 2 SST files");
        assert_eq!(
            scan.snapshot_prefixes.len(),
            2,
            "should find 2 snapshot directories"
        );
        assert_eq!(
            scan.snapshot_prefixes
                .get("lsn_00000000000000000100-snap1")
                .map(|(has_meta, _)| *has_meta),
            Some(true),
            "snap1 should have metadata"
        );
        assert_eq!(
            scan.snapshot_prefixes
                .get("lsn_00000000000000000200-snap2")
                .map(|(has_meta, _)| *has_meta),
            Some(false),
            "snap2 should not have metadata"
        );

        Ok(())
    }

    #[restate_core::test]
    async fn sweep_staging_dir_removes_leftovers() -> anyhow::Result<()> {
        let staging = TempDir::new()?;

        // A leftover snapshot directory with a file inside, plus a stray file.
        let leftover_dir = staging.path().join("snap-abc123");
        tokio::fs::create_dir_all(&leftover_dir).await?;
        tokio::fs::write(leftover_dir.join("data.sst"), b"x").await?;
        tokio::fs::write(staging.path().join("stray.tmp"), b"y").await?;

        SnapshotRepository::sweep_staging_dir(staging.path()).await;

        assert!(
            !staging.path().exists(),
            "staging directory should be removed by the sweep"
        );

        // A missing staging directory is a no-op (must not panic).
        SnapshotRepository::sweep_staging_dir(&staging.path().join("does-not-exist")).await;

        Ok(())
    }

    fn mock_local_snapshot(base_dir: super::SnapshotDir) -> super::LocalPartitionSnapshot {
        super::LocalPartitionSnapshot {
            base_dir,
            log_id: LogId::MIN,
            min_applied_lsn: Lsn::new(1),
            db_comparator_name: "leveldb.BytewiseComparator".to_owned(),
            files: vec![],
            key_range: KeyRange::new(0, 100),
        }
    }

    #[restate_core::test]
    async fn snapshot_dir_cleaned_on_drop_unless_disarmed() -> anyhow::Result<()> {
        let staging = TempDir::new()?;

        // An owned SnapshotDir removes its directory when the snapshot is dropped -- this is
        // what protects the import failure paths in #4838.
        let dir = TempDir::with_prefix_in("snap-", staging.path())?.keep();
        assert!(dir.exists());
        drop(mock_local_snapshot(SnapshotDir::new(dir.clone())));
        assert!(
            !dir.exists(),
            "an owned snapshot directory must be removed on drop"
        );

        // into_path() disarms the guard: the directory survives the snapshot.
        let dir = TempDir::with_prefix_in("snap-", staging.path())?.keep();
        let snapshot = mock_local_snapshot(SnapshotDir::new(dir.clone()));
        let kept = snapshot.base_dir.into_path();
        assert_eq!(kept, dir);
        assert!(dir.exists(), "into_path() must leave the directory intact");

        Ok(())
    }

    #[restate_core::test]
    async fn acquire_lease_read_only_repository_returns_unavailable() -> anyhow::Result<()> {
        use crate::snapshots::LeaseError;
        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination),
            ..SnapshotsOptions::default()
        };

        let repository =
            SnapshotRepository::new_read_only_from_config(&opts, TempDir::new().unwrap().keep())
                .await?
                .unwrap();

        let result = repository.acquire_lease(PartitionId::MIN).await;
        assert!(
            matches!(result, Err(LeaseError::Unavailable)),
            "expected Err(LeaseError::Unavailable) from a read-only repository"
        );
        Ok(())
    }
}
