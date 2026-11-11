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
use futures::TryStreamExt;
use metrics::Gauge;
use object_store::path::Path as ObjectPath;
use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use serde_with::{DefaultOnError, DisplayFromStr, FromInto, serde_as};
use tempfile::TempDir;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, info, instrument, warn};
use url::Url;

use restate_clock::WallClock;
use restate_core::Metadata;
use restate_memory::ByteCount;
use restate_metadata_store::MetadataStoreClient;
use restate_object_store_util::create_object_store_client;
use restate_types::GenerationalNodeId;
use restate_types::config::SnapshotsOptions;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::{LogId, Lsn};
use restate_types::nodes_config::ClusterFingerprint;
use restate_types::time::MillisSinceEpoch;

#[cfg(any(test, feature = "test-util"))]
use super::leases::NoOpLeaseManager;
use super::leases::{LeaseError, SnapshotLeaseGuard, SnapshotLeaseManager};
use super::{
    LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotCompatibility, SnapshotDir,
    SnapshotFormatVersion,
};
use crate::metric_definitions::{
    SNAPSHOT_DOWNLOAD_BYTES, SNAPSHOT_DOWNLOAD_INFLIGHT, SNAPSHOT_UPLOAD_BYTES,
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
    lease_provider: Option<LeaseProvider>,
    #[cfg(any(test, feature = "test-util"))]
    enable_cleanup: bool,
}

/// S3 and other stores require a certain minimum size for the parts of a multipart upload. It is an
/// API error to attempt a multipart put below this size, apart from the final segment.
const MULTIPART_UPLOAD_CHUNK_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// How long `acquire_lease` waits for a lease held by this same node before reporting it as held.
const SAME_NODE_LEASE_WAIT: std::time::Duration = std::time::Duration::from_secs(10);
const SAME_NODE_LEASE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// Maximum number of concurrent downloads when getting snapshots from the repository.
const DOWNLOAD_CONCURRENCY_LIMIT: usize = 8;

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

    // The next fields, here and on each retained snapshot, are `None` when unknown. Servers that
    // predate them drop them when they publish a snapshot, both for that snapshot and for the
    // retained ones, so they are never carried over from a different snapshot.
    /// Generational id of the node that produced the snapshot.
    #[serde_as(as = "DefaultOnError<Option<DisplayFromStr>>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<GenerationalNodeId>,

    /// What a server needs in order to use the snapshot's data.
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<SnapshotCompatibility>,

    /// Total size of the snapshot's data files, written as a number of bytes.
    #[serde_as(as = "DefaultOnError<Option<FromInto<u64>>>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_size_in_bytes: Option<ByteCount>,

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
    /// Generational id of the node that produced the snapshot.
    #[serde_as(as = "DefaultOnError<Option<DisplayFromStr>>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<GenerationalNodeId>,
    /// What a server needs in order to use the snapshot's data.
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<SnapshotCompatibility>,
    /// Total size of the snapshot's data files, written as a number of bytes.
    #[serde_as(as = "DefaultOnError<Option<FromInto<u64>>>")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_size_in_bytes: Option<ByteCount>,
}

impl SnapshotReference {
    fn from_metadata(snapshot: &PartitionSnapshotMetadata) -> Self {
        SnapshotReference {
            snapshot_id: snapshot.snapshot_id,
            min_applied_lsn: snapshot.min_applied_lsn,
            created_at: snapshot.created_at,
            path: UniqueSnapshotKey::from_metadata(snapshot).padded_key(),
            node_id: snapshot.node_id,
            compatibility: snapshot.compatibility.clone(),
            data_size_in_bytes: Some(snapshot.data_size_in_bytes()),
        }
    }
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
                    node_id: self.node_id,
                    compatibility: self.compatibility.clone(),
                    data_size_in_bytes: self.data_size_in_bytes,
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
    /// What a server needs in order to use the latest snapshot's data, if known.
    pub latest_snapshot_compatibility: Option<SnapshotCompatibility>,
    /// Total size of the latest snapshot's data files, if known.
    pub latest_snapshot_data_size_in_bytes: Option<ByteCount>,
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
            latest_snapshot_compatibility: None,
            latest_snapshot_data_size_in_bytes: None,
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
            latest_snapshot_compatibility: latest.compatibility.clone(),
            latest_snapshot_data_size_in_bytes: latest.data_size_in_bytes,
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
        Ok(Self::new_internal(snapshots_options, staging_dir, None)
            .await?
            .map(|mut repository| {
                repository.lease_provider = Some(LeaseProvider::NoOp(NoOpLeaseManager::new()));
                repository
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
        // A lease held by this node usually belongs to the previous snapshot's cleanup task, which
        // releases it shortly. Wait for that briefly rather than failing back-to-back requests.
        let my_node_id = Metadata::with_current(|m| m.my_node_id());
        let deadline = tokio::time::Instant::now() + SAME_NODE_LEASE_WAIT;
        let guard = loop {
            match provider.acquire(partition_id).await {
                Err(LeaseError::Held { holder, .. })
                    if holder == my_node_id && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(SAME_NODE_LEASE_POLL_INTERVAL).await;
                }
                result => break result?,
            }
        };
        let guard = Arc::new(guard);
        guard.start_renewal_task()?;
        Ok(guard)
    }

    /// Removes any entries left over in the snapshot staging directory by a previous run.
    ///
    /// This is best-effort: failures are logged and ignored. It must only be called at
    /// startup, before any snapshot download can be in flight.
    async fn sweep_staging_dir(staging_dir: &Path) {
        // Remove the whole staging directory; `get_snapshot` recreates it on demand before
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
            SNAPSHOT_UPLOAD_DURATION, SNAPSHOT_UPLOAD_FAILED, SNAPSHOT_UPLOAD_INFLIGHT,
            SNAPSHOT_UPLOAD_SUCCESS,
        };

        debug!("Publishing partition snapshot to: {}", self.destination);

        let _inflight = InflightGuard::new(metrics::gauge!(SNAPSHOT_UPLOAD_INFLIGHT));
        let start = tokio::time::Instant::now();
        let put_result = self
            .put_snapshot_inner(snapshot, local_snapshot.path(), Arc::clone(&lease_guard))
            .await;

        // We own the local snapshot directory; remove it asynchronously (it may hold large SST
        // files) regardless of the upload outcome. Failure is only logged: if the snapshot was
        // uploaded we still want to return the result to the caller.
        local_snapshot.remove().await;

        metrics::histogram!(SNAPSHOT_UPLOAD_DURATION).record(start.elapsed());

        match put_result {
            Ok(PutOutcome::Published(status)) => {
                metrics::counter!(SNAPSHOT_UPLOAD_SUCCESS).increment(1);
                Ok(status)
            }
            Ok(PutOutcome::Superseded { latest, discarded }) => {
                // Not a failure: the repository already holds a snapshot at least this new, which
                // is what the caller asked for. Report it so the caller sees the newest snapshot.
                metrics::counter!(SNAPSHOT_UPLOAD_SUCCESS).increment(1);
                self.discard_uploaded_files(discarded, &lease_guard).await;
                Ok(latest)
            }
            Err(put_error) => {
                metrics::counter!(SNAPSHOT_UPLOAD_FAILED).increment(1);
                self.discard_uploaded_files(put_error.progress, &lease_guard)
                    .await;
                Err(put_error.error)
            }
        }
    }

    /// Deletes the objects of a snapshot that will not be published. Errors are only logged; the
    /// orphan sweep catches anything left behind.
    ///
    /// Shared SSTs this upload created are deleted only while the lease is still valid. Without
    /// it, another node may already have published a snapshot that deduplicated against them.
    async fn discard_uploaded_files(
        &self,
        progress: SnapshotUploadProgress,
        lease_guard: &SnapshotLeaseGuard,
    ) {
        let shared = if lease_guard.is_valid() {
            progress.created_shared_ssts
        } else {
            vec![]
        };
        for path in progress.own_objects.iter().chain(&shared) {
            if let Err(err) = self.object_store.delete(path).await {
                info!(%err, %path, "Failed to delete object of unpublished snapshot");
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

            // `creates_shared` marks a shared SST that did not exist before this upload. Only those
            // may be deleted if the snapshot is not published: any other shared key may be in use.
            let (repository_key, relative_key, must_upload, creates_shared) = match self
                .snapshot_type
            {
                SnapshotType::Full => {
                    let key = self.snapshot_file_path(snapshot, filename);
                    (key, None, true, false)
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
                    let head = self.object_store.head(&sst_key).await;
                    let creates_shared = matches!(head, Err(object_store::Error::NotFound { .. }));
                    let must_upload = match head {
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
                    (sst_key, Some(relative_key), must_upload, creates_shared)
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
                if relative_key.is_none() {
                    progress.own_objects.push(repository_key);
                } else if creates_shared {
                    progress.created_shared_ssts.push(repository_key);
                }
            }

            if let Some(key) = relative_key {
                file_keys.insert(file.name.clone(), key);
            }
        }

        if matches!(self.snapshot_type, SnapshotType::Incremental) {
            use crate::metric_definitions::SNAPSHOT_UPLOAD_BYTES_DEDUPLICATED;

            let bytes_saved = total_size - uploaded_size;
            let dedup_rate = if total_size > 0 {
                (bytes_saved as f64 / total_size as f64) * 100.0
            } else {
                0.0
            };

            metrics::counter!(SNAPSHOT_UPLOAD_BYTES_DEDUPLICATED).increment(bytes_saved as u64);

            info!(
                partition_id = %snapshot.partition_id,
                snapshot_id = %snapshot.snapshot_id,
                files_uploaded = files_uploaded,
                files_skipped = files_skipped,
                bytes_uploaded = %ByteCount::from(uploaded_size),
                bytes_saved = %ByteCount::from(bytes_saved),
                dedup_rate = format!("{dedup_rate:.1}%"),
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
    ) -> Result<PutOutcome, PutSnapshotError> {
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

        let Some((new_latest, evicted_snapshots)) =
            self.build_latest_v2(snapshot, maybe_stored.as_ref().map(|(l, _)| l))
        else {
            let (stored, _) = maybe_stored.expect("only a stored snapshot can supersede");
            let latest = PartitionSnapshotStatus::try_from(&stored)
                .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;
            return Ok(PutOutcome::Superseded {
                latest,
                discarded: progress,
            });
        };

        let latest_payload = PutPayload::from(
            serde_json::to_string_pretty(&new_latest)
                .map_err(|e| PutSnapshotError::from(e, progress.clone()))?,
        );

        // Uploads can outlive a lease duration, so re-check immediately before publishing rather
        // than trusting the lease we held when the upload started. This narrows, but cannot close,
        // the window: the lease may still lapse between here and the CAS landing. A late writer
        // that loses the race to a newer snapshot is turned away as superseded by build_latest_v2
        // or, if it checked first, by the conditional put.
        if !lease_guard.is_valid() {
            return Err(PutSnapshotError::from(
                anyhow::anyhow!(
                    "lease lost before publishing the snapshot pointer; abandoning snapshot"
                ),
                progress.clone(),
            ));
        }

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
            .map(PutOutcome::Published)
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))
    }

    /// Full snapshots keep format V1 so that released nodes can still restore from them;
    /// incremental ones are V2, which those nodes reject up front instead of failing to find
    /// the SSTs.
    fn with_file_keys(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        file_keys: std::collections::BTreeMap<String, String>,
    ) -> PartitionSnapshotMetadata {
        use restate_types::config::SnapshotType;

        let mut snapshot = snapshot.clone();
        snapshot.version = match self.snapshot_type {
            SnapshotType::Full => SnapshotFormatVersion::V1,
            SnapshotType::Incremental => SnapshotFormatVersion::V2,
        };
        snapshot.file_keys = file_keys;
        snapshot
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

        let snapshot_with_keys = self.with_file_keys(snapshot, file_keys);

        let metadata_key = self.snapshot_file_path(snapshot, "metadata.json");
        let metadata_json_payload = PutPayload::from(
            serde_json::to_string_pretty(&snapshot_with_keys).expect("Can always serialize JSON"),
        );

        let put_result = self
            .object_store
            .put(&metadata_key, metadata_json_payload)
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;
        progress.own_objects.push(metadata_key.clone());

        debug!(
            key = %metadata_key,
            etag = %put_result.e_tag.unwrap_or_default(),
            "Successfully published snapshot metadata",
        );

        Ok(())
    }

    /// Builds V2 latest snapshot metadata.
    ///
    /// Returns `(LatestSnapshot, Vec<SnapshotReference>)` where the second element contains
    /// snapshots that no longer need to be retained following the metadata update.
    /// If cleanup fails, these snapshots will be orphaned until cleaned up by the sweeper.
    ///
    /// Returns `None` if the snapshot is superseded: it is not newer than every snapshot already
    /// in `current`, so publishing it would move the pointer backwards.
    fn build_latest_v2(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        current: Option<&LatestSnapshot>,
    ) -> Option<(LatestSnapshot, Vec<SnapshotReference>)> {
        let new_snapshot_ref = SnapshotReference::from_metadata(snapshot);

        let mut retained_snapshots = current
            .map(|l| l.effective_retained_snapshots())
            .unwrap_or_default();

        // The retained list is checked too, because an older release could leave the top-level
        // pointer behind the newest retained snapshot.
        if let Some(current) = current {
            let newest_lsn = retained_snapshots
                .iter()
                .map(|s| s.min_applied_lsn)
                .fold(current.min_applied_lsn, Lsn::max);
            if snapshot.min_applied_lsn == newest_lsn {
                // An idle partition: nothing was applied since the latest snapshot.
                debug!(
                    snapshot_lsn = %snapshot.min_applied_lsn,
                    "A snapshot at this LSN is already published; not publishing a duplicate"
                );
                return None;
            }
            if snapshot.min_applied_lsn < newest_lsn {
                // Only a writer that lost its lease mid-upload can get here.
                warn!(
                    snapshot_id = %snapshot.snapshot_id,
                    snapshot_lsn = %snapshot.min_applied_lsn,
                    latest_snapshot_id = %current.snapshot_id,
                    newest_lsn = %newest_lsn,
                    "Abandoning snapshot: a newer snapshot was published meanwhile; \
                     leaving the latest snapshot pointer unchanged"
                );
                return None;
            }
        }

        // Retained snapshots are kept in descending LSN order, and the new one is the newest.
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
            // Taken from the new snapshot alone, never from `current`.
            node_id: new_snapshot_ref.node_id,
            compatibility: new_snapshot_ref.compatibility,
            data_size_in_bytes: new_snapshot_ref.data_size_in_bytes,
            retained_snapshots,
        };

        Some((latest, evicted_snapshots))
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

    /// Downloads the partition's latest snapshot, or returns `None` if the repository holds no
    /// snapshot for the partition. Unlike [`super::Snapshots::download_snapshot`], this does not
    /// fall back to older retained snapshots. It is the caller's responsibility to delete the
    /// snapshot directory when it is no longer needed.
    pub async fn get_latest(
        &self,
        partition_id: PartitionId,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<Option<LocalPartitionSnapshot>> {
        let candidates = self.get_snapshot_candidates(partition_id).await?;
        let Some(latest) = candidates.first() else {
            return Ok(None);
        };
        Ok(Some(
            self.get_snapshot(partition_id, latest, cancellation)
                .await?,
        ))
    }

    /// Downloads the snapshot `snapshot_ref` into a staging directory. It is the caller's
    /// responsibility to delete the snapshot directory when it is no longer needed.
    ///
    /// If `cancellation` fires mid-download, in-flight downloads are aborted and drained before
    /// the staging directory is removed, and an error is returned.
    #[instrument(
        level = "error",
        skip_all,
        fields(%partition_id, snapshot_id = %snapshot_ref.snapshot_id),
    )]
    pub(crate) async fn get_snapshot(
        &self,
        partition_id: PartitionId,
        snapshot_ref: &SnapshotReference,
        cancellation: &CancellationToken,
    ) -> Result<LocalPartitionSnapshot, GetSnapshotError> {
        let _inflight = InflightGuard::new(metrics::gauge!(SNAPSHOT_DOWNLOAD_INFLIGHT));

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
            .map_err(|err| {
                GetSnapshotError::from_object_store(
                    err,
                    format!("failed to fetch snapshot metadata '{snapshot_metadata_path}'"),
                )
            })?
            .bytes()
            .await
            .map_err(GetSnapshotError::unavailable)?;
        let mut snapshot_metadata =
            parse_snapshot_metadata(&snapshot_metadata).map_err(GetSnapshotError::Defective)?;

        if !self.staging_dir.exists() {
            std::fs::create_dir_all(&self.staging_dir).map_err(GetSnapshotError::unavailable)?;
        }

        // The snapshot ingest directory should be on the same filesystem as the partition store
        // to minimize IO and disk space usage during import.
        let snapshot_dir = TempDir::with_prefix_in(
            format!("{}-", snapshot_metadata.snapshot_id),
            &self.staging_dir,
        )
        .map_err(GetSnapshotError::unavailable)?;
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
                    .map(String::as_str),
                &snapshot_ref.path,
                filename,
            );
            let local_path = snapshot_dir.path().join(filename);
            let concurrency_limiter = Arc::clone(&concurrency_limiter);
            let object_store = Arc::clone(&self.object_store);
            let snapshot_id = snapshot_metadata.snapshot_id;
            let snapshot_filename = filename.to_owned();
            let downloaded_bytes = metrics::counter!(SNAPSHOT_DOWNLOAD_BYTES);

            let handle = downloads.build_task().name(filename).spawn(async move {
                let _permit = concurrency_limiter.acquire().await.map_err(GetSnapshotError::unavailable)?;
                debug!(%key, "Downloading snapshot object");
                let mut file_data = StreamReader::new(
                    object_store
                        .get(&key)
                        .await
                        .map_err(|err| GetSnapshotError::from_object_store(err, format!("failed to download partition {partition_id} snapshot {snapshot_id} file {key:?}")))?
                        .into_stream()
                        .inspect_ok(|chunk| downloaded_bytes.increment(chunk.len() as u64)),
                );

                let mut snapshot_file =
                    tokio::fs::File::create_new(&local_path).await.map_err(|e| {
                        GetSnapshotError::Unavailable(anyhow!("Failed to create local partition {partition_id} snapshot file {local_path:?}: {e}"))
                    })?;
                let size = io::copy(&mut file_data, &mut snapshot_file)
                    .await
                    .map_err(|e| GetSnapshotError::Unavailable(anyhow!("Failed to download snapshot object {:?}: {}", key, e)))?;
                snapshot_file.shutdown().await.map_err(GetSnapshotError::unavailable)?;

                if size != expected_size as u64 {
                    return Err(GetSnapshotError::Defective(anyhow!("Downloaded partition {partition_id} snapshot {snapshot_id} component file {:?} has unexpected size: expected: {}, actual: {}", snapshot_filename, expected_size, size)));
                }
                debug!(
                    %key,
                    ?size,
                    "Downloaded snapshot object {}",
                    local_path.display(),
                );
                Ok(())
            }.instrument(Span::current())).map_err(GetSnapshotError::unavailable)?;
            task_handles.insert(handle.id(), filename.to_string());
            // patch the directory path to reflect the actual location on the restoring node
            file.directory.clone_from(&directory);
        }

        loop {
            let next = tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    abort_tasks(downloads).await;
                    return Err(GetSnapshotError::Unavailable(anyhow!("Snapshot download cancelled")));
                }
                next = downloads.join_next() => next,
            };
            match next {
                None => {
                    debug!(snapshot_id = %snapshot_metadata.snapshot_id, "All download tasks completed");
                    break;
                }
                Some(Err(join_error)) => {
                    let failed_id = task_handles.get(&join_error.id());
                    abort_tasks(downloads).await;
                    return Err(GetSnapshotError::Unavailable(anyhow!(
                        "Failed to download snapshot object {:?}: {}",
                        failed_id,
                        join_error
                    )));
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
            compatibility: snapshot_metadata.compatibility,
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
                let latest: LatestSnapshot = serde_json::from_slice(&result.bytes().await?)
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
}

// Strip the leading "/" character from RocksDB LiveFile names
fn strip_leading_slash(name: &str) -> &str {
    name.trim_start_matches('/')
}

/// Objects written by a snapshot upload, for deleting them if the snapshot is not published.
#[derive(Clone, Debug)]
struct SnapshotUploadProgress {
    pub snapshot_complete_path: ObjectPath,
    /// Objects under the snapshot's own prefix; nothing else references them.
    pub own_objects: Vec<ObjectPath>,
    /// Shared content-addressed SSTs that this upload created.
    pub created_shared_ssts: Vec<ObjectPath>,
}

impl SnapshotUploadProgress {
    fn with_snapshot_path(snapshot_complete_path: ObjectPath) -> Self {
        SnapshotUploadProgress {
            snapshot_complete_path,
            own_objects: vec![],
            created_shared_ssts: vec![],
        }
    }
}

enum PutOutcome {
    Published(PartitionSnapshotStatus),
    /// The repository already holds a snapshot at least as new; this one was not published and
    /// its uploaded files are to be discarded.
    Superseded {
        latest: PartitionSnapshotStatus,
        discarded: SnapshotUploadProgress,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("failed to upload snapshot to {}: {error}", progress.snapshot_complete_path)]
struct PutSnapshotError {
    pub progress: SnapshotUploadProgress,
    pub error: anyhow::Error,
}

impl PutSnapshotError {
    fn from<E>(error: E, progress: SnapshotUploadProgress) -> Self
    where
        E: Into<anyhow::Error>,
    {
        PutSnapshotError {
            error: error.into(),
            progress,
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
    let uploaded_bytes = metrics::counter!(SNAPSHOT_UPLOAD_BYTES);
    let mut snapshot = tokio::fs::File::open(file_path).await?;

    let file_len = snapshot.metadata().await?.len();
    if file_len < MULTIPART_UPLOAD_CHUNK_SIZE_BYTES as u64 {
        let payload = PutPayload::from(tokio::fs::read(file_path).await?);
        let result = object_store.put(key, payload).await?;
        uploaded_bytes.increment(file_len);
        return Ok(result);
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
                let part = buf.split().freeze();
                let part_len = part.len() as u64;
                upload.put_part(PutPayload::from_bytes(part)).await?;
                uploaded_bytes.increment(part_len);
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

/// Why [`SnapshotRepository::get_snapshot`] failed, which decides whether restoring an older
/// retained snapshot instead can help.
#[derive(Debug, thiserror::Error)]
pub(crate) enum GetSnapshotError {
    /// The snapshot itself is missing or invalid; an older retained snapshot may still restore.
    #[error("snapshot is missing or invalid: {0:#}")]
    Defective(anyhow::Error),
    /// The repository, its credentials or the local node failed, or the download was cancelled.
    /// An older snapshot would fail the same way; retrying this one later may succeed.
    #[error(transparent)]
    Unavailable(anyhow::Error),
}

impl GetSnapshotError {
    fn unavailable(err: impl Into<anyhow::Error>) -> Self {
        Self::Unavailable(err.into())
    }

    /// The object store has already retried transient failures, so only a missing object says
    /// something about the snapshot.
    fn from_object_store(err: object_store::Error, context: String) -> Self {
        match err {
            object_store::Error::NotFound { .. } => {
                Self::Defective(anyhow::Error::new(err).context(context))
            }
            err => Self::Unavailable(anyhow::Error::new(err).context(context)),
        }
    }
}

/// Parses and validates a snapshot's `metadata.json`. Any error means the snapshot is unusable.
fn parse_snapshot_metadata(bytes: &[u8]) -> anyhow::Result<PartitionSnapshotMetadata> {
    let snapshot_metadata: PartitionSnapshotMetadata = serde_json::from_slice(bytes)?;
    Metadata::with_current(|m| {
        let nodes_config = m.nodes_config_ref();
        snapshot_metadata.validate(
            nodes_config.cluster_name(),
            nodes_config.cluster_fingerprint(),
        )
    })
    .with_context(|| {
        format!(
            "failed validating metadata of snapshot {}",
            snapshot_metadata.snapshot_id
        )
    })?;
    Ok(snapshot_metadata)
}

/// Holds a gauge incremented for as long as the guard lives, so cancelled transfers are released too.
struct InflightGuard(Gauge);

impl InflightGuard {
    fn new(gauge: Gauge) -> Self {
        gauge.increment(1);
        Self(gauge)
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.decrement(1);
    }
}

async fn abort_tasks<T: 'static>(mut join_set: JoinSet<T>) {
    join_set.abort_all();
    while join_set.join_next().await.is_some() {}
}

#[cfg(test)]
#[path = "compatibility_tests.rs"]
mod compatibility_tests;

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
    use tokio_util::sync::CancellationToken;
    use tracing::info;
    use url::Url;

    use restate_clock::time::MillisSinceEpoch;
    use restate_core::{Metadata, TestCoreEnv};
    use restate_object_store_util::create_object_store_client;
    use restate_test_util::tempdir;
    use restate_types::config::{ObjectStoreOptions, SnapshotsOptions};
    use restate_types::identifiers::{PartitionId, SnapshotId};
    use restate_types::logs::{LogId, Lsn, SequenceNumber};
    use restate_types::retries::RetryPolicy;
    use restate_types::sharding::KeyRange;
    use restate_types::{GenerationalNodeId, SemanticRestateVersion};

    use crate::snapshots::SnapshotLeaseGuard;
    use crate::snapshots::repository::{LatestSnapshotVersion, SnapshotUploadProgress};

    use super::{LatestSnapshot, SnapshotReference, SnapshotRepository, UniqueSnapshotKey};
    use super::{
        PartitionSnapshotMetadata, SnapshotCompatibility, SnapshotDir, SnapshotFormatVersion,
    };

    #[restate_core::test]
    async fn overwrite_unparsable_latest() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshot_source = tempdir()?;
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

        let snapshots_destination: TempDir = tempdir()?;
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
            tempdir()?.keep(),
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
        let snapshots_destination = tempdir()?;
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

        let snapshot_source = tempdir()?;
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

        let staging_dir = tempdir()?.keep();
        let repository = SnapshotRepository::new_from_config(
            &opts,
            staging_dir.clone(),
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
        let (expected_latest, _) = repository.build_latest_v2(&snapshot1, None).unwrap();
        assert_eq!(expected_latest, latest);

        let snapshot_source = tempdir()?;
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
            .get(&partition_prefix.clone().join("latest.json"))
            .await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        let (expected_latest2, _) = repository.build_latest_v2(&snapshot2, None).unwrap();
        assert_eq!(expected_latest2, latest);

        // Publishing a lower-LSN snapshot after a higher one is abandoned: latest.json stays
        // byte-for-byte unchanged, the stale snapshot's files are deleted, and the caller gets the
        // newer published snapshot back.
        let latest_before = object_store
            .get(&partition_prefix.clone().join("latest.json"))
            .await?
            .bytes()
            .await?;
        let stale_source = tempdir()?;
        tokio::fs::write(stale_source.path().join("data.sst"), data).await?;
        let mut stale = mock_snapshot_metadata(
            "/data.sst".to_owned(),
            stale_source.path().to_string_lossy().to_string(),
            data.len(),
        );
        stale.min_applied_lsn = snapshot1.min_applied_lsn;
        let status = repository
            .put(
                &stale,
                SnapshotDir::new(stale_source.path().to_path_buf()),
                Arc::new(SnapshotLeaseGuard::noop()),
            )
            .await?;
        assert_eq!(status.latest_snapshot_id, snapshot2.snapshot_id);
        assert!(
            object_store
                .list(Some(&repository.base_prefix(&stale)))
                .try_next()
                .await?
                .is_none(),
            "a superseded snapshot's files are deleted"
        );
        let latest_after = object_store
            .get(&partition_prefix.clone().join("latest.json"))
            .await?
            .bytes()
            .await?;
        assert_eq!(latest_before, latest_after);
        let unchanged: LatestSnapshot = serde_json::from_slice(&latest_after)?;
        assert_eq!(unchanged.snapshot_id, snapshot2.snapshot_id);
        assert_eq!(unchanged.min_applied_lsn, snapshot2.min_applied_lsn);

        // A cancelled download fails and leaves nothing behind in the staging directory.
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let candidates = repository.get_snapshot_candidates(PartitionId::MIN).await?;
        assert!(
            repository
                .get_snapshot(PartitionId::MIN, &candidates[0], &cancelled)
                .await
                .is_err()
        );
        assert!(std::fs::read_dir(&staging_dir)?.next().is_none());

        let latest = repository
            .get_snapshot(PartitionId::MIN, &candidates[0], &CancellationToken::new())
            .await?;
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
        let snapshot_dir = tempdir()?.keep();

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

    /// A snapshot published by a writer that lost its lease mid-upload can reach the pointer CAS
    /// after a newer snapshot was already published. It must be abandoned rather than regress the
    /// pointer fields or evict the newer snapshot.
    #[restate_core::test]
    async fn build_latest_v2_abandons_stale_snapshot() -> anyhow::Result<()> {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;
        let repository = mock_repository(2);

        let newest = SnapshotReference {
            snapshot_id: SnapshotId::new(),
            min_applied_lsn: Lsn::new(100),
            created_at: Timestamp::now(),
            path: "lsn_00000000000000000100-snap_1".to_owned(),
            node_id: None,
            compatibility: None,
            data_size_in_bytes: None,
        };
        let current = mock_latest_snapshot(&newest);

        // Lower and equal LSNs are both abandoned.
        for lsn in [50, 100] {
            let mut late = mock_snapshot_metadata("/data.sst".to_owned(), "/".to_owned(), 0);
            late.min_applied_lsn = Lsn::new(lsn);
            assert!(
                repository.build_latest_v2(&late, Some(&current)).is_none(),
                "a snapshot that is not newer than latest must be abandoned"
            );
        }

        // A latest.json whose top-level pointer already regressed behind its retained list (as an
        // older release could write) is judged by the newest retained snapshot.
        let regressed_head = SnapshotReference {
            snapshot_id: SnapshotId::new(),
            min_applied_lsn: Lsn::new(80),
            path: "lsn_00000000000000000080-snap_2".to_owned(),
            ..newest.clone()
        };
        let mut regressed = mock_latest_snapshot(&regressed_head);
        regressed.retained_snapshots = vec![newest, regressed_head];
        let mut late = mock_snapshot_metadata("/data.sst".to_owned(), "/".to_owned(), 0);
        late.min_applied_lsn = Lsn::new(90);
        assert!(
            repository
                .build_latest_v2(&late, Some(&regressed))
                .is_none()
        );

        // The ordinary case - a genuinely newer snapshot - takes the head position and the
        // top-level pointer fields.
        let mut newer = mock_snapshot_metadata("/data.sst".to_owned(), "/".to_owned(), 0);
        newer.min_applied_lsn = Lsn::new(150);
        let (latest, evicted) = repository.build_latest_v2(&newer, Some(&current)).unwrap();
        assert_eq!(latest.snapshot_id, newer.snapshot_id);
        assert_eq!(latest.min_applied_lsn, Lsn::new(150));
        assert_eq!(
            latest
                .retained_snapshots
                .iter()
                .map(|s| s.min_applied_lsn)
                .collect::<Vec<_>>(),
            vec![Lsn::new(150), Lsn::new(100)]
        );
        assert!(evicted.is_empty());

        Ok(())
    }

    /// Back-to-back snapshots on one node: the second waits for the lease the first one's
    /// cleanup still holds instead of failing with `Held`.
    #[restate_core::test]
    async fn acquire_lease_waits_for_lease_held_by_same_node() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;
        let mut repository = mock_repository(1);
        repository.lease_provider = Some(super::LeaseProvider::Real(
            super::SnapshotLeaseManager::new(env.metadata_store_client.clone()),
        ));

        let first = repository.acquire_lease(PartitionId::MIN).await?;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            drop(first);
        });
        let second = repository.acquire_lease(PartitionId::MIN).await?;
        assert!(second.is_valid());

        Ok(())
    }

    fn mock_repository(num_retained: u8) -> SnapshotRepository {
        SnapshotRepository {
            object_store: Arc::new(object_store::memory::InMemory::new()),
            destination: Url::parse("memory:///").unwrap(),
            prefix: ObjectPath::default(),
            staging_dir: PathBuf::new(),
            num_retained: std::num::NonZeroU8::new(num_retained).unwrap(),
            snapshot_type: restate_types::config::SnapshotType::default(),
            lease_provider: None,
            enable_cleanup: false,
        }
    }

    fn mock_latest_snapshot(reference: &SnapshotReference) -> LatestSnapshot {
        LatestSnapshot {
            version: LatestSnapshotVersion::V2,
            partition_id: PartitionId::MIN,
            log_id: Some(LogId::MIN),
            cluster_name: Metadata::with_current(|m| {
                m.nodes_config_ref().cluster_name().to_string()
            }),
            cluster_fingerprint: Metadata::with_current(|m| {
                m.nodes_config_ref().cluster_fingerprint()
            }),
            node_name: "node".to_string(),
            created_at: reference.created_at,
            snapshot_id: reference.snapshot_id,
            min_applied_lsn: reference.min_applied_lsn,
            path: reference.path.clone(),
            node_id: None,
            compatibility: None,
            data_size_in_bytes: None,
            retained_snapshots: vec![reference.clone()],
        }
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
            node_id: Some(GenerationalNodeId::new(1, 1)),
            partition_id: PartitionId::MIN,
            created_at: jiff::Timestamp::now(),
            snapshot_id: SnapshotId::new(),
            key_range: KeyRange::FULL,
            log_id: LogId::MIN,
            min_applied_lsn: Lsn::new(1),
            db_comparator_name: "leveldb.BytewiseComparator".to_string(),
            compatibility: Some(SnapshotCompatibility {
                min_restate_version: SemanticRestateVersion::new(1, 7, 9),
                storage_features: vec!["scoped-state-table".into()],
                partition_features: vec!["journal_v2".into()],
            }),
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

        let snapshots_destination = tempdir()?;
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
            tempdir()?.keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

        for i in 1..=4 {
            let snapshot_source = tempdir()?;
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

        let snapshots_destination = tempdir()?;
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
            tempdir()?.keep(),
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

        let snapshots_destination = tempdir()?;
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
            tempdir()?.keep(),
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
                .download_snapshot(PartitionId::MIN, None, &CancellationToken::new())
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
        let sst_path = |snapshot: &PartitionSnapshotMetadata| {
            snapshots_destination
                .path()
                .join(PartitionId::MIN.to_string())
                .join(UniqueSnapshotKey::from_metadata(snapshot).padded_key())
                .join("data.sst")
        };

        // Happy path: the latest snapshot is restored.
        let restored = snapshots
            .download_snapshot(PartitionId::MIN, None, &CancellationToken::new())
            .await?
            .expect("a snapshot is available");
        assert_eq!(restored.min_applied_lsn, Lsn::new(3000));

        // A target LSN selects only snapshots at or above it: 2500 still allows the latest (3000).
        let restored = snapshots
            .download_snapshot(
                PartitionId::MIN,
                Some(Lsn::new(2500)),
                &CancellationToken::new(),
            )
            .await?
            .expect("latest snapshot satisfies the target LSN");
        assert_eq!(restored.min_applied_lsn, Lsn::new(3000));

        // A target above every retained snapshot yields no suitable candidate (Ok(None)).
        assert!(
            snapshots
                .download_snapshot(
                    PartitionId::MIN,
                    Some(Lsn::new(5000)),
                    &CancellationToken::new()
                )
                .await?
                .is_none()
        );

        // A failure that says nothing about the snapshot (here the store can't read B's data file,
        // a symlink loop) fails the download rather than falling back to A.
        #[cfg(unix)]
        {
            let sst = sst_path(&snapshot_b);
            let saved = sst.with_extension("saved");
            std::fs::rename(&sst, &saved)?;
            std::os::unix::fs::symlink(&sst, &sst)?;
            assert!(
                snapshots
                    .download_snapshot(PartitionId::MIN, None, &CancellationToken::new())
                    .await
                    .is_err()
            );
            std::fs::remove_file(&sst)?;
            std::fs::rename(&saved, &sst)?;
        }

        // Corrupt the latest by removing its data file; the download must fall back to A.
        std::fs::remove_file(sst_path(&snapshot_b))?;
        let restored = snapshots
            .download_snapshot(PartitionId::MIN, None, &CancellationToken::new())
            .await?
            .expect("falls back to the older snapshot");
        assert_eq!(restored.min_applied_lsn, Lsn::new(2000));

        // Remove A's data too: every candidate now fails -> error, not a silent "no snapshot".
        std::fs::remove_file(sst_path(&snapshot_a))?;
        assert!(
            snapshots
                .download_snapshot(PartitionId::MIN, None, &CancellationToken::new())
                .await
                .is_err()
        );

        Ok(())
    }

    #[restate_core::test]
    async fn v1_to_v2_migration() -> anyhow::Result<()> {
        let env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = tempdir()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let snapshot_source = tempdir()?;
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
            tempdir()?.keep(),
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
            node_id: None,
            compatibility: None,
            data_size_in_bytes: None,
            retained_snapshots: vec![],
        };

        let latest_path = repository.latest_snapshot_pointer_path(snapshot.partition_id);
        object_store
            .put(
                &latest_path,
                PutPayload::from(serde_json::to_string(&latest_snapshot_v1)?),
            )
            .await?;

        let snapshot_source_2 = tempdir()?;
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

        let snapshots_destination = tempdir()?;
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
            tempdir()?.keep(),
            env.metadata_store_client.clone(),
        )
        .await?
        .unwrap();

        for i in 1..=3 {
            let snapshot_source = tempdir()?;
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

        let snapshots_destination = tempdir()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            num_retained: std::num::NonZeroU8::new(3).unwrap(),
            enable_cleanup: true,
            ..SnapshotsOptions::default()
        };

        let repository =
            SnapshotRepository::new_from_config_with_stub_leases(&opts, tempdir()?.keep())
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

    /// If a retained snapshot's metadata cannot be read (missing or unparseable), cleanup cannot
    /// tell which shared SSTs are live and must delete nothing.
    #[restate_core::test]
    async fn cleanup_aborts_if_retained_metadata_unreadable() -> anyhow::Result<()> {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;
        let repository = mock_repository(1);
        let store = &repository.object_store;
        let partition = ObjectPath::from(PartitionId::MIN.to_string());

        let snapshot_with_shared_sst = |lsn: u64| {
            let mut s = mock_snapshot_metadata("/000001.sst".to_owned(), "/".to_owned(), 0);
            s.min_applied_lsn = Lsn::new(lsn);
            s.file_keys
                .insert("/000001.sst".to_owned(), "ssts/shared.sst".to_owned());
            s
        };
        let evicted = snapshot_with_shared_sst(1);
        let retained = snapshot_with_shared_sst(2);
        let evicted_ref = SnapshotReference::from_metadata(&evicted);
        let retained_ref = SnapshotReference::from_metadata(&retained);
        let evicted_metadata = partition
            .clone()
            .join(evicted_ref.path.as_str())
            .join("metadata.json");
        let retained_metadata = partition
            .clone()
            .join(retained_ref.path.as_str())
            .join("metadata.json");
        let sst = partition.join("ssts").join("shared.sst");

        store.put(&sst, PutPayload::from_static(b"sst")).await?;
        store
            .put(
                &evicted_metadata,
                PutPayload::from(serde_json::to_vec(&evicted)?),
            )
            .await?;
        store
            .put(
                &repository.latest_snapshot_pointer_path(PartitionId::MIN),
                PutPayload::from(serde_json::to_vec(&mock_latest_snapshot(&retained_ref))?),
            )
            .await?;

        let cleanup = || async {
            repository
                .cleanup_evicted_snapshots_inner(
                    PartitionId::MIN,
                    vec![evicted_ref.clone()],
                    &Arc::new(SnapshotLeaseGuard::noop()),
                )
                .await
        };

        // Retained metadata.json missing, then corrupt: cleanup errors and deletes nothing.
        for corrupt in [false, true] {
            if corrupt {
                store
                    .put(&retained_metadata, PutPayload::from_static(b"not json"))
                    .await?;
            }
            assert!(cleanup().await.is_err());
            assert!(store.head(&sst).await.is_ok());
            assert!(store.head(&evicted_metadata).await.is_ok());
        }

        // Once readable, the evicted snapshot's metadata goes but the SST the retained snapshot
        // still references stays.
        store
            .put(
                &retained_metadata,
                PutPayload::from(serde_json::to_vec(&retained)?),
            )
            .await?;
        cleanup().await?;
        assert!(store.head(&sst).await.is_ok());
        assert!(store.head(&evicted_metadata).await.is_err());

        Ok(())
    }

    /// Full snapshots stay V1 so released nodes can restore from them; incremental ones are V2 so
    /// those nodes reject them up front. New code reads both.
    #[restate_core::test]
    async fn snapshot_format_version_follows_snapshot_type() -> anyhow::Result<()> {
        use restate_types::config::SnapshotType;

        let _env = TestCoreEnv::create_with_single_node(1, 1).await;
        let mut repository = mock_repository(1);
        let snapshot = mock_snapshot_metadata("/data.sst".to_owned(), "/".to_owned(), 0);
        let keys = || {
            std::collections::BTreeMap::from([("/data.sst".to_owned(), "ssts/x.sst".to_owned())])
        };

        for (snapshot_type, version, name) in [
            (SnapshotType::Full, SnapshotFormatVersion::V1, "V1"),
            (SnapshotType::Incremental, SnapshotFormatVersion::V2, "V2"),
        ] {
            repository.snapshot_type = snapshot_type;
            let json = serde_json::to_string(&repository.with_file_keys(&snapshot, keys()))?;
            assert!(json.contains(&format!("\"version\":\"{name}\"")), "{json}");
            let parsed = super::parse_snapshot_metadata(json.as_bytes())?;
            assert_eq!(parsed.version, version);
        }

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
            node_id: None,
            compatibility: None,
            data_size_in_bytes: None,
            retained_snapshots: vec![
                SnapshotReference {
                    snapshot_id: SnapshotId::new(),
                    min_applied_lsn: Lsn::new(3484), // Newest (index 0)
                    created_at: Timestamp::from_second(2).unwrap(),
                    path: "snap1".to_string(),
                    node_id: None,
                    compatibility: None,
                    data_size_in_bytes: None,
                },
                SnapshotReference {
                    snapshot_id: SnapshotId::new(),
                    min_applied_lsn: Lsn::new(1342), // Oldest by LSN but at index 1
                    created_at: Timestamp::from_second(0).unwrap(),
                    path: "snap3".to_string(),
                    node_id: None,
                    compatibility: None,
                    data_size_in_bytes: None,
                },
                SnapshotReference {
                    snapshot_id: SnapshotId::new(),
                    min_applied_lsn: Lsn::new(2839), // Middle LSN at index 2
                    created_at: Timestamp::from_second(1).unwrap(),
                    path: "snap2".to_string(),
                    node_id: None,
                    compatibility: None,
                    data_size_in_bytes: None,
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

    #[restate_core::test]
    async fn sweep_staging_dir_removes_leftovers() -> anyhow::Result<()> {
        let staging = tempdir()?;

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
            compatibility: None,
        }
    }

    #[restate_core::test]
    async fn snapshot_dir_cleaned_on_drop_unless_disarmed() -> anyhow::Result<()> {
        let staging = tempdir()?;

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
        let snapshots_destination = tempdir()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination),
            ..SnapshotsOptions::default()
        };

        let repository = SnapshotRepository::new_read_only_from_config(&opts, tempdir()?.keep())
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
