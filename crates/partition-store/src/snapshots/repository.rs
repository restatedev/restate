// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use itertools::Itertools;
use object_store::path::Path as ObjectPath;
use object_store::{
    MultipartUpload, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
};
use restate_types::SemanticRestateVersion;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tempfile::TempDir;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::io::StreamReader;
use tracing::{Instrument, Span, debug, error, info, instrument, warn};
use url::Url;

use restate_clock::WallClock;
use restate_core::Metadata;
use restate_object_store_util::create_object_store_client;
use restate_types::config::SnapshotsOptions;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::{LogId, Lsn};
use restate_types::nodes_config::ClusterFingerprint;
use restate_types::time::MillisSinceEpoch;

use super::{LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotFormatVersion};

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
    staging_dir: PathBuf,
    num_retained: Option<std::num::NonZeroU8>,
}

/// S3 and other stores require a certain minimum size for the parts of a multipart upload. It is an
/// API error to attempt a multipart put below this size, apart from the final segment.
const MULTIPART_UPLOAD_CHUNK_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// Maximum number of concurrent downloads when getting snapshots from the repository.
const DOWNLOAD_CONCURRENCY_LIMIT: usize = 8;

/// Maximum number of pending deletions tracked. Deleting old snapshots is handled asynchronously
/// from snapshot uploads as a follow-up metadata update. We want to cap how many pending deletions
/// we store before we leak them. We intentionally do not stop producing new snapshots as it's more
/// important to keep log trimming unblocked than to clean up old snapshots in the object store.
const PENDING_DELETIONS_MAX: usize = 100;

const PENDING_DELETIONS_WARNING: usize = 10;

/// Maximum number of failed CAS retries
const OBJECT_STORE_CAS_RETRIES: usize = 3;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum LatestSnapshotVersion {
    #[default]
    V1,
    /// V2 adds support for retained snapshots
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

    /// Retained snapshots ordered from latest to earliest. Introduced in V2 format.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained_snapshots: Vec<SnapshotReference>,

    /// Snapshots marked for deletion but not yet deleted. Introduced in V2 format.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_deletions: Vec<SnapshotReference>,
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
            pending_deletions: vec![],
        }
    }

    fn effective_retained_snapshots(&self) -> Vec<SnapshotReference> {
        if self.retained_snapshots.is_empty() {
            // upgrade path from V1 - implicitly the "latest" snapshot is always retained
            vec![SnapshotReference {
                snapshot_id: self.snapshot_id,
                min_applied_lsn: self.min_applied_lsn,
                created_at: self.created_at,
                path: self.path.clone(),
            }]
        } else {
            self.retained_snapshots.clone()
        }
    }

    pub fn validate(
        &self,
        cluster_name: &str,
        cluster_fingerprint: ClusterFingerprint,
    ) -> anyhow::Result<()> {
        if cluster_name != self.cluster_name {
            anyhow::bail!(
                "snapshot does not match the cluster name of this cluster, \
                 expected: '{cluster_name}' got: '{}'",
                self.cluster_name
            );
        }

        // Snapshots from earlier Restate versions might not have the fingerprint set. Hence, only
        // compare the fingerprints if the snapshot's fingerprint is present.
        if let Some(incoming_fingerprint) = self.cluster_fingerprint
            && cluster_fingerprint != incoming_fingerprint
        {
            bail!(
                "cluster fingerprint mismatch, \
                 expected:'{cluster_fingerprint}' {cluster_fingerprint:?} got:'{incoming_fingerprint}' {incoming_fingerprint:?}. \
                 This often happens if this cluster is reusing a snapshot repository path from a different cluster"
            );
        }

        Ok(())
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, derive_more::Display)]
#[display("{}", latest_snapshot_id)]
pub struct PartitionSnapshotStatus {
    // Field ordering is intentional to naturally order items by LSN
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

impl From<&LatestSnapshot> for PartitionSnapshotStatus {
    fn from(latest: &LatestSnapshot) -> Self {
        let log_id = latest.log_id.unwrap_or_else(|| {
            warn!(
                snapshot_id = %latest.snapshot_id,
                partition_id = %latest.partition_id,
                "LatestSnapshot V2 metadata with no stored log id, this is probably a bug"
            );
            Metadata::with_current(|m| m.partition_table_ref())
                .get(&latest.partition_id)
                .map(|p| p.log_id())
                .unwrap_or_else(|| LogId::default_for_partition(latest.partition_id))
        });

        let (archived_lsn, latest_snapshot_created_at) = match latest.version {
            LatestSnapshotVersion::V1 => (latest.min_applied_lsn, latest.created_at.into()),
            LatestSnapshotVersion::V2 => {
                // Report the earliest as the archived LSN, enables restoring from any of them
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

        PartitionSnapshotStatus {
            archived_lsn,
            latest_snapshot_lsn: latest.min_applied_lsn,
            latest_snapshot_created_at,
            latest_snapshot_id: latest.snapshot_id,
            log_id,
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
    pub async fn new_from_config(
        snapshots_options: &SnapshotsOptions,
        staging_dir: PathBuf,
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
            num_retained: snapshots_options.experimental_num_retained,
        }))
    }

    /// Write a partition snapshot to the snapshot repository
    ///
    /// Returns an archived LSN that reflects a successful upload. Depending on snapshot retention
    /// settings, the archived LSN may reflect an older snapshot than the one which was uploaded.
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
    ) -> anyhow::Result<PartitionSnapshotStatus> {
        debug!("Publishing partition snapshot to: {}", self.destination);

        let put_result = self
            .put_snapshot_inner(snapshot, local_snapshot_path.as_path(), true)
            .await;

        // We only log the error here since (a) it's relatively unlikely for rmdir to fail, and (b)
        // if we've uploaded the snapshot, we should get the response back to the caller. Logging at
        // WARN level as repeated failures could compromise the cluster.
        let _ = tokio::fs::remove_dir_all(local_snapshot_path.as_path())
            .await
            .inspect_err(|e| warn!("Failed to delete local snapshot files: {}", e));

        match put_result {
            Ok(archived_lsn) => Ok(archived_lsn),
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
        enable_cleanup: bool,
    ) -> Result<PartitionSnapshotStatus, PutSnapshotError> {
        let snapshot_prefix = self.base_prefix(snapshot);
        debug!(
            "Uploading snapshot from {:?} to {}",
            local_snapshot_path, snapshot_prefix
        );

        let mut progress = SnapshotUploadProgress::with_snapshot_path(snapshot_prefix);
        let mut buf = BytesMut::new();
        for file in &snapshot.files {
            let filename = file.name.trim_start_matches("/");
            let key = self.snapshot_file_path(snapshot, filename);

            let put_result = put_snapshot_object(
                local_snapshot_path.join(filename).as_path(),
                &key,
                &self.object_store,
                &mut buf,
            )
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;

            debug!(etag = %put_result.e_tag.unwrap_or_default(), %key, "Put snapshot object completed");
            progress.push(file.name.clone());
        }

        let metadata_key = self.snapshot_file_path(snapshot, "metadata.json");
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
            key = %metadata_key,
            etag = %put_result.e_tag.unwrap_or_default(),
            "Successfully published snapshot metadata",
        );

        let latest_path = self.latest_snapshot_pointer_path(snapshot.partition_id);

        let maybe_stored = self
            .get_latest_snapshot_metadata_for_update(snapshot, &latest_path)
            .await
            .map_err(|e| PutSnapshotError::from(e, progress.clone()))?;

        if let Some((latest_stored, _)) = &maybe_stored
            && latest_stored.min_applied_lsn >= snapshot.min_applied_lsn
        {
            info!(
                repository_latest_lsn = ?latest_stored.min_applied_lsn,
                new_snapshot_lsn = ?snapshot.min_applied_lsn,
                "The newly uploaded snapshot is no newer than the already-stored latest snapshot, \
                will not update latest pointer"
            );

            let snapshot_ref = SnapshotReference::from_metadata(snapshot);
            let partition_id = snapshot.partition_id;
            let repository = self.clone();
            let _ = restate_core::TaskCenter::spawn_unmanaged_child(
                restate_core::TaskKind::Disposable,
                "snapshot-cleanup-superseded",
                async move {
                    let _ = repository
                        .delete_snapshot_files(partition_id, &snapshot_ref)
                        .await;
                    Ok::<(), anyhow::Error>(())
                },
            );

            return Ok(PartitionSnapshotStatus::from(latest_stored));
        }

        let format_version = self.determine_format_version(maybe_stored.as_ref().map(|(l, _)| l));
        let new_latest = match format_version {
            LatestSnapshotVersion::V1 => self.build_latest_v1(snapshot),
            LatestSnapshotVersion::V2 => self
                .build_latest_v2(snapshot, maybe_stored.as_ref().map(|(l, _)| l))
                .map_err(|e| PutSnapshotError::from(e, progress.clone()))?,
        };

        let latest_payload = PutPayload::from(
            serde_json::to_string_pretty(&new_latest)
                .map_err(|e| PutSnapshotError::from(e, progress.clone()))?,
        );

        // The object_store file provider supports create-if-not-exists but not update-version on
        // put. The file:// protocol is only be enabled in test because of this.
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

        if !new_latest.pending_deletions.is_empty() && enable_cleanup {
            self.spawn_cleanup_task(snapshot.partition_id, new_latest.pending_deletions.clone());
        }

        Ok(PartitionSnapshotStatus::from(&new_latest))
    }

    fn determine_format_version(&self, current: Option<&LatestSnapshot>) -> LatestSnapshotVersion {
        // V2 is the default from 1.7.0, including pre-releases
        const GATE_VERSION: SemanticRestateVersion = SemanticRestateVersion::new(1, 6, u64::MAX);

        if SemanticRestateVersion::current().is_equal_or_newer_than(&GATE_VERSION)
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

    fn build_latest_v2(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        current: Option<&LatestSnapshot>,
    ) -> anyhow::Result<LatestSnapshot> {
        let new_snapshot_ref = SnapshotReference::from_metadata(snapshot);

        let (retained_snapshots, pending_deletions) = match self.num_retained {
            None => (vec![], vec![]), // tracking is only enabled if num-retained is set
            Some(num_retained) => {
                let mut retained_snapshots = current
                    .map(|l| l.effective_retained_snapshots())
                    .unwrap_or_default();
                retained_snapshots.insert(0, new_snapshot_ref.clone());
                retained_snapshots.sort_by_key(|s| std::cmp::Reverse(s.min_applied_lsn)); // just in case

                let mut pending_deletions = retained_snapshots
                    .split_off((num_retained.get() as usize).min(retained_snapshots.len()));
                pending_deletions.extend(
                    current
                        .map(|l| l.pending_deletions.clone())
                        .unwrap_or_default(),
                );

                if pending_deletions.len() > PENDING_DELETIONS_MAX {
                    let orphaned = pending_deletions.split_off(PENDING_DELETIONS_MAX);
                    warn!(
                        orphaned_ids = %orphaned.iter().map(|s| &s.snapshot_id).join(", "),
                        "Snapshot cleanup backlog exceeds the tracking limit ({}), \
                        older snapshots will be orphaned. Please check object store access permissions.",
                        PENDING_DELETIONS_MAX,
                    );
                } else if pending_deletions.len() >= PENDING_DELETIONS_WARNING {
                    warn!(
                        pending_deletions = pending_deletions.len(),
                        hard_limit = PENDING_DELETIONS_MAX,
                        "Snapshot cleanup is falling behind. Please check object store access permissions. \
                        When the hard limit is reached, older snapshots will no longer be tracked, \
                        resulting in increased storage usage."
                    );
                }

                (retained_snapshots, pending_deletions)
            }
        };

        Ok(LatestSnapshot {
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
            pending_deletions,
        })
    }

    fn spawn_cleanup_task(
        &self,
        partition_id: PartitionId,
        cleanup_snapshots: Vec<SnapshotReference>,
    ) {
        let repository = self.clone();
        let task_name = format!("snapshot-cleanup-{}", partition_id);

        let _ = restate_core::TaskCenter::spawn_unmanaged_child(
            restate_core::TaskKind::Disposable,
            task_name,
            async move {
                repository
                    .cleanup_pending_deletions(partition_id, cleanup_snapshots)
                    .await;
                Ok::<(), anyhow::Error>(())
            },
        );
    }

    async fn cleanup_pending_deletions(
        &self,
        partition_id: PartitionId,
        cleanup_snapshots: Vec<SnapshotReference>,
    ) {
        let mut successfully_deleted = HashSet::new();

        for snapshot_ref in &cleanup_snapshots {
            if self
                .delete_snapshot_files(partition_id, snapshot_ref)
                .await
                .is_ok()
            {
                successfully_deleted.insert(snapshot_ref.snapshot_id);
            }
        }

        if !successfully_deleted.is_empty() {
            let _ = self
                .update_latest_post_cleanup(partition_id, successfully_deleted)
                .await;
        }
    }

    async fn delete_snapshot_files(
        &self,
        partition_id: PartitionId,
        snapshot_ref: &SnapshotReference,
    ) -> anyhow::Result<()> {
        let metadata_path = self
            .prefix
            .child(partition_id.to_string())
            .child(snapshot_ref.path.as_str())
            .child("metadata.json");

        let metadata = match self.object_store.get(&metadata_path).await {
            Ok(data) => {
                let bytes = data.bytes().await?;
                serde_json::from_slice::<PartitionSnapshotMetadata>(&bytes)?
            }
            Err(object_store::Error::NotFound { .. }) => {
                // already deleted, this is fine
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let mut failed_deletes = vec![];
        for path in metadata
            .files
            .iter()
            .map(|filename| {
                self.snapshot_file_path(&metadata, filename.name.trim_start_matches("/"))
            })
            .chain(std::iter::once(metadata_path))
        {
            if let Err(err) = self.object_store.delete(&path).await
                && !matches!(err, object_store::Error::NotFound { .. })
            {
                warn!(%path, "Failed deleting snapshot object: {err}");
                failed_deletes.push(err);
            }
        }

        if failed_deletes.is_empty() {
            Ok(())
        } else {
            warn!(
                %partition_id,
                snapshot_id = %snapshot_ref.snapshot_id,
                "Failed to clean up old snapshot; repeated failures will impact the ability to create new snapshots"
            );
            Err(anyhow!(
                "Could not fully delete snapshot {}",
                snapshot_ref.snapshot_id
            ))
        }
    }

    pub(crate) async fn update_latest_post_cleanup(
        &self,
        partition_id: PartitionId,
        successfully_deleted: HashSet<SnapshotId>,
    ) -> anyhow::Result<()> {
        let latest_path = self.latest_snapshot_pointer_path(partition_id);

        for _ in 0..OBJECT_STORE_CAS_RETRIES {
            let maybe_stored = match self.object_store.get(&latest_path).await {
                Ok(result) => {
                    let version = UpdateVersion {
                        e_tag: result.meta.e_tag.clone(),
                        version: result.meta.version.clone(),
                    };
                    match result.bytes().await {
                        Ok(bytes) => match serde_json::from_slice::<LatestSnapshot>(&bytes) {
                            Ok(latest) => Some((latest, version)),
                            Err(e) => {
                                error!(
                                    %partition_id,
                                    error = %e,
                                    "Failed to deserialize latest snapshot metadata during cleanup! Treating as non-existent."
                                );
                                None
                            }
                        },
                        Err(e) => {
                            debug!(
                                %partition_id,
                                error = %e,
                                "Failed to read bytes for latest snapshot metadata during cleanup"
                            );
                            None
                        }
                    }
                }
                Err(e) => {
                    debug!(
                        %partition_id,
                        error = %e,
                        "Failed to fetch latest snapshot metadata during cleanup"
                    );
                    None
                }
            };

            let Some((mut latest, version)) = maybe_stored else {
                warn!(
                    %partition_id,
                    "Unable to read latest snapshot metadata after cleanup",
                );
                return Ok(());
            };

            // We need V2 or newer format for tracking retained snapshots
            if latest.version == LatestSnapshotVersion::V1 {
                return Ok(());
            }

            latest
                .pending_deletions
                .retain(|pending| !successfully_deleted.contains(&pending.snapshot_id));

            let latest_payload = PutPayload::from(serde_json::to_string_pretty(&latest)?);

            let use_conditional_update = !matches!(self.destination.scheme(), "file");
            let conditions = PutOptions {
                mode: match use_conditional_update {
                    true => PutMode::Update(version),
                    false => PutMode::Overwrite,
                },
                ..PutOptions::default()
            };

            match self
                .object_store
                .put_opts(&latest_path, latest_payload, conditions)
                .await
            {
                Ok(_) => {
                    debug!(
                        partition_id = %partition_id,
                        removed_count = successfully_deleted.len(),
                        "Successfully removed snapshots from pending deletions"
                    );
                    return Ok(());
                }
                Err(object_store::Error::Precondition { .. }) => {
                    // Retry on CAS conflict
                    continue;
                }
                Err(e) => {
                    warn!(
                        partition_id = %partition_id,
                        error = %e,
                        "Failed to update pending deletions list"
                    );
                    return Err(e.into());
                }
            }
        }

        warn!(
            partition_id = %partition_id,
            "Failed to clean up snapshots pending deletion, will resume on next snapshot upload"
        );
        anyhow::bail!("Failed to clean up snapshots pending deletion")
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
        let latest_path = self.latest_snapshot_pointer_path(partition_id);

        let latest = match self.object_store.get(&latest_path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                debug!("Latest snapshot data not found in repository");
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
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
            Err(e) => return Err(e.into()),
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
            let filename = file.name.trim_start_matches("/");
            let expected_size = file.size;
            let key = self
                .prefix
                .child(partition_id.to_string())
                .child(latest.path.as_str())
                .child(filename);
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
    pub async fn get_latest_snapshot_status(
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
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!(
                    "couldn't fetch '{latest_path}' from snapshot repository"
                )));
            }
        };

        let latest: LatestSnapshot = serde_json::from_slice(&latest.bytes().await?)?;
        debug!(partition_id = %partition_id, snapshot_id = %latest.snapshot_id, "Latest snapshot metadata: {:?}", latest);

        Ok(Some(PartitionSnapshotStatus::from(&latest)))
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

                Metadata::with_current(|m| {
                    let nodes_config = m.nodes_config_ref();
                    let fingerprint = nodes_config.cluster_fingerprint();
                    let cluster_name = nodes_config.cluster_name();
                    latest.validate(cluster_name, fingerprint)?;
                    snapshot.validate(cluster_name, fingerprint)?;
                    anyhow::Ok(())
                })?;

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
    use std::path::PathBuf;
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

    use crate::snapshots::repository::LatestSnapshotVersion;

    use super::{LatestSnapshot, SnapshotReference, SnapshotRepository, UniqueSnapshotKey};
    use super::{PartitionSnapshotMetadata, SnapshotFormatVersion};

    #[restate_core::test]
    async fn test_overwrite_unparsable_latest() -> anyhow::Result<()> {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;

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
        let repository = SnapshotRepository::new_from_config(&opts, TempDir::new().unwrap().keep())
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
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;

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

        let repository = SnapshotRepository::new_from_config(&opts, TempDir::new().unwrap().keep())
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
            cluster_fingerprint: Some(Metadata::with_current(|m| {
                m.nodes_config_ref().cluster_fingerprint()
            })),
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
        }
    }

    #[restate_core::test]
    async fn test_snapshot_retention_v2() -> anyhow::Result<()> {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            experimental_num_retained: Some(std::num::NonZeroU8::new(3).unwrap()),
            ..SnapshotsOptions::default()
        };

        let repository = SnapshotRepository::new_from_config(&opts, TempDir::new().unwrap().keep())
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

            repository.put(&snapshot, source_dir).await?;
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
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts_v1 = SnapshotsOptions {
            destination: Some(destination.clone()),
            experimental_num_retained: None,
            ..SnapshotsOptions::default()
        };

        let repository_v1 =
            SnapshotRepository::new_from_config(&opts_v1, TempDir::new().unwrap().keep())
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

        repository_v1.put(&snapshot_v1, source_dir.clone()).await?;

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

        let repository_v2 =
            SnapshotRepository::new_from_config(&opts_v2, TempDir::new().unwrap().keep())
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

        repository_v2.put(&snapshot_v2, source_dir_2).await?;

        let latest_data = object_store.get(&latest_path).await?;
        let latest: LatestSnapshot = serde_json::from_slice(&latest_data.bytes().await?)?;
        assert_eq!(latest.version, LatestSnapshotVersion::V2);
        assert_eq!(latest.retained_snapshots.len(), 2);

        Ok(())
    }

    #[restate_core::test]
    async fn test_archived_lsn_v2() -> anyhow::Result<()> {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;

        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            experimental_num_retained: Some(std::num::NonZeroU8::new(3).unwrap()),
            ..SnapshotsOptions::default()
        };

        let repository = SnapshotRepository::new_from_config(&opts, TempDir::new().unwrap().keep())
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

            repository.put(&snapshot, source_dir).await?;
        }

        let status = repository
            .get_latest_snapshot_status(PartitionId::MIN)
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
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;

        // Upload snapshots with cleanup disabled, to check pending_deletions
        let snapshots_destination = TempDir::new()?;
        let destination = Url::from_file_path(snapshots_destination.path())
            .unwrap()
            .to_string();

        let mut opts = SnapshotsOptions {
            destination: Some(destination.clone()),
            experimental_num_retained: Some(std::num::NonZeroU8::new(3).unwrap()),
            enable_cleanup: false,
            ..SnapshotsOptions::default()
        };

        let repository = SnapshotRepository::new_from_config(&opts, TempDir::new().unwrap().keep())
            .await?
            .unwrap();

        let mut all_snapshot_paths = Vec::new();
        for i in 1..=4 {
            let (snapshot, source_dir) =
                mock_snapshot(format!("data-{}", i).as_bytes(), Lsn::new(100 * i)).await?;
            all_snapshot_paths.push(SnapshotReference::from_metadata(&snapshot).path);
            repository.put(&snapshot, source_dir).await?;
        }

        let latest_path = repository.latest_snapshot_pointer_path(PartitionId::MIN);
        let partition_prefix = repository.partition_snapshots_prefix(PartitionId::MIN);
        let result = repository.object_store.get(&latest_path).await?;
        let mut latest: LatestSnapshot = serde_json::from_slice(&result.bytes().await?)?;
        assert_eq!(latest.pending_deletions.len(), 1);
        assert_eq!(latest.retained_snapshots.len(), 3);

        // Enable cleanup, upload another snapshot to trigger spawning a cleanup task
        opts.enable_cleanup = true;
        let repository = SnapshotRepository::new_from_config(&opts, TempDir::new().unwrap().keep())
            .await?
            .unwrap();

        let (snapshot, source_dir) = mock_snapshot("data-5".as_bytes(), Lsn::new(500)).await?;
        all_snapshot_paths.push(SnapshotReference::from_metadata(&snapshot).path);
        repository.put(&snapshot, source_dir).await?;

        // Allow the cleanup task to run, check that snapshots were cleared
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let result = repository.object_store.get(&latest_path).await?;
            latest = serde_json::from_slice(&result.bytes().await?)?;
            if latest.pending_deletions.is_empty() {
                break;
            }
        }

        assert!(latest.pending_deletions.is_empty());
        assert_eq!(latest.retained_snapshots.len(), 3);
        assert_eq!(
            latest
                .retained_snapshots
                .iter()
                .map(|s| s.min_applied_lsn)
                .collect::<Vec<_>>(),
            vec![Lsn::new(500), Lsn::new(400), Lsn::new(300)]
        );

        let full_list: Vec<_> = repository
            .object_store
            .list(Some(&partition_prefix))
            .try_collect()
            .await?;
        let repository_snapshot_paths: HashSet<_> = full_list
            .iter()
            .filter_map(|obj| {
                // path should be in form <partition>/lsn_xxx-snap_yyy/data.sst
                let path_str = obj.location.as_ref();
                path_str.split('/').find(|s| s.starts_with("lsn_"))
            })
            .filter(|p| p.starts_with("lsn_"))
            .collect();
        let retained_snapshot_paths: HashSet<_> = latest
            .retained_snapshots
            .iter()
            .map(|s| s.path.as_str())
            .collect();

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

        // Create a V2 latest snapshot with multiple retained snapshots
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
                    min_applied_lsn: Lsn::new(1342), // Oldest by LSN (last index)
                    created_at: Timestamp::from_second(0).unwrap(),
                    path: "snap3".to_string(),
                },
                // we intentionally re-order some items within retained - should not influence archived LSN
                SnapshotReference {
                    snapshot_id: SnapshotId::new(),
                    min_applied_lsn: Lsn::new(2839),
                    created_at: Timestamp::from_second(1).unwrap(),
                    path: "snap2".to_string(),
                },
            ],
            pending_deletions: vec![],
        };

        let status = PartitionSnapshotStatus::from(&latest);

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
