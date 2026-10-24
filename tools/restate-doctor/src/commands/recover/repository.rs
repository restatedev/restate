// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path};
use std::sync::Arc;

use anyhow::{Context, ensure};
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use url::Url;

use restate_object_store_util::create_object_store_client;
use restate_partition_store::snapshots::{
    LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotDir, SnapshotFormatVersion,
};
use restate_types::config::{ObjectStoreOptions, SnapshotsOptions};
use restate_types::identifiers::PartitionId;

/// Pins the exact metadata read during planning, rather than following a moving latest pointer
/// during execution. No writes (including retention pins) are made to the source repository.
#[derive(Clone, Debug, Serialize)]
pub(super) struct Artifact {
    pub metadata_path: String,
    pub metadata: PartitionSnapshotMetadata,
}

pub(super) struct Repository {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

impl Repository {
    pub async fn new(url: Url, options: &ObjectStoreOptions) -> anyhow::Result<Self> {
        let store = if url.scheme() == "file" {
            // Reading a downloaded repository is useful offline. Only this tool enables local
            // file input; normal server snapshot destinations remain unchanged.
            object_store::parse_url(&url)?.0.into()
        } else {
            create_object_store_client(
                url.clone(),
                options,
                &SnapshotsOptions::default().object_store_retry_policy,
            )
            .await?
        };
        Ok(Self {
            store,
            prefix: ObjectPath::from(url.path()),
        })
    }

    pub async fn discover(
        &self,
        excluded: &HashSet<PartitionId>,
    ) -> anyhow::Result<BTreeMap<PartitionId, Artifact>> {
        // List immutable metadata, not only latest.json: infra loss may have left the pointer
        // missing while recoverable SSTs and metadata still exist. Partial uploads fail on import;
        // an operator can explicitly select an older generation or skip the partition.
        let mut objects = self.store.list(Some(&self.prefix));
        let mut artifacts: BTreeMap<PartitionId, Artifact> = BTreeMap::new();
        while let Some(object) = objects.try_next().await? {
            if object.location.filename() != Some("metadata.json") {
                continue;
            }
            let relative = object
                .location
                .prefix_match(&self.prefix)
                .context("listing returned an object outside the repository")?
                .collect::<Vec<_>>();
            ensure!(
                relative.len() == 3,
                "unexpected snapshot metadata layout at {}",
                object.location
            );
            let directory_id = PartitionId::new_unchecked(relative[0].as_ref().parse::<u16>()?);
            if excluded.contains(&directory_id) {
                continue;
            }
            let artifact = self.read(object.location).await?;
            let id = artifact.metadata.partition_id;
            ensure!(
                id == directory_id,
                "snapshot partition ID does not match its repository directory"
            );
            let replace = artifacts.get(&id).is_none_or(|old| {
                (
                    artifact.metadata.min_applied_lsn,
                    artifact.metadata.created_at,
                    artifact.metadata.snapshot_id,
                ) > (
                    old.metadata.min_applied_lsn,
                    old.metadata.created_at,
                    old.metadata.snapshot_id,
                )
            });
            if replace {
                artifacts.insert(id, artifact);
            }
        }
        Ok(artifacts)
    }

    pub async fn select(&self, selection: &str) -> anyhow::Result<(PartitionId, Artifact)> {
        let (id, path) = selection
            .split_once('=')
            .context("--snapshot expects PARTITION_ID=RELATIVE_METADATA_PATH")?;
        let id = PartitionId::new_unchecked(id.parse::<u16>()?);
        ensure!(
            safe_relative_path(path),
            "snapshot metadata path must be relative and contain no traversal"
        );
        // join() takes ONE segment and escapes slashes. Preserve the validated relative path
        // instead of requesting a key with literal encoded slashes.
        let metadata_path = path
            .split('/')
            .fold(self.prefix.clone(), |prefix, segment| prefix.join(segment));
        let artifact = self.read(metadata_path).await?;
        ensure!(
            artifact.metadata.partition_id == id,
            "selected snapshot belongs to another partition"
        );
        Ok((id, artifact))
    }

    async fn read(&self, path: ObjectPath) -> anyhow::Result<Artifact> {
        let bytes = self.store.get(&path).await?.bytes().await?;
        let metadata: PartitionSnapshotMetadata = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid snapshot metadata at {path}"))?;
        // Exhaustive: a new format variant must explicitly opt into this offline importer.
        // Unknown versions are already rejected by deserialization.
        match metadata.version {
            SnapshotFormatVersion::V1 => {}
        }
        ensure!(
            !metadata.files.is_empty(),
            "snapshot at {path} contains no SST files"
        );
        let mut names = std::collections::HashSet::new();
        for file in &metadata.files {
            let name = sst_name(&file.name)?;
            ensure!(
                names.insert(name),
                "snapshot at {path} lists the same SST more than once"
            );
        }
        Ok(Artifact {
            metadata_path: path.to_string(),
            metadata,
        })
    }

    pub async fn download(
        &self,
        artifact: &Artifact,
        staging: &Path,
    ) -> anyhow::Result<LocalPartitionSnapshot> {
        let directory = tempfile::tempdir_in(staging)?;
        let guard = SnapshotDir::new(directory.keep());
        let metadata = &artifact.metadata;
        let parent = artifact
            .metadata_path
            .rsplit_once('/')
            .map(|(p, _)| p)
            .unwrap_or("");
        let mut files = metadata.files.clone();
        // Bound memory independently of snapshot size. Download sequentially for this emergency
        // tool; concurrency can be added without changing the recovery format or server.
        for file in &mut files {
            let name = sst_name(&file.name)?;
            let key = ObjectPath::from(parent).join(name);
            let mut chunks = self.store.get(&key).await?.into_stream();
            let local_path = guard.path().join(name);
            let mut output = tokio::fs::File::create_new(&local_path).await?;
            let mut size: usize = 0;
            while let Some(chunk) = chunks.try_next().await? {
                size = size
                    .checked_add(chunk.len())
                    .context("SST file size overflow")?;
                ensure!(
                    size <= file.size,
                    "snapshot SST {key} exceeds its declared size"
                );
                output.write_all(&chunk).await?;
            }
            ensure!(
                size == file.size,
                "snapshot SST {key} has size {size}, expected {}",
                file.size
            );
            output.sync_all().await?;
            guard
                .path()
                .to_str()
                .context("recovery path must be UTF-8")?
                .clone_into(&mut file.directory);
        }
        Ok(LocalPartitionSnapshot {
            base_dir: guard,
            log_id: metadata.log_id,
            min_applied_lsn: metadata.min_applied_lsn,
            db_comparator_name: metadata.db_comparator_name.clone(),
            files,
            key_range: metadata.key_range,
        })
    }
}

fn safe_relative_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\\')
        && !path.contains('\0')
        && path
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
        && Path::new(path)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}

fn sst_name(name: &str) -> anyhow::Result<&str> {
    // RocksDB checkpoints conventionally serialize names with one leading slash.
    let name = name.strip_prefix('/').unwrap_or(name);
    ensure!(
        safe_relative_path(name) && !name.contains('/') && name.ends_with(".sst"),
        "unsafe snapshot SST filename: {name}"
    );
    Ok(name)
}
