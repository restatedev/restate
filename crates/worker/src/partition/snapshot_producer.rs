// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::bail;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use tempfile::NamedTempFile;
use tracing::{debug, info, warn};
use url::Url;

use restate_partition_store::snapshots::{PartitionSnapshotMetadata, SnapshotFormatVersion};
use restate_partition_store::PartitionStore;
use restate_storage_api::fsm_table::{FsmTable, ReadOnlyFsmTable};
use restate_storage_api::Transaction;
use restate_types::identifiers::SnapshotId;

/// Encapsulates producing a Restate partition snapshot out of a partition store.
pub struct SnapshotProducer {}

pub struct SnapshotSource {
    pub cluster_name: String,
    pub node_name: String,
}

impl SnapshotProducer {
    pub async fn create(
        snapshot_source: SnapshotSource,
        mut partition_store: PartitionStore,
        partition_snapshots_path: PathBuf,
    ) -> anyhow::Result<PartitionSnapshotMetadata> {
        if let Err(e) = tokio::fs::create_dir_all(&partition_snapshots_path).await {
            warn!(
                path = ?partition_snapshots_path,
                error = ?e,
                "Failed to create partition snapshot directory"
            );
            bail!("Failed to create partition snapshot directory: {:?}", e);
        }

        // The snapshot directory structure is: <base_path>/<partition_id>/<snapshot_id>/*
        let snapshot_id = SnapshotId::new();
        let snapshot_path = partition_snapshots_path.join(snapshot_id.to_string());
        debug!(%snapshot_id, ?snapshot_path, "Creating partition snapshot directory");
        let snapshot = partition_store
            .create_snapshot(snapshot_path.clone())
            .await?;

        let snapshot_meta = PartitionSnapshotMetadata {
            version: SnapshotFormatVersion::V1,
            cluster_name: snapshot_source.cluster_name,
            node_name: snapshot_source.node_name,
            partition_id: partition_store.partition_id(),
            created_at: humantime::Timestamp::from(SystemTime::now()),
            snapshot_id,
            key_range: partition_store.partition_key_range().clone(),
            min_applied_lsn: snapshot.min_applied_lsn,
            db_comparator_name: snapshot.db_comparator_name.clone(),
            files: snapshot.files.clone(),
        };
        let metadata_json = serde_json::to_string_pretty(&snapshot_meta)?;

        let snapshot_lsn = snapshot_meta.min_applied_lsn;
        let metadata_path = snapshot_path.join("metadata.json");
        tokio::fs::write(metadata_path.clone(), metadata_json).await?;
        info!(
            lsn = %snapshot_lsn,
            metadata = ?metadata_path,
            "Partition snapshot written"
        );

        let aws_config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
        let credentials_provider = DefaultCredentialsChain::builder().build().await;
        let creds = credentials_provider.provide_credentials().await?;

        let url = Url::parse("s3://pavel-restate-snapshots-test/test-cluster-snapshots/")?;
        // let url = Url::parse("file:///Users/pavel/restate/restate/snapshots")?;
        // let (store, path) = object_store::parse_url(&url)?;

        let mut store = AmazonS3Builder::new()
            .with_url(url.clone())
            .with_region(aws_config.region().expect("region is set").to_string())
            .with_access_key_id(creds.access_key_id())
            .with_secret_access_key(creds.secret_access_key());

        if let Some(token) = creds.session_token() {
            store = store.with_token(token);
        }
        let store = store.build()?;

        let inverted_sort_key = format!("{:016x}", u64::MAX - snapshot_lsn.as_u64());

        let mut tarball =
            tar::Builder::new(NamedTempFile::new_in(partition_snapshots_path.as_path())?);
        tarball.append_dir_all(".", &snapshot_path.as_os_str())?;
        tarball.finish()?;
        debug!("Created snapshot: {:?}", tarball.get_ref());

        // The snapshot data / metadata key format is: [<base_prefix>/]<partition_id>/<sort_key>/<snapshot_id>_<lsn>.tar
        let snapshot_key = match url.path() {
            "" | "/" => format!(
                "{partition_id}/{sk}/{snapshot_id}_{lsn}.tar",
                partition_id = partition_store.partition_id(),
                sk = inverted_sort_key,
                lsn = snapshot_lsn,
            ),
            prefix => format!(
                "{trimmed_prefix}/{partition_id}/{sk}/{snapshot_id}_{lsn}.tar",
                trimmed_prefix = prefix.trim_start_matches('/').trim_end_matches('/'),
                partition_id = partition_store.partition_id(),
                sk = inverted_sort_key,
                lsn = snapshot_lsn,
            ),
        };
        let data = tokio::fs::read(tarball.get_ref().path()).await?;
        let upload = store
            .put(&Path::from(snapshot_key.clone()), PutPayload::from(data))
            .await?;
        debug!(
            %snapshot_id,
            "Successfully uploaded snapshot archive to: {} ({:?})",
            snapshot_key,
            upload.e_tag
        );

        let previous_archived_snapshot_lsn = partition_store.get_archived_lsn().await?;
        let mut tx = partition_store.transaction();
        tx.put_archived_lsn(snapshot_lsn).await;
        tx.commit().await?;

        debug!(
            %snapshot_id,
            previous_archived_lsn = ?previous_archived_snapshot_lsn,
            updated_archived_lsn = ?snapshot_lsn,
            "Updated persisted archived snapshot LSN"
        );

        Ok(snapshot_meta)
    }
}
