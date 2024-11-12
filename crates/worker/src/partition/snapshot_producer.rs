// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use std::time::SystemTime;

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, PutPayload};
use tempfile::NamedTempFile;
use tracing::{debug, error, info, trace, trace_span, warn};
use url::Url;

use restate_core::task_center;
use restate_partition_store::snapshots::{PartitionSnapshotMetadata, SnapshotFormatVersion};
use restate_partition_store::PartitionStore;
use restate_storage_api::fsm_table::{FsmTable, ReadOnlyFsmTable};
use restate_storage_api::Transaction;
use restate_types::config::{Configuration, SnapshotsOptions};
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::live::Live;

/// Provides read and write access to the long-term partition snapshot storage destination.
#[derive(Clone)]
pub struct SnapshotRepository {
    object_store: Arc<dyn ObjectStore>,
    destination: Url,
    prefix: String,
    staging_path: PathBuf,
}

impl SnapshotRepository {
    pub async fn create(
        base_dir: PathBuf,
        snapshots_options: &SnapshotsOptions,
    ) -> anyhow::Result<SnapshotRepository> {
        let destination = snapshots_options
            .destination
            .as_ref()
            .map(|s| Ok(s.clone()))
            .unwrap_or_else(|| {
                base_dir
                    .join("pp-snapshots")
                    .into_os_string()
                    .into_string()
                    .map(|path| format!("file://{path}"))
            })
            .map_err(|e| anyhow!("Unable to convert path to string: {:?}", e))?;
        let destination =
            Url::parse(&destination).context("Failed parsing snapshot repository URL")?;

        // AWS-specific ergonomics optimization: without explicit configuration, we use the AWS SDK
        // detected region and default credentials provider. This makes object_store behave
        // similarly to the Lambda invoker, respecting AWS_PROFILE and available session creds.
        let object_store: Arc<dyn ObjectStore> = if destination.scheme() == "s3"
            && !destination.query().is_some()
            && snapshots_options.additional_options.is_empty()
        {
            let aws_region = aws_config::load_defaults(BehaviorVersion::v2024_03_28())
                .await
                .region()
                .context("Unable to determine AWS region to use with S3")?
                .clone();

            let store = AmazonS3Builder::new()
                .with_url(destination.clone())
                .with_region(aws_region.to_string())
                .with_credentials(Arc::new(AwsSdkCredentialsProvider {
                    credentials_provider: DefaultCredentialsChain::builder().build().await,
                }))
                .build()?;

            Arc::new(store)
        } else {
            object_store::parse_url_opts(&destination, &snapshots_options.additional_options)?
                .0
                .into()
        };

        let prefix = destination.path().into();
        Ok(SnapshotRepository {
            object_store,
            destination,
            prefix,
            staging_path: base_dir.clone(),
        })
    }

    /// Write a partition snapshot to the snapshot repository.
    pub(crate) async fn put(
        &self,
        partition_id: PartitionId,
        metadata: &PartitionSnapshotMetadata,
        snapshot_path: &Path,
    ) -> anyhow::Result<()> {
        let snapshot_id = metadata.snapshot_id;
        let lsn = metadata.min_applied_lsn;

        debug!(
            %snapshot_id,
            partition_id = ?partition_id,
            %lsn,
            "Publishing partition snapshot to: {}",
            self.destination,
        );

        // All common object stores list objects in lexicographical order, with no option for
        // reverse order. We inject an explicit sort key into the snapshot prefix to make sure that
        // the latest snapshot is always first.
        let inverted_sort_key = format!("{:016x}", u64::MAX - lsn.as_u64());
        let key = format!(
            "{partition_id}/{sk}/{snapshot_id}_{lsn}.tar",
            sk = inverted_sort_key,
        );

        // The snapshot data / metadata key format is: [<base_prefix>/]<partition_id>/<sort_key>/<snapshot_id>_<lsn>.tar
        let snapshot_key = match self.prefix.as_str() {
            "" | "/" => format!(
                "{partition_id}/{sk}/{snapshot_id}_{lsn}.tar",
                sk = inverted_sort_key,
                lsn = metadata.min_applied_lsn,
            ),
            prefix => format!(
                "{trimmed_prefix}/{partition_id}/{sk}/{snapshot_id}_{lsn}.tar",
                trimmed_prefix = prefix.trim_start_matches('/').trim_end_matches('/'),
                sk = inverted_sort_key,
            ),
        };

        let snapshot_path = snapshot_path.to_owned();
        let staging_path = self.staging_path.clone();
        let packaging_task = task_center().spawn_blocking_unmanaged(
            "package-snapshot",
            Some(partition_id),
            async move {
                trace_span!("package-snapshot", %snapshot_id).in_scope(|| {
                    let mut tarball = tar::Builder::new(NamedTempFile::new_in(&staging_path)?);
                    debug!(
                        "Creating snapshot tarball of {:?} in: {:?}...",
                        &staging_path,
                        tarball.get_ref()
                    );
                    tarball.append_dir_all(".", &snapshot_path)?;
                    tarball.finish()?;
                    tarball.into_inner()
                })
            },
        );
        let tarball = packaging_task.await??;

        // todo(pavel): don't buffer the entire snapshot in memory!
        let payload = PutPayload::from(tokio::fs::read(tarball.path()).await?);

        let upload = self
            .object_store
            .put(&object_store::path::Path::from(snapshot_key), payload)
            .await
            .context("Failed to put snapshot in repository")?;

        debug!(
            %snapshot_id,
            etag = upload.e_tag.unwrap_or_default(),
            "Successfully published snapshot to repository as: {}",
            key,
        );
        Ok(())
    }
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
                warn!(error = ?e, "Failed to get AWS credentials from credentials provider");
                object_store::Error::Generic {
                    store: "snapshot repository store",
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

/// Encapsulates exporting and publishing partition snapshots.
#[derive(Clone)]
pub struct SnapshotProducer {
    snapshot_source: SnapshotSource,
    partition_store: PartitionStore,
    partition_snapshots_path: PathBuf,
    snapshot_repository: SnapshotRepository,
}

#[derive(Clone)]
pub struct SnapshotSource {
    pub cluster_name: String,
    pub node_name: String,
}

impl SnapshotProducer {
    pub async fn create(
        partition_store: PartitionStore,
        config: Live<Configuration>,
        snapshot_repository: SnapshotRepository,
    ) -> anyhow::Result<Self> {
        let config = config.pinned();
        let partition_id = partition_store.partition_id();

        Ok(SnapshotProducer {
            snapshot_source: SnapshotSource {
                cluster_name: config.common.cluster_name().into(),
                node_name: config.common.node_name().into(),
            },
            partition_store,
            snapshot_repository,
            partition_snapshots_path: config.worker.snapshots.snapshots_dir(partition_id),
        })
    }

    /// Exports a partition store snapshot and writes it to the snapshot repository.
    ///
    /// The final snapshot key will follow the structure:
    /// `[<prefix>/]<partition_id>/<sort_key>/<snapshot_id>_<lsn>.tar`.
    pub async fn create_snapshot(&mut self) -> anyhow::Result<PartitionSnapshotMetadata> {
        if let Err(e) = tokio::fs::create_dir_all(&self.partition_snapshots_path).await {
            warn!(
                path = ?self.partition_snapshots_path,
                error = ?e,
                "Failed to create partition snapshot directory"
            );
            bail!("Failed to create partition snapshot directory: {:?}", e);
        }

        let snapshot_id = SnapshotId::new();
        let snapshot_path = self.partition_snapshots_path.join(snapshot_id.to_string());

        trace!(%snapshot_id, "Creating partition snapshot export directory: {:?}", snapshot_path);
        let snapshot = self
            .partition_store
            .export_snapshot(snapshot_path.clone())
            .await
            .context("Export partition snapshot")?;

        let snapshot_metadata = PartitionSnapshotMetadata {
            version: SnapshotFormatVersion::V1,
            cluster_name: self.snapshot_source.cluster_name.clone(),
            node_name: self.snapshot_source.node_name.clone(),
            partition_id: self.partition_store.partition_id(),
            created_at: humantime::Timestamp::from(SystemTime::now()),
            snapshot_id,
            key_range: self.partition_store.partition_key_range().clone(),
            min_applied_lsn: snapshot.min_applied_lsn,
            db_comparator_name: snapshot.db_comparator_name.clone(),
            files: snapshot.files.clone(),
        };
        let metadata_json = serde_json::to_string_pretty(&snapshot_metadata)?;

        let snapshot_lsn = snapshot_metadata.min_applied_lsn;
        let metadata_path = snapshot_path.join("metadata.json");
        tokio::fs::write(metadata_path.clone(), metadata_json)
            .await
            .context("Writing snapshot metadata failed")?;

        self.snapshot_repository
            .put(
                self.partition_store.partition_id(),
                &snapshot_metadata,
                snapshot_path.as_path(),
            )
            .await
            .context("Snapshot repository upload failed")?;

        let previous_archived_snapshot_lsn = self.partition_store.get_archived_lsn().await?;
        let mut tx = self.partition_store.transaction();
        tx.put_archived_lsn(snapshot_lsn).await;
        tx.commit()
            .await
            .context("Updating archived snapshot LSN")?;
        trace!(
            %snapshot_id,
            previous_archived_lsn = ?previous_archived_snapshot_lsn,
            updated_archived_lsn = ?snapshot_lsn,
            "Updated persisted archived snapshot LSN"
        );

        let cleanup = tokio::fs::remove_dir_all(snapshot_path.clone()).await;
        match cleanup {
            Ok(_) => {
                debug!(%snapshot_id, "Cleaned up snapshot export directory: {:?}", snapshot_path);
            }
            Err(e) => {
                error!(%snapshot_id, "Failed to clean up snapshot directory: {}", e);
            }
        }

        info!(
            %snapshot_id,
            partition_id = ?self.partition_store.partition_id(),
            ?snapshot_lsn,
            "Successfully published partition snapshot"
        );
        Ok(snapshot_metadata)
    }
}
