// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, PutPayload};
use tempfile::NamedTempFile;
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::{debug, trace, trace_span, warn};
use url::Url;

use restate_core::task_center;
use restate_partition_store::snapshots::{LocalPartitionSnapshot, PartitionSnapshotMetadata};
use restate_types::config::SnapshotsOptions;
use restate_types::identifiers::PartitionId;

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
            && destination.query().is_none()
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
            staging_path: base_dir.clone().join("snapshot-staging"),
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

        // The snapshot data / metadata key format is: [<base_prefix>/]<partition_id>/<sort_key>_<lsn>_<snapshot_id>.tar
        let snapshot_key = match self.prefix.as_str() {
            "" | "/" => format!(
                "{partition_id}/{sk}_{lsn}_{snapshot_id}.tar",
                sk = inverted_sort_key,
                lsn = metadata.min_applied_lsn,
            ),
            prefix => format!(
                "{trimmed_prefix}/{partition_id}/{sk}_{lsn}_{snapshot_id}.tar",
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
            .put(
                &object_store::path::Path::from(snapshot_key.clone()),
                payload,
            )
            .await
            .context("Failed to put snapshot in repository")?;

        debug!(
            %snapshot_id,
            etag = upload.e_tag.unwrap_or_default(),
            "Successfully published snapshot to repository as: {}",
            snapshot_key,
        );
        Ok(())
    }

    pub(crate) async fn find_latest(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Option<LocalPartitionSnapshot>> {
        let list_prefix = match self.prefix.as_str() {
            "" | "/" => format!("{}/", partition_id),
            prefix => format!("{}/{}/", prefix, partition_id),
        };
        let list_prefix = object_store::path::Path::from(list_prefix.as_str());

        let list = self
            .object_store
            .list_with_delimiter(Some(&list_prefix))
            .await?;

        let latest = list.objects.first();

        let Some(snapshot_entry) = latest else {
            debug!(%partition_id, "No snapshots found in the snapshots repository");
            return Ok(None);
        };

        let snapshot_object = self
            .object_store
            .get(&snapshot_entry.location)
            .await
            .context("Failed to get snapshot from repository")?;

        // construct the bridge in a Tokio context, before moving to blocking pool
        let snapshot_reader = SyncIoBridge::new(StreamReader::new(snapshot_object.into_stream()));

        let snapshot_name = snapshot_entry.location.filename().expect("has a name");
        let snapshot_base_path = &self.staging_path.join(snapshot_name);
        tokio::fs::create_dir_all(snapshot_base_path).await?;

        let snapshot_dir = snapshot_base_path.clone();
        trace!(%partition_id, "Unpacking snapshot {} to: {:?}", snapshot_entry.location, snapshot_dir);
        task_center()
            .spawn_blocking_fn_unmanaged("unpack-snapshot", Some(partition_id), move || {
                let mut tarball = tar::Archive::new(snapshot_reader);
                for file in tarball.entries()? {
                    let mut file = file?;
                    trace!("Unpacking snapshot file: {:?}", file.header().path()?);
                    file.unpack_in(&snapshot_dir)?;
                }
                Ok::<(), anyhow::Error>(())
            })
            .await??;

        let metadata = tokio::fs::read(snapshot_base_path.join("metadata.json")).await?;
        let mut metadata: PartitionSnapshotMetadata = serde_json::from_slice(metadata.as_slice())?;

        // Patch the file paths in the snapshot metadata to point to the correct staging directory on the local node.
        let snapshot_base_path = snapshot_base_path
            .to_path_buf()
            .into_os_string()
            .into_string()
            .map_err(|path| anyhow::anyhow!("Invalid string: {:?}", path))?
            .trim_end_matches('/')
            .to_owned();
        metadata
            .files
            .iter_mut()
            .for_each(|f| f.directory = snapshot_base_path.clone());
        trace!(%partition_id, "Restoring from snapshot metadata: {:?}", metadata);

        Ok(Some(LocalPartitionSnapshot {
            base_dir: self.staging_path.clone(),
            min_applied_lsn: metadata.min_applied_lsn,
            db_comparator_name: metadata.db_comparator_name,
            files: metadata.files,
        }))
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
