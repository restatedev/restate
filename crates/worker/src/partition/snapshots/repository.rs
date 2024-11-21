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
use object_store::{MultipartUpload, ObjectStore, PutPayload};
use tokio::io::AsyncReadExt;
use tracing::{debug, trace};
use url::Url;

use restate_partition_store::snapshots::PartitionSnapshotMetadata;
use restate_types::config::SnapshotsOptions;

/// Provides read and write access to the long-term partition snapshot storage destination.
///
/// The repository wraps access to an object store "bucket" that contains snapshot metadata and data
/// optimised for efficient retrieval. The bucket layout is split into two top-level prefixes for
/// snapshot metadata and data respectively. While full snapshot archives contain all relevant
/// metadata, this split layout allows for efficient retrieval of only the metadata upfront. It also
/// enables us to evolve the data storage layout independently in the future.
///
/// A single top-level `latest.json` file is the only key which is repeatedly overwritten; all other
/// data is immutable until the pruning policy allows for deletion.
///
/// - `[<prefix>/]<partition_id>/latest.json` - latest snapshot metadata for the partition
/// - `[<prefix>/]<partition_id>/YYYY-MM-DD/{lsn}/metadata.json` - snapshot descriptor
/// - `[<prefix>/]<partition_id>/YYYY-MM-DD/{lsn}/*.sst` - data files (explicitly named in `metadata.json`)
#[derive(Clone)]
pub struct SnapshotRepository {
    object_store: Arc<dyn ObjectStore>,
    destination: Url,
    prefix: String,
}

impl SnapshotRepository {
    pub async fn create(
        base_dir: PathBuf,
        snapshots_options: &SnapshotsOptions,
    ) -> anyhow::Result<SnapshotRepository> {
        let destination = if let Some(ref destination) = snapshots_options.destination {
            destination.clone()
        } else {
            base_dir
                .join("pp-snapshots")
                .into_os_string()
                .into_string()
                .map(|path| format!("file://{path}"))
                .map_err(|e| anyhow!("Unable to convert path to string: {:?}", e))?
        };
        let destination =
            Url::parse(&destination).context("Failed parsing snapshot repository URL")?;

        // AWS-specific ergonomics optimization: without explicit configuration, we set up the AWS
        // SDK credentials provider so that the conventional environment variables and config
        // locations just work. This makes object_store behave similarly to the Lambda invoker.
        let object_store: Arc<dyn ObjectStore> = if destination.scheme() == "s3"
            && destination.query().is_none()
            && snapshots_options.additional_options.is_empty()
        {
            debug!("Using AWS SDK credentials provider");
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
            debug!("Using object_store credentials configuration");
            object_store::parse_url_opts(&destination, &snapshots_options.additional_options)?
                .0
                .into()
        };

        // prefix must be stripped of any leading slash and, unless zero-length, end in a single "/" character
        let prefix: String = destination.path().into();
        let prefix = match prefix.as_str() {
            "" | "/" => "".to_string(),
            prefix => format!("{}/", prefix.trim_start_matches('/').trim_end_matches('/')),
        };

        Ok(SnapshotRepository {
            object_store,
            destination,
            prefix,
        })
    }

    /// Write a partition snapshot to the snapshot repository.
    pub(crate) async fn put(
        &self,
        snapshot: &PartitionSnapshotMetadata,
        local_snapshot_path: PathBuf,
    ) -> anyhow::Result<()> {
        let partition_id = snapshot.partition_id;
        let lsn = snapshot.min_applied_lsn;

        debug!(
            %lsn,
            "Publishing partition snapshot to: {}",
            self.destination,
        );

        let snapshot_prefix = format!(
            "{prefix}{partition_id}/lsn_{lsn}",
            prefix = self.prefix,
            lsn = snapshot.min_applied_lsn,
        );

        debug!(
            "Uploading snapshot from {:?} to {}",
            local_snapshot_path.as_path(),
            snapshot_prefix
        );

        for file in &snapshot.files {
            let filename = file.name.trim_start_matches("/");
            let key = object_store::path::Path::from(format!(
                "{}/{}",
                snapshot_prefix.as_str(),
                filename
            ));
            let put_result = put_snapshot_object(
                local_snapshot_path.join(filename).as_path(),
                &key,
                &self.object_store,
            )
            .await?;
            debug!(
                etag = put_result.e_tag.unwrap_or_default(),
                ?key,
                "Put snapshot data file completed",
            );
        }

        // todo(pavel): don't write `metadata.json` to disk, serialize it from the struct directly.
        //  this gives us a chance to include data file checksums into it removes some IO
        let metadata_json_path = local_snapshot_path.join("metadata.json");
        let metadata_key =
            object_store::path::Path::from(format!("{}/metadata.json", snapshot_prefix.as_str()));
        let metadata_json_payload = PutPayload::from(tokio::fs::read(metadata_json_path).await?);
        let put_result = self
            .object_store
            .put(&metadata_key, metadata_json_payload)
            .await?;
        debug!(
            etag = put_result.e_tag.unwrap_or_default(),
            key = ?metadata_key,
            "Successfully published snapshot metadata",
        );

        // todo(pavel): (re)write latest.json pointer object

        tokio::fs::remove_dir_all(local_snapshot_path.as_path()).await?;
        trace!(
            "Removed local snapshot files: {}",
            local_snapshot_path.display()
        );

        Ok(())
    }
}

/// S3 and other stores require a certain minimum size for the parts of a multipart upload. It is an
/// API error to attempt a multipart put below this size, apart from the final segment.
const MULTIPART_UPLOAD_THRESHOLD_BYTES: usize = 5 * 1024 * 1024;

async fn put_snapshot_object(
    snapshot_path: &Path,
    key: &object_store::path::Path,
    object_store: &Arc<dyn ObjectStore>,
) -> anyhow::Result<object_store::PutResult> {
    let mut snapshot = tokio::fs::File::open(snapshot_path).await?;

    if snapshot.metadata().await?.len() < MULTIPART_UPLOAD_THRESHOLD_BYTES as u64 {
        let payload = PutPayload::from(tokio::fs::read(snapshot_path).await?);
        object_store.put(key, payload).await.map_err(|e| e.into())
    } else {
        let mut upload = object_store.put_multipart(key).await?;
        loop {
            let mut buf = vec![0; MULTIPART_UPLOAD_THRESHOLD_BYTES];
            let n = snapshot.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let part = PutPayload::from(buf);
            upload
                .put_part(part)
                .await
                .context("Failed to put snapshot part in repository")?;
            trace!("Uploaded chunk of {} bytes", n);
        }
        upload
            .complete()
            .await
            .context("Failed to put snapshot in repository")
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
