// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use tracing::{info, instrument};

use restate_core::{MetadataKind, MetadataWriter};
use restate_metadata_store::MetadataStoreClient;
use restate_types::config::Configuration;
use restate_types::logs::builder::BuilderError;
use restate_types::logs::metadata::{LogletParams, Logs, ProviderKind, SegmentIndex};
use restate_types::logs::{LogId, Lsn};
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;
use restate_types::Version;

use crate::error::AdminError;
use crate::{Bifrost, Error, Result, TailState};

/// Bifrost's Admin API
#[derive(Clone, Copy)]
pub struct BifrostAdmin<'a> {
    bifrost: &'a Bifrost,
    metadata_writer: &'a MetadataWriter,
    metadata_store_client: &'a MetadataStoreClient,
}

impl<'a> AsRef<Bifrost> for BifrostAdmin<'a> {
    fn as_ref(&self) -> &Bifrost {
        self.bifrost
    }
}

impl<'a> Deref for BifrostAdmin<'a> {
    type Target = Bifrost;
    fn deref(&self) -> &Self::Target {
        self.bifrost
    }
}

impl<'a> BifrostAdmin<'a> {
    pub fn new(
        bifrost: &'a Bifrost,
        metadata_writer: &'a MetadataWriter,
        metadata_store_client: &'a MetadataStoreClient,
    ) -> Self {
        Self {
            bifrost,
            metadata_writer,
            metadata_store_client,
        }
    }
    /// Trim the log prefix up to and including the `trim_point`.
    /// Set `trim_point` to the value returned from `find_tail()` or `Lsn::MAX` to
    /// trim all records of the log.
    #[instrument(level = "debug", skip(self), err)]
    pub async fn trim(&self, log_id: LogId, trim_point: Lsn) -> Result<()> {
        self.bifrost.inner.fail_if_shutting_down()?;
        self.bifrost.inner.trim(log_id, trim_point).await
    }

    /// Seals a loglet under a set of conditions.
    ///
    /// The loglet will be sealed if and only if the following is true:
    ///   - log metadata is at least at version `min_version`. If not, this will wait for this
    ///     version to be synced (set to `Version::MIN` to ignore this step)
    ///   - if segment_index is set, the tail loglet must match segment_index.
    ///
    /// This will continue to retry sealing for seal retryable errors automatically.
    #[instrument(level = "debug", skip(self), err)]
    pub async fn seal_and_extend_chain(
        &self,
        log_id: LogId,
        segment_index: Option<SegmentIndex>,
        min_version: Version,
        provider: ProviderKind,
        params: LogletParams,
    ) -> Result<()> {
        self.bifrost.inner.fail_if_shutting_down()?;
        let _ = self
            .bifrost
            .inner
            .metadata
            .wait_for_version(MetadataKind::Logs, min_version)
            .await?;

        let segment_index = segment_index
            .or_else(|| {
                self.bifrost
                    .inner
                    .metadata
                    .logs()
                    .chain(&log_id)
                    .map(|c| c.tail_index())
            })
            .ok_or(Error::UnknownLogId(log_id))?;

        let tail = self.seal(log_id, segment_index).await?;
        assert!(tail.is_sealed());

        self.add_segment_with_params(log_id, segment_index, tail.offset(), provider, params)
            .await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    pub(crate) async fn seal(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
    ) -> Result<TailState> {
        self.bifrost.inner.fail_if_shutting_down()?;
        // first find the tail segment for this log.
        let loglet = self.bifrost.inner.writeable_loglet(log_id).await?;

        if segment_index != loglet.segment_index() {
            // Not the same segment. Bail!
            return Err(AdminError::SegmentMismatch {
                expected: segment_index,
                found: loglet.segment_index(),
            }
            .into());
        }

        while let Err(e) = loglet.seal().await {
            match e {
                crate::loglet::OperationError::Shutdown(e) => return Err(e.into()),
                crate::loglet::OperationError::Other(e) if e.retryable() => {
                    // sleep and retry later.
                    info!(
                        log_id = %log_id,
                        segment = %segment_index,
                        loglet = ?loglet,
                        ?e,
                        "Seal operation failed. Retrying later..."
                    );
                    tokio::time::sleep(Configuration::pinned().bifrost.seal_retry_interval.into())
                        .await;
                }
                crate::loglet::OperationError::Other(e) => {
                    // give up.
                    return Err(Error::LogletError(e));
                }
            }
        }

        Ok(loglet.find_tail().await?)
    }

    /// Adds a segment to the end of the chain
    ///
    /// The loglet must be sealed first. This operations assumes that the loglet with
    /// `last_segment_index` has been sealed prior to this call.
    #[instrument(level = "debug", skip(self), err)]
    async fn add_segment_with_params(
        &self,
        log_id: LogId,
        last_segment_index: SegmentIndex,
        base_lsn: Lsn,
        provider: ProviderKind,
        params: LogletParams,
    ) -> Result<()> {
        self.bifrost.inner.fail_if_shutting_down()?;
        let logs = self
            .metadata_store_client
            .read_modify_write(BIFROST_CONFIG_KEY.clone(), move |logs: Option<Logs>| {
                let logs = logs.ok_or(Error::UnknownLogId(log_id))?;

                let mut builder = logs.into_builder();
                let mut chain_builder =
                    builder.chain(&log_id).ok_or(Error::UnknownLogId(log_id))?;

                if chain_builder.tail().index() != last_segment_index {
                    // tail is not what we expected.
                    return Err(Error::from(AdminError::SegmentMismatch {
                        expected: last_segment_index,
                        found: chain_builder.tail().index(),
                    }));
                }

                match chain_builder.append_segment(base_lsn, provider, params.clone()) {
                    Err(e) => match e {
                        BuilderError::SegmentConflict(lsn) => {
                            Err(Error::from(AdminError::SegmentConflict(lsn)))
                        }
                        _ => unreachable!("the log must exist at this point"),
                    },
                    Ok(_) => Ok(builder.build()),
                }
            })
            .await
            .map_err(|e| e.transpose())?;

        self.metadata_writer.update(logs).await?;
        Ok(())
    }
}
