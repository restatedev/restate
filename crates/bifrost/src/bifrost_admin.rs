// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use tracing::{debug, instrument, warn};

use restate_core::metadata_store::retry_on_retryable_error;
use restate_core::{Metadata, MetadataKind};
use restate_types::Version;
use restate_types::config::Configuration;
use restate_types::logs::metadata::{Chain, LogletParams, Logs, ProviderKind, SegmentIndex};
use restate_types::logs::{LogId, Lsn, TailState};
use restate_types::metadata_store::keys::BIFROST_CONFIG_KEY;

use crate::bifrost::BifrostInner;
use crate::error::AdminError;
use crate::loglet::FindTailOptions;
use crate::loglet_wrapper::LogletWrapper;
use crate::{Error, Result};

/// Bifrost's Admin API
#[derive(Clone, Copy)]
pub struct BifrostAdmin<'a> {
    inner: &'a Arc<BifrostInner>,
}

#[derive(Debug)]
pub struct SealedSegment {
    pub segment_index: SegmentIndex,
    pub provider: ProviderKind,
    pub params: LogletParams,
    pub tail: TailState,
}

impl<'a> BifrostAdmin<'a> {
    pub(crate) fn new(inner: &'a Arc<BifrostInner>) -> Self {
        Self { inner }
    }
    /// Trim the log prefix up to and including the `trim_point`.
    /// Set `trim_point` to the value returned from `find_tail()` or `Lsn::MAX` to
    /// trim all records of the log.
    #[instrument(level = "debug", skip(self))]
    pub async fn trim(&self, log_id: LogId, trim_point: Lsn) -> Result<()> {
        self.inner.fail_if_shutting_down()?;
        self.inner.trim(log_id, trim_point).await
    }

    /// Seals a loglet under a set of conditions.
    ///
    /// The loglet will be sealed if and only if the following is true:
    ///   - if segment_index is set, the tail loglet must match segment_index.
    ///   If the intention is to create the log, then `segment_index` must be set to `None`.
    ///
    /// This will continue to retry sealing for seal retryable errors automatically.
    #[instrument(level = "debug", skip(self))]
    pub async fn seal_and_auto_extend_chain(
        &self,
        log_id: LogId,
        segment_index: Option<SegmentIndex>,
    ) -> Result<()> {
        self.inner.fail_if_shutting_down()?;
        let logs = Metadata::with_current(|m| m.logs_snapshot());
        let provider_config = &logs.configuration().default_provider;
        let provider = self.inner.provider_for(provider_config.kind())?;
        // if this is a new log, we don't need to seal and we can immediately write to metadata
        // store, otherwise, we need to seal first.
        if logs.chain(&log_id).is_none() && segment_index.is_none() {
            let proposed_params =
                provider.propose_new_loglet_params(log_id, None, provider_config)?;
            self.add_log(log_id, provider_config.kind(), proposed_params)
                .await?;
            return Ok(());
        }

        let segment_index = segment_index
            .or_else(|| logs.chain(&log_id).map(|c| c.tail_index()))
            .ok_or(Error::UnknownLogId(log_id))?;

        let sealed_segment = loop {
            let sealed_segment = self.seal(log_id, segment_index).await?;
            if sealed_segment.tail.is_sealed() {
                break sealed_segment;
            }
            debug!(%log_id, %segment_index, "Segment is not sealed yet");
            tokio::time::sleep(Configuration::pinned().bifrost.seal_retry_interval.into()).await;
        };

        let proposed_params =
            provider.propose_new_loglet_params(log_id, logs.chain(&log_id), provider_config)?;

        self.add_segment_with_params(
            log_id,
            segment_index,
            sealed_segment.tail.offset(),
            provider_config.kind(),
            proposed_params,
        )
        .await?;

        Ok(())
    }

    /// Seals a loglet under a set of conditions.
    ///
    /// The loglet will be sealed if and only if the following is true:
    ///   - log metadata is at least at version `min_version`. If not, this will wait for this
    ///     version to be synced (set to `Version::MIN` to ignore this step)
    ///   - if segment_index is set, the tail loglet must match segment_index.
    ///
    /// This will continue to retry sealing for seal retryable errors automatically.
    #[instrument(level = "debug", skip(self, params))]
    pub async fn seal_and_extend_chain(
        &self,
        log_id: LogId,
        segment_index: Option<SegmentIndex>,
        min_version: Version,
        provider: ProviderKind,
        params: LogletParams,
    ) -> Result<SealedSegment> {
        self.inner.fail_if_shutting_down()?;
        let metadata = Metadata::current();
        let _ = metadata
            .wait_for_version(MetadataKind::Logs, min_version)
            .await?;

        let segment_index = segment_index
            .or_else(|| metadata.logs_ref().chain(&log_id).map(|c| c.tail_index()))
            .ok_or(Error::UnknownLogId(log_id))?;

        let sealed_segment = loop {
            let sealed_segment = self.seal(log_id, segment_index).await?;
            if sealed_segment.tail.is_sealed() {
                break sealed_segment;
            }
            debug!(%log_id, %segment_index, "Segment is not sealed yet");
            tokio::time::sleep(Configuration::pinned().bifrost.seal_retry_interval.into()).await;
        };

        self.add_segment_with_params(
            log_id,
            segment_index,
            sealed_segment.tail.offset(),
            provider,
            params,
        )
        .await?;
        Ok(sealed_segment)
    }

    pub async fn writeable_loglet(&self, log_id: LogId) -> Result<LogletWrapper> {
        self.inner.writeable_loglet(log_id).await
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn seal(&self, log_id: LogId, segment_index: SegmentIndex) -> Result<SealedSegment> {
        self.inner.fail_if_shutting_down()?;
        // first find the tail segment for this log.
        let loglet = self.inner.writeable_loglet(log_id).await?;

        if segment_index != loglet.segment_index() {
            // Not the same segment. Bail!
            return Err(AdminError::SegmentMismatch {
                expected: segment_index,
                found: loglet.segment_index(),
            }
            .into());
        }

        if let Err(err) = loglet.seal().await {
            match err {
                crate::loglet::OperationError::Shutdown(err) => return Err(err.into()),
                crate::loglet::OperationError::Other(err) => {
                    warn!(
                        log_id = %log_id,
                        segment = %segment_index,
                        %err,
                        "Seal operation failed"
                    );
                    return Err(Error::LogletError(err));
                }
            }
        }
        let tail = loglet.find_tail(FindTailOptions::default()).await?;

        Ok(SealedSegment {
            segment_index: loglet.segment_index(),
            provider: loglet.config.kind,
            params: loglet.config.params,
            tail,
        })
    }

    /// Adds a segment to the end of the chain
    ///
    /// The loglet must be sealed first. This operations assumes that the loglet with
    /// `last_segment_index` has been sealed prior to this call.
    #[instrument(level = "debug", skip(self))]
    async fn add_segment_with_params(
        &self,
        log_id: LogId,
        last_segment_index: SegmentIndex,
        base_lsn: Lsn,
        provider: ProviderKind,
        params: LogletParams,
    ) -> Result<()> {
        self.inner.fail_if_shutting_down()?;
        let retry_policy = Configuration::pinned()
            .common
            .network_error_retry_policy
            .clone();
        let logs = retry_on_retryable_error(retry_policy, || {
            self.inner
                .metadata_writer
                .metadata_store_client()
                .read_modify_write(BIFROST_CONFIG_KEY.clone(), |logs: Option<Logs>| {
                    let logs = logs.ok_or(Error::UnknownLogId(log_id))?;

                    let mut builder = logs.into_builder();
                    let mut chain_builder =
                        builder.chain(log_id).ok_or(Error::UnknownLogId(log_id))?;

                    if chain_builder.tail().index() != last_segment_index {
                        // tail is not what we expected.
                        return Err(Error::from(AdminError::SegmentMismatch {
                            expected: last_segment_index,
                            found: chain_builder.tail().index(),
                        }));
                    }

                    let _ = chain_builder
                        .append_segment(base_lsn, provider, params.clone())
                        .map_err(AdminError::from)?;
                    Ok(builder.build())
                })
        })
        .await
        .map_err(|e| e.into_inner().transpose())?;

        self.inner.metadata_writer.update(Arc::new(logs)).await?;
        Ok(())
    }

    /// Adds a new log if it doesn't exist.
    #[instrument(level = "debug", skip(self, params))]
    async fn add_log(
        &self,
        log_id: LogId,
        provider: ProviderKind,
        params: LogletParams,
    ) -> Result<()> {
        self.inner.fail_if_shutting_down()?;
        let retry_policy = Configuration::pinned()
            .common
            .network_error_retry_policy
            .clone();
        let logs = retry_on_retryable_error(retry_policy, || {
            self.inner
                .metadata_writer
                .metadata_store_client()
                .read_modify_write::<_, _, Error>(
                    BIFROST_CONFIG_KEY.clone(),
                    |logs: Option<Logs>| {
                        // We assume that we'll always see a value set in metadata for BIFROST_CONFIG_KEY,
                        // provisioning the empty logs metadata is not our responsibility.
                        let logs = logs.ok_or(Error::LogsMetadataNotProvisioned)?;

                        let mut builder = logs.into_builder();
                        builder
                            .add_log(log_id, Chain::new(provider, params.clone()))
                            .map_err(AdminError::from)?;
                        Ok(builder.build())
                    },
                )
        })
        .await
        .map_err(|e| e.into_inner().transpose())?;

        self.inner.metadata_writer.update(Arc::new(logs)).await?;
        Ok(())
    }

    /// Creates empty metadata if none exists for bifrost and publishes it to metadata
    /// manager.
    pub async fn init_metadata(&self) -> Result<(), Error> {
        let retry_policy = Configuration::pinned()
            .common
            .network_error_retry_policy
            .clone();

        let logs = retry_on_retryable_error(retry_policy, || {
            self.inner
                .metadata_writer
                .metadata_store_client()
                .get_or_insert(BIFROST_CONFIG_KEY.clone(), || {
                    debug!("Attempting to initialize logs metadata in metadata store");
                    Logs::from_configuration(&Configuration::pinned())
                })
        })
        .await
        .map_err(|err| err.into_inner())?;

        self.inner.metadata_writer.update(Arc::new(logs)).await?;
        Ok(())
    }
}
