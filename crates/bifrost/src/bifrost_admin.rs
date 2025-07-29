// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::{debug, instrument, warn};

use restate_core::{Metadata, MetadataKind};
use restate_types::Version;
use restate_types::config::Configuration;
use restate_types::logs::metadata::{
    InternalKind, LogletParams, ProviderKind, SealMetadata, SegmentIndex,
};
use restate_types::logs::{LogId, Lsn, TailState};

use crate::bifrost::BifrostInner;
use crate::error::AdminError;
use crate::loglet::FindTailOptions;
use crate::loglet_wrapper::LogletWrapper;
use crate::{Error, Result};

/// Bifrost's Admin API
#[derive(Clone, Copy)]
pub struct BifrostAdmin<'a> {
    inner: &'a BifrostInner,
}

#[derive(Debug)]
pub struct MaybeSealedSegment {
    pub segment_index: SegmentIndex,
    pub provider: InternalKind,
    pub params: LogletParams,
    pub tail: TailState,
}

impl<'a> BifrostAdmin<'a> {
    pub(crate) fn new(inner: &'a BifrostInner) -> Self {
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

    /// Creates the log if it doesn't exist
    #[instrument(level = "debug", skip(self))]
    pub async fn ensure_log_exists(&self, log_id: LogId) -> Result<()> {
        self.inner.fail_if_shutting_down()?;
        let logs = Metadata::with_current(|m| m.logs_snapshot());
        let provider_config = &logs.configuration().default_provider;
        let provider = self.inner.provider_for(provider_config.kind())?;
        if logs.chain(&log_id).is_none() {
            let proposed_params =
                provider.propose_new_loglet_params(log_id, None, provider_config)?;
            debug!(
                %log_id,
                provider = %provider_config.kind(),
                "Provisioning log"
            );
            self.add_log(log_id, provider_config.kind(), proposed_params)
                .await?;
        }

        Ok(())
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
            let sealed_segment = self.seal_inner(log_id, segment_index, None).await?;
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
    ) -> Result<MaybeSealedSegment> {
        self.inner.fail_if_shutting_down()?;
        let metadata = Metadata::current();
        let _ = metadata
            .wait_for_version(MetadataKind::Logs, min_version)
            .await?;

        let segment_index = segment_index
            .or_else(|| metadata.logs_ref().chain(&log_id).map(|c| c.tail_index()))
            .ok_or(Error::UnknownLogId(log_id))?;

        let sealed_segment = loop {
            let sealed_segment = self.seal_inner(log_id, segment_index, None).await?;
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
        self.inner.tail_loglet(log_id).await
    }

    async fn seal_inner(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
        seal_metadata: Option<SealMetadata>,
    ) -> Result<MaybeSealedSegment> {
        self.inner.fail_if_shutting_down()?;
        // first find the tail segment for this log.
        let loglet = self.inner.tail_loglet(log_id).await?;

        if segment_index != loglet.segment_index() {
            // Not the same segment. Bail!
            return Err(AdminError::SegmentMismatch {
                expected: segment_index,
                found: loglet.segment_index(),
            }
            .into());
        }

        // This loglet has already been sealed.
        if let Some(tail_lsn) = loglet.tail_lsn {
            return Ok(MaybeSealedSegment {
                segment_index: loglet.segment_index(),
                provider: loglet.config.kind,
                params: loglet.config.params,
                tail: TailState::Sealed(tail_lsn),
            });
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

        let tail = loglet.find_tail(FindTailOptions::ConsistentRead).await?;

        if let Some(seal_metadata) = seal_metadata
            && tail.is_sealed()
            && Configuration::pinned().bifrost.experimental_chain_sealing
        {
            let tail_lsn = self
                .inner
                .seal_log_chain(log_id, segment_index, tail.offset(), seal_metadata)
                .await?;

            return Ok(MaybeSealedSegment {
                segment_index: loglet.segment_index(),
                provider: loglet.config.kind,
                params: loglet.config.params,
                tail: TailState::Sealed(tail_lsn),
            });
        }

        Ok(MaybeSealedSegment {
            segment_index: loglet.segment_index(),
            provider: loglet.config.kind,
            params: loglet.config.params,
            tail,
        })
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn seal(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
        metadata: SealMetadata,
    ) -> Result<Lsn> {
        let maybe_sealed = self
            .seal_inner(log_id, segment_index, Some(metadata))
            .await?;
        if let TailState::Sealed(lsn) = maybe_sealed.tail {
            Ok(lsn)
        } else {
            Err(AdminError::ChainSealIncomplete.into())
        }
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
        self.inner
            .extend_log_chain(log_id, last_segment_index, base_lsn, provider, params)
            .await?;

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
        self.inner.add_log(log_id, provider, params).await?;

        Ok(())
    }
}
