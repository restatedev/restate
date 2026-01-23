// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tracing::{debug, info, instrument};

use restate_core::{Metadata, MetadataKind, TaskCenter};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_types::Versioned;
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{LogId, Lsn, Record};
use restate_types::storage::StorageEncode;

use crate::bifrost::{BifrostInner, ErrorRecoveryStrategy, PreferenceToken};
use crate::loglet::AppendError;
use crate::loglet_wrapper::LogletWrapper;
use crate::{BifrostAdmin, Error, InputRecord, Result};

#[derive(derive_more::Debug)]
pub struct Appender<T> {
    log_id: LogId,
    #[debug(skip)]
    pub(super) config: Live<Configuration>,
    error_recovery_strategy: ErrorRecoveryStrategy,
    loglet_cache: Option<LogletWrapper>,
    #[debug(skip)]
    bifrost_inner: Arc<BifrostInner>,
    #[debug(skip)]
    preference_token: Option<PreferenceToken>,
    arena: BytesMut,
    _phantom: PhantomData<T>,
}

impl<T> Clone for Appender<T> {
    fn clone(&self) -> Self {
        Self {
            log_id: self.log_id,
            config: self.config.clone(),
            error_recovery_strategy: self.error_recovery_strategy,
            loglet_cache: self.loglet_cache.clone(),
            bifrost_inner: self.bifrost_inner.clone(),
            preference_token: self.preference_token.clone(),
            arena: BytesMut::default(),
            _phantom: PhantomData,
        }
    }
}

impl<T: StorageEncode> Appender<T> {
    pub(crate) fn new(
        log_id: LogId,
        error_recovery_strategy: ErrorRecoveryStrategy,
        bifrost_inner: Arc<BifrostInner>,
    ) -> Self {
        let config = Configuration::live();
        Self {
            log_id,
            config,
            error_recovery_strategy,
            loglet_cache: Default::default(),
            bifrost_inner,
            preference_token: None,
            arena: BytesMut::default(),
            _phantom: PhantomData,
        }
    }

    /// Returns the record size limit for this appender.
    pub fn record_size_limit(&self) -> NonZeroUsize {
        self.config.pinned().bifrost.record_size_limit()
    }

    /// Marks this node as a preferred writer for the underlying log
    pub fn mark_as_preferred(&mut self) {
        if self.preference_token.is_none() {
            self.preference_token = Some(self.bifrost_inner.acquire_preference_token(self.log_id));
        }
    }

    /// Removes the preference about this node being the preferred writer for the log
    pub fn forget_preference(&mut self) {
        self.preference_token.take();
    }

    /// Appends a single record to the log.
    #[instrument(
        level = "info",
        skip(self, body),
        err,
        fields(
            log_id = %self.log_id,
        )
    )]
    pub async fn append(&mut self, body: impl Into<InputRecord<T>>) -> Result<Lsn> {
        let body = body.into().into_record().ensure_encoded(&mut self.arena);
        // Validate record sizes before attempting to append
        let batch_size_bytes = body.estimated_encode_size();
        let limit = self.record_size_limit();
        if batch_size_bytes > limit.get() {
            return Err(Error::BatchTooLarge {
                batch_size_bytes,
                limit,
            });
        }

        self.append_batch_erased(Arc::new([body])).await
    }

    /// Appends a batch of records to the log.
    ///
    /// The returned Lsn is the Lsn of the last record committed in this batch .
    /// This will only return after all records have been stored.
    #[instrument(
        level = "debug",
        skip(self, batch),
        err,
        fields(
            log_id = %self.log_id,
        )
    )]
    #[cfg(test)]
    pub async fn append_batch(&mut self, batch: Vec<impl Into<InputRecord<T>>>) -> Result<Lsn> {
        // todo(azmy): Make sure to do a single arena.reserve() for the entire batch
        // if this method is going to be used in production code. Right now it's okay
        // since it's only used by tests.
        let batch: Arc<[_]> = batch
            .into_iter()
            .map(|r| r.into().into_record().ensure_encoded(&mut self.arena))
            .collect();

        let batch_size_bytes = batch.iter().map(|r| r.estimated_encode_size()).sum();
        // Validate record sizes before attempting to append
        let limit = self.record_size_limit();
        if batch_size_bytes > limit.get() {
            return Err(Error::BatchTooLarge {
                batch_size_bytes,
                limit,
            });
        }
        self.append_batch_erased(batch).await
    }

    pub(crate) async fn append_batch_erased(&mut self, batch: Arc<[Record]>) -> Result<Lsn> {
        self.bifrost_inner.fail_if_shutting_down()?;

        let retry_iter = self.config.live_load().bifrost.append_retry_policy();

        debug_assert!(retry_iter.max_attempts().is_none());
        let mut retry_iter = retry_iter.into_iter();

        let mut attempt = 0;
        let mut reconfiguration_attempt = 0;
        let mut first_failed_at: Option<Instant> = None;
        loop {
            attempt += 1;
            let auto_recovery_threshold: Duration = self
                .config
                .live_load()
                .bifrost
                .auto_recovery_interval
                .into();

            let loglet = match self.loglet_cache.as_mut() {
                None => self
                    .loglet_cache
                    .insert(self.bifrost_inner.tail_loglet(self.log_id).await?),
                Some(wrapper) => wrapper,
            };
            match loglet.append_batch(batch.clone()).await {
                Ok(lsn) => return Ok(lsn),
                Err(err @ AppendError::Sealed | err @ AppendError::ReconfigurationNeeded(_)) => {
                    debug!(
                        log_id = %self.log_id,
                        attempt = attempt,
                        segment_index = %loglet.segment_index(),
                        error_recovery_strategy = %self.error_recovery_strategy,
                        "Batch append failed but will be retried ({err}). Waiting for reconfiguration to complete"
                    );
                    let new_loglet = Self::on_sealed_loglet(
                        self.log_id,
                        &self.bifrost_inner,
                        loglet.segment_index(),
                        self.error_recovery_strategy,
                    )
                    .await?;
                    debug!(
                        log_id = %self.log_id,
                        segment_index = %loglet.segment_index(),
                        error_recovery_strategy = %self.error_recovery_strategy,
                        "Log reconfiguration has been completed, appender will resume now"
                    );

                    self.loglet_cache = Some(new_loglet);
                }
                Err(AppendError::Other(err)) if err.retryable() => {
                    if first_failed_at.is_none() {
                        first_failed_at = Some(Instant::now());
                    }
                    let failing_since = first_failed_at.as_ref().unwrap().elapsed();
                    let recovery_strategy = self.error_recovery_strategy;

                    match recovery_strategy {
                        ErrorRecoveryStrategy::ExtendChainAllowed
                            if failing_since >= auto_recovery_threshold =>
                        {
                            // auto-recover
                            reconfiguration_attempt += 1;
                            info!(
                                %err,
                                log_id = %self.log_id,
                                reconfiguration_attempt,
                                attempt,
                                segment_index = %loglet.segment_index(),
                                error_recovery_strategy = %self.error_recovery_strategy,
                                "Failed to append this batch. Will attempt to reconfigure the log",
                            );
                            let new_loglet = Self::perform_reconfiguration(
                                self.log_id,
                                &self.bifrost_inner,
                                loglet.segment_index(),
                            )
                            .await?;
                            self.loglet_cache = Some(new_loglet);
                            first_failed_at = None; // reset the timer
                        }
                        ErrorRecoveryStrategy::Wait | ErrorRecoveryStrategy::ExtendChainAllowed => {
                            let retry_dur =
                                retry_iter.next().expect("append retries must be unbounded");
                            info!(
                                %err,
                                log_id = %self.log_id,
                                attempt,
                                segment_index = %loglet.segment_index(),
                                error_recovery_strategy = %self.error_recovery_strategy,
                                "Failed to append this batch. Since underlying error is retryable, will retry in {:?}",
                                retry_dur
                            );
                            tokio::time::sleep(retry_dur).await;
                        }
                        ErrorRecoveryStrategy::ExtendChainPreferred => {
                            // auto-recover
                            reconfiguration_attempt += 1;
                            info!(
                                %err,
                                log_id = %self.log_id,
                                reconfiguration_attempt,
                                attempt,
                                segment_index = %loglet.segment_index(),
                                error_recovery_strategy = %self.error_recovery_strategy,
                                "Failed to append this batch. Will attempt to reconfigure the log",
                            );
                            let new_loglet = Self::perform_reconfiguration(
                                self.log_id,
                                &self.bifrost_inner,
                                loglet.segment_index(),
                            )
                            .await?;
                            self.loglet_cache = Some(new_loglet);
                            first_failed_at = None; // reset the timer
                        }
                    }
                }
                Err(AppendError::Other(err)) => return Err(Error::LogletError(err)),
                Err(AppendError::Shutdown(err)) => return Err(Error::Shutdown(err)),
            }
        }
    }

    #[instrument(level = "error" err, skip(bifrost_inner))]
    async fn perform_reconfiguration(
        log_id: LogId,
        bifrost_inner: &Arc<BifrostInner>,
        current_segment: SegmentIndex,
    ) -> Result<LogletWrapper> {
        let mut retry_iter = Configuration::pinned()
            .bifrost
            .append_retry_policy()
            .into_iter();

        let metadata = Metadata::current();
        let mut logs = metadata.updateable_logs_metadata();
        // initial sleep duration.
        let mut sleep_dur = retry_iter
            .next()
            .expect("append retries should be infinite");
        loop {
            bifrost_inner.fail_if_shutting_down()?;
            let log_metadata = logs.live_load();
            let auto_recovery_threshold: Duration = Configuration::pinned()
                .bifrost
                .auto_recovery_interval
                .into();

            let loglet = bifrost_inner
                .tail_loglet_from_metadata(log_metadata, log_id)
                .await?;
            // Do we think that the last tail loglet is different and unsealed?
            if loglet.tail_lsn.is_none() && loglet.segment_index() > current_segment {
                return Ok(loglet);
            }

            // taking the matter into our own hands
            let admin = BifrostAdmin::new(bifrost_inner);
            let log_metadata_version = log_metadata.version();
            if let Err(err) = admin
                .seal_and_auto_extend_chain(log_id, Some(current_segment))
                .log_slow_after(
                    auto_recovery_threshold / 2,
                    tracing::Level::INFO,
                    "Extending the chain with new configuration",
                )
                .with_overdue(auto_recovery_threshold, tracing::Level::WARN)
                .await
            {
                // we couldn't reconfigure. Let the loop handle retries as normal
                info!(
                    %err,
                    %log_metadata_version,
                    "Could not reconfigure the log, perhaps another node beat us to it? We'll check",
                );
                tokio::time::sleep(sleep_dur).await;
                // backoff. This is at the bottom to avoid unnecessary sleeps in the happy path
                sleep_dur = retry_iter
                    .next()
                    .expect("append retries should be infinite");
            } else {
                info!(
                    log_metadata_version = %metadata.logs_version(),
                    "Reconfiguration complete",
                );
                continue;
            }
        }
    }

    #[instrument(level = "error" err, skip(bifrost_inner))]
    async fn on_sealed_loglet(
        log_id: LogId,
        bifrost_inner: &Arc<BifrostInner>,
        sealed_segment: SegmentIndex,
        error_recovery_strategy: ErrorRecoveryStrategy,
    ) -> Result<LogletWrapper> {
        let mut retry_iter = Configuration::pinned()
            .bifrost
            .append_retry_policy()
            .into_iter();

        let start = Instant::now();

        let metadata = Metadata::current();
        let mut logs = metadata.updateable_logs_metadata();
        // initial sleep duration.
        let mut sleep_dur = retry_iter
            .next()
            .expect("append retries should be infinite");
        loop {
            bifrost_inner.fail_if_shutting_down()?;
            let log_metadata = logs.live_load();
            let auto_recovery_threshold: Duration = Configuration::pinned()
                .bifrost
                .auto_recovery_interval
                .into();

            let loglet = bifrost_inner
                .tail_loglet_from_metadata(log_metadata, log_id)
                .await?;
            let tone_escalated = start.elapsed() > auto_recovery_threshold;
            // Do we think that the last tail loglet is different and unsealed?
            if loglet.tail_lsn.is_none() && loglet.segment_index() > sealed_segment {
                let total_dur = start.elapsed();
                if tone_escalated {
                    info!(
                        open_segment = %loglet.segment_index(),
                        %error_recovery_strategy,
                        "New segment detected after {:?}",
                        total_dur
                    );
                } else {
                    debug!(
                        open_segment = %loglet.segment_index(),
                        %error_recovery_strategy,
                        "New segment detected after {:?}",
                        total_dur
                    );
                }
                return Ok(loglet);
            }

            // Okay, tail segment is still sealed or requires reconfiguration
            let log_metadata_version = log_metadata.version();
            if (error_recovery_strategy == ErrorRecoveryStrategy::ExtendChainPreferred
                || (start.elapsed() > auto_recovery_threshold
                    && error_recovery_strategy == ErrorRecoveryStrategy::ExtendChainAllowed))
                && !TaskCenter::is_shutdown_requested()
                // only attempt to seal/acquire ownership if we are alive in FD.
                && TaskCenter::is_my_node_alive()
            {
                // taking the matter into our own hands
                let admin = BifrostAdmin::new(bifrost_inner);
                info!(
                    %sealed_segment,
                    "[Auto Recovery] Attempting to extend the chain to recover log availability with a new configuration. It has been {:?} since encountering the sealed loglet",
                    start.elapsed(),
                );
                if let Err(err) = admin
                    .seal_and_auto_extend_chain(log_id, Some(sealed_segment))
                    .log_slow_after(
                        auto_recovery_threshold / 2,
                        tracing::Level::INFO,
                        "Extending the chain with new configuration",
                    )
                    .with_overdue(auto_recovery_threshold, tracing::Level::WARN)
                    .await
                {
                    // we couldn't reconfigure. Let the loop handle retries as normal
                    info!(
                        %err,
                        %log_metadata_version,
                        %error_recovery_strategy,
                        "Could not reconfigure the log, perhaps another node beat us to it? We'll check",
                    );
                } else {
                    // note that we are reporting metadata version directly from `metadata` since
                    // it might have been updated while sealing the loglet
                    info!(
                        log_metadata_version = %metadata.logs_version(),
                        %error_recovery_strategy,
                        "[Auto Recovery] Reconfiguration complete",
                    );
                    // reconfiguration successful. Metadata is updated at this point
                    // Do not fall-through to the backoff sleep
                    continue;
                }
            } else {
                // Holding pattern
                if tone_escalated {
                    info!(
                        %log_metadata_version,
                        %error_recovery_strategy,
                        "In holding pattern, still waiting for log reconfiguration to complete. Elapsed={:?}",
                        start.elapsed(),
                    );
                } else {
                    debug!(
                        %log_metadata_version,
                        %error_recovery_strategy,
                        "In holding pattern, waiting for log reconfiguration to complete. Elapsed={:?}",
                        start.elapsed(),
                    );
                }
            }

            tokio::select! {
                biased;
                // if error it means that metadata manager has stopped. We are shutting down.
                // the check for shutdown in the loop above will catch if this happened and bubble
                // up the shutdown error.
                _ = metadata.wait_for_version(MetadataKind::Logs, log_metadata_version.next()) => {
                    // do not advance the sleep duration. Successive metadata updates that are
                    // irrelavant to this loglet should not increase the sleep duration.
                }
                // if no metadata changes happened, let's sleep to pace down the retries.
                _ = tokio::time::sleep(sleep_dur) => {
                    // backoff. This is at the bottom to avoid unnecessary sleeps in the happy path
                    sleep_dur = retry_iter
                        .next()
                        .expect("append retries should be infinite");
               }
            }
        }
    }
}
