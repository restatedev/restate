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
use std::time::{Duration, Instant};

use restate_futures_util::overdue::OverdueLoggingExt;
use tracing::{debug, info, instrument, warn};

use restate_core::Metadata;
use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{LogId, Lsn, Record};
use restate_types::retries::RetryIter;
use restate_types::storage::StorageEncode;

use crate::bifrost::{BifrostInner, ErrorRecoveryStrategy};
use crate::loglet::AppendError;
use crate::loglet_wrapper::LogletWrapper;
use crate::{BifrostAdmin, Error, InputRecord, Result};

#[derive(Clone, derive_more::Debug)]
pub struct Appender {
    log_id: LogId,
    #[debug(skip)]
    pub(super) config: Live<Configuration>,
    // todo: asoli remove
    #[allow(unused)]
    error_recovery_strategy: ErrorRecoveryStrategy,
    loglet_cache: Option<LogletWrapper>,
    #[debug(skip)]
    bifrost_inner: Arc<BifrostInner>,
}

impl Appender {
    pub(crate) fn new(
        log_id: LogId,
        error_recovery_strategy: ErrorRecoveryStrategy,
        bifrost_inner: Arc<BifrostInner>,
    ) -> Self {
        let config = Configuration::updateable();
        Self {
            log_id,
            config,
            error_recovery_strategy,
            loglet_cache: Default::default(),
            bifrost_inner,
        }
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
    pub async fn append<T: StorageEncode>(
        &mut self,
        body: impl Into<InputRecord<T>>,
    ) -> Result<Lsn> {
        let body = body.into().into_record();
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
    pub async fn append_batch<T: StorageEncode>(
        &mut self,
        batch: Vec<impl Into<InputRecord<T>>>,
    ) -> Result<Lsn> {
        let batch: Arc<[_]> = batch.into_iter().map(|r| r.into().into_record()).collect();
        self.append_batch_erased(batch).await
    }

    pub(crate) async fn append_batch_erased(&mut self, batch: Arc<[Record]>) -> Result<Lsn> {
        self.bifrost_inner.fail_if_shutting_down()?;
        let mut retry_iter = self
            .config
            .live_load()
            .bifrost
            .append_retry_policy()
            .into_iter();

        let mut attempt = 0;
        loop {
            attempt += 1;
            let loglet = match self.loglet_cache.as_mut() {
                None => self
                    .loglet_cache
                    .insert(self.bifrost_inner.writeable_loglet(self.log_id).await?),
                Some(wrapper) => wrapper,
            };
            match loglet.append_batch(batch.clone()).await {
                Ok(lsn) => return Ok(lsn),
                Err(AppendError::Sealed) => {
                    debug!(
                        log_id = %self.log_id,
                        attempt = attempt,
                        segment_index = %loglet.segment_index(),
                        "Batch append failed but will be retried (loglet has been sealed). Waiting for reconfiguration to complete"
                    );
                    let new_loglet = Self::on_sealed_loglet(
                        self.log_id,
                        &self.bifrost_inner,
                        loglet.segment_index(),
                        &mut retry_iter,
                        self.error_recovery_strategy,
                    )
                    .await?;
                    debug!(
                        log_id = %self.log_id,
                        segment_index = %loglet.segment_index(),
                        "Log reconfiguration has been completed, appender will resume now"
                    );

                    self.loglet_cache = Some(new_loglet);
                }
                Err(AppendError::Other(err)) if err.retryable() => {
                    if let Some(retry_dur) = retry_iter.next() {
                        info!(
                            %err,
                            log_id = %self.log_id,
                            attempt = attempt,
                            segment_index = %loglet.segment_index(),
                            "Failed to append this batch. Since underlying error is retryable, will retry in {:?}",
                            retry_dur
                        );
                        tokio::time::sleep(retry_dur).await;
                    } else {
                        warn!(
                            %err,
                            log_id = %self.log_id,
                            attempt = attempt,
                            segment_index = %loglet.segment_index(),
                            "Failed to append this batch and exhausted all attempts to retry",
                        );
                        return Err(Error::LogletError(err));
                    }
                }
                Err(AppendError::Other(err)) => return Err(Error::LogletError(err)),
                Err(AppendError::Shutdown(err)) => return Err(Error::Shutdown(err)),
            }
        }
    }

    #[instrument(level = "error" err, skip(retry_iter, bifrost_inner))]
    async fn on_sealed_loglet(
        log_id: LogId,
        bifrost_inner: &Arc<BifrostInner>,
        sealed_segment: SegmentIndex,
        retry_iter: &mut RetryIter<'_>,
        error_recovery_strategy: ErrorRecoveryStrategy,
    ) -> Result<LogletWrapper> {
        // todo: configuration
        let recovery_patience_dur = Duration::from_secs(5);
        let mut tone_escalated = false;
        let start = Instant::now();
        for sleep_dur in retry_iter.by_ref() {
            bifrost_inner.fail_if_shutting_down()?;
            tokio::time::sleep(sleep_dur).await;
            let loglet = bifrost_inner.writeable_loglet(log_id).await?;
            // Do we think that the last tail loglet is different and unsealed?
            if loglet.tail_lsn.is_none() && loglet.segment_index() > sealed_segment {
                let total_dur = start.elapsed();
                if tone_escalated {
                    info!(
                        open_segment = %loglet.segment_index(),
                        "Reconfiguration complete, found an open segment after {:?}",
                        total_dur
                    );
                } else {
                    info!(
                        open_segment = %loglet.segment_index(),
                        "Reconfiguration complete, found an open segment after {:?}",
                        total_dur
                    );
                }
                return Ok(loglet);
            } else {
                let log_version = Metadata::with_current(|m| m.logs_version());
                if start.elapsed() > recovery_patience_dur {
                    tone_escalated = true;

                    if error_recovery_strategy >= ErrorRecoveryStrategy::ExtendChainAllowed {
                        // take the matter in our own hands
                        let admin = BifrostAdmin::new(bifrost_inner);
                        info!(
                            %sealed_segment,
                            "[Auto Recovery] Attempting to extend the chain to allow new appends to proceed on a new configuration. We waited for {:?} before triggering automatic recovery",
                            start.elapsed(),
                        );
                        if let Err(err) = admin
                            .seal_and_auto_extend_chain(log_id, Some(sealed_segment))
                            .log_slow_after(
                                recovery_patience_dur / 2,
                                tracing::Level::INFO,
                                "Extending the chain with new configuration",
                            )
                            .with_overdue(recovery_patience_dur, tracing::Level::WARN)
                            .await
                        {
                            // we couldn't reconfigure. Let the loop handle retries as normal
                            info!(
                                %err,
                                %log_version,
                                "Could not reconfigure the log, perhaps someone else beat us to it? We'll check",
                            );
                        }
                    } else {
                        // Holding pattern
                        if tone_escalated {
                            info!(
                                %log_version,
                                "In holding pattern, still waiting for log reconfiguration to complete. Elapsed={:?}",
                                start.elapsed(),
                            );
                        } else {
                            debug!(
                                %log_version,
                                "In holding pattern, waiting for log reconfiguration to complete. Elapsed={:?}",
                                start.elapsed(),
                            );
                        }
                    }
                }
            }
        }

        Err(Error::LogSealed(log_id))
    }
}
