// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use restate_types::storage::StorageEncode;
use tracing::{debug, info, instrument};

use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{LogId, Lsn};
use restate_types::retries::RetryIter;

use crate::bifrost::BifrostInner;
use crate::loglet::AppendError;
use crate::loglet_wrapper::LogletWrapper;
use crate::record::ErasedInputRecord;
use crate::{Error, InputRecord, Result};

#[derive(Clone, derive_more::Debug)]
pub struct Appender {
    log_id: LogId,
    #[debug(skip)]
    pub(super) config: Live<Configuration>,
    loglet_cache: Option<LogletWrapper>,
    #[debug(skip)]
    bifrost_inner: Arc<BifrostInner>,
}

impl Appender {
    pub(crate) fn new(log_id: LogId, bifrost_inner: Arc<BifrostInner>) -> Self {
        let config = Configuration::updateable();
        Self {
            log_id,
            config,
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
        self.bifrost_inner.fail_if_shutting_down()?;

        let body = body.into().into_erased();
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

            match loglet.append(body.clone()).await {
                Ok(lsn) => return Ok(lsn),
                Err(AppendError::Sealed) => {
                    info!(
                        attempt = attempt,
                        segment_index = %loglet.segment_index(),
                        "Append will be retried (loglet being sealed), waiting for tail to be determined"
                    );
                    let new_loglet = Self::wait_next_unsealed_loglet(
                        self.log_id,
                        &self.bifrost_inner,
                        loglet.segment_index(),
                        &mut retry_iter,
                    )
                    .await?;
                    self.loglet_cache = Some(new_loglet);
                }
                Err(AppendError::Shutdown(e)) => return Err(Error::Shutdown(e)),
                Err(AppendError::Other(e)) => return Err(Error::LogletError(e)),
            }
        }
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
        let batch: Arc<[_]> = batch.into_iter().map(|r| r.into().into_erased()).collect();
        self.append_batch_erased(batch).await
    }

    pub(crate) async fn append_batch_erased(
        &mut self,
        batch: Arc<[ErasedInputRecord]>,
    ) -> Result<Lsn> {
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
                    info!(
                        attempt = attempt,
                        "Append batch will be retried (loglet being sealed), waiting for tail to be determined"
                    );
                    let new_loglet = Self::wait_next_unsealed_loglet(
                        self.log_id,
                        &self.bifrost_inner,
                        loglet.segment_index(),
                        &mut retry_iter,
                    )
                    .await?;

                    self.loglet_cache = Some(new_loglet);
                }
                Err(AppendError::Shutdown(e)) => return Err(Error::Shutdown(e)),
                Err(AppendError::Other(e)) => return Err(Error::LogletError(e)),
            }
        }
    }

    #[instrument(level = "debug" err, skip(retry_iter, bifrost_inner))]
    async fn wait_next_unsealed_loglet(
        log_id: LogId,
        bifrost_inner: &Arc<BifrostInner>,
        sealed_segment: SegmentIndex,
        retry_iter: &mut RetryIter<'_>,
    ) -> Result<LogletWrapper> {
        let start = Instant::now();
        for sleep_dur in retry_iter.by_ref() {
            bifrost_inner.fail_if_shutting_down()?;
            tokio::time::sleep(sleep_dur).await;
            let loglet = bifrost_inner.writeable_loglet(log_id).await?;
            // Do we think that the last tail loglet is different and unsealed?
            if loglet.tail_lsn.is_none() && loglet.segment_index() > sealed_segment {
                let total_dur = start.elapsed();
                debug!(
                    "Found an unsealed segment {} after {:?}",
                    loglet.segment_index(),
                    total_dur
                );
                return Ok(loglet);
            } else {
                debug!("Still waiting for sealing to complete");
            }
        }

        Err(Error::LogSealed(log_id))
    }
}
