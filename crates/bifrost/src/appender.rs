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

use bytes::{Bytes, BytesMut};
use tracing::{info, instrument, Span};

use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::logs::{HasRecordKeys, Keys, LogId, Lsn, Payload};
use restate_types::storage::{StorageCodec, StorageEncode};

use crate::bifrost::BifrostInner;
use crate::loglet::{AppendError, LogletBase};
use crate::loglet_wrapper::LogletWrapper;
use crate::{Error, Result};

pub(crate) const RECORD_SIZE_HINT: usize = 128_000; // 128KB per record
const INITIAL_SERDE_BUFFER_SIZE: usize = 64_000; // Initial capacity 64KB

#[derive(Clone)]
pub struct Appender {
    log_id: LogId,
    config: Live<Configuration>,
    pub(super) serde_buffer: BytesMut,
    loglet_cache: Option<LogletWrapper>,
    bifrost_inner: Arc<BifrostInner>,
}

impl Appender {
    pub(crate) fn new(log_id: LogId, bifrost_inner: Arc<BifrostInner>) -> Self {
        Self::with_buffer_size(log_id, bifrost_inner, INITIAL_SERDE_BUFFER_SIZE)
    }

    pub(crate) fn with_buffer_size(
        log_id: LogId,
        bifrost_inner: Arc<BifrostInner>,
        serde_buffer_size: usize,
    ) -> Self {
        Self::with_serde_buffer(
            log_id,
            bifrost_inner,
            BytesMut::with_capacity(serde_buffer_size),
        )
    }

    pub(crate) fn with_serde_buffer(
        log_id: LogId,
        bifrost_inner: Arc<BifrostInner>,
        serde_buffer: BytesMut,
    ) -> Self {
        let config = Configuration::updateable();

        Self {
            log_id,
            config,
            serde_buffer,
            loglet_cache: Default::default(),
            bifrost_inner,
        }
    }

    pub async fn append_raw(&mut self, raw_bytes: impl Into<Bytes>) -> Result<Lsn> {
        self.append_raw_with_keys(raw_bytes, Keys::None).await
    }

    #[instrument(
        level = "debug",
        skip(self, raw_bytes),
        err,
        fields(
            log_id = %self.log_id,
            keys = ?keys,
            segment_index = tracing::field::Empty,
        )
    )]
    pub async fn append_raw_with_keys(
        &mut self,
        raw_bytes: impl Into<Bytes>,
        keys: Keys,
    ) -> Result<Lsn> {
        self.bifrost_inner.fail_if_shutting_down()?;
        // todo review
        self.serde_buffer.reserve(RECORD_SIZE_HINT);
        let payload = Payload::new(raw_bytes);
        StorageCodec::encode(payload, &mut self.serde_buffer).expect("record serde is infallible");
        let raw_bytes = self.serde_buffer.split().freeze();

        let mut retry_iter = self
            .config
            .live_load()
            .bifrost
            .append_retry_policy
            .clone()
            .into_iter();
        let mut attempt = 0;

        loop {
            attempt += 1;
            // attempt to use cached loglets
            let loglet = match self.loglet_cache.as_mut() {
                None => self
                    .loglet_cache
                    .insert(self.bifrost_inner.writeable_loglet(self.log_id).await?),
                Some(wrapper) => wrapper,
            };

            Span::current().record(
                "segment_index",
                tracing::field::display(loglet.segment_index()),
            );
            match loglet.append(&raw_bytes, &keys).await {
                Ok(lsn) => return Ok(lsn),
                Err(AppendError::Sealed) => {
                    info!(
                        attempt = attempt,
                        "Append will be retried (loglet being sealed), waiting for tail to be determined"
                    );
                    let new_loglet = self
                        .bifrost_inner
                        .wait_next_unsealed_loglet(
                            self.log_id,
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

    /// Appends a single record to a log. The log id must exist, otherwise the
    /// operation fails with [`Error::UnknownLogId`]
    #[instrument(
        level = "debug",
        skip(self, body),
        err,
        fields(
            log_id = %self.log_id,
            segment_index = tracing::field::Empty
        )
    )]
    pub async fn append<T>(&mut self, body: T) -> Result<Lsn>
    where
        T: HasRecordKeys + StorageEncode,
    {
        let keys = body.record_keys();
        StorageCodec::encode(&body, &mut self.serde_buffer).expect("record serde is infallible");
        let raw_bytes = self.serde_buffer.split().freeze();
        self.append_raw_with_keys(raw_bytes, keys).await
    }

    #[instrument(
        level = "debug",
        skip(self, bodies_with_keys)
        err,
        fields(
            log_id = %self.log_id,
            segment_index = tracing::field::Empty,
        )
    )]
    pub(crate) async fn append_raw_batch_with_keys(
        &mut self,
        bodies_with_keys: &[(Bytes, Keys)],
    ) -> Result<Lsn> {
        self.bifrost_inner.fail_if_shutting_down()?;

        let mut retry_iter = self
            .config
            .live_load()
            .bifrost
            .append_retry_policy
            .clone()
            .into_iter();
        let mut attempt = 0;

        loop {
            attempt += 1;
            // attempt to use cached loglets
            let loglet = match self.loglet_cache.as_mut() {
                None => self
                    .loglet_cache
                    .insert(self.bifrost_inner.writeable_loglet(self.log_id).await?),
                Some(wrapper) => wrapper,
            };
            Span::current().record(
                "segment_index",
                tracing::field::display(loglet.segment_index()),
            );
            match loglet.append_batch(bodies_with_keys).await {
                Ok(lsn) => return Ok(lsn),
                Err(AppendError::Sealed) => {
                    info!(
                        attempt = attempt,
                        "Append batch will be retried (loglet being sealed), waiting for tail to be determined"
                    );
                    let new_loglet = self
                        .bifrost_inner
                        .wait_next_unsealed_loglet(
                            self.log_id,
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

    pub async fn append_raw_batch(
        &mut self,
        batch: impl IntoIterator<Item = impl Into<Bytes>>,
    ) -> Result<Lsn> {
        let bodies_with_keys: Vec<_> = batch
            .into_iter()
            .map(|record| {
                let raw_bytes: Bytes = record.into();
                let keys = Keys::None;
                let payload = Payload::new(raw_bytes);
                StorageCodec::encode(payload, &mut self.serde_buffer)
                    .expect("record serde is infallible");
                (self.serde_buffer.split().freeze(), keys)
            })
            .collect();

        self.append_raw_batch_with_keys(&bodies_with_keys).await
    }

    /// Appends a batch of records to a log. The log id must exist, otherwise the
    /// operation fails with [`Error::UnknownLogId`]. The returned Lsn is the Lsn of the first
    /// record in this batch. This will only return after all records have been stored.
    #[instrument(
        level = "debug",
        skip(self, batch),
        err,
        fields(
            log_id = %self.log_id,
            count = batch.len(),
            segment_index = tracing::field::Empty
        )
    )]
    pub async fn append_batch<T>(&mut self, batch: &[T]) -> Result<Lsn>
    where
        T: HasRecordKeys + StorageEncode,
    {
        // Attempt to reserve enough bytes for the payloads
        self.serde_buffer.reserve(batch.len() * RECORD_SIZE_HINT);

        let bodies_with_keys: Vec<_> = batch
            .iter()
            .map(|record| {
                // todo (estimate size to reserve)
                let keys = record.record_keys();
                StorageCodec::encode(record, &mut self.serde_buffer)
                    .expect("record serde is infallible");
                let payload = Payload::new(self.serde_buffer.split().freeze());
                StorageCodec::encode(payload, &mut self.serde_buffer)
                    .expect("record serde is infallible");
                (self.serde_buffer.split().freeze(), keys)
            })
            .collect();

        self.append_raw_batch_with_keys(&bodies_with_keys).await
    }
}
