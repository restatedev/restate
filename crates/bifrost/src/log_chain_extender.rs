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

use tokio::sync::oneshot;
use tracing::trace;

use restate_core::{ShutdownError, cancellation_token};
use restate_metadata_store::ReadModifyWriteError;
use restate_types::logs::builder::LogsBuilder;
use restate_types::logs::metadata::{LogletParams, Logs, ProviderKind, SegmentIndex};
use restate_types::logs::{LogId, Lsn};

use crate::Error;
use crate::bifrost::{BifrostInner, ExtendLogChainReceiver};
use crate::error::AdminError;

const MAX_BATCH_SIZE_LOG_CHAIN_EXTENSIONS: usize = 128;

pub(super) struct ExtendLogChain {
    pub log_id: LogId,
    pub last_segment_index: SegmentIndex,
    pub base_lsn: Lsn,
    pub provider: ProviderKind,
    pub params: LogletParams,
    pub response_tx: Option<oneshot::Sender<Result<(), Error>>>,
}

impl ExtendLogChain {
    fn fail(&mut self, err: Error) {
        if let Some(response_tx) = self.response_tx.take() {
            // ignore if the receiver disappeared
            let _ = response_tx.send(Err(err));
        }
    }

    fn complete(&mut self) {
        if let Some(response_tx) = self.response_tx.take() {
            // ignore if the receiver disappeared
            let _ = response_tx.send(Ok(()));
        }
    }
}

/// Component which coalesces multiple log-chain updates into a single [`Logs`] update. It works
/// by draining all available [`ExtendLogChain`] commands and applying them to the current logs
/// configuration using a read-modify-write metadata operation. A log chain can only be extended if
/// the last segment index equals the value specified by the [`ExtendLogChain`] command.
pub struct LogChainExtender {
    inner: Arc<BifrostInner>,
    extend_log_chain_rx: ExtendLogChainReceiver,
}

impl LogChainExtender {
    pub fn new(inner: Arc<BifrostInner>, extend_log_chain_rx: ExtendLogChainReceiver) -> Self {
        Self {
            inner,
            extend_log_chain_rx,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        trace!("Bifrost log chain extender started");

        cancellation_token()
            .run_until_cancelled(self.run_inner())
            .await
            .ok_or(ShutdownError)?;

        Ok(())
    }

    pub async fn run_inner(mut self) {
        let mut buffer = Vec::new();

        // await the first extend log chain command
        loop {
            let received = self
                .extend_log_chain_rx
                .recv_many(&mut buffer, MAX_BATCH_SIZE_LOG_CHAIN_EXTENSIONS)
                .await;

            if received == 0 {
                break;
            }

            // batch-apply all collected extend log chain commands
            match self
                .inner
                .metadata_writer
                .global_metadata()
                .read_modify_write(|logs: Option<Arc<Logs>>| {
                    let mut builder = logs
                        .ok_or(Error::LogsMetadataNotProvisioned)?
                        .as_ref()
                        .clone()
                        .into_builder();

                    for extend_log_chain in &mut buffer {
                        if let Err(err) = Self::extend_log_chain(
                            &mut builder,
                            extend_log_chain.log_id,
                            extend_log_chain.last_segment_index,
                            extend_log_chain.base_lsn,
                            extend_log_chain.provider,
                            extend_log_chain.params.clone(),
                        ) {
                            extend_log_chain.fail(err);
                        }
                    }
                    Ok(builder.build())
                })
                .await
                .map_err(|err: ReadModifyWriteError<Error>| err.transpose())
            {
                Ok(_) => {
                    for mut extend_log_chain in buffer.drain(..) {
                        extend_log_chain.complete();
                    }
                }
                Err(err) => {
                    for mut extend_log_chain in buffer.drain(..) {
                        extend_log_chain.fail(err.clone());
                    }
                }
            }
        }
    }

    fn extend_log_chain(
        builder: &mut LogsBuilder,
        log_id: LogId,
        last_segment_index: SegmentIndex,
        base_lsn: Lsn,
        provider_kind: ProviderKind,
        params: LogletParams,
    ) -> Result<(), Error> {
        let mut chain_builder = builder.chain(log_id).ok_or(Error::UnknownLogId(log_id))?;

        if chain_builder.tail().index() != last_segment_index {
            // tail is not what we expected.
            Err(AdminError::SegmentMismatch {
                expected: last_segment_index,
                found: chain_builder.tail().index(),
            })?;
        }

        let _ = chain_builder
            .append_segment(base_lsn, provider_kind, params.clone())
            .map_err(AdminError::from)?;

        Ok(())
    }
}
