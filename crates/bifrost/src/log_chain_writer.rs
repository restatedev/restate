// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace};

use restate_core::{MetadataWriter, TaskCenter, TaskHandle, TaskKind, cancellation_token};
use restate_metadata_store::ReadModifyWriteError;
use restate_types::logs::builder::{BuilderError, LogsBuilder};
use restate_types::logs::metadata::{
    Chain, LogletParams, Logs, ProviderKind, SealMetadata, SegmentIndex,
};
use restate_types::logs::{LogId, Lsn, SequenceNumber};

use crate::Error;
use crate::error::AdminError;

const MAX_BATCH_SIZE: usize = 1024;

struct OpOutput<T> {
    tx: oneshot::Sender<Result<T, Error>>,
    staged_result: Option<Result<T, Error>>,
}

impl<T> OpOutput<T> {
    fn stage_output(&mut self, result: Result<T, Error>) {
        self.staged_result = Some(result);
    }

    fn fail(self, err: Error) {
        // ignore if the receiver disappeared
        let _ = self.tx.send(Err(err));
    }

    fn complete(self) {
        let result = self
            .staged_result
            .expect("complete must be called on a staged result");
        // ignore if the receiver disappeared
        let _ = self.tx.send(result);
    }
}

#[derive(Debug)]
pub(super) struct LogChainCommand {
    log_id: LogId,
    op: ChainOp,
}

impl LogChainCommand {
    pub fn add_log(
        log_id: LogId,
        provider: ProviderKind,
        params: LogletParams,
    ) -> (oneshot::Receiver<Result<(), Error>>, Self) {
        let (tx, rx) = oneshot::channel();
        let cmd = Self {
            log_id,
            op: ChainOp::AddLog {
                provider,
                params,
                response: OpOutput {
                    tx,
                    staged_result: None,
                },
            },
        };
        (rx, cmd)
    }

    pub fn extend(
        log_id: LogId,
        last_segment_index: SegmentIndex,
        base_lsn: Lsn,
        provider: ProviderKind,
        params: LogletParams,
    ) -> (oneshot::Receiver<Result<(), Error>>, Self) {
        let (tx, rx) = oneshot::channel();
        let cmd = Self {
            log_id,
            op: ChainOp::Extend {
                last_segment_index,
                base_lsn,
                provider,
                params,
                response: OpOutput {
                    tx,
                    staged_result: None,
                },
            },
        };
        (rx, cmd)
    }

    pub fn seal_chain(
        log_id: LogId,
        last_segment_index: SegmentIndex,
        tail_lsn: Lsn,
        metadata: SealMetadata,
    ) -> (oneshot::Receiver<Result<Lsn, Error>>, Self) {
        let (tx, rx) = oneshot::channel();
        let cmd = Self {
            log_id,
            op: ChainOp::SealChain {
                last_segment_index,
                tail_lsn,
                metadata,
                response: OpOutput {
                    tx,
                    staged_result: None,
                },
            },
        };
        (rx, cmd)
    }

    pub fn trim_prefix(log_id: LogId, trim_point: Lsn) -> Self {
        Self {
            log_id,
            op: ChainOp::TrimPrefix { trim_point },
        }
    }

    fn fail(self, err: Error) {
        match self.op {
            ChainOp::Extend { response, .. } => response.fail(err),
            ChainOp::SealChain { response, .. } => response.fail(err),
            ChainOp::AddLog { response, .. } => response.fail(err),
            ChainOp::TrimPrefix { .. } => { /* do nothing */ }
        }
    }

    fn complete(self) {
        match self.op {
            ChainOp::Extend { response, .. } => response.complete(),
            ChainOp::SealChain { response, .. } => response.complete(),
            ChainOp::AddLog { response, .. } => response.complete(),
            ChainOp::TrimPrefix { trim_point } => {
                debug!(
                    "Log {} chain has been trimmed to trim-point {}",
                    self.log_id, trim_point,
                );
            }
        }
    }
}

#[derive(derive_more::Debug)]
enum ChainOp {
    Extend {
        last_segment_index: SegmentIndex,
        base_lsn: Lsn,
        provider: ProviderKind,
        #[debug(skip)]
        params: LogletParams,
        #[debug(skip)]
        response: OpOutput<()>,
    },
    SealChain {
        last_segment_index: SegmentIndex,
        tail_lsn: Lsn,
        metadata: SealMetadata,
        #[debug(skip)]
        response: OpOutput<Lsn>,
    },
    AddLog {
        provider: ProviderKind,
        #[debug(skip)]
        params: LogletParams,
        #[debug(skip)]
        response: OpOutput<()>,
    },
    TrimPrefix {
        trim_point: Lsn,
    },
}

/// Component which coalesces multiple log-chain updates into a single [`Logs`] update. It works
/// by draining all available [`LogChainCommand`] commands and applying them to the current logs
/// configuration using a read-modify-write metadata operation.
pub struct LogChainWriter {
    metadata_writer: MetadataWriter,
    rx: mpsc::UnboundedReceiver<LogChainCommand>,
}

impl LogChainWriter {
    pub fn start(
        metadata_writer: MetadataWriter,
    ) -> anyhow::Result<(mpsc::UnboundedSender<LogChainCommand>, TaskHandle<()>)> {
        let (tx, rx) = mpsc::unbounded_channel();

        let writer = Self {
            metadata_writer,
            rx,
        };
        let handle = TaskCenter::spawn_unmanaged(
            TaskKind::BifrostBackgroundHighPriority,
            "log-chain-writer",
            writer.run(),
        )?;

        Ok((tx, handle))
    }

    pub async fn run(self) {
        trace!("Bifrost log chain writer started");

        cancellation_token()
            .run_until_cancelled(self.run_inner())
            .await;
    }

    pub async fn run_inner(mut self) {
        let mut buffer = Vec::new();

        // await the first log chain command
        loop {
            let received = self.rx.recv_many(&mut buffer, MAX_BATCH_SIZE).await;

            if received == 0 {
                break;
            }

            // batch-apply all collected log chain commands
            match self
                .metadata_writer
                .global_metadata()
                .read_modify_write(|logs: Option<Arc<Logs>>| {
                    let mut builder =
                        Arc::unwrap_or_clone(logs.ok_or(Error::LogsMetadataNotProvisioned)?)
                            .try_into_builder()
                            .map_err(|err| Error::Other(format!("LogsBuilder error: {err}")))?;

                    for cmd in &mut buffer {
                        match cmd.op {
                            ChainOp::AddLog {
                                provider,
                                ref params,
                                ref mut response,
                            } => {
                                response.stage_output(Self::add_log(
                                    &mut builder,
                                    cmd.log_id,
                                    provider,
                                    params,
                                ));
                            }
                            ChainOp::Extend {
                                last_segment_index,
                                base_lsn,
                                provider,
                                ref params,
                                ref mut response,
                            } => {
                                response.stage_output(Self::extend_log_chain(
                                    &mut builder,
                                    cmd.log_id,
                                    last_segment_index,
                                    base_lsn,
                                    provider,
                                    params,
                                ));
                            }
                            ChainOp::SealChain {
                                last_segment_index,
                                tail_lsn,
                                ref metadata,
                                ref mut response,
                            } => {
                                response.stage_output(Self::seal_log_chain(
                                    &mut builder,
                                    cmd.log_id,
                                    last_segment_index,
                                    tail_lsn,
                                    metadata,
                                ));
                            }
                            ChainOp::TrimPrefix { trim_point } => {
                                // ignores the error if the log is unknown.
                                let _ = Self::trim_prefix(&mut builder, cmd.log_id, trim_point);
                            }
                        }
                    }

                    Ok(builder.build())
                })
                .await
                .map_err(|err: ReadModifyWriteError<Error>| err.transpose())
            {
                Ok(_) => {
                    for cmd in buffer.drain(..) {
                        cmd.complete();
                    }
                }
                Err(err) => {
                    for cmd in buffer.drain(..) {
                        cmd.fail(err.clone());
                    }
                }
            }
        }
    }

    fn add_log(
        builder: &mut LogsBuilder,
        log_id: LogId,
        provider_kind: ProviderKind,
        params: &LogletParams,
    ) -> Result<(), Error> {
        match builder.add_log(log_id, Chain::new(provider_kind, params.clone())) {
            Ok(_) => Ok(()),
            // If the log already exists, it's okay to ignore the error.
            Err(BuilderError::LogAlreadyExists(_)) => Ok(()),
            Err(other) => Err(other),
        }
        .map_err(AdminError::from)?;

        Ok(())
    }

    fn extend_log_chain(
        builder: &mut LogsBuilder,
        log_id: LogId,
        last_segment_index: SegmentIndex,
        base_lsn: Lsn,
        provider_kind: ProviderKind,
        params: &LogletParams,
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

    fn seal_log_chain(
        builder: &mut LogsBuilder,
        log_id: LogId,
        last_segment_index: SegmentIndex,
        tail_lsn: Lsn,
        metadata: &SealMetadata,
    ) -> Result<Lsn, Error> {
        let mut chain_builder = builder.chain(log_id).ok_or(Error::UnknownLogId(log_id))?;

        if chain_builder.tail().index() != last_segment_index {
            // tail segment is not what we expected.
            Err(AdminError::SegmentMismatch {
                expected: last_segment_index,
                found: chain_builder.tail().index(),
            })?;
        }

        let lsn = chain_builder
            .seal(tail_lsn, metadata)
            .map_err(AdminError::from)?;

        Ok(lsn)
    }

    fn trim_prefix(builder: &mut LogsBuilder, log_id: LogId, trim_point: Lsn) -> Result<(), Error> {
        let mut chain_builder = builder.chain(log_id).ok_or(Error::UnknownLogId(log_id))?;

        // trim_prefix's lsn is exclusive. Trim-point is inclusive of the last trimmed lsn,
        // therefore, we need to trim _including_ the trim point.
        chain_builder.trim_prefix(trim_point.next());
        Ok(())
    }
}
