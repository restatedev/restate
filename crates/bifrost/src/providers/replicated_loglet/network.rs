// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo(asoli): remove when fleshed out
#![allow(dead_code)]

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use restate_types::errors::MaybeRetryableError;
use tracing::trace;

use restate_core::network::{Incoming, MessageRouterBuilder, Reciprocal, TransportConnect};
use restate_core::{cancellation_watcher, task_center, Metadata, TaskKind};
use restate_types::config::ReplicatedLogletOptions;
use restate_types::logs::{LogletOffset, SequenceNumber};
use restate_types::net::replicated_loglet::{
    Append, Appended, CommonRequestHeader, CommonResponseHeader, SequencerStatus,
};

use super::error::ReplicatedLogletError;
use super::loglet::ReplicatedLoglet;
use super::provider::ReplicatedLogletProvider;
use crate::loglet::util::TailOffsetWatch;
use crate::loglet::{AppendError, Loglet, LogletCommit, OperationError};

type MessageStream<T> = Pin<Box<dyn Stream<Item = Incoming<T>> + Send + Sync + 'static>>;

macro_rules! return_error_status {
    ($reciprocal:ident, $status:expr, $tail:expr) => {{
        let msg = Appended {
            first_offset: LogletOffset::INVALID,
            header: CommonResponseHeader {
                known_global_tail: Some($tail.latest_offset()),
                sealed: Some($tail.is_sealed()),
                status: $status,
            },
        };

        let _ =
            task_center().spawn_child(TaskKind::Disposable, "append-return-error", None, async {
                $reciprocal.prepare(msg).send().await?;
                Ok(())
            });

        return;
    }};
    ($reciprocal:ident, $status:expr) => {{
        let msg = Appended {
            first_offset: LogletOffset::INVALID,
            header: CommonResponseHeader {
                known_global_tail: None,
                sealed: None,
                status: $status,
            },
        };

        let _ =
            task_center().spawn_child(TaskKind::Disposable, "append-return-error", None, async {
                $reciprocal.prepare(msg).send().await?;
                Ok(())
            });

        return;
    }};
}

pub struct RequestPump {
    metadata: Metadata,
    append_stream: MessageStream<Append>,
}

impl RequestPump {
    pub fn new(
        _opts: &ReplicatedLogletOptions,
        metadata: Metadata,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        // todo(asoli) read from opts
        let queue_length = 10;
        let append_stream = router_builder.subscribe_to_stream(queue_length);
        Self {
            metadata,
            append_stream,
        }
    }

    /// Must run in task-center context
    pub async fn run<T: TransportConnect>(
        mut self,
        provider: Arc<ReplicatedLogletProvider<T>>,
    ) -> anyhow::Result<()> {
        trace!("Starting replicated loglet request pump");
        let mut cancel = std::pin::pin!(cancellation_watcher());
        loop {
            tokio::select! {
                _ = &mut cancel => {
                    break;
                }
                Some(append) = self.append_stream.next() => {
                    self.handle_append(&provider, append).await;
                }
            }
        }

        Ok(())
    }

    /// Infailable handle_append method
    async fn handle_append<T: TransportConnect>(
        &mut self,
        provider: &ReplicatedLogletProvider<T>,
        incoming: Incoming<Append>,
    ) {
        let (reciprocal, append) = incoming.split();

        let loglet = match self.get_loglet(provider, &append.header).await {
            Ok(loglet) => loglet,
            Err(err) => {
                return_error_status!(reciprocal, err);
            }
        };

        if !loglet.is_sequencer_local() {
            return_error_status!(reciprocal, SequencerStatus::NotSequencer);
        }

        let global_tail = loglet.known_global_tail();

        let loglet_commit = match loglet.enqueue_batch(append.payloads).await {
            Ok(loglet_commit) => loglet_commit,
            Err(err) => {
                return_error_status!(reciprocal, SequencerStatus::from(err), global_tail);
            }
        };

        let task = WaitAppendedTask {
            reciprocal,
            loglet_commit,
            global_tail: global_tail.clone(),
        };

        let _ = task_center().spawn_child(TaskKind::Disposable, "wait-appended", None, task.run());
    }

    async fn get_loglet<T: TransportConnect>(
        &self,
        provider: &ReplicatedLogletProvider<T>,
        header: &CommonRequestHeader,
    ) -> Result<Arc<ReplicatedLoglet<T>>, SequencerStatus> {
        let loglet = match provider.get_active_loglet(header.log_id, header.segment_index) {
            Some(loglet) if loglet.params().loglet_id == header.loglet_id => loglet,
            Some(_) => return Err(SequencerStatus::LogletIdMismatch),
            None => {
                let logs = self.metadata.logs();
                let chain = logs
                    .chain(&header.log_id)
                    .ok_or(SequencerStatus::UnknownLogId)?;

                let segment = chain
                    .iter()
                    .rev()
                    .find(|segment| segment.index() == header.segment_index)
                    .ok_or(SequencerStatus::UnknownSegmentIndex)?;

                provider
                    .get_or_create_loglet(
                        header.log_id,
                        header.segment_index,
                        &segment.config.params,
                    )
                    .map_err(SequencerStatus::from)?
            }
        };

        Ok(loglet)
    }
}

impl From<OperationError> for SequencerStatus {
    fn from(value: OperationError) -> Self {
        match value {
            OperationError::Shutdown(_) => SequencerStatus::Shutdown,
            OperationError::Other(err) => Self::Error {
                retryable: err.retryable(),
                message: err.to_string(),
            },
        }
    }
}

impl From<ReplicatedLogletError> for SequencerStatus {
    fn from(value: ReplicatedLogletError) -> Self {
        Self::Error {
            retryable: value.retryable(),
            message: value.to_string(),
        }
    }
}

struct WaitAppendedTask {
    loglet_commit: LogletCommit,
    reciprocal: Reciprocal,
    global_tail: TailOffsetWatch,
}

impl WaitAppendedTask {
    async fn run(self) -> anyhow::Result<()> {
        let appended = match self.loglet_commit.await {
            Ok(offset) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(self.global_tail.latest_offset()),
                    sealed: Some(self.global_tail.is_sealed()),
                    status: SequencerStatus::Ok,
                },
                first_offset: offset,
            },
            Err(AppendError::Sealed) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(self.global_tail.latest_offset()),
                    sealed: Some(self.global_tail.is_sealed()), // this must be true
                    status: SequencerStatus::Sealed,
                },
                first_offset: LogletOffset::INVALID,
            },
            Err(AppendError::Shutdown(_)) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(self.global_tail.latest_offset()),
                    sealed: Some(self.global_tail.is_sealed()),
                    status: SequencerStatus::Shutdown,
                },
                first_offset: LogletOffset::INVALID,
            },
            Err(AppendError::Other(err)) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(self.global_tail.latest_offset()),
                    sealed: Some(self.global_tail.is_sealed()),
                    status: SequencerStatus::Error {
                        retryable: err.retryable(),
                        message: err.to_string(),
                    },
                },
                first_offset: LogletOffset::INVALID,
            },
        };

        self.reciprocal.prepare(appended).send().await?;

        Ok(())
    }
}
