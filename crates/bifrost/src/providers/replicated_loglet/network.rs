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
use std::time::Duration;

use futures::{Stream, StreamExt};
use restate_types::errors::MaybeRetryableError;
use tracing::trace;

use restate_core::network::{
    Incoming, MessageRouterBuilder, PeerMetadataVersion, Reciprocal, TransportConnect,
};
use restate_core::{
    cancellation_watcher, task_center, Metadata, MetadataKind, SyncError, TargetVersion, TaskKind,
};
use restate_types::config::ReplicatedLogletOptions;
use restate_types::logs::{LogletOffset, SequenceNumber};
use restate_types::net::replicated_loglet::{
    Append, Appended, CommonRequestHeader, CommonResponseHeader, GetSequencerState, SequencerState,
    SequencerStatus,
};

use super::error::ReplicatedLogletError;
use super::loglet::ReplicatedLoglet;
use super::provider::ReplicatedLogletProvider;
use crate::loglet::util::TailOffsetWatch;
use crate::loglet::{AppendError, Loglet, LogletCommit, OperationError};

type MessageStream<T> = Pin<Box<dyn Stream<Item = Incoming<T>> + Send + Sync + 'static>>;

macro_rules! return_error_status {
    ($reciprocal:expr, $status:expr, $tail:expr) => {{
        let msg = Appended {
            first_offset: LogletOffset::INVALID,
            header: CommonResponseHeader {
                known_global_tail: Some($tail.latest_offset()),
                sealed: Some($tail.is_sealed()),
                status: $status,
            },
        };

        let _ = task_center().spawn_child(
            TaskKind::Disposable,
            "append-return-error",
            None,
            async move {
                $reciprocal.prepare(msg).send().await?;
                Ok(())
            },
        );

        return;
    }};
    ($reciprocal:expr, $status:expr) => {{
        let msg = Appended {
            first_offset: LogletOffset::INVALID,
            header: CommonResponseHeader {
                known_global_tail: None,
                sealed: None,
                status: $status,
            },
        };

        let _ = task_center().spawn_child(
            TaskKind::Disposable,
            "append-return-error",
            None,
            async move {
                $reciprocal.prepare(msg).send().await?;
                Ok(())
            },
        );

        return;
    }};
}

pub struct RequestPump {
    metadata: Metadata,
    append_stream: MessageStream<Append>,
    get_sequencer_state_stream: MessageStream<GetSequencerState>,
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
        let get_sequencer_state_stream = router_builder.subscribe_to_stream(queue_length);
        Self {
            metadata,
            append_stream,
            get_sequencer_state_stream,
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
                Some(get_sequencer_state) = self.get_sequencer_state_stream.next() => {
                    self.handle_get_sequencer_state(&provider, get_sequencer_state).await;
                }
            }
        }

        Ok(())
    }

    async fn handle_get_sequencer_state<T: TransportConnect>(
        &mut self,
        provider: &ReplicatedLogletProvider<T>,
        incoming: Incoming<GetSequencerState>,
    ) {
        let loglet = match self
            .get_loglet(
                provider,
                incoming.metadata_version(),
                &incoming.body().header,
            )
            .await
        {
            Ok(loglet) => loglet,
            Err(err) => {
                return_error_status!(incoming.create_reciprocal(), err);
            }
        };

        let (reciprocal, _) = incoming.split();

        if !loglet.is_sequencer_local() {
            return_error_status!(reciprocal, SequencerStatus::NotSequencer);
        }

        let tail = loglet.known_global_tail().get();
        let sequencer_state = SequencerState {
            header: CommonResponseHeader {
                known_global_tail: Some(tail.offset()),
                sealed: Some(tail.is_sealed()),
                status: if tail.is_sealed() {
                    SequencerStatus::Sealed
                } else {
                    SequencerStatus::Ok
                },
            },
        };

        let _ = reciprocal.prepare(sequencer_state).try_send();
    }

    /// Infailable handle_append method
    async fn handle_append<T: TransportConnect>(
        &mut self,
        provider: &ReplicatedLogletProvider<T>,
        incoming: Incoming<Append>,
    ) {
        let loglet = match self
            .get_loglet(
                provider,
                incoming.metadata_version(),
                &incoming.body().header,
            )
            .await
        {
            Ok(loglet) => loglet,
            Err(err) => {
                return_error_status!(incoming.create_reciprocal(), err);
            }
        };

        let (reciprocal, append) = incoming.split();
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
        peer_version: &PeerMetadataVersion,
        header: &CommonRequestHeader,
    ) -> Result<Arc<ReplicatedLoglet<T>>, SequencerStatus> {
        let mut current_logs_version = provider.networking().metadata().logs_version();
        let request_logs_version = peer_version.logs.unwrap_or(current_logs_version);

        loop {
            if let Some(loglet) = provider.get_active_loglet(header.log_id, header.segment_index) {
                if loglet.params().loglet_id == header.loglet_id {
                    return Ok(loglet);
                }

                return Err(SequencerStatus::LogletIdMismatch);
            }

            match self.create_loglet(provider, header).await {
                Ok(loglet) => return Ok(loglet),
                Err(SequencerStatus::UnknownLogId | SequencerStatus::UnknownSegmentIndex) => {
                    // possible outdated metadata
                }
                Err(status) => return Err(status),
            }

            if request_logs_version > current_logs_version {
                match provider
                    .networking()
                    .metadata()
                    .sync(
                        MetadataKind::Logs,
                        TargetVersion::Version(request_logs_version),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(SyncError::Shutdown(_)) => return Err(SequencerStatus::Shutdown),
                    Err(SyncError::MetadataStore(err)) => {
                        tracing::trace!(error=%err, target_version=%request_logs_version, "Failed to sync metadata");
                        //todo(azmy): make timeout configurable
                        let result = tokio::time::timeout(
                            Duration::from_secs(2),
                            self.metadata
                                .wait_for_version(MetadataKind::Logs, request_logs_version),
                        )
                        .await;

                        match result {
                            Err(_elapsed) => {
                                tracing::trace!(
                                    "Timeout waiting on logs metadata version update to '{}'",
                                    request_logs_version
                                );
                            }
                            Ok(Err(_shutdown)) => return Err(SequencerStatus::Shutdown),
                            Ok(_) => {}
                        }
                    }
                }

                current_logs_version = provider.networking().metadata().logs_version();
            } else {
                return Err(SequencerStatus::UnknownLogId);
            }
        }
    }

    async fn create_loglet<T: TransportConnect>(
        &self,
        provider: &ReplicatedLogletProvider<T>,
        header: &CommonRequestHeader,
    ) -> Result<Arc<ReplicatedLoglet<T>>, SequencerStatus> {
        // search the chain
        let logs = self.metadata.logs_ref();
        let chain = logs
            .chain(&header.log_id)
            .ok_or(SequencerStatus::UnknownLogId)?;

        let segment = chain
            .iter()
            .rev()
            .find(|segment| segment.index() == header.segment_index)
            .ok_or(SequencerStatus::UnknownSegmentIndex)?;

        provider
            .get_or_create_loglet(header.log_id, header.segment_index, &segment.config.params)
            .map_err(SequencerStatus::from)
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
