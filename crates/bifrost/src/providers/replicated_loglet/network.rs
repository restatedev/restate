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

use tracing::instrument;

use restate_core::network::{
    Handler, Incoming, Oneshot, RawSvcRpc, Reciprocal, Rpc, TransportConnect,
};
use restate_core::{Metadata, MetadataKind, TaskCenter, TaskKind};
use restate_types::Version;
use restate_types::errors::MaybeRetryableError;
use restate_types::logs::metadata::ProviderKind;
use restate_types::logs::{LogletOffset, SequenceNumber, TailOffsetWatch};
use restate_types::net::RpcRequest;
use restate_types::net::replicated_loglet::{
    Append, Appended, CommonRequestHeader, CommonResponseHeader, GetSequencerState,
    SequencerDataService, SequencerMetaService, SequencerState, SequencerStatus,
};

use super::error::ReplicatedLogletError;
use super::loglet::{FindTailFlags, ReplicatedLoglet};
use super::provider::ReplicatedLogletProvider;
use crate::loglet::{AppendError, Loglet, LogletCommit, OperationError};

macro_rules! return_error_status {
    ($reciprocal:expr, $status:expr, $tail:expr) => {{
        let msg = Appended {
            last_offset: LogletOffset::INVALID,
            header: CommonResponseHeader {
                known_global_tail: Some($tail.latest_offset()),
                sealed: Some($tail.is_sealed()),
                status: Some($status),
            },
        };

        $reciprocal.send(msg);
        return;
    }};
    ($reciprocal:expr, $status:expr) => {{
        let msg = Appended {
            last_offset: LogletOffset::INVALID,
            header: CommonResponseHeader {
                known_global_tail: None,
                sealed: None,
                status: Some($status),
            },
        };

        $reciprocal.send(msg);
        return;
    }};
}

pub struct SequencerDataRpcHandler<T> {
    provider: Arc<ReplicatedLogletProvider<T>>,
}

impl<T> SequencerDataRpcHandler<T> {
    pub fn new(provider: Arc<ReplicatedLogletProvider<T>>) -> Self {
        Self { provider }
    }
}

impl<T: TransportConnect> Handler for SequencerDataRpcHandler<T> {
    type Service = SequencerDataService;
    /// handle rpc request
    async fn on_rpc(&mut self, message: Incoming<RawSvcRpc<Self::Service>>) {
        if message.msg_type() == Append::TYPE {
            let request = message.into_typed::<Append>();
            self.handle_append(request).await;
        }
    }
}

impl<T: TransportConnect> SequencerDataRpcHandler<T> {
    /// Infallible handle_append method
    #[instrument(
        level="trace",
        skip_all,
        fields(
            otel.name = "replicatged_loglet::network: handle_append",
        )
    )]
    async fn handle_append(&mut self, incoming: Incoming<Rpc<Append>>) {
        let peer_logs_version = incoming.metadata_version().get(MetadataKind::Logs);
        let Some((reciprocal, append)) = incoming.split() else {
            return;
        };

        let loglet = match get_loglet(&self.provider, peer_logs_version, &append.header).await {
            Ok(loglet) => loglet,
            Err(err) => {
                return_error_status!(reciprocal, err);
            }
        };

        if !loglet.is_sequencer_local() {
            return_error_status!(reciprocal, SequencerStatus::NotSequencer);
        }

        let global_tail = loglet.known_global_tail();

        let loglet_commit = match loglet.enqueue_batch(append.payloads.into()).await {
            Ok(loglet_commit) => loglet_commit,
            Err(err) => {
                return_error_status!(reciprocal, SequencerStatus::from(err), global_tail);
            }
        };

        let task = WaitForCommitTask {
            reciprocal,
            loglet_commit,
            global_tail: global_tail.clone(),
        };

        tokio::spawn(task.run());
    }
}

pub struct SequencerInfoRpcHandler<T> {
    provider: Arc<ReplicatedLogletProvider<T>>,
}

impl<T> SequencerInfoRpcHandler<T> {
    pub fn new(provider: Arc<ReplicatedLogletProvider<T>>) -> Self {
        Self { provider }
    }
}

impl<T: TransportConnect> Handler for SequencerInfoRpcHandler<T> {
    type Service = SequencerMetaService;
    /// handle rpc request
    async fn on_rpc(&mut self, message: Incoming<RawSvcRpc<Self::Service>>) {
        if message.msg_type() == GetSequencerState::TYPE {
            let request = message.into_typed::<GetSequencerState>();
            self.handle_get_sequencer_state(request).await;
        }
    }
}

impl<T: TransportConnect> SequencerInfoRpcHandler<T> {
    #[instrument(level = "debug", skip_all)]
    async fn handle_get_sequencer_state(&mut self, incoming: Incoming<Rpc<GetSequencerState>>) {
        let peer_logs_version = incoming.metadata_version().get(MetadataKind::Logs);
        let Some((reciprocal, msg)) = incoming.split() else {
            return;
        };

        let loglet = match get_loglet(&self.provider, peer_logs_version, &msg.header).await {
            Ok(loglet) => loglet,
            Err(err) => {
                let response = SequencerState {
                    header: CommonResponseHeader {
                        known_global_tail: None,
                        sealed: None,
                        status: Some(err),
                    },
                };
                reciprocal.send(response);
                return;
            }
        };

        if !loglet.is_sequencer_local() {
            let response = SequencerState {
                header: CommonResponseHeader {
                    known_global_tail: None,
                    sealed: None,
                    status: Some(SequencerStatus::NotSequencer),
                },
            };
            reciprocal.send(response);
            return;
        }

        if msg.force_seal_check {
            let _ = TaskCenter::spawn(TaskKind::Disposable, "remote-check-seal", async move {
                match loglet.find_tail_inner(FindTailFlags::ForceSealCheck).await {
                    Ok(tail) => {
                        let sequencer_state = SequencerState {
                            header: CommonResponseHeader {
                                known_global_tail: Some(tail.offset()),
                                sealed: Some(tail.is_sealed()),
                                status: None,
                            },
                        };
                        reciprocal.send(sequencer_state);
                    }
                    Err(err) => {
                        let failure = SequencerState {
                            header: CommonResponseHeader {
                                known_global_tail: None,
                                sealed: None,
                                status: Some(SequencerStatus::Error {
                                    retryable: true,
                                    message: err.to_string(),
                                }),
                            },
                        };
                        reciprocal.send(failure);
                    }
                }
                Ok(())
            });
        } else {
            // if we are not forced to check the seal, we can just return the last known tail from the
            // sequencer's view
            let tail = loglet.last_known_global_tail();
            let sequencer_state = SequencerState {
                header: CommonResponseHeader {
                    known_global_tail: Some(tail.offset()),
                    sealed: Some(tail.is_sealed()),
                    status: None,
                },
            };
            reciprocal.send(sequencer_state);
        }
    }
}

fn create_loglet<T: TransportConnect>(
    provider: &ReplicatedLogletProvider<T>,
    header: &CommonRequestHeader,
) -> Result<Arc<ReplicatedLoglet<T>>, SequencerStatus> {
    // search the chain
    let logs = Metadata::with_current(|m| m.logs_ref());
    let chain = logs
        .chain(&header.log_id)
        .ok_or(SequencerStatus::UnknownLogId)?;

    let segment = chain
        .find_segment_by_index(header.segment_index, ProviderKind::Replicated)
        .ok_or(SequencerStatus::UnknownSegmentIndex)?;

    debug_assert_eq!(segment.config.kind, ProviderKind::Replicated);

    provider
        .get_or_create_loglet(header.log_id, header.segment_index, &segment.config.params)
        .map_err(SequencerStatus::from)
}

async fn get_loglet<T: TransportConnect>(
    provider: &ReplicatedLogletProvider<T>,
    request_logs_version: Version,
    header: &CommonRequestHeader,
) -> Result<Arc<ReplicatedLoglet<T>>, SequencerStatus> {
    let metadata = Metadata::current();
    let mut current_logs_version = metadata.logs_version();

    loop {
        if let Some(loglet) = provider.get_active_loglet(header.log_id, header.segment_index) {
            if loglet.params().loglet_id == header.loglet_id {
                return Ok(loglet);
            }

            return Err(SequencerStatus::LogletIdMismatch);
        }

        match create_loglet(provider, header) {
            Ok(loglet) => return Ok(loglet),
            Err(SequencerStatus::UnknownLogId | SequencerStatus::UnknownSegmentIndex) => {
                // possible outdated metadata
            }
            Err(err) => return Err(err),
        }

        if request_logs_version > current_logs_version {
            tracing::trace!(%current_logs_version, target_version=%request_logs_version, "We don't have the required logs metadata version, waiting");
            match metadata
                .wait_for_version(MetadataKind::Logs, request_logs_version)
                .await
            {
                Err(_) => return Err(SequencerStatus::Shutdown),
                Ok(version) => {
                    current_logs_version = version;
                }
            }
        } else {
            return Err(SequencerStatus::UnknownLogId);
        }
    }
}

impl From<OperationError> for SequencerStatus {
    fn from(value: OperationError) -> Self {
        match value {
            OperationError::Shutdown(_) => SequencerStatus::Shutdown,
            OperationError::Other(err) => SequencerStatus::Error {
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

struct WaitForCommitTask {
    loglet_commit: LogletCommit,
    reciprocal: Reciprocal<Oneshot<Appended>>,
    global_tail: TailOffsetWatch,
}

impl WaitForCommitTask {
    async fn run(self) {
        let appended = match self.loglet_commit.await {
            Ok(offset) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(self.global_tail.latest_offset()),
                    sealed: Some(self.global_tail.is_sealed()),
                    status: None,
                },
                last_offset: offset,
            },
            Err(AppendError::Sealed) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(self.global_tail.latest_offset()),
                    sealed: Some(self.global_tail.is_sealed()), // this must be true
                    status: Some(SequencerStatus::Sealed),
                },
                last_offset: LogletOffset::INVALID,
            },
            Err(AppendError::ReconfigurationNeeded(_)) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(self.global_tail.latest_offset()),
                    sealed: Some(self.global_tail.is_sealed()), // this must be true
                    status: Some(SequencerStatus::Gone),
                },
                last_offset: LogletOffset::INVALID,
            },
            Err(AppendError::Shutdown(_)) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(self.global_tail.latest_offset()),
                    sealed: Some(self.global_tail.is_sealed()),
                    status: Some(SequencerStatus::Shutdown),
                },
                last_offset: LogletOffset::INVALID,
            },
            Err(AppendError::Other(err)) => Appended {
                header: CommonResponseHeader {
                    known_global_tail: Some(self.global_tail.latest_offset()),
                    sealed: Some(self.global_tail.is_sealed()),
                    status: Some(SequencerStatus::Error {
                        retryable: err.retryable(),
                        message: err.to_string(),
                    }),
                },
                last_offset: LogletOffset::INVALID,
            },
        };

        self.reciprocal.send(appended);
    }
}
