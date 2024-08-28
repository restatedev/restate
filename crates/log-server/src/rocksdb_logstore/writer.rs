// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo: remove after scaffolding is complete
#![allow(unused)]

use std::sync::Arc;

use bytes::BytesMut;
use futures::StreamExt as FutureStreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::debug;

use restate_bifrost::loglet::OperationError;
use restate_core::{cancellation_watcher, ShutdownError, TaskCenter, TaskKind};
use restate_rocksdb::RocksDb;
use restate_types::config::LogServerOptions;
use restate_types::live::BoxedLiveLoad;

type Ack = oneshot::Sender<Result<(), OperationError>>;
type AckRecv = oneshot::Receiver<Result<(), OperationError>>;

const INITIAL_SERDE_BUFFER_SIZE: usize = 16_384; // Initial capacity 16KiB

pub struct LogStoreWriteCommand {}

pub(crate) struct LogStoreWriter {
    rocksdb: Arc<RocksDb>,
    batch_acks_buf: Vec<Ack>,
    buffer: BytesMut,
    updateable_options: BoxedLiveLoad<LogServerOptions>,
}

impl LogStoreWriter {
    pub(crate) fn new(
        rocksdb: Arc<RocksDb>,
        updateable_options: BoxedLiveLoad<LogServerOptions>,
    ) -> Self {
        Self {
            rocksdb,
            batch_acks_buf: Vec::default(),
            buffer: BytesMut::with_capacity(INITIAL_SERDE_BUFFER_SIZE),
            updateable_options,
        }
    }

    /// Must be called from task_center context
    pub fn start(mut self, tc: &TaskCenter) -> Result<RocksDbLogWriterHandle, ShutdownError> {
        // big enough to allows a second full batch to queue up while the existing one is being processed
        let batch_size = std::cmp::max(
            1,
            self.updateable_options
                .live_load()
                .writer_batch_commit_count,
        );
        // leave twice as much space in the the channel to ensure we can enqueue up-to a full batch in
        // the backlog while we process this one.
        let (sender, receiver) = mpsc::channel(batch_size * 2);

        tc.spawn_child(
            TaskKind::SystemService,
            "log-server-rocksdb-writer",
            None,
            async move {
                debug!("Start running LogStoreWriter");
                let mut opts = self.updateable_options.clone();
                let mut receiver =
                    std::pin::pin!(ReceiverStream::new(receiver).ready_chunks(batch_size));
                let mut cancel = std::pin::pin!(cancellation_watcher());

                loop {
                    tokio::select! {
                        biased;
                        _ = &mut cancel => {
                            break;
                        }
                        Some(cmds) = TokioStreamExt::next(&mut receiver) => {
                                self.handle_commands(opts.live_load(), cmds).await;
                        }
                    }
                }
                debug!("LogStore loglet writer task finished");
                Ok(())
            },
        )?;
        Ok(RocksDbLogWriterHandle { sender })
    }

    async fn handle_commands(
        &mut self,
        opts: &LogServerOptions,
        commands: Vec<LogStoreWriteCommand>,
    ) {
    }
}

#[derive(Debug, Clone)]
pub struct RocksDbLogWriterHandle {
    sender: mpsc::Sender<LogStoreWriteCommand>,
}
