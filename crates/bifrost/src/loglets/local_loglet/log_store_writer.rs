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

use bytes::Bytes;
use restate_rocksdb::{IoMode, Priority, RocksDb};
use restate_types::arc_util::Updateable;
use restate_types::config::LocalLogletOptions;
use rocksdb::{BoundColumnFamily, WriteBatch, DB};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, trace, warn};

use restate_core::{cancellation_watcher, task_center, ShutdownError, TaskKind};

use crate::loglet::LogletOffset;
use crate::Error;

use super::keys::{MetadataKey, MetadataKind, RecordKey};
use super::log_state::LogStateUpdates;
use super::log_store::{DATA_CF, METADATA_CF};

type Ack = oneshot::Sender<Result<(), Error>>;
type AckRecv = oneshot::Receiver<Result<(), Error>>;

pub struct LogStoreWriteCommand {
    log_id: u64,
    data_update: Option<DataUpdate>,
    log_state_updates: Option<LogStateUpdates>,
    ack: Option<Ack>,
}

enum DataUpdate {
    PutRecord { offset: LogletOffset, data: Bytes },
}

pub(crate) struct LogStoreWriter {
    db: Arc<DB>,
    rocksdb: Arc<RocksDb>,
    batch_acks_buf: Vec<Ack>,
    manual_wal_flush: bool,
}

impl LogStoreWriter {
    pub(crate) fn new(db: Arc<DB>, rocksdb: Arc<RocksDb>, manual_wal_flush: bool) -> Self {
        Self {
            db,
            rocksdb,
            batch_acks_buf: Vec::default(),
            manual_wal_flush,
        }
    }

    /// Must be called from task_center context
    pub fn start(
        mut self,
        mut updateable: impl Updateable<LocalLogletOptions> + Send + 'static,
    ) -> Result<RocksDbLogWriterHandle, ShutdownError> {
        // big enough to allows a second full batch to queue up while the existing one is being processed
        let batch_size = std::cmp::max(1, updateable.load().writer_batch_commit_count);
        let (sender, receiver) = mpsc::channel(batch_size * 2);

        task_center().spawn_child(
            TaskKind::LogletProvider,
            "local-loglet-writer",
            None,
            async move {
                let opts = updateable.load();
                let batch_size = std::cmp::max(1, opts.writer_batch_commit_count);
                let batch_duration = opts.writer_batch_commit_duration.into();
                let receiver =
                    ReceiverStream::new(receiver).chunks_timeout(batch_size, batch_duration);
                tokio::pin!(receiver);

                loop {
                    tokio::select! {
                        biased;
                        _ = cancellation_watcher() => {
                            break;
                        }
                        Some(cmds) = receiver.next() => {
                                let opts = updateable.load();
                                self.handle_commands(opts, cmds).await;
                        }
                    }
                }
                debug!("Local loglet writer task finished");
                Ok(())
            },
        )?;
        Ok(RocksDbLogWriterHandle { sender })
    }

    async fn handle_commands(
        &mut self,
        opts: &LocalLogletOptions,
        commands: Vec<LogStoreWriteCommand>,
    ) {
        let mut write_batch = WriteBatch::default();
        self.batch_acks_buf.clear();
        self.batch_acks_buf.reserve(commands.len());
        let batch_acks = &mut self.batch_acks_buf;

        {
            let data_cf = self.db.cf_handle(DATA_CF).expect("data cf exists");
            let metadata_cf = self.db.cf_handle(METADATA_CF).expect("metadata cf exists");

            for command in commands {
                if let Some(data_command) = command.data_update {
                    match data_command {
                        DataUpdate::PutRecord { offset, data } => Self::put_record(
                            &data_cf,
                            &mut write_batch,
                            command.log_id,
                            offset,
                            data,
                        ),
                    }
                }

                if let Some(logstate_updates) = command.log_state_updates {
                    Self::update_log_state(
                        &metadata_cf,
                        &mut write_batch,
                        command.log_id,
                        logstate_updates,
                    )
                }

                if let Some(ack) = command.ack {
                    batch_acks.push(ack);
                }
            }
        }

        self.commit(opts, write_batch).await;
    }

    fn update_log_state(
        metadata_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        log_id: u64,
        updates: LogStateUpdates,
    ) {
        write_batch.merge_cf(
            metadata_cf,
            MetadataKey::new(log_id, MetadataKind::LogState).to_bytes(),
            updates.to_bytes(),
        );
    }

    fn put_record(
        data_cf: &Arc<BoundColumnFamily>,
        write_batch: &mut WriteBatch,
        id: u64,
        offset: LogletOffset,
        data: Bytes,
    ) {
        let key = RecordKey::new(id, offset);
        write_batch.put_cf(data_cf, &key.to_bytes(), data);
    }

    async fn commit(&mut self, opts: &LocalLogletOptions, write_batch: WriteBatch) {
        let mut write_opts = rocksdb::WriteOptions::new();
        write_opts.disable_wal(opts.rocksdb.rocksdb_disable_wal());

        trace!(
            "Committing local loglet current write batch: {} items",
            write_batch.len(),
        );
        let result = self
            .rocksdb
            .write_batch(Priority::High, IoMode::Default, write_opts, write_batch)
            .await;

        if let Err(e) = result {
            error!("Failed to commit local loglet write batch: {}", e);
            self.send_acks(Err(Error::LogStoreError(e.into())));
            return;
        }

        if self.manual_wal_flush {
            // WAL flush is done in the forground, but sync will happen in the background to avoid
            // blocking IO.
            if let Err(e) = self.db.flush_wal(false) {
                warn!("Failed to flush rocksdb WAL in local loglet : {}", e);
                self.send_acks(Err(Error::LogStoreError(e.into())));
                return;
            }
            self.rocksdb.run_bg_wal_sync();
        }

        self.send_acks(Ok(()));
    }

    fn send_acks(&mut self, result: Result<(), Error>) {
        self.batch_acks_buf.drain(..).for_each(|a| {
            let _ = a.send(result.clone());
        });
    }
}

#[derive(Debug, Clone)]
pub struct RocksDbLogWriterHandle {
    sender: mpsc::Sender<LogStoreWriteCommand>,
}

impl RocksDbLogWriterHandle {
    pub async fn enqueue_put_record(
        &self,
        log_id: u64,
        offset: LogletOffset,
        data: Bytes,
        release_immediately: bool,
    ) -> Result<AckRecv, ShutdownError> {
        let (ack, receiver) = oneshot::channel();
        let data_update = Some(DataUpdate::PutRecord { offset, data });
        let log_state_updates = if release_immediately {
            Some(LogStateUpdates::default().update_release_pointer(offset))
        } else {
            None
        };
        if let Err(e) = self
            .sender
            .send(LogStoreWriteCommand {
                log_id,
                data_update,
                log_state_updates,
                ack: Some(ack),
            })
            .await
        {
            warn!(
                "Local loglet writer task is gone, not accepting the record: {}",
                e
            );
            return Err(ShutdownError);
        }

        Ok(receiver)
    }
}
