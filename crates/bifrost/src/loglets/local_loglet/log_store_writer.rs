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
use restate_types::arc_util::Updateable;
use restate_types::config::LocalLogletOptions;
use rocksdb::{WriteBatch, DB};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace, warn};

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
    current_batch: WriteBatch,
    current_batch_acks: Vec<Ack>,
    db: Arc<DB>,
    manual_wal_flush: bool,
}

impl LogStoreWriter {
    pub(crate) fn new(db: Arc<DB>, manual_wal_flush: bool) -> Self {
        Self {
            current_batch: WriteBatch::default(),
            current_batch_acks: Default::default(),
            db,
            manual_wal_flush,
        }
    }

    /// Must be called from task_center context
    pub fn start(
        mut self,
        mut updateable: impl Updateable<LocalLogletOptions> + Send + 'static,
    ) -> Result<RocksDbLogWriterHandle, ShutdownError> {
        let (sender, mut receiver) = mpsc::channel(updateable.load().writer_queue_len);

        task_center().spawn_child(
            TaskKind::LogletProvider,
            "local-loglet-writer",
            None,
            async move {
                loop {
                    let writer_commit_time = updateable.load().writer_commit_time_interval.into();
                    tokio::select! {
                        biased;
                        _ = cancellation_watcher() => {
                            break;
                        }
                        // freeze block. change this to be a timer that gets reset on every
                        // iteratoion
                        _ = tokio::time::sleep(writer_commit_time) => {
                            if !self.current_batch.is_empty() {
                                let opts = updateable.load();
                                trace!("Triggering local loglet write batch commit due to max latency");
                                self.commit(opts);
                            }
                        }
                        // watch config changes
                        cmd = receiver.recv() => {
                            if let Some(cmd) = cmd {
                                let opts = updateable.load();
                                self.handle_command(opts, cmd);
                            } else {
                                break;
                            }
                        }
                    }
                }
                self.commit(updateable.load());
                debug!("Local loglet writer task finished");
                Ok(())
            },
        )?;
        Ok(RocksDbLogWriterHandle { sender })
    }

    fn handle_command(&mut self, opts: &LocalLogletOptions, command: LogStoreWriteCommand) {
        if let Some(data_command) = command.data_update {
            match data_command {
                DataUpdate::PutRecord { offset, data } => {
                    self.put_record(command.log_id, offset, data)
                }
            }
        }

        if let Some(logstate_updates) = command.log_state_updates {
            self.update_log_state(command.log_id, logstate_updates)
        }

        if let Some(ack) = command.ack {
            self.current_batch_acks.push(ack);
        }

        if self.current_batch.len() >= opts.writer_commit_batch_size_threshold {
            trace!("Local loglet write batch is full, committing");
            self.commit(opts);
        }
    }

    fn update_log_state(&mut self, log_id: u64, updates: LogStateUpdates) {
        let metadata_cf = self.db.cf_handle(METADATA_CF).expect("metadata cf exists");
        self.current_batch.merge_cf(
            metadata_cf,
            MetadataKey::new(log_id, MetadataKind::LogState).to_bytes(),
            updates.to_bytes(),
        );
    }

    fn put_record(&mut self, id: u64, offset: LogletOffset, data: Bytes) {
        let key = RecordKey::new(id, offset);
        let data_cf = self.db.cf_handle(DATA_CF).expect("data cf exists");
        self.current_batch.put_cf(data_cf, &key.to_bytes(), data);
    }

    fn commit(&mut self, opts: &LocalLogletOptions) {
        let mut rocksdb_write_options = rocksdb::WriteOptions::new();
        rocksdb_write_options.disable_wal(opts.rocksdb.rocksdb_disable_wal());

        let current_batch = std::mem::take(&mut self.current_batch);
        trace!(
            "Committing local loglet current write batch: {} items",
            current_batch.len(),
        );

        if let Err(e) = self.db.write_opt(current_batch, &rocksdb_write_options) {
            warn!("Failed to commit local loglet write batch: {}", e);
            self.send_acks(Err(Error::LogStoreError(e.into())));
            return;
        }

        if self.manual_wal_flush {
            if let Err(e) = self.db.flush_wal(true) {
                warn!("Failed to flush rocksdb WAL in local loglet : {}", e);
                self.send_acks(Err(Error::LogStoreError(e.into())));
                return;
            }
        }

        self.send_acks(Ok(()));
    }

    fn send_acks(&mut self, result: Result<(), Error>) {
        self.current_batch_acks.drain(..).for_each(|a| {
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
