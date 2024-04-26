// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{WriteBatch, DB};
use futures::ready;
use futures_util::FutureExt;
use restate_storage_api::StorageError;
use restate_types::arc_util::Updateable;
use restate_types::config::StorageOptions;
use restate_types::errors::ThreadJoinError;
use rocksdb::WriteOptions;
use std::future::Future;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tracing::debug;

pub struct WriteCommand {
    write_batch: WriteBatch,
    response_tx: Sender<Result<(), StorageError>>,
}

impl WriteCommand {
    pub fn new(write_batch: WriteBatch, response_tx: Sender<Result<(), StorageError>>) -> Self {
        Self {
            write_batch,
            response_tx,
        }
    }
}

type Error = anyhow::Error;

pub struct Writer {
    db: Arc<DB>,
    opts: Box<dyn Updateable<StorageOptions> + Send + 'static>,
    rx: UnboundedReceiver<WriteCommand>,

    // for creating `WriterHandle`s
    tx: UnboundedSender<WriteCommand>,
}

impl Writer {
    pub fn new(db: Arc<DB>, opts: impl Updateable<StorageOptions> + Send + 'static) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            db,
            opts: Box::new(opts),
            rx,
            tx,
        }
    }

    pub fn create_writer_handle(&self) -> WriterHandle {
        WriterHandle {
            tx: self.tx.clone(),
        }
    }

    pub fn run(self, shutdown_watch: drain::Watch) -> JoinHandle {
        let (tx, rx) = tokio::sync::oneshot::channel();

        std::thread::Builder::new()
            .name("rs:rocksdb".to_owned())
            .spawn(|| {
                // AssertUnwindSafe is safe because we don't access self after catch_unwind again
                let result = panic::catch_unwind(AssertUnwindSafe(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .build()
                        .expect("current thread runtime should be creatable")
                        .block_on(self.run_inner(shutdown_watch))
                }));

                // we don't care if the receiver is dropped
                let _ = tx.send(result);
            })
            .expect("RocksDB writer thread should be spawnable");

        JoinHandle::new(rx)
    }

    async fn run_inner(mut self, shutdown_watch: drain::Watch) -> Result<(), Error> {
        let mut replies: Vec<Sender<Result<(), StorageError>>> = Vec::new();

        let shutdown_signal = shutdown_watch.signaled();
        tokio::pin!(shutdown_signal);

        loop {
            tokio::select! {
                Some(write_command) = self.rx.recv() => {
                    self.handle_write_command(write_command, &mut replies)?;
                },
                _ = &mut shutdown_signal => {
                    debug!("Stopping RocksDB writer thread");
                    break;
                }
            }
        }

        Ok(())
    }

    fn handle_write_command(
        &mut self,
        WriteCommand {
            write_batch,
            response_tx,
        }: WriteCommand,
        replies: &mut Vec<Sender<Result<(), StorageError>>>,
    ) -> Result<(), Error> {
        replies.push(response_tx);
        let mut write_options = WriteOptions::default();
        let opts = self.opts.load();
        write_options.disable_wal(opts.rocksdb.rocksdb_disable_wal());

        if !try_write_batch(&self.db, replies, &write_batch, &write_options) {
            return Ok(());
        }
        //
        // optimistically try taking more batches
        //
        while let Ok(WriteCommand {
            write_batch,
            response_tx,
        }) = self.rx.try_recv()
        {
            replies.push(response_tx);
            if !try_write_batch(&self.db, replies, &write_batch, &write_options) {
                return Ok(());
            }
        }

        if !opts.rocksdb.rocksdb_disable_wal() {
            //
            // okay now that we wrote everything, let us commit
            //
            self.db.flush_wal(opts.sync_wal_on_flush)?;
        }
        //
        // notify everyone of the success
        //
        replies.drain(..).for_each(|f| {
            let _ = f.send(Ok(()));
        });

        Ok(())
    }
}

fn try_write_batch(
    db: &Arc<DB>,
    futures: &mut Vec<Sender<Result<(), StorageError>>>,
    batch: &WriteBatch,
    write_opts: &WriteOptions,
) -> bool {
    let result = db
        .write_opt(batch, write_opts)
        .map_err(|error| StorageError::Generic(error.into()));
    if result.is_ok() {
        return true;
    }
    //
    // oops one of the batches failed, notify the others.
    //
    debug_assert!(!futures.is_empty());
    let last = futures.len() - 1;
    for f in futures.drain(..last) {
        let _ = f.send(Err(StorageError::OperationalError));
    }
    let oneshot = futures.drain(..).last().unwrap();
    let _ = oneshot.send(result);
    false
}

#[derive(Debug, Clone)]
pub struct WriterHandle {
    tx: UnboundedSender<WriteCommand>,
}

impl WriterHandle {
    pub async fn write(&self, write_batch: WriteBatch) -> Result<(), StorageError> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let command = WriteCommand::new(write_batch, response_tx);
        self.tx
            .send(command)
            .map_err(|_| StorageError::OperationalError)?;
        response_rx
            .await
            .map_err(|_| StorageError::OperationalError)?
    }
}

pub struct JoinHandle {
    rx: tokio::sync::oneshot::Receiver<std::thread::Result<Result<(), Error>>>,
}

impl JoinHandle {
    fn new(rx: tokio::sync::oneshot::Receiver<std::thread::Result<Result<(), Error>>>) -> Self {
        Self { rx }
    }
}

impl Future for JoinHandle {
    type Output = Result<Result<(), Error>, ThreadJoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(self.rx.poll_unpin(cx));

        Poll::Ready(
            result
                .map_err(|_| ThreadJoinError::UnexpectedTermination)
                .and_then(|result| {
                    result.map_err(|panic| {
                        ThreadJoinError::Panic(sync_wrapper::SyncWrapper::new(panic))
                    })
                }),
        )
    }
}
