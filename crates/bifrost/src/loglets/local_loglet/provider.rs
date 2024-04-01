// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{hash_map, HashMap};
use std::sync::{Arc, OnceLock};

use anyhow::Context;
use async_trait::async_trait;
use restate_types::logs::metadata::LogletParams;
use tokio::sync::Mutex as AsyncMutex;
use tracing::debug;

use super::log_store::RocksDbLogStore;
use super::log_store_writer::RocksDbLogWriterHandle;
use super::{LocalLoglet, Options};
use crate::loglet::{Loglet, LogletOffset, LogletProvider};
use crate::loglets::local_loglet::log_store_writer::WriterOptions;
use crate::Error;
use crate::ProviderError;

#[derive(Debug)]
pub struct LocalLogletProvider {
    log_store: RocksDbLogStore,
    active_loglets: AsyncMutex<HashMap<String, Arc<LocalLoglet>>>,
    log_writer: OnceLock<RocksDbLogWriterHandle>,
    opts: Options,
}

impl LocalLogletProvider {
    pub fn new(opts: Options) -> Result<Arc<Self>, ProviderError> {
        let log_store = RocksDbLogStore::new(&opts).context("RockDb LogStore")?;

        Ok(Arc::new(Self {
            log_store,
            active_loglets: Default::default(),
            log_writer: OnceLock::new(),
            opts,
        }))
    }
}

#[async_trait]
impl LogletProvider for LocalLogletProvider {
    async fn get_loglet(
        &self,
        params: &LogletParams,
    ) -> Result<Arc<dyn Loglet<Offset = LogletOffset>>, Error> {
        let mut guard = self.active_loglets.lock().await;
        let log_writer = self
            .log_writer
            .get()
            .cloned()
            .expect("local loglet properly started");
        let loglet = match guard.entry(params.id().to_owned()) {
            hash_map::Entry::Vacant(entry) => {
                // Create loglet
                //
                // todo: Fix by making params richer config object.
                //
                // NOTE: We blatently assume that id() is a u64 unique id under the hood. Obviously
                // this is a shortcut and should be fixed by making LogletParams a richer config
                // object that can be used to create the loglet.
                let loglet = LocalLoglet::create(
                    params
                        .id()
                        .parse()
                        .expect("loglet params can be converted into u64"),
                    self.log_store.clone(),
                    log_writer,
                )
                .await?;
                let loglet = entry.insert(Arc::new(loglet));
                Arc::clone(loglet)
            }
            hash_map::Entry::Occupied(entry) => entry.get().clone(),
        };

        Ok(loglet as Arc<dyn Loglet>)
    }

    fn start(&self) -> Result<(), ProviderError> {
        let writer_options = WriterOptions {
            channel_size: self.opts.writer_queue_len,
            commit_time_interval: self.opts.writer_commit_time_interval.into(),
            batch_size_threshold: self.opts.writer_queue_len,
            flush_wal_on_commit: self.opts.flush_wal_on_commit,
            disable_wal: self.opts.rocksdb_disable_wal,
        };

        let log_writer = self.log_store.create_writer(writer_options).start()?;
        self.log_writer
            .set(log_writer)
            .expect("local loglet started once");
        debug!("Started a bifrost local loglet provider");
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ProviderError> {
        debug!("Shutting down local loglet provider");
        self.log_store.shutdown();
        Ok(())
    }
}
