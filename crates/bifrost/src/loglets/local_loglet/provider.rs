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
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use restate_types::config::{LocalLogletOptions, RocksDbOptions};
use restate_types::live::LiveLoad;
use restate_types::logs::metadata::{LogletParams, ProviderKind};
use tokio::sync::Mutex as AsyncMutex;
use tracing::debug;

use super::log_store::RocksDbLogStore;
use super::log_store_writer::RocksDbLogWriterHandle;
use super::{metric_definitions, LocalLoglet};
use crate::loglet::{Loglet, LogletOffset};
use crate::ProviderError;
use crate::{Error, LogletProvider};

pub struct Factory<T>
where
    T: LiveLoad<LocalLogletOptions> + Send + 'static,
{
    log_store: RocksDbLogStore,
    options: T,
}

impl<T> Factory<T>
where
    T: LiveLoad<LocalLogletOptions> + Send + 'static,
{
    pub fn new(
        mut options: T,
        updateable_rocksdb_options: impl LiveLoad<RocksDbOptions> + Send + 'static,
    ) -> Self {
        let log_store = RocksDbLogStore::new(options.live_load(), updateable_rocksdb_options)
            .context("RocksDb LogStore")
            .unwrap();
        Self { log_store, options }
    }
}

#[async_trait]
impl<T> crate::LogletProviderFactory for Factory<T>
where
    T: LiveLoad<LocalLogletOptions> + Send + 'static,
{
    fn kind(&self) -> ProviderKind {
        ProviderKind::Local
    }

    async fn create(self: Box<Self>) -> Result<Arc<dyn LogletProvider>, ProviderError> {
        metric_definitions::describe_metrics();
        let Factory {
            log_store,
            options,
            // updateable_rocksdb_options,
        } = *self;
        let log_writer = log_store.create_writer().start(options)?;
        debug!("Started a bifrost local loglet provider");
        Ok(Arc::new(LocalLogletProvider {
            log_store,
            active_loglets: Default::default(),
            log_writer,
        }))
    }
}

pub(crate) struct LocalLogletProvider {
    log_store: RocksDbLogStore,
    active_loglets: AsyncMutex<HashMap<String, Arc<LocalLoglet>>>,
    log_writer: RocksDbLogWriterHandle,
}

#[async_trait]
impl LogletProvider for LocalLogletProvider {
    async fn get_loglet(
        &self,
        params: &LogletParams,
    ) -> Result<Arc<dyn Loglet<Offset = LogletOffset>>, Error> {
        let mut guard = self.active_loglets.lock().await;
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
                    self.log_writer.clone(),
                )
                .await?;
                let loglet = entry.insert(Arc::new(loglet));
                Arc::clone(loglet)
            }
            hash_map::Entry::Occupied(entry) => entry.get().clone(),
        };

        Ok(loglet as Arc<dyn Loglet>)
    }

    async fn shutdown(&self) -> Result<(), ProviderError> {
        Ok(())
    }
}
