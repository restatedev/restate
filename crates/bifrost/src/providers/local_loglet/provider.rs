// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, hash_map};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::debug;

use restate_core::{TaskCenter, TaskKind};
use restate_types::config::LocalLogletOptions;
use restate_types::live::BoxLiveLoad;
use restate_types::logs::metadata::{
    Chain, LogletParams, ProviderConfiguration, ProviderKind, SegmentIndex,
};
use restate_types::logs::{LogId, LogletId};

use super::log_store::RocksDbLogStore;
use super::log_store_writer::RocksDbLogWriterHandle;
use super::{LocalLoglet, metric_definitions};
use crate::Error;
use crate::loglet::{Loglet, LogletProvider, LogletProviderFactory, OperationError};

pub struct Factory {
    options: BoxLiveLoad<LocalLogletOptions>,
}

impl Factory {
    pub fn new(options: BoxLiveLoad<LocalLogletOptions>) -> Self {
        Self { options }
    }
}

#[async_trait]
impl LogletProviderFactory for Factory {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Local
    }

    async fn create(self: Box<Self>) -> Result<Arc<dyn LogletProvider>, OperationError> {
        metric_definitions::describe_metrics();
        let Factory { options } = *self;

        debug!("Started a bifrost local loglet provider");
        Ok(Arc::new(LocalLogletProvider {
            options,
            inner: Arc::new(Mutex::new(LocalLogletProviderInner::default())),
        }))
    }
}

#[derive(Clone, Default)]
struct LocalLogletProviderInner {
    log_store: Option<RocksDbLogStore>,
    log_writer: Option<RocksDbLogWriterHandle>,
    active_loglets: HashMap<(LogId, SegmentIndex), Arc<LocalLoglet>>,
}

pub(crate) struct LocalLogletProvider {
    options: BoxLiveLoad<LocalLogletOptions>,
    inner: Arc<Mutex<LocalLogletProviderInner>>,
}

#[async_trait]
impl LogletProvider for LocalLogletProvider {
    async fn get_loglet(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
        params: &LogletParams,
    ) -> Result<Arc<dyn Loglet>, Error> {
        let inner = self.inner.clone();
        let options = self.options.clone();
        let params = params.clone();

        // RockesDbLogStore::create is NOT cancellation safe. This is why we spawn this code as its own task.
        // This way we make sure RocksDbLogStore::create is not cancelled when the get_loglet() call is cancelled.
        TaskCenter::spawn_unmanaged(
            TaskKind::Background,
            "local-loglet-provider-inner",
            async move {
                let mut inner = inner.lock().await;

                let (log_store, log_writer) =
                    match (inner.log_store.as_ref(), inner.log_writer.as_ref()) {
                        (None, None) => {
                            debug!("Creating local loglet log store");
                            let log_store = RocksDbLogStore::create(options.clone())
                                .await
                                .map_err(OperationError::other)?;

                            let log_writer = log_store.create_writer().start(options)?;

                            inner.log_store = Some(log_store.clone());
                            inner.log_writer = Some(log_writer.clone());
                            (log_store, log_writer)
                        }
                        (Some(log_store), Some(log_writer)) => {
                            // unfortunately we need to clone even if later the entry() came out to be occupied.
                            // luckily both log_store and log_writer are cheap to clone.
                            (log_store.clone(), log_writer.clone())
                        }
                        _ => unreachable!(),
                    };

                let loglet = match inner.active_loglets.entry((log_id, segment_index)) {
                    hash_map::Entry::Vacant(entry) => {
                        // Create loglet
                        // NOTE: local-loglet expects params to be a `u64` string-encoded unique identifier under the hood.
                        let loglet = LocalLoglet::create(
                            params
                                .parse()
                                .expect("loglet params can be converted into u64"),
                            log_store,
                            log_writer,
                        )?;
                        let loglet = entry.insert(Arc::new(loglet));
                        Arc::clone(loglet)
                    }
                    hash_map::Entry::Occupied(entry) => entry.get().clone(),
                };

                drop(inner);
                Ok(loglet)
            },
        )?
        .await?
        .map(|loglet| loglet as Arc<dyn Loglet>)
    }

    fn propose_new_loglet_params(
        &self,
        log_id: LogId,
        chain: Option<&Chain>,
        _defaults: &ProviderConfiguration,
    ) -> Result<LogletParams, OperationError> {
        let new_segment_index = chain
            .map(|c| c.tail_index().next())
            .unwrap_or(SegmentIndex::OLDEST);
        Ok(LogletParams::from(
            u64::from(LogletId::new(log_id, new_segment_index)).to_string(),
        ))
    }

    async fn shutdown(&self) -> Result<(), OperationError> {
        Ok(())
    }
}
