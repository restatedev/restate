// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use enum_map::EnumMap;
use restate_types::config::Configuration;
use restate_types::live::Live;
use tracing::{debug, error, trace};

use restate_core::{cancellation_watcher, Metadata, TaskCenter, TaskKind};
use restate_types::logs::metadata::ProviderKind;

use crate::bifrost::BifrostInner;
use crate::providers::{local_loglet, memory_loglet};
use crate::watchdog::{Watchdog, WatchdogCommand};
use crate::{loglet::LogletProviderFactory, Bifrost};

pub struct BifrostService {
    task_center: TaskCenter,
    inner: Arc<BifrostInner>,
    bifrost: Bifrost,
    watchdog: Watchdog,
    factories: HashMap<ProviderKind, Box<dyn LogletProviderFactory>>,
}

impl BifrostService {
    pub fn new(task_center: TaskCenter, metadata: Metadata) -> Self {
        let (watchdog_sender, watchdog_receiver) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(BifrostInner::new(metadata.clone(), watchdog_sender.clone()));
        let bifrost = Bifrost::new(inner.clone());
        let watchdog = Watchdog::new(
            task_center.clone(),
            inner.clone(),
            watchdog_sender,
            watchdog_receiver,
        );
        Self {
            task_center,
            inner,
            bifrost,
            watchdog,
            factories: HashMap::new(),
        }
    }

    pub fn with_factory(mut self, factory: impl LogletProviderFactory) -> Self {
        self.factories.insert(factory.kind(), Box::new(factory));
        self
    }

    pub fn enable_in_memory_loglet(mut self) -> Self {
        let factory = memory_loglet::Factory::default();
        self.factories.insert(factory.kind(), Box::new(factory));
        self
    }

    pub fn enable_local_loglet(mut self, config: &Live<Configuration>) -> Self {
        let factory = local_loglet::Factory::new(
            config.clone().map(|c| &c.bifrost.local).boxed(),
            config.clone().map(|c| &c.bifrost.local.rocksdb).boxed(),
        );
        self.factories.insert(factory.kind(), Box::new(factory));
        self
    }

    pub fn handle(&self) -> Bifrost {
        self.bifrost.clone()
    }
    /// Runs initialization phase, then returns a handle to join on shutdown.
    /// In this phase the system should wait until this is completed before
    /// continuing. For instance, a worker mark itself as `STARTING_UP` and not
    /// accept any requests until this is completed.
    ///
    /// This requires to run within a task_center context.
    pub async fn start(self) -> anyhow::Result<()> {
        // Perform an initial metadata sync.
        self.inner
            .sync_metadata()
            .await
            .context("Initial bifrost metadata sync has failed!")?;

        // initialize all enabled providers.
        if self.factories.is_empty() {
            anyhow::bail!("No loglet providers enabled!");
        }

        // TODO (asoli): Validate that we can operate with current log metadata.
        let mut tasks = tokio::task::JoinSet::new();
        // Start all enabled providers.
        for (kind, factory) in self.factories {
            let tc = self.task_center.clone();
            let watchdog = self.watchdog.sender();
            tasks.spawn(async move {
                tc.run_in_scope("loglet-provider-start", None, async move {
                    trace!("Starting loglet provider {}", kind);
                    match factory.create().await {
                        Err(e) => {
                            error!("Failed to start loglet provider {}: {}", kind, e);
                            Err(anyhow::anyhow!(
                                "Failed to start loglet provider {}: {}",
                                kind,
                                e
                            ))
                        }
                        Ok(provider) => {
                            // tell watchdog about it.
                            // We can always send because we own both sender and receiver.
                            watchdog
                                .send(WatchdogCommand::WatchProvider(provider.clone()))
                                .expect("watchdog sends always succeed");
                            Ok((kind, provider))
                        }
                    }
                })
                .await
            });
        }
        let mut shutdown = std::pin::pin!(cancellation_watcher());

        // Wait until all providers have started.
        let mut providers = EnumMap::default();
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    return Err(anyhow::anyhow!("Bifrost initialization cancelled"));
                }
                maybe_res = tasks.join_next() => {
                    match maybe_res {
                        None => {
                            // No more tasks.
                            break;
                        },
                        Some(maybe_res) => {
                            // We are only allowed to continue if all providers started successfully.
                            let (kind, provider) = maybe_res??;
                            providers[kind] = Some(provider);
                        }
                    }
                }
            }
        }
        debug!("All loglet providers started successfully!");

        self.inner
            .providers
            .set(providers.clone())
            .map_err(|_| anyhow::anyhow!("bifrost must be initialized only once"))?;

        // We spawn the watchdog as a background long-running task
        self.task_center.spawn(
            TaskKind::BifrostBackgroundHighPriority,
            "bifrost-watchdog",
            None,
            self.watchdog.run(),
        )?;

        // Bifrost started!
        Ok(())
    }
}
