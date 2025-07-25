// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use enum_map::EnumMap;
use tracing::{debug, error, trace};

use restate_core::{
    MetadataWriter, TaskCenter, TaskCenterFutureExt, TaskKind, cancellation_watcher,
};
#[cfg(feature = "local-loglet")]
use restate_types::config::LocalLogletOptions;
#[cfg(feature = "local-loglet")]
use restate_types::live::BoxLiveLoad;
use restate_types::logs::metadata::ProviderKind;

use crate::bifrost::BifrostInner;
use crate::log_chain_extender::LogChainExtender;
#[cfg(any(test, feature = "memory-loglet"))]
use crate::providers::memory_loglet;
use crate::watchdog::{Watchdog, WatchdogCommand};
use crate::{Bifrost, loglet::LogletProviderFactory};

pub struct BifrostService {
    inner: Arc<BifrostInner>,
    bifrost: Bifrost,
    watchdog: Watchdog,
    log_chain_extender: LogChainExtender,
    factories: HashMap<ProviderKind, Box<dyn LogletProviderFactory>>,
}

impl BifrostService {
    pub fn new(metadata_writer: MetadataWriter) -> Self {
        let (watchdog_sender, watchdog_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (extend_log_chain_tx, extend_log_chain_rx) = tokio::sync::mpsc::unbounded_channel();
        let inner = Arc::new(BifrostInner::new(
            watchdog_sender.clone(),
            extend_log_chain_tx,
            metadata_writer,
        ));
        let bifrost = Bifrost::new(inner.clone());
        let watchdog = Watchdog::new(inner.clone(), watchdog_sender, watchdog_receiver);
        let log_chain_extender = LogChainExtender::new(inner.clone(), extend_log_chain_rx);
        Self {
            inner,
            bifrost,
            watchdog,
            log_chain_extender,
            factories: HashMap::new(),
        }
    }

    pub fn with_factory(mut self, factory: impl LogletProviderFactory) -> Self {
        self.factories.insert(factory.kind(), Box::new(factory));
        self
    }

    #[cfg(any(test, feature = "memory-loglet"))]
    pub fn enable_in_memory_loglet(mut self) -> Self {
        let factory = memory_loglet::Factory;
        self.factories.insert(factory.kind(), Box::new(factory));
        self
    }

    #[cfg(feature = "local-loglet")]
    pub fn enable_local_loglet(mut self, config: BoxLiveLoad<LocalLogletOptions>) -> Self {
        let factory = crate::providers::local_loglet::Factory::new(config);
        self.factories.insert(factory.kind(), Box::new(factory));
        self
    }

    pub fn handle(&self) -> Bifrost {
        self.bifrost.clone()
    }

    /// Runs initialization phase. In this phase the system should wait until this is completed
    /// before continuing. For instance, a worker mark itself as `STARTING_UP` and not accept any
    /// requests until this is completed.
    ///
    /// This requires to run within a task_center context. Expects that logs metadata is synced.
    pub async fn start(self) -> anyhow::Result<()> {
        // Initialize enabled providers.
        if self.factories.is_empty() {
            anyhow::bail!("No loglet providers enabled!");
        }

        // TODO (asoli): Validate that we can operate with current log metadata.
        let mut tasks = tokio::task::JoinSet::new();
        // Start all enabled providers.
        for (kind, factory) in self.factories {
            let watchdog = self.watchdog.sender();
            tasks
                .build_task()
                .name(&format!("start-provider-{kind}"))
                .spawn(
                    async move {
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
                    }
                    .in_current_tc_as_task(TaskKind::LogletProvider, "loglet-provider-start"),
                )
                .expect("to spawn start provider task");
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
        TaskCenter::spawn(
            TaskKind::BifrostWatchdog,
            "bifrost-watchdog",
            self.watchdog.run(),
        )?;

        TaskCenter::spawn(
            TaskKind::BifrostBackgroundHighPriority,
            "log-chain-extender",
            self.log_chain_extender.run(),
        )?;

        // Bifrost started!
        Ok(())
    }
}
