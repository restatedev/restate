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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
use tracing::info;

use crate::loglet::{Loglet, LogletBase, LogletOffset, LogletProvider};
use crate::metadata::LogletParams;
use crate::{AppendAttributes, DataRecord, Error};

pub fn default_config() -> serde_json::Value {
    serde_json::Value::Null
}

#[derive(Default)]
pub struct MemoryLogletProvider {
    store: AsyncMutex<HashMap<LogletParams, Arc<MemoryLoglet>>>,
    init_delay: Option<Duration>,
}

impl MemoryLogletProvider {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    pub fn with_init_delay(init_delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            init_delay: Some(init_delay),
            ..Default::default()
        })
    }
}

#[async_trait]
impl LogletProvider for MemoryLogletProvider {
    async fn get_loglet(&self, params: &LogletParams) -> Result<std::sync::Arc<dyn Loglet>, Error> {
        let mut guard = self.store.lock().await;

        let loglet = match guard.entry(params.clone()) {
            hash_map::Entry::Vacant(entry) => {
                if let Some(init_delay) = self.init_delay {
                    // Artificial delay to simulate slow loglet creation
                    info!(
                        "Simulating slow loglet creation, delaying for {:?}",
                        init_delay
                    );
                    tokio::time::sleep(init_delay).await;
                }

                // Create loglet
                let loglet = Arc::new(MemoryLoglet {
                    params: params.clone(),
                    log: Mutex::new(Vec::new()),
                });
                let loglet = entry.insert(loglet);
                Arc::clone(loglet)
            }
            hash_map::Entry::Occupied(entry) => entry.get().clone(),
        };

        Ok(loglet as Arc<dyn Loglet>)
    }

    async fn start(&self) -> Result<(), Error> {
        info!("Starting in-memory loglet provider");
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Error> {
        info!("Shutting down in-memory loglet provider");
        Ok(())
    }
}

pub struct MemoryLoglet {
    // We treat params as an opaque identifier for the underlying loglet.
    params: LogletParams,
    log: Mutex<Vec<DataRecord>>,
}

#[async_trait]
impl LogletBase for MemoryLoglet {
    type Offset = LogletOffset;

    async fn append(
        &self,
        record: DataRecord,
        _attributes: AppendAttributes,
    ) -> Result<LogletOffset, Error> {
        let mut log = self.log.lock().unwrap();
        let offset = LogletOffset::from(log.len() as u64);
        info!(
            "Appending record to in-memory loglet {:?} at offset {}",
            self.params, offset,
        );
        log.push(record);
        Ok(offset)
    }
}
