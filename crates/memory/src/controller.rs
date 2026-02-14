// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hashbrown::HashMap;
use metrics::{Gauge, gauge};
use parking_lot::RwLock;
use tracing::warn;

use restate_serde_util::NonZeroByteCount;

use crate::MemoryPool;
use crate::metric_definitions::{POOL_CAPACITY, POOL_USAGE};

struct Bin {
    pool: MemoryPool,
    updater: Box<dyn Fn() -> NonZeroByteCount + Send + Sync>,
    used_gauge: Gauge,
    capacity_gauge: Gauge,
}

// tracks registered memory budgets, reports memory pressure
#[derive(Default)]
pub struct MemoryController {
    pools: RwLock<HashMap<&'static str, Bin>>,
}

impl MemoryController {
    pub fn create_pool(
        &self,
        name: &'static str,
        updater: impl Fn() -> NonZeroByteCount + Send + Sync + 'static,
    ) -> MemoryPool {
        let mut pools = self.pools.write();

        if let Some(pool) = pools.get(name) {
            return pool.pool.clone();
        }
        let initial_capacity = updater();
        let pool = MemoryPool::with_capacity(initial_capacity);

        let bin = Bin {
            pool: pool.clone(),
            updater: Box::from(updater),
            used_gauge: gauge!(POOL_USAGE, "name" => name),
            capacity_gauge: gauge!(POOL_CAPACITY, "name" => name),
        };
        pools.insert(name, bin);
        pool
    }

    pub fn submit_metrics(&self) {
        let pools = self.pools.read();
        for bin in pools.values() {
            bin.used_gauge.set(bin.pool.used().as_u64() as f64);
            bin.capacity_gauge.set(bin.pool.capacity().as_u64() as f64);
        }
    }

    pub fn notify_config_update(&self) {
        let pools = self.pools.read();
        for (name, bin) in pools.iter() {
            let current_capacity = bin.pool.capacity();
            let new_capacity = (&bin.updater)();
            if new_capacity.as_usize() != current_capacity.as_usize() {
                warn!(
                    "[config update] Setting memory-pool {name} capacity from {current_capacity} to {new_capacity}"
                );
                bin.pool.set_capacity(new_capacity);
            }
        }
    }
}
