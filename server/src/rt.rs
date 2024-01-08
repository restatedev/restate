// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tokio::runtime::{Builder, Runtime};

/// # Runtime options
///
/// Configuration for the Tokio runtime used by Restate.
#[serde_as]
#[derive(Debug, Default, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[builder(default)]
#[cfg_attr(feature = "options_schema", schemars(default))]
pub struct Options {
    /// # Worker threads
    ///
    /// Configure the number of [worker threads](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.worker_threads) of the Tokio runtime.
    /// If not set, it uses the Tokio default, where worker_threads is equal to number of cores.
    worker_threads: Option<usize>,

    /// # Max blocking threads
    ///
    /// Configure the number of [max blocking threads](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.max_blocking_threads) of the Tokio runtime.
    /// If not set, it uses the Tokio default 512.
    max_blocking_threads: Option<usize>,
}

impl Options {
    #[allow(dead_code)]
    pub fn build(self) -> Result<Runtime, std::io::Error> {
        let mut builder = Builder::new_multi_thread();
        builder.enable_all().thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("rs:worker-{}", id)
        });

        if let Some(worker_threads) = self.worker_threads {
            builder.worker_threads(worker_threads);
        }
        if let Some(max_blocking_threads) = self.max_blocking_threads {
            builder.max_blocking_threads(max_blocking_threads);
        }

        builder.build()
    }
}
