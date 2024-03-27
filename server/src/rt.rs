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

use restate_core::options::CommonOptions;
use tokio::runtime::{Builder, Runtime};

// todo: move to task_center factory
pub fn build_tokio(common_opts: &CommonOptions) -> Result<Runtime, std::io::Error> {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all().thread_name_fn(|| {
        static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
        let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
        format!("rs:worker-{}", id)
    });

    if let Some(worker_threads) = *common_opts.default_thread_pool_size() {
        builder.worker_threads(worker_threads);
    }

    builder.build()
}
