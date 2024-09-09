// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod error;
mod loglet_worker;
mod logstore;
mod metadata;
mod metric_definitions;
mod network;
mod rocksdb_logstore;
mod service;

pub use error::LogServerBuildError;
pub use service::LogServerService;

#[cfg(test)]
pub(crate) fn setup_panic_handler() {
    // Make sure that panics exits the process.
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        std::process::exit(1);
    }));
}
