// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod appender;
mod background_appender;
mod bifrost;
mod bifrost_admin;
mod error;
pub mod loglet;
mod loglet_wrapper;
pub mod providers;
mod read_stream;
mod record;
mod service;
mod types;
mod watchdog;

pub use appender::Appender;
pub use background_appender::{AppenderHandle, BackgroundAppender, CommitToken, LogSender};
pub use bifrost::Bifrost;
pub use bifrost_admin::BifrostAdmin;
pub use error::{Error, Result};
pub use read_stream::LogReadStream;
pub use record::*;
pub use service::BifrostService;
pub use types::*;

pub const SMALL_BATCH_THRESHOLD_COUNT: usize = 4;

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
