// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod bifrost;
mod error;
mod loglet;
mod loglets;
mod metadata;
mod options;
mod read_stream;
mod service;
mod types;
mod watchdog;

use std::collections::HashMap;

pub use bifrost::Bifrost;
pub use error::{Error, ProviderError};
pub use options::Options;
pub use read_stream::LogReadStream;
use restate_types::logs::LogId;
use restate_types::Version;
pub use service::BifrostService;
pub use types::*;

use self::metadata::{Chain, LogletParams, Logs};

/// Initializes the bifrost metadata with static log metadata, it creates a log for every partition
/// with a chain of the default loglet provider kind.
pub(crate) fn create_static_metadata(opts: &Options, num_partitions: u64) -> Logs {
    // Get metadata from somewhere
    let mut log_chain: HashMap<LogId, Chain> = HashMap::with_capacity(num_partitions as usize);

    // pre-fill with all possible logs up to `num_partitions`
    (0..num_partitions).for_each(|i| {
        // fixed config that uses the log-id as loglet identifier/config
        let config = LogletParams::from(i.to_string());
        log_chain.insert(LogId::from(i), Chain::new(opts.default_provider, config));
    });

    Logs::new(Version::MIN, log_chain)
}
