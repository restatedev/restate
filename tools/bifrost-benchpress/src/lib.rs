// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use restate_types::config::CommonOptionCliOverride;

use self::append_latency::AppendLatencyOpts;
use self::write_to_read::WriteToReadOpts;

pub mod append_latency;
pub mod util;
pub mod write_to_read;

#[derive(Debug, Clone, clap::Parser)]
#[command(author, version, about)]
pub struct Arguments {
    /// Set a configuration file to use for Restate.
    /// For more details, check the documentation.
    #[arg(
        short,
        long = "config-file",
        env = "RESTATE_CONFIG",
        value_name = "FILE",
        global = true
    )]
    pub config_file: Option<PathBuf>,

    #[arg(long, global = true)]
    pub no_prometheus_stats: bool,

    #[arg(long, global = true)]
    pub no_rocksdb_stats: bool,

    #[arg(long, global = true)]
    pub retain_test_dir: bool,

    #[clap(flatten)]
    pub opts_overrides: CommonOptionCliOverride,

    /// Choose the benchmark to run
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, clap::Parser)]
pub enum Command {
    /// Measures the write-to-read latency for a single log
    WriteToRead(WriteToReadOpts),
    /// Measures the append latency for a single log
    AppendLatency(AppendLatencyOpts),
}
