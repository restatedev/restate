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

use restate_cli_util::CommonOpts;
use restate_time_util::FriendlyDuration;
use restate_types::config::CommonOptionCliOverride;

pub mod metrics_server;
pub mod mixed_workload;
pub mod payload;
pub mod report;
pub mod write_throughput;

#[derive(Clone, clap::Parser)]
#[command(author, version, about = "Log-server RocksDB benchmark tool")]
pub struct Arguments {
    /// Set a configuration file to use for Restate.
    #[arg(
        short,
        long = "config-file",
        env = "RESTATE_CONFIG",
        value_name = "FILE",
        global = true
    )]
    pub config_file: Option<PathBuf>,

    /// Print the full raw RocksDB statistics string at the end.
    #[arg(long, global = true)]
    pub raw_rocksdb_stats: bool,

    /// Keep the data directory after the run (useful for inspecting RocksDB).
    #[arg(long, global = true)]
    pub retain_test_dir: bool,

    /// Port for Prometheus metrics HTTP endpoint. Set to 0 to disable.
    #[arg(long, global = true, default_value = "9090")]
    pub metrics_port: u16,

    /// Interval between periodic progress reports (e.g. "5s", "10s").
    #[arg(long, global = true, default_value = "5s")]
    pub report_interval: FriendlyDuration,

    #[clap(flatten)]
    pub common_opts: CommonOpts,

    #[clap(flatten)]
    pub opts_overrides: CommonOptionCliOverride,

    /// Choose the benchmark to run.
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, clap::Parser)]
pub enum Command {
    /// Sequential write throughput benchmark.
    WriteThroughput(write_throughput::WriteThroughputOpts),
    /// Concurrent write + read + trim workload.
    MixedWorkload(mixed_workload::MixedWorkloadOpts),
    /// Pre-generate a payload file for deterministic, zero-overhead benchmarks.
    GeneratePayload(payload::GeneratePayloadOpts),
}
