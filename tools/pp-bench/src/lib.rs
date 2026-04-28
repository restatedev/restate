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
use restate_serde_util::ByteCount;
use restate_time_util::FriendlyDuration;
use restate_types::config::CommonOptionCliOverride;

pub mod command_gen;
pub mod command_pool;
pub mod extract;
pub mod metrics_server;
pub mod report;
pub mod workload;

#[derive(Clone, clap::Parser)]
#[command(
    author,
    version,
    about = "Partition processor state machine replay benchmark"
)]
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

    /// Port for Prometheus metrics HTTP endpoint. Set to 0 to disable.
    #[arg(long, global = true, default_value = "9090")]
    pub metrics_port: u16,

    /// Interval between periodic progress reports (e.g. "5s", "10s").
    #[arg(long, global = true, default_value = "5s")]
    pub report_interval: FriendlyDuration,

    /// Emit a machine-readable JSON summary instead of the human-readable table.
    #[arg(long, global = true)]
    pub json: bool,

    #[clap(flatten)]
    pub common_opts: CommonOpts,

    #[clap(flatten)]
    pub opts_overrides: CommonOptionCliOverride,

    /// Choose the subcommand.
    #[clap(subcommand)]
    pub command: BenchCommand,
}

#[derive(Debug, Clone, clap::Parser)]
pub enum BenchCommand {
    /// Pre-generate a command file for deterministic, zero-overhead benchmark runs.
    Generate(GenerateOpts),
    /// Run the state-machine replay benchmark.
    Run(RunOpts),
    /// Inspect the contents of a pre-generated command file.
    Inspect(InspectOpts),
    /// Extract a snapshot bundle from a stopped Restate server's data directory.
    Extract(ExtractOpts),
}

// ---------------------------------------------------------------------------
// Workload specification (shared between generate and inline run)
// ---------------------------------------------------------------------------

/// Which workload to generate.
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum WorkloadType {
    /// External state mutation commands (`PatchState`).
    #[default]
    PatchState,
    /// Service invocation commands (`Invoke`).
    Invoke,
}

/// Parameters that control the shape of generated commands.
#[derive(Debug, Clone, clap::Args)]
pub struct WorkloadSpec {
    /// Workload type to generate.
    #[arg(long, default_value = "patch-state")]
    pub workload: WorkloadType,

    /// Number of distinct virtual-object keys. Controls key-space cardinality.
    #[arg(long, default_value = "10000")]
    pub num_keys: u32,

    /// Number of state entries per PatchState mutation (patch-state only).
    #[arg(long, default_value = "1")]
    pub state_entries: u32,

    /// Size of each state value in bytes (patch-state only).
    #[arg(long, default_value = "128")]
    pub value_size: ByteCount,

    /// RNG seed for deterministic command generation.
    #[arg(long, default_value = "42")]
    pub seed: u64,
}

// ---------------------------------------------------------------------------
// `generate` subcommand
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, clap::Parser)]
pub struct GenerateOpts {
    /// Output file path.
    #[arg(long, short)]
    pub output: PathBuf,

    /// Number of commands to generate.
    #[arg(long, default_value = "1000000")]
    pub num_commands: u64,

    #[clap(flatten)]
    pub spec: WorkloadSpec,
}

// ---------------------------------------------------------------------------
// `run` subcommand
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, clap::Parser)]
pub struct RunOpts {
    /// Path to a pre-generated command file (from `generate`).
    /// If omitted, commands are generated in-memory before measurement.
    #[arg(long)]
    pub command_file: Option<PathBuf>,

    /// Path to a snapshot bundle directory (from `extract`).
    /// Clones the partition store checkpoint and replays extracted commands on top.
    #[arg(long)]
    pub snapshot: Option<PathBuf>,

    /// Commands per RocksDB transaction commit.
    #[arg(long, default_value = "1")]
    pub batch_size: usize,

    /// Total number of commands to apply during the measurement phase.
    #[arg(long, default_value = "1000000")]
    pub num_commands: u64,

    /// Number of warmup commands to apply before measurement starts.
    #[arg(long, default_value = "50000")]
    pub warmup: u64,

    /// Simulate a state machine restart after every N commands.
    /// Flushes the partition store, re-reads FSM state, and re-creates the StateMachine.
    #[arg(long)]
    pub restart_every: Option<u64>,

    /// Workload spec (used when --command-file and --snapshot are not provided).
    #[clap(flatten)]
    pub spec: WorkloadSpec,
}

// ---------------------------------------------------------------------------
// `inspect` subcommand
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, clap::Parser)]
pub struct InspectOpts {
    /// Path to the command file to inspect.
    pub file: PathBuf,

    /// Number of commands to display (0 = header only).
    #[arg(long, short, default_value = "10")]
    pub num: usize,
}

// ---------------------------------------------------------------------------
// `extract` subcommand
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, clap::Parser)]
pub struct ExtractOpts {
    /// Path to the node data directory (e.g. ./restate-data/node1).
    /// Expects log-store/ and db/ subdirectories.
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Output snapshot bundle directory (will be created).
    #[arg(long, short)]
    pub output: PathBuf,

    /// Partition to extract.
    #[arg(long, default_value = "0")]
    pub partition_id: u16,

    /// Start extracting from this LSN (inclusive).
    /// Default: partition store's applied_lsn + 1.
    #[arg(long)]
    pub from_lsn: Option<u64>,

    /// Stop extracting at this LSN (inclusive). Default: end of log.
    #[arg(long)]
    pub to_lsn: Option<u64>,

    /// Maximum number of commands to extract.
    #[arg(long)]
    pub limit: Option<u64>,
}
