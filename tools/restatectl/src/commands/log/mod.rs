// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod describe_log;
mod dump_log;
mod gen_metadata;
pub mod list_logs;
mod reconfigure;
mod trim_log;

use cling::prelude::*;

use restate_cli_util::_comfy_table::{Cell, Color};
use restate_cli_util::c_println;
use restate_types::logs::metadata::{ProviderKind, Segment};
use restate_types::replicated_loglet::ReplicatedLogletParams;

#[derive(Run, Subcommand, Clone)]
pub enum Logs {
    /// List the logs by partition
    List(list_logs::ListLogsOpts),
    /// Prints a generated log-metadata in JSON format
    GenerateMetadata(gen_metadata::GenerateLogMetadataOpts),
    /// Get the details of a specific log
    Describe(describe_log::DescribeLogIdOpts),
    /// Dump the contents of a bifrost log
    Dump(dump_log::DumpLogOpts),
    /// Trim a log to a particular Log Sequence Number (LSN)
    Trim(trim_log::TrimLogOpts),
    /// Reconfigure a log by sealing the tail segment
    /// and extending the chain with a new one
    Reconfigure(reconfigure::ReconfigureOpts),
}

pub fn render_loglet_params<F>(params: &Option<ReplicatedLogletParams>, render_fn: F) -> Cell
where
    F: FnOnce(&ReplicatedLogletParams) -> Cell,
{
    params
        .as_ref()
        .map(render_fn)
        .unwrap_or_else(|| Cell::new("N/A").fg(Color::Red))
}

pub fn deserialize_replicated_log_params(segment: &Segment) -> Option<ReplicatedLogletParams> {
    match segment.config.kind {
        ProviderKind::Replicated => {
            ReplicatedLogletParams::deserialize_from(segment.config.params.as_bytes())
                .inspect_err(|e| {
                    c_println!(
                        "⚠️ Failed to deserialize ReplicatedLogletParams for segment {}: {}",
                        segment.index(),
                        e
                    );
                })
                .ok()
        }
        _ => None,
    }
}
