// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
mod find_tail;
mod gen_metadata;
pub mod list_logs;
mod reconfigure;
mod seal;
mod trim_log;

use std::{ops::RangeInclusive, str::FromStr};

use cling::prelude::*;

use restate_cli_util::_comfy_table::{Cell, Color};
use restate_cli_util::c_println;
use restate_types::logs::metadata::{ProviderKind, Segment};
use restate_types::logs::LogId;
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
    /// Reconfigure a log manually by sealing the tail segment
    /// and extending the chain with a new one
    /// It's always preferred to use `seal` if possible
    Reconfigure(reconfigure::ReconfigureOpts),
    /// Force cluster controller to seal a loglet and
    /// recreate it with latest cluster configuration.
    Seal(seal::SealOpts),
    /// Find and show tail state of a log
    FindTail(find_tail::FindTailOpts),
}

#[derive(Parser, Collect, Clone, Debug)]
struct LogIdRange {
    from: u32,
    to: u32,
}

impl LogIdRange {
    fn new(from: u32, to: u32) -> anyhow::Result<Self> {
        if from > to {
            anyhow::bail!(
                "Invalid log id range: {}..{}, start must be <= end range",
                from,
                to
            )
        } else {
            Ok(LogIdRange { from, to })
        }
    }

    fn iter(&self) -> impl Iterator<Item = u32> {
        self.from..=self.to
    }
}

impl From<&LogId> for LogIdRange {
    fn from(log_id: &LogId) -> Self {
        let id = (*log_id).into();
        LogIdRange::new(id, id).unwrap()
    }
}

impl IntoIterator for LogIdRange {
    type IntoIter = RangeInclusive<u32>;
    type Item = u32;
    fn into_iter(self) -> Self::IntoIter {
        self.from..=self.to
    }
}

impl FromStr for LogIdRange {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("-").collect();
        match parts.len() {
            1 => {
                let n = parts[0].parse()?;
                Ok(LogIdRange::new(n, n)?)
            }
            2 => {
                let from = parts[0].parse()?;
                let to = parts[1].parse()?;
                Ok(LogIdRange::new(from, to)?)
            }
            _ => anyhow::bail!("Invalid log id or log range: {}", s),
        }
    }
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
