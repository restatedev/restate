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
mod list_logs;
mod reconfigure;
mod trim_log;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum Log {
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
