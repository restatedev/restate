// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::Subcommand;
use cling::Run;

mod create;
mod list;

const KEY_METADATA_LOGS: &str = "bifrost_config";

#[derive(Run, Subcommand, Clone)]
pub enum Logs {
    /// List the logs by partition
    List(list::ListLogsOpts),
    /// Generate/update log chain configurations
    CreateLogConfig(create::CreateLogConfigOpts),
}
