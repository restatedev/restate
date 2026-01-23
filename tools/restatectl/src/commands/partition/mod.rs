// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod gen_metadata;
pub mod list;
mod reconfigure;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum Partitions {
    /// Print the partitions and their respective processors' status
    #[clap(alias = "status")]
    List(list::ListPartitionsOpts),
    /// Prints a generated partition table in JSON format
    GenerateMetadata(gen_metadata::GeneratePartitionTableOpts),
    /// Reconfigures the processors of the specified partition
    Reconfigure(reconfigure::ReconfigureOpts),
}
