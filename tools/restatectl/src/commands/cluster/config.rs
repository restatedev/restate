// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod get;
mod set;

use cling::prelude::*;

// use crate::commands::cluster::overview::ClusterStatusOpts;

#[derive(Run, Subcommand, Clone)]
pub enum Config {
    /// Print a brief overview of the cluster status (nodes, logs, partitions)
    Get(get::ConfigGetOpts),
    Set(set::ConfigSetOpts),
}
