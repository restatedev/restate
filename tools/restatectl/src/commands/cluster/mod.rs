// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub(crate) mod overview;

use cling::prelude::*;

use crate::commands::cluster::overview::ClusterStatusOpts;

#[derive(Run, Subcommand, Clone)]
pub enum Cluster {
    /// Print a brief overview of the cluster status (nodes, logs, partitions)
    Overview(ClusterStatusOpts),
}
