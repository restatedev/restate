// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::commands::metadata_server::list_servers::ListMetadataServers;
use crate::commands::metadata_server::nodes::{AddNodeOpts, RemoveNodeOpts};
use clap::Subcommand;
use cling::Run;

pub mod list_servers;
mod nodes;

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "ms")]
pub enum MetadataServer {
    /// Add a node to the metadata store cluster
    AddNode(AddNodeOpts),
    /// Remove a node from the metadata store cluster
    RemoveNode(RemoveNodeOpts),
    /// List metadata server status
    ListServers(ListMetadataServers),
}
