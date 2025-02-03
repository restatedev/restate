// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::Parser;
use cling::{Collect, Run};

use restate_cli_util::c_println;

use crate::commands::log::list_logs::{list_logs, ListLogsOpts};
use crate::commands::node::list_nodes::{list_nodes, ListNodesOpts};
use crate::commands::partition::list::{list_partitions, ListPartitionsOpts};
use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "cluster_status")]
pub struct ClusterStatusOpts {
    /// Display additional status information
    #[arg(long)]
    extra: bool,
}

async fn cluster_status(
    connection: &ConnectionInfo,
    status_opts: &ClusterStatusOpts,
) -> anyhow::Result<()> {
    list_nodes(
        connection,
        &ListNodesOpts {
            extra: status_opts.extra,
        },
    )
    .await?;
    c_println!();

    list_logs(connection, &ListLogsOpts {}).await?;
    c_println!();

    list_partitions(connection, &ListPartitionsOpts::default()).await?;

    Ok(())
}
