use clap::Parser;
use cling::{Collect, Run};

use restate_cli_util::c_println;

use crate::app::ConnectionInfo;
use crate::commands::log::list_logs::{list_logs, ListLogsOpts};
use crate::commands::node::list_nodes::{list_nodes, ListNodesOpts};
use crate::commands::partition::list::{list_partitions, ListPartitionsOpts};

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

    list_partitions(connection, &ListPartitionsOpts {}).await?;

    Ok(())
}
