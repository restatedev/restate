// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use cling::prelude::*;
use tonic::{Code, Status};

use restate_cli_util::ui::console::confirm_or_exit;
use restate_cli_util::{CliContext, c_eprintln, c_noop, c_success, c_warn};
use restate_core::protobuf::cluster_ctrl_svc::{
    ClusterStateRequest, DropPartitionStoreRequest, new_cluster_ctrl_client,
};
use restate_types::PlainNodeId;
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_types::protobuf::cluster::{BrokenReason, node_state};

use crate::connection::{ConnectionInfo, NodeOperationError, SimpleStatusWrapper};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "drop_partition_store")]
#[command(
    after_long_help = "Deletes one node's local copy of a partition. All data that node \
    holds for the partition is lost; it recovers by downloading a snapshot or by replaying the \
    log. This is meant for nodes whose partition store has been sealed, which the node reports \
    as a broken partition processor (see `restatectl partition list`)."
)]
pub struct DropStoreOpts {
    /// The partition whose local store should be deleted
    #[arg(required = true)]
    partition_id: PartitionId,

    /// The node to delete the partition store from
    #[arg(long, short = 'n', required = true)]
    node: PlainNodeId,

    /// Stop a running partition processor before deleting. Without this, the node only accepts
    /// the request if it has already given up on the partition.
    #[arg(long)]
    force: bool,
}

async fn drop_partition_store(
    connection: &ConnectionInfo,
    opts: &DropStoreOpts,
) -> anyhow::Result<()> {
    let cluster_state = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .get_cluster_state(ClusterStateRequest::default())
                .await
        })
        .await?
        .into_inner()
        .cluster_state;

    let Some(node_state) = cluster_state
        .and_then(|state| state.nodes.get(&u32::from(opts.node)).cloned())
        .and_then(|node| node.state)
    else {
        bail!("Node {} is not known to the cluster.", opts.node);
    };

    let node_state::State::Alive(alive_node) = node_state else {
        bail!(
            "Node {} is not alive. Its partition store can only be dropped while it is running.",
            opts.node
        );
    };

    let reported_state = alive_node
        .partitions
        .get(&u32::from(u16::from(opts.partition_id)));

    let broken_reason = reported_state.map(|status| status.broken_reason());

    match broken_reason {
        Some(BrokenReason::AheadOfLog) => {
            c_eprintln!(
                "Node {} reports partition {} as broken: its local store is sealed because the \
                applied LSN is ahead of the log tail.",
                opts.node,
                opts.partition_id,
            );
        }
        Some(BrokenReason::NotBroken) if !opts.force => {
            bail!(
                "Node {} is running a partition processor for partition {} and does not report it \
                as broken. Re-run with --force to stop the processor and delete its store anyway.",
                opts.node,
                opts.partition_id,
            );
        }
        Some(BrokenReason::NotBroken) => {
            c_warn!(
                "Node {} does not report partition {} as broken. Forcing the drop will stop a \
                running partition processor and destroy the only copy of the partition this node \
                holds.",
                opts.node,
                opts.partition_id,
            );
        }
        None => {
            c_eprintln!(
                "Node {} is not running a partition processor for partition {}.",
                opts.node,
                opts.partition_id,
            );
        }
    }

    confirm_or_exit(&format!(
        "Delete node {}'s local copy of partition {}? This cannot be undone",
        opts.node, opts.partition_id
    ))?;

    let request = DropPartitionStoreRequest {
        partition_id: u32::from(u16::from(opts.partition_id)),
        node_id: Some(opts.node.into()),
        force: opts.force,
    };

    let response = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .drop_partition_store(request)
                .await
                .map_err(classify_drop_partition_store_status)
        })
        .await?
        .into_inner();

    if response.dropped {
        c_success!(
            "Dropped node {}'s local copy of partition {}",
            opts.node,
            opts.partition_id
        );
    } else {
        c_noop!(
            "Node {} had no local copy of partition {} to drop",
            opts.node,
            opts.partition_id
        );
    }

    Ok(())
}

fn classify_drop_partition_store_status(status: Status) -> NodeOperationError {
    let retry_elsewhere = status.code() == Code::Unimplemented;
    let status = SimpleStatusWrapper::from(status);
    if retry_elsewhere {
        NodeOperationError::RetryElsewhere(status)
    } else {
        NodeOperationError::Terminal(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_retries_drop_store_when_the_admin_does_not_implement_it() {
        assert!(matches!(
            classify_drop_partition_store_status(Status::unimplemented("unsupported")),
            NodeOperationError::RetryElsewhere(_)
        ));

        for status in [
            Status::failed_precondition("processor running"),
            Status::aborted("drop in progress"),
            Status::not_found("unknown partition"),
            Status::internal("drop failed"),
            Status::unavailable("response lost"),
            Status::deadline_exceeded("response timed out"),
            Status::unknown("outcome unknown"),
        ] {
            assert!(matches!(
                classify_drop_partition_store_status(status),
                NodeOperationError::Terminal(_)
            ));
        }
    }
}
