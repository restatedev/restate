// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use cling::prelude::*;
use itertools::Itertools;
use tonic::codec::CompressionEncoding;
use tracing::{info, warn};

use restate_bifrost::loglet::util::TailOffsetWatch;
use restate_bifrost::providers::replicated_loglet::replication::NodeSetChecker;
use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_core::network::protobuf::node_ctl_svc::node_ctl_svc_client::NodeCtlSvcClient;
use restate_core::network::protobuf::node_ctl_svc::GetMetadataRequest;
use restate_core::MetadataKind;
use restate_log_server::protobuf::log_server_svc_client::LogServerSvcClient;
use restate_log_server::protobuf::GetDigestRequest;
use restate_types::logs::metadata::Logs;
use restate_types::logs::{LogletOffset, SequenceNumber, TailState};
use restate_types::net::log_server::RecordStatus;
use restate_types::nodes_config::{NodesConfiguration, Role};
use restate_types::replicated_loglet::{EffectiveNodeSet, ReplicatedLogletId};
use restate_types::storage::StorageCodec;
use restate_types::Versioned;

use crate::app::ConnectionInfo;
use crate::commands::replicated_loglet::digest_util::DigestsHelper;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "get_digest")]
pub struct DigestOpts {
    /// The replicated loglet id
    loglet_id: ReplicatedLogletId,
    /// Sync metadata from metadata store first
    #[arg(long)]
    sync_metadata: bool,
    /// From offset. Requests from oldest if unset.
    #[arg(long, default_value = "1")]
    from: u32,
    /// to offset
    #[arg(long)]
    to: u32,
}

async fn get_digest(connection: &ConnectionInfo, opts: &DigestOpts) -> anyhow::Result<()> {
    let channel = grpc_connect(connection.cluster_controller.clone())
        .await
        .with_context(|| {
            format!(
                "cannot connect to node at {}",
                connection.cluster_controller
            )
        })?;
    let mut client = NodeCtlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);
    let req = GetMetadataRequest {
        kind: MetadataKind::Logs.into(),
        sync: opts.sync_metadata,
    };
    let mut response = client.get_metadata(req).await?.into_inner();
    let logs = StorageCodec::decode::<Logs, _>(&mut response.encoded)?;

    c_println!("Log Configuration ({})", logs.version());
    let Some(loglet_ref) = logs.get_replicated_loglet(&opts.loglet_id) else {
        return Err(anyhow::anyhow!("loglet {} not found", opts.loglet_id));
    };

    let req = GetMetadataRequest {
        kind: MetadataKind::NodesConfiguration.into(),
        sync: opts.sync_metadata,
    };
    let mut response = client.get_metadata(req).await?.into_inner();
    let nodes_config = StorageCodec::decode::<NodesConfiguration, _>(&mut response.encoded)?;
    c_println!("Nodes Configuration ({})", nodes_config.version());
    c_println!();

    let nodeset = EffectiveNodeSet::new(&loglet_ref.params.nodeset, &nodes_config);
    c_println!("Loglet Id: {}", opts.loglet_id);
    c_println!("Nodeset: {nodeset}");
    c_println!("Replication: {}", loglet_ref.params.replication);
    let known_global_tail = TailOffsetWatch::new(TailState::new(false, LogletOffset::INVALID));
    let mut digests = DigestsHelper::new(&loglet_ref.params, opts.from.into(), opts.to.into());
    for node_id in nodeset.iter() {
        let node = nodes_config.find_node_by_id(*node_id).with_context(|| {
            format!("Node {node_id} doesn't seem to exist in nodes configuration")
        })?;
        info!(
            "Requesting digest from node {} at {}",
            node_id, node.address
        );
        if !node.has_role(Role::LogServer) {
            warn!(
                "Node {} is not running the log-server role, will not connect to it",
                node_id
            );
            continue;
        }

        let Ok(channel) = grpc_connect(node.address.clone()).await else {
            warn!("Failed to connect to node {} at {}", node_id, node.address);
            continue;
        };

        let mut client =
            LogServerSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

        let req = GetDigestRequest {
            loglet_id: opts.loglet_id.into(),
            from_offset: opts.from,
            to_offset: opts.to,
        };
        let digest = client
            .get_digest(req)
            .await?
            .into_inner()
            .digest
            .expect("always set by servers");
        digests.add_digest_message(*node_id, digest.into(), &known_global_tail);
    }
    c_println!(
        "Max Returned Global Tail: {}",
        *known_global_tail.get().offset()
    );
    c_println!("Seal Observed?: {}", known_global_tail.get().is_sealed());
    c_println!("Max Observed Trim Point: {}", digests.max_trim_point());
    c_println!("Max Local Tail: {:?}", digests.max_local_tail());
    c_println!();

    let mut records_table = Table::new_styled();
    let node_ids = nodeset.iter().sorted().map(|n| {
        match digests.is_sealed(n) {
            Some(true) => Cell::new(format!("{n}(S)")).fg(Color::Magenta),
            Some(false) => Cell::new(n.to_string()),
            // we didn't hear from this node
            None => Cell::new(format!("{n}(E)")).fg(Color::Red),
        }
    });
    let mut heading = vec![Cell::new("OFFSET")];
    heading.extend(node_ids.into_iter());
    heading.extend(vec![Cell::new("ISSUES")]);
    records_table.set_header(heading);

    for (offset, responses) in digests.iter() {
        if *offset >= digests.max_local_tail() {
            break;
        }
        if *offset == known_global_tail.latest_offset() {
            // divider to indicate that everything after global tail
            records_table.add_row(std::iter::repeat("────").take(nodeset.len() + 2));
        }
        let mut checker =
            NodeSetChecker::new(&nodeset, &nodes_config, &loglet_ref.params.replication);
        let mut status_row = Vec::with_capacity(nodeset.len() + 2);
        status_row.push(Cell::new(offset.to_string()));
        for node in nodeset.iter().sorted() {
            if let Some(status) = responses.get(node) {
                status_row.push(Cell::new(status.to_string()));
                if let RecordStatus::Exists = status {
                    checker.set_attribute(*node, true);
                }
            } else if digests.is_known(node) {
                // record doesn't exist on this node
                status_row.push(Cell::new("-"));
            } else {
                // we don't know.
                status_row.push(Cell::new("?").fg(Color::Red));
            }
        }
        if !checker.check_write_quorum(|t| *t) {
            // record is under-replicated
            status_row.push(Cell::new("U").fg(Color::Red));
        }
        records_table.add_row(status_row);
    }
    c_println!("{}", records_table);
    Ok(())
}
