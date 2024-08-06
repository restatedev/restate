// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use anyhow::Context;
use cling::prelude::*;
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::LogStateRequest;
use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_types::logs::metadata::{Chain, Logs};
use restate_types::logs::LogId;
use restate_types::storage::StorageCodec;

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "get_log_state")]
pub struct StateOpts {}

async fn get_log_state(connection: &ConnectionInfo, _opts: &StateOpts) -> anyhow::Result<()> {
    let channel = grpc_connect(connection.cluster_controller.clone())
        .await
        .with_context(|| {
            format!(
                "cannot connect to cluster controller at {}",
                connection.cluster_controller
            )
        })?;
    let mut client =
        ClusterCtrlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let req = LogStateRequest::default();
    let response = client.get_log_state(req).await?.into_inner();

    let mut logs_table = Table::new_styled();
    logs_table.set_styled_header(vec!["P-ID", "HEAD", "TAIL", "KIND"]);

    let mut buf = response.log_state;
    let logs = StorageCodec::decode::<Logs, _>(&mut buf)?;
    // sort by log-id for display
    let logs: BTreeMap<LogId, &Chain> = logs.iter().map(|(id, chain)| (*id, chain)).collect();

    for (log_id, chain) in logs {
        logs_table.add_row(vec![
            Cell::new(log_id),
            Cell::new(match chain.num_segments() {
                0..=1 => "∅".to_string(),
                2 => "1 segment".to_string(),
                3.. => format!("{} segments", chain.num_segments() - 1),
            })
            .fg(Color::DarkGrey),
            Cell::new(format!("{}", chain.tail())).fg(Color::Green),
            Cell::new(format!("{:?}", chain.tail().config.kind)),
        ]);
    }

    c_println!("{}", logs_table);

    Ok(())
}
