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
use restate_admin::cluster_controller::protobuf::ListLogsRequest;
use restate_cli_util::_comfy_table::{Attribute, Cell, Color, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_types::logs::metadata::{Chain, Logs};
use restate_types::logs::LogId;
use restate_types::storage::StorageCodec;

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "list_logs")]
pub struct ListLogsOpts {}

async fn list_logs(connection: &ConnectionInfo, _opts: &ListLogsOpts) -> anyhow::Result<()> {
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

    let req = ListLogsRequest::default();
    let response = client.list_logs(req).await?.into_inner();

    let mut logs_table = Table::new_styled();
    logs_table.set_styled_header(vec!["LOG-ID", "SEGMENTS", "TAIL BASE LSN", "KIND"]);

    let mut buf = response.data;
    let logs = StorageCodec::decode::<Logs, _>(&mut buf)?;
    // sort by log-id for display
    let logs: BTreeMap<LogId, &Chain> = logs.iter().map(|(id, chain)| (*id, chain)).collect();

    for (log_id, chain) in logs {
        logs_table.add_row(vec![
            Cell::new(log_id),
            Cell::new(chain.num_segments()).fg(Color::DarkGrey),
            Cell::new(format!("{}", &chain.tail().base_lsn))
                .fg(Color::Green)
                .add_attribute(Attribute::Bold),
            Cell::new(format!("{:?}", chain.tail().config.kind)),
        ]);
    }

    c_println!("{}", logs_table);

    Ok(())
}
