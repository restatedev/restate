// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::StyledTable;
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::{FindTailRequest, TailState};
use restate_cli_util::c_println;

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

use super::LogIdRange;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "find_tail")]
pub struct FindTailOpts {
    /// The log id(s) to find its tail.
    #[arg(required = true)]
    log_id: Vec<LogIdRange>,
}

async fn find_tail(connection: &ConnectionInfo, opts: &FindTailOpts) -> anyhow::Result<()> {
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

    let mut chain_table = Table::new_styled();
    let header_row = vec!["LOG-ID", "SEGMENT", "STATE", "TAIL-LSN"];

    chain_table.set_styled_header(header_row);

    for log_id in opts.log_id.clone().into_iter().flatten() {
        let find_tail_request = FindTailRequest { log_id };
        let response = match client.find_tail(find_tail_request).await {
            Ok(response) => response.into_inner(),
            Err(err) => {
                chain_table.add_row(vec![
                    Cell::new(log_id),
                    Cell::new(err.code()).fg(Color::DarkRed),
                    Cell::new(err.message()).fg(Color::DarkRed),
                ]);
                continue;
            }
        };

        chain_table.add_row(vec![
            Cell::new(response.log_id),
            Cell::new(response.segment_index),
            Cell::new(
                TailState::try_from(response.tail_state)
                    .map(|state| state.as_str_name())
                    .unwrap_or("unknown"),
            ),
            Cell::new(response.tail_lsn),
        ]);
    }

    c_println!("{}", chain_table);

    Ok(())
}
