// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use cling::prelude::*;

use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::cluster_ctrl_svc::{
    FindTailRequest, TailState, new_cluster_ctrl_client,
};
use restate_types::nodes_config::Role;
use tonic::IntoRequest;

use crate::connection::ConnectionInfo;
use crate::util::RangeParam;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "find_tail")]
pub struct FindTailOpts {
    /// The log id or range to find its tail, e.g. "0", "1-4".
    #[arg(required = true)]
    log_id: Vec<RangeParam>,
}

async fn find_tail(connection: &ConnectionInfo, opts: &FindTailOpts) -> anyhow::Result<()> {
    let mut chain_table = Table::new_styled();
    let header_row = vec!["LOG-ID", "SEGMENT", "STATE", "TAIL-LSN"];

    chain_table.set_styled_header(header_row);

    for log_id in opts.log_id.clone().into_iter().flatten() {
        let find_tail_request = FindTailRequest { log_id };

        let response = match connection
            .try_each(Some(Role::Admin), |channel| async {
                let mut find_tail_request = find_tail_request.into_request();
                find_tail_request.set_timeout(Duration::from_secs(10));

                new_cluster_ctrl_client(channel, &CliContext::get().network)
                    .find_tail(find_tail_request)
                    .await
            })
            .await
        {
            Ok(response) => response,
            Err(err) => {
                chain_table.add_row(vec![
                    Cell::new(log_id),
                    Cell::new("").fg(Color::DarkRed),
                    Cell::new(err).fg(Color::DarkRed),
                ]);

                continue;
            }
        }
        .into_inner();

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
