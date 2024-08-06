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
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::DescribeLogRequest;
use restate_cli_util::_comfy_table::{Attribute, Cell, Color, Table};
use restate_cli_util::c_println;
use restate_cli_util::ui::console::StyledTable;
use restate_types::logs::metadata::{Chain, Segment};
use restate_types::storage::StorageCodec;

use crate::app::ConnectionInfo;
use crate::commands::display_util::LsnRange;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "describe_log")]
pub struct DescribeLogIdOpts {
    #[arg(short, long)]
    /// The log id to describe
    log_id: u64,
}

async fn describe_log(connection: &ConnectionInfo, opts: &DescribeLogIdOpts) -> anyhow::Result<()> {
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

    let req = DescribeLogRequest {
        log_id: opts.log_id,
    };
    let response = client.describe_log(req).await?.into_inner();

    let mut chain_table = Table::new_styled();
    chain_table.set_styled_header(vec!["SEGMENT", "LSN RANGE", "KIND"]);

    let mut buf = response.data;
    let chain = StorageCodec::decode::<Chain, _>(&mut buf)?;

    let mut segments: Vec<Segment> = chain.iter().collect();
    if segments.len() > 1 {
        for (segment, next_segment) in segments.iter_mut().zip(chain.iter().skip(1)) {
            segment.tail_lsn = Some(next_segment.base_lsn);
        }
    }

    for (idx, segment) in chain.iter().enumerate() {
        let mut segment_range = Cell::new(format!("{}", LsnRange::from(&segment)));
        if segment.base_lsn == chain.tail().base_lsn {
            segment_range = segment_range
                .fg(Color::Green)
                .add_attribute(Attribute::Bold);
        }
        chain_table.add_row(vec![
            Cell::new(idx),
            segment_range,
            Cell::new(format!("{:?}", segment.config.kind)),
        ]);
    }

    c_println!("{}", chain_table);

    Ok(())
}
