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

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::ListLogsRequest;
use restate_cli_util::{c_indentln, c_println};
use restate_types::logs::metadata::Logs;
use restate_types::replicated_loglet::ReplicatedLogletId;
use restate_types::storage::StorageCodec;
use restate_types::Versioned;
use tonic::codec::CompressionEncoding;

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "get_info")]
pub struct InfoOpts {
    /// The replicated loglet id
    loglet_id: ReplicatedLogletId,
}

async fn get_info(connection: &ConnectionInfo, opts: &InfoOpts) -> anyhow::Result<()> {
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

    let mut buf = response.logs;
    let logs = StorageCodec::decode::<Logs, _>(&mut buf)?;
    c_println!("Log Configuration ({})", logs.version());

    let Some(loglet_ref) = logs.get_replicated_loglet(&opts.loglet_id) else {
        return Err(anyhow::anyhow!("loglet {} not found", opts.loglet_id));
    };

    c_println!("Loglet Referenced in: ");
    for (log_id, segment) in &loglet_ref.references {
        c_indentln!(2, "- LogId={}, Segment={}", log_id, segment);
    }
    c_println!();
    c_println!("Loglet Parameters:");
    c_indentln!(
        2,
        "Loglet Id: {} ({})",
        loglet_ref.params.loglet_id,
        *loglet_ref.params.loglet_id
    );
    c_indentln!(2, "Sequencer: {}", loglet_ref.params.sequencer);
    c_indentln!(2, "Replication: {}", loglet_ref.params.replication);
    c_indentln!(2, "Node Set: {}", loglet_ref.params.nodeset);

    Ok(())
}
