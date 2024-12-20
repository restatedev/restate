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
use clap::Parser;
use cling::{Collect, Run};
use tonic::{codec::CompressionEncoding, Code};

use restate_admin::cluster_controller::protobuf::{
    cluster_ctrl_svc_client::ClusterCtrlSvcClient, GetClusterConfigurationRequest,
};
use restate_cli_util::c_println;

use crate::{
    app::ConnectionInfo, commands::cluster::config::cluster_config_string, util::grpc_connect,
};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "config_get")]
pub struct ConfigGetOpts {}

async fn config_get(connection: &ConnectionInfo, _get_opts: &ConfigGetOpts) -> anyhow::Result<()> {
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

    let response = client
        .get_cluster_configuration(GetClusterConfigurationRequest {})
        .await;

    let response = match response {
        Ok(response) => response,
        Err(status) if status.code() == Code::NotFound => {
            c_println!("👻 Cluster is not configured");
            return Ok(());
        }
        Err(status) => {
            anyhow::bail!("Failed to get cluster configuration: {status}");
        }
    };

    let configuration = response.into_inner();
    let cluster_configuration = configuration.cluster_configuration.expect("is set");

    let output = cluster_config_string(&cluster_configuration)?;

    c_println!("{}", output);

    Ok(())
}
