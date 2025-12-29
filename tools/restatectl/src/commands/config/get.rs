// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::Parser;
use cling::{Collect, Run};
use restate_core::protobuf::cluster_ctrl_svc::{
    GetClusterConfigurationRequest, new_cluster_ctrl_client,
};

use restate_cli_util::{CliContext, c_println};
use restate_types::nodes_config::Role;

use super::cluster_config_string;
use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "config_get")]
pub struct ConfigGetOpts {}

async fn config_get(connection: &ConnectionInfo, _get_opts: &ConfigGetOpts) -> anyhow::Result<()> {
    let response = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .get_cluster_configuration(GetClusterConfigurationRequest {})
                .await
        })
        .await?;

    let configuration = response.into_inner();
    let cluster_configuration = configuration.cluster_configuration.expect("is set");

    let output = cluster_config_string(&cluster_configuration)?;

    c_println!("{}", output);
    Ok(())
}
