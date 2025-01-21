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
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::SetClusterConfigurationRequest;
use restate_admin::cluster_controller::protobuf::{
    cluster_ctrl_svc_client::ClusterCtrlSvcClient, GetClusterConfigurationRequest,
};
use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::{confirm_or_exit, StyledTable};
use restate_cli_util::{c_println, c_warn};
use restate_types::logs::metadata::{ProviderConfiguration, ProviderKind};
use restate_types::partition_table::ReplicationStrategy;
use restate_types::replicated_loglet::ReplicationProperty;

use crate::commands::cluster::config::cluster_config_string;
use crate::commands::cluster::provision::extract_default_provider;
use crate::{app::ConnectionInfo, util::grpc_channel};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "config_set")]
pub struct ConfigSetOpts {
    /// Replication strategy. Possible values
    /// are `on-all-nodes` or `factor(n)`
    #[clap(long)]
    replication_strategy: Option<ReplicationStrategy>,

    /// Bifrost provider kind.
    #[clap(long)]
    bifrost_provider: Option<ProviderKind>,

    /// Replication property
    #[clap(long, required_if_eq("bifrost_provider", "replicated"))]
    replication_property: Option<ReplicationProperty>,
}

async fn config_set(connection: &ConnectionInfo, set_opts: &ConfigSetOpts) -> anyhow::Result<()> {
    let channel = grpc_channel(connection.cluster_controller.clone());
    let mut client =
        ClusterCtrlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let response = client
        .get_cluster_configuration(GetClusterConfigurationRequest {})
        .await
        .context("Failed to get cluster configuration")?
        .into_inner();

    let mut current = response.cluster_configuration.expect("must be set");

    let current_config_string = cluster_config_string(&current)?;

    if let Some(replication_strategy) = set_opts.replication_strategy {
        current.replication_strategy = Some(replication_strategy.into());
    }

    if let Some(provider) = set_opts.bifrost_provider {
        let default_provider =
            extract_default_provider(provider, set_opts.replication_property.clone());

        match default_provider {
            ProviderConfiguration::InMemory | ProviderConfiguration::Local => {
                c_warn!("You are about to reconfigure your cluster with a Bifrost provider that only supports a single node cluster.");
            }
            ProviderConfiguration::Replicated(_) => {
                // nothing to do
            }
        }

        current.bifrost_provider = Some(default_provider.into());
    }

    let updated_config_string = cluster_config_string(&current)?;

    let mut diff_table = Table::new_styled();

    let mut modified = false;
    for line in diff::lines(&current_config_string, &updated_config_string) {
        let (is_diff, cell) = match line {
            diff::Result::Both(l, _) => (false, Cell::new(format!(" {}", l))),
            diff::Result::Left(l) => (true, Cell::new(format!("-{}", l)).fg(Color::Red)),
            diff::Result::Right(r) => (true, Cell::new(format!("+{}", r)).fg(Color::Green)),
        };

        diff_table.add_row(vec![cell]);
        modified |= is_diff;
    }

    if !modified {
        c_println!("🤷 No changes");
        return Ok(());
    }

    c_println!("{}", diff_table);
    c_println!();

    confirm_or_exit("Apply changes?")?;

    let request = SetClusterConfigurationRequest {
        cluster_configuration: Some(current),
    };

    client
        .set_cluster_configuration(request)
        .await
        .map_err(|err| anyhow::anyhow!("Failed to set configuration: {}", err.message()))?;

    c_println!("✅ Configuration updated successfully");

    Ok(())
}
