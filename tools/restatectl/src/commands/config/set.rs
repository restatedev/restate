// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{CliContext, c_println, c_warn};
use restate_core::protobuf::cluster_ctrl_svc::{
    GetClusterConfigurationRequest, SetClusterConfigurationRequest, new_cluster_ctrl_client,
};
use restate_types::logs::metadata::ProviderKind;
use restate_types::nodes_config::Role;
use restate_types::replication::ReplicationProperty;

use super::cluster_config_string;
use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "config_set")]
pub struct ConfigSetOpts {
    /// Replication property of logs and partitions.
    ///
    /// (mutually exclusive with `log-replication` and/or `partition-replication`)
    #[clap(short, long, group = "sep1", group = "sep2")]
    replication: Option<ReplicationProperty>,

    /// Replication property of bifrost logs if using replicated log provider
    #[clap(long, group = "sep1")]
    log_replication: Option<ReplicationProperty>,

    /// Partition replication. If unset, uses
    /// the default `admin.default-partition-replication`
    #[clap(long, group = "sep2")]
    partition_replication: Option<ReplicationProperty>,

    /// Default log provider kind
    #[clap(long)]
    log_provider: Option<ProviderKind>,

    /// The nodeset size used for replicated log, this is an advanced feature.
    /// It's recommended to leave it unset (defaults to 0)
    #[clap(long)]
    log_default_nodeset_size: Option<u16>,

    /// Number of partitions.
    ///
    /// It is only possible to change the number of partitions if the current cluster value is 0.
    /// Otherwise, the set operation will fail.
    #[clap(short, long)]
    num_partitions: Option<u16>,
}

async fn config_set(connection: &ConnectionInfo, set_opts: &ConfigSetOpts) -> anyhow::Result<()> {
    let response = connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .get_cluster_configuration(GetClusterConfigurationRequest {})
                .await
        })
        .await
        .context("Failed to get cluster configuration")?
        .into_inner();

    let mut current = response.cluster_configuration.expect("must be set");

    let current_config_string = cluster_config_string(&current)?;

    let partition_replication = set_opts
        .partition_replication
        .clone()
        .or(set_opts.replication.clone());

    let log_replication = set_opts
        .log_replication
        .clone()
        .or(set_opts.replication.clone());

    if let Some(replication_property) = partition_replication {
        current.partition_replication = Some(replication_property.into());
    }

    set_opts.log_provider.inspect(|provider| {
        match provider {
            ProviderKind::InMemory => {
                c_warn!("You are about to reconfigure your cluster with a Bifrost provider that only supports a single node cluster.");
            }
            ProviderKind::Local => {
                c_warn!("You are about to reconfigure your cluster with a Bifrost provider that only supports a single node cluster.");
            }
            ProviderKind::Replicated => {
                // nothing to do
            }
        }
    });

    let Some(bifrost_provider) = current.bifrost_provider.as_mut() else {
        anyhow::bail!(
            "The cluster has no Bifrost provider configured. This indicates a problem with the cluster."
        );
    };

    if let Some(provider) = set_opts.log_provider {
        bifrost_provider.provider = provider.to_string();
    }

    if let Some(log_replication) = log_replication {
        bifrost_provider.replication_property = Some(log_replication.into());
    }

    if let Some(nodeset_size) = set_opts.log_default_nodeset_size {
        bifrost_provider.target_nodeset_size = u32::from(nodeset_size);
    }

    if let Some(num_partitions) = set_opts.num_partitions {
        if current.num_partitions != 0 {
            anyhow::bail!(
                "The cluster has already been provisioned with partitions. Restate does not support repartitioning, yet."
            );
        }
        current.num_partitions = num_partitions.into();
    }

    let updated_config_string = cluster_config_string(&current)?;

    let mut diff_table = Table::new_styled();

    let mut modified = false;
    for line in diff::lines(&current_config_string, &updated_config_string) {
        let (is_diff, cell) = match line {
            diff::Result::Both(l, _) => (false, Cell::new(format!(" {l}"))),
            diff::Result::Left(l) => (true, Cell::new(format!("-{l}")).fg(Color::Red)),
            diff::Result::Right(r) => (true, Cell::new(format!("+{r}")).fg(Color::Green)),
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

    connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .set_cluster_configuration(request.clone())
                .await
        })
        .await
        .context("Failed to set configuration")?;

    c_println!("✅ Configuration updated successfully");

    Ok(())
}
