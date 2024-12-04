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
use clap_stdin::FileOrStdin;
use cling::{Collect, Run};
use restate_admin::cluster_controller::protobuf::SetClusterConfigurationRequest;
use restate_cli_util::ui::console::{confirm_or_exit, StyledTable};
use restate_types::logs::metadata::ProviderKind;
use restate_types::Version;
use tonic::codec::CompressionEncoding;

use restate_admin::cluster_controller::protobuf::{
    cluster_ctrl_svc_client::ClusterCtrlSvcClient, GetClusterConfigurationRequest,
};
use restate_cli_util::_comfy_table::{Cell, Color, Table};
use restate_cli_util::{c_println, c_tip, c_warn};
use restate_types::cluster_controller::ClusterConfigurationSeed;
use tonic::Code;

use crate::{app::ConnectionInfo, util::grpc_connect};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "config_set")]
pub struct ConfigSetOpts {
    doc: FileOrStdin,
}

async fn config_set(connection: &ConnectionInfo, set_opts: &ConfigSetOpts) -> anyhow::Result<()> {
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

    let response = match client
        .get_cluster_configuration(GetClusterConfigurationRequest {})
        .await
    {
        Ok(response) => Some(response.into_inner()),
        Err(status) if status.code() == Code::NotFound => None,
        Err(status) => {
            anyhow::bail!("Failed to get cluster configuration: {status}");
        }
    };

    let (current_config_str, expected_version) = match response {
        None => (String::default(), Version::INVALID),
        Some(response) => {
            let version = response.version.into();
            let cluster_configuration: ClusterConfigurationSeed = response
                .cluster_configuration
                .expect("is set")
                .try_into()
                .context("Configuration type mismatch")?;

            (
                toml::to_string(&cluster_configuration).expect("is serializable"),
                version,
            )
        }
    };

    let input = set_opts.doc.clone().contents()?;

    let new_config: ClusterConfigurationSeed =
        toml::from_str(&input).context("Invalid configuration syntax")?;

    // we re serialize to make sure both current and new have a consistent format to
    // be easier to diff
    let new_config_str = toml::to_string(&new_config).expect("is serializable");

    let mut diff_table = Table::new_styled();

    let mut modified = false;
    for line in diff::lines(&current_config_str, &new_config_str) {
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

    c_tip!(
        r#"Please note that changes in configuration can take up to 5 seconds
before they are seen by all nodes.

If Configuration is being set for the first time. The system will initialize the logs with the
given configuration.

Changes of configuration does not automatically trigger a reconfigure of the logs. Reconfiguration
will happen when detected to be needed by the system.
"#
    );

    if new_config.default_provider == ProviderKind::Local {
        c_warn!(
            "
using `local` provider type is not recommended since it's currently not possible
to migrate from a `local` provider type to `replicated`.

It's recommended to use `replicated` provider type even with a `single` node, to be
able to add more nodes to the cluster in the future
        "
        )
    }

    confirm_or_exit("Apply changes?")?;

    let request = SetClusterConfigurationRequest {
        expected_version: expected_version.into(),
        cluster_configuration: Some(new_config.into()),
    };

    client
        .set_cluster_configuration(request)
        .await
        .context("Failed to set configuration")?;

    c_println!("✅ Configuration updated successfully");
    Ok(())
}
