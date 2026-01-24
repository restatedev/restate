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
use bytes::Bytes;
use clap::Parser;
use clap_stdin::FileOrStdin;
use cling::{Collect, Run};

use restate_cli_util::_comfy_table::Table;
use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::{CliContext, c_println};
use restate_core::protobuf::cluster_ctrl_svc::{MigrateMetadataRequest, new_cluster_ctrl_client};
use restate_types::config::MetadataClientOptions;
use restate_types::nodes_config::Role;

use crate::commands::metadata::MetadataCommonOpts;
use crate::connection::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "migrate")]
pub struct MigrateOpts {
    #[clap(flatten)]
    metadata: MetadataCommonOpts,

    /// The TOML configuration of the target metadata store
    #[arg(long)]
    toml: FileOrStdin,

    /// Force override stored metadata in target store
    #[arg(long, default_value_t = false)]
    force: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ConfigWrapper {
    metadata_client: MetadataClientOptions,
}

async fn migrate(connection: &ConnectionInfo, opts: &MigrateOpts) -> anyhow::Result<()> {
    let config: ConfigWrapper = toml::from_str(opts.toml.clone().contents()?.as_str())
        .context("Failed to load provider config from toml input")?;

    let content = Bytes::from(serde_json::to_vec(&config.metadata_client)?);

    connection
        .try_each(Some(Role::Admin), |channel| async {
            new_cluster_ctrl_client(channel, &CliContext::get().network)
                .migrate_metadata(MigrateMetadataRequest {
                    force_override: opts.force,
                    target_client_config: content.clone(),
                })
                .await
        })
        .await?;

    c_println!("\n");

    let mut table = Table::new_styled();
    table.add_kv_row("✅", "Metadata migration completed");
    table.add_kv_row("1.", "Please make sure to update all nodes config with\n");
    table.add_kv_row("", toml::to_string(&config).expect("to serialize"));
    table.add_kv_row(
        "2.",
        "Restart all nodes without the --metadata-migration-mode",
    );

    c_println!("{table}\n");
    Ok(())
}
