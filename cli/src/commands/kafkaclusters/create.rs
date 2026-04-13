// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use cling::prelude::*;
use tempfile::tempdir;

use restate_admin_rest_model::kafka_clusters::CreateKafkaClusterRequest;
use restate_cli_util::ui::console::confirm_or_exit;
use restate_cli_util::{c_println, c_success};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::util::properties::{
    collect_kv_pairs, parse_kv_arg, parse_librdkafka_properties, parse_properties_file,
};

use super::utils::render_properties_table;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_create")]
pub struct Create {
    /// Kafka cluster name (must be a valid hostname).
    name: String,

    /// Read properties from a file. `.properties` / `.conf` / `-` (stdin) are
    /// parsed as flat librdkafka properties; other extensions are parsed as
    /// TOML with a top-level `[properties]` table.
    #[clap(short = 'f', long = "from-file", value_name = "FILE")]
    from_file: Option<PathBuf>,

    /// Open `$EDITOR` on a librdkafka properties template instead of taking
    /// properties from the command line. Mutually exclusive with positional
    /// properties and `--from-file`.
    #[clap(long, conflicts_with = "from_file")]
    edit: bool,

    /// `key=value` properties (e.g. `bootstrap.servers=broker:9092`).
    #[clap(value_name = "KEY=VALUE", value_parser = parse_kv_arg, trailing_var_arg = true, num_args = 0..)]
    properties: Vec<(String, String)>,
}

pub async fn run_create(State(env): State<CliEnv>, opts: &Create) -> Result<()> {
    if opts.edit && !opts.properties.is_empty() {
        bail!("--edit cannot be combined with positional KEY=VALUE properties");
    }
    if opts.from_file.is_some() && !opts.properties.is_empty() {
        bail!("--from-file cannot be combined with positional KEY=VALUE properties");
    }

    let properties = if let Some(path) = opts.from_file.as_ref() {
        parse_properties_file(path, "properties")?
    } else if !opts.properties.is_empty() {
        collect_kv_pairs(opts.properties.iter().cloned())?
    } else {
        edit_template_properties(&env, &opts.name)?
    };

    if properties.is_empty() {
        bail!(
            "no properties were supplied. Pass them as positional KEY=VALUE pairs, via --from-file, or via --edit."
        );
    }

    let client = AdminClient::new(&env).await?;

    c_println!("{}", render_properties_table(&properties));
    confirm_or_exit(&format!("Create Kafka cluster {}?", opts.name))?;

    let cluster_name = opts
        .name
        .parse()
        .with_context(|| format!("invalid Kafka cluster name `{}`", opts.name))?;

    let response = client
        .create_kafka_cluster(CreateKafkaClusterRequest {
            name: cluster_name,
            properties,
        })
        .await?
        .into_body()
        .await?;

    c_success!("Kafka cluster {} created", response.name.as_str());
    Ok(())
}

fn edit_template_properties(env: &CliEnv, name: &str) -> Result<HashMap<String, String>> {
    let dir = tempdir().context("failed to create a temporary directory")?;
    let path = dir.path().join("kafka-cluster.properties");
    std::fs::write(
        &path,
        format!(
            "# Define properties for Kafka cluster '{name}'.\n\
             # Format: librdkafka properties (key=value, # comments).\n\
             # At least one of bootstrap.servers / metadata.broker.list is required.\n\
             # See: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md\n\
             #\n\
             # Example:\n\
             # bootstrap.servers=broker-1:9092,broker-2:9092\n\
             # security.protocol=SASL_SSL\n\
             # sasl.mechanism=PLAIN\n\
             # sasl.username=...\n\
             # sasl.password=...\n\
             \n",
        ),
    )
    .context("failed to write the editor template")?;

    env.open_default_editor(&path)?;

    let edited = std::fs::read_to_string(&path).context("failed to read the edited file")?;
    parse_librdkafka_properties(&edited)
}
