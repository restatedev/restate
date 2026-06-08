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
use comfy_table::Table;
use http::Uri;
use tempfile::tempdir;

use restate_admin_rest_model::kafka_clusters::KafkaClusterResponse;
use restate_admin_rest_model::subscriptions::CreateSubscriptionRequest;
use restate_cli_util::ui::console::{StyledTable, confirm_or_exit};
use restate_cli_util::{c_println, c_success};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::commands::kafkaclusters::utils as kc_shared;
use crate::util::properties::{
    collect_kv_pairs, parse_kv_arg, parse_librdkafka_properties, parse_properties_file,
};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_create")]
pub struct Create {
    /// Source URI, e.g. `kafka://<cluster_name>/<topic>`. May be omitted
    /// when `--from-file` or `--edit` is used.
    source: Option<String>,

    /// Sink URI, e.g. `service://<service>/<handler>`. May be omitted when
    /// `--from-file` or `--edit` is used.
    sink: Option<String>,

    /// Read source/sink/options from a file. `.properties` / `.conf` / `-`
    /// are parsed as flat librdkafka properties (recognised top-level keys:
    /// `source`, `sink`); other extensions are TOML with top-level `source`,
    /// `sink`, and an `[options]` table.
    #[clap(short = 'f', long = "from-file", value_name = "FILE")]
    from_file: Option<PathBuf>,

    /// Open `$EDITOR` on a librdkafka template instead of taking values from
    /// the command line.
    #[clap(long, conflicts_with = "from_file")]
    edit: bool,

    /// `key=value` options (e.g. `group.id=my-group`).
    #[clap(value_name = "KEY=VALUE", value_parser = parse_kv_arg, trailing_var_arg = true, num_args = 0..)]
    options: Vec<(String, String)>,
}

pub async fn run_create(State(env): State<CliEnv>, opts: &Create) -> Result<()> {
    if opts.edit && (!opts.options.is_empty() || opts.source.is_some() || opts.sink.is_some()) {
        bail!(
            "--edit cannot be combined with positional arguments; pass everything through the editor"
        );
    }
    if opts.from_file.is_some() && !opts.options.is_empty() {
        bail!("--from-file cannot be combined with positional KEY=VALUE options");
    }

    let (source, sink, options) = if let Some(path) = opts.from_file.as_ref() {
        let mut full = parse_properties_file(path, "options")?;
        let source = opts
            .source
            .clone()
            .or_else(|| full.remove("source"))
            .context("`source` was not provided on the CLI nor in the file")?;
        let sink = opts
            .sink
            .clone()
            .or_else(|| full.remove("sink"))
            .context("`sink` was not provided on the CLI nor in the file")?;
        (source, sink, full)
    } else if opts.edit {
        let (source, sink, options) = edit_template(&env)?;
        (source, sink, options)
    } else {
        let source = opts
            .source
            .clone()
            .context("`source` is required (or pass --from-file/--edit)")?;
        let sink = opts
            .sink
            .clone()
            .context("`sink` is required (or pass --from-file/--edit)")?;
        let options = collect_kv_pairs(opts.options.iter().cloned())?;
        (source, sink, options)
    };

    let client = AdminClient::new(&env).await?;

    let source_uri: Uri = source
        .parse()
        .with_context(|| format!("invalid source URI `{source}`"))?;
    let sink_uri: Uri = sink
        .parse()
        .with_context(|| format!("invalid sink URI `{sink}`"))?;

    print_summary(&source, &sink, &options, None);
    confirm_or_exit("Create this subscription?")?;

    let response = client
        .create_subscription(CreateSubscriptionRequest {
            source: source_uri,
            sink: sink_uri,
            options: if options.is_empty() {
                None
            } else {
                Some(options)
            },
        })
        .await?
        .into_body()
        .await?;

    c_success!("Subscription {} created", response.id);
    Ok(())
}

fn print_summary(
    source: &str,
    sink: &str,
    options: &HashMap<String, String>,
    cluster: Option<&KafkaClusterResponse>,
) {
    let mut table = Table::new_styled();
    table.add_kv_row("Source:", source);
    table.add_kv_row("Sink:", sink);
    if let Some(c) = cluster
        && let Some(brokers) = kc_shared::brokers_property(&c.properties)
    {
        table.add_kv_row("Kafka brokers:", brokers);
    }
    c_println!("{table}");

    if options.is_empty() {
        c_println!("Options: (none)");
    } else {
        c_println!("Options:");
        c_println!("{}", kc_shared::render_properties_table(options));
    }
}

fn edit_template(env: &CliEnv) -> Result<(String, String, HashMap<String, String>)> {
    let dir = tempdir().context("failed to create a temporary directory")?;
    let path = dir.path().join("subscription.properties");
    std::fs::write(
        &path,
        "# Define a subscription.\n\
         # Format: librdkafka properties (key=value, # comments).\n\
         # Required:\n\
         #   source=kafka://<cluster_name>/<topic_name>\n\
         #   sink=service://<service_name>/<handler_name>\n\
         # All other keys are passed through as Kafka consumer options.\n\
         #\n\
         # Example:\n\
         # source=kafka://my-cluster/orders\n\
         # sink=service://Counter/count\n\
         # group.id=order-processor\n\
         \n",
    )
    .context("failed to write the editor template")?;

    env.open_default_editor(&path)?;

    let edited_text = std::fs::read_to_string(&path).context("failed to read the edited file")?;
    let mut full = parse_librdkafka_properties(&edited_text)?;
    let source = full
        .remove("source")
        .context("the edited file is missing `source=...`")?;
    let sink = full
        .remove("sink")
        .context("the edited file is missing `sink=...`")?;
    Ok((source, sink, full))
}
