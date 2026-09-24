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

use anyhow::{Result, bail};
use cling::prelude::*;

use restate_admin_rest_model::kafka_clusters::UpdateKafkaClusterRequest;
use restate_cli_util::{CliContext, c_println, c_success};

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::ui::fmt::{DryRun, Field, Formatter, OutputFormatter};
use crate::util::properties::{
    collect_kv_pairs, parse_kv_arg, parse_properties_file, redacted_keys,
};

use super::utils::{property_diff, property_diff_rows};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_patch")]
pub struct Patch {
    /// Kafka cluster name
    name: String,

    /// Set or replace a property (`KEY=VALUE`). Repeatable.
    #[clap(long = "set", value_name = "KEY=VALUE", value_parser = parse_kv_arg)]
    set: Vec<(String, String)>,

    /// Remove a property. Repeatable.
    #[clap(long = "unset", value_name = "KEY")]
    unset: Vec<String>,

    /// Use the given file as the new properties baseline (full replacement).
    /// `--set` / `--unset` are applied on top. Without `-f`, the current
    /// server-side properties are used as the baseline.
    #[clap(short = 'f', long = "from-file", value_name = "FILE")]
    from_file: Option<PathBuf>,

    #[clap(flatten)]
    dry_run: DryRun,
}

pub async fn run_patch(State(env): State<CliEnv>, opts: &Patch) -> Result<()> {
    if opts.set.is_empty() && opts.unset.is_empty() && opts.from_file.is_none() {
        bail!("no changes requested: pass --set KEY=VALUE, --unset KEY, or --from-file FILE");
    }
    // Detect duplicate --set keys eagerly so the error mentions --set instead
    // of bubbling up from collect_kv_pairs as a generic message.
    let sets = collect_kv_pairs(opts.set.iter().cloned())?;

    let client = AdminClient::new(&env).await?;

    // Baseline + visible-baseline: visible-baseline is what we render in the
    // diff (without any *** values). The baseline used for the actual PATCH
    // payload may include redacted values from the server, which is why we
    // refuse to send those when they survived through to the final map.
    let (baseline, visible_baseline) = if let Some(path) = opts.from_file.as_ref() {
        let m = parse_properties_file(path, "properties")?;
        (m.clone(), m)
    } else {
        let current = client
            .get_kafka_cluster(&opts.name, false)
            .await?
            .into_body()
            .await?
            .properties;
        let visible = current
            .iter()
            .filter(|(_, v)| v.as_str() != crate::util::properties::REDACTION_PLACEHOLDER)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        (current, visible)
    };

    let mut new_props = baseline.clone();
    for k in &opts.unset {
        new_props.remove(k);
    }
    for (k, v) in sets {
        new_props.insert(k, v);
    }

    let still_redacted = redacted_keys(&new_props);
    if !still_redacted.is_empty() {
        bail!(
            "the following properties have redacted (***) values: {}. \
             Re-set them via --set KEY=VALUE, or pass a complete config with --from-file.",
            still_redacted.join(", ")
        );
    }

    apply_kafka_cluster_update(
        &client,
        &opts.name,
        &visible_baseline,
        new_props,
        &opts.dry_run,
    )
    .await
}

/// Renders the diff, asks for confirmation and submits the PATCH. Used both
/// here and from `edit`.
pub(super) async fn apply_kafka_cluster_update(
    client: &AdminClient,
    name: &str,
    visible_baseline: &HashMap<String, String>,
    new_props: HashMap<String, String>,
    dry_run: &DryRun,
) -> Result<()> {
    let json = CliContext::get().json_output();
    let mut f = Formatter::new();
    let diff = property_diff(visible_baseline, &new_props);
    if json {
        let rows: Vec<Vec<Field>> = diff
            .iter()
            .map(|(key, old, new)| {
                vec![
                    Field::new(name),
                    Field::new("update"),
                    Field::new(*key),
                    Field::new(*old),
                    Field::new(*new),
                ]
            })
            .collect();
        f.table(
            "changes",
            &[
                "kafka_cluster",
                "change",
                "property",
                "old_value",
                "new_value",
            ],
            &rows,
        );
    } else if !diff.is_empty() {
        f.table(
            "changes",
            &["property", "old", "new"],
            property_diff_rows(&diff),
        );
    }
    if diff.is_empty() {
        if !json {
            c_println!("No changes detected. Aborting.");
        }
        return f.finish();
    }
    f.confirm(
        dry_run,
        &format!("Apply these changes to Kafka cluster {name}?"),
    )?;

    let _ = client
        .update_kafka_cluster(
            name,
            UpdateKafkaClusterRequest {
                properties: new_props,
            },
        )
        .await?
        .into_body()
        .await?;

    if !json {
        c_success!("Kafka cluster {name} updated");
    }
    f.next_step(
        &format!("restate kafka-clusters describe {name}"),
        "see the updated Kafka cluster",
    );
    f.finish()
}
