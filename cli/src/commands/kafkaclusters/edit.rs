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

use anyhow::{Context, Result};
use cling::prelude::*;
use tempfile::tempdir;

use restate_cli_util::c_println;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::util::properties::{
    REDACTION_PLACEHOLDER, parse_librdkafka_properties, redacted_keys,
    serialize_librdkafka_properties,
};

use super::patch::apply_kafka_cluster_update;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_edit")]
pub struct Edit {
    /// Kafka cluster name
    name: String,
}

pub async fn run_edit(State(env): State<CliEnv>, opts: &Edit) -> Result<()> {
    let client = AdminClient::new(&env).await?;
    let cluster = client
        .get_kafka_cluster(&opts.name, false)
        .await?
        .into_body()
        .await?;

    let baseline_with_redaction = cluster.properties.clone();
    let redacted = redacted_keys(&baseline_with_redaction);
    // The non-redacted baseline is used for diffing — secret values are
    // unknown to us so we treat them as "preserved" rather than "changed".
    let mut baseline_visible: HashMap<String, String> = baseline_with_redaction
        .iter()
        .filter(|(_, v)| v.as_str() != REDACTION_PLACEHOLDER)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let dir = tempdir().context("failed to create a temporary directory")?;
    let path = dir.path().join("kafka-cluster.properties");

    let mut prelude = format!(
        "# Editing Kafka cluster '{}'.\n\
         # Format: librdkafka properties (key=value, # comments).\n\
         # Save and exit to apply changes; abort by leaving the file unchanged.\n",
        opts.name
    );
    if !redacted.is_empty() {
        prelude.push_str(
            "#\n\
             # Lines beginning with `# REDACTED:` are server-redacted secret\n\
             # properties. To preserve them, leave the line commented. To replace\n\
             # one, uncomment the line and supply the new value.\n",
        );
    }
    prelude.push('\n');

    let body = serialize_librdkafka_properties(&baseline_with_redaction);
    std::fs::write(&path, format!("{prelude}{body}"))
        .context("failed to write the editor template")?;

    env.open_default_editor(&path)?;

    let edited_text = std::fs::read_to_string(&path).context("failed to read the edited file")?;
    let edited = parse_librdkafka_properties(&edited_text)?;

    // Merge: any redacted key the user did not explicitly re-set is preserved
    // by carrying the original (still-redacted) value forward. The PATCH
    // endpoint replaces the full properties map, so we have to send those
    // back; the server will preserve them if they're identical to the current
    // value (placeholder included).
    let mut to_send = edited.clone();
    for k in &redacted {
        if !to_send.contains_key(k) {
            to_send.insert(k.clone(), REDACTION_PLACEHOLDER.to_string());
            // Make redacted properties show as "unchanged" in the diff by
            // adding them to the visible baseline with the same placeholder.
            baseline_visible.insert(k.clone(), REDACTION_PLACEHOLDER.to_string());
        }
    }

    if to_send == baseline_with_redaction {
        c_println!("No changes detected. Aborting.");
        return Ok(());
    }

    apply_kafka_cluster_update(&client, &opts.name, &baseline_visible, to_send).await
}
