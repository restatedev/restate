// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License,

use std::num::{NonZeroU32, NonZeroU8};

use anyhow::Context;
use clap::Parser;
use cling::{Collect, Run};
use restate_types::{
    logs::{
        builder::LogsBuilder,
        metadata::{Chain, LogletParams, ProviderKind},
        LogId,
    },
    replicated_loglet::{NodeSet, ReplicatedLogletId, ReplicatedLogletParams, ReplicationProperty},
    GenerationalNodeId, PlainNodeId, Version,
};

use crate::commands::metadata::{logs::KEY_METADATA_LOGS, set_value, MetadataCommonOpts};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "generate_log_metadata")]
pub struct GenerateLogMetadataOpts {
    #[clap(flatten)]
    metadata: MetadataCommonOpts,
    #[clap(long, default_value = "2")]
    version: NonZeroU32,
    /// Replication property
    #[clap(long, short)]
    replication_factor: NonZeroU8,
    /// A comma-separated list of the nodes in the nodeset. e.g. N1,N2,N4 or 1,2,3
    #[clap(long, required = true, value_delimiter=',', num_args = 1..)]
    nodeset: Vec<PlainNodeId>,
    /// The generational node id of the sequencer node, e.g. N1:1
    #[clap(long, short)]
    sequencer: GenerationalNodeId,
    /// The number of logs
    #[clap(long, short)]
    num_logs: u32,
    /// Base LSN
    #[clap(long, short, default_value_t = 1)]
    base_lsn: u64,
    /// Dry run
    #[clap(long, default_value_t = false)]
    dry_run: bool,
}

async fn generate_log_metadata(opts: &GenerateLogMetadataOpts) -> anyhow::Result<()> {
    let mut builder = LogsBuilder::default();
    for log_id in 0..opts.num_logs {
        let minor = 1;
        // format is, log_id in the higher order u32, and 1 in the lower.
        let loglet_id: u64 = (u64::from(log_id) << (size_of::<LogId>() * 8)) + minor;
        let loglet_params = ReplicatedLogletParams {
            loglet_id: ReplicatedLogletId::new(loglet_id),
            sequencer: opts.sequencer,
            replication: ReplicationProperty::new(opts.replication_factor),
            nodeset: NodeSet::from_iter(opts.nodeset.clone()),
            write_set: None,
        };
        let params = LogletParams::from(loglet_params.serialize()?);
        builder
            .add_log(
                LogId::from(log_id),
                Chain::with_base_lsn(opts.base_lsn.into(), ProviderKind::Replicated, params),
            )
            .unwrap();
    }

    builder.set_version(opts.version);

    let logs = builder.build();
    let output = serde_json::to_string_pretty(&logs)?;
    println!("{}", output);

    if !opts.dry_run {
        set_value(
            &opts.metadata,
            KEY_METADATA_LOGS,
            &logs,
            Version::from(opts.version.get() - 1),
        )
        .await
        .context("Failed to set metadata")?;
    }
    Ok(())
}
