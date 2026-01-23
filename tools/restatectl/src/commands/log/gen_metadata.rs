// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;

use cling::prelude::*;

use restate_types::logs::builder::LogsBuilder;
use restate_types::logs::metadata::{Chain, LogletParams, ProviderKind, SegmentIndex};
use restate_types::logs::{LogId, LogletId};
use restate_types::replicated_loglet::ReplicatedLogletParams;
use restate_types::replication::{NodeSet, ReplicationProperty};
use restate_types::{GenerationalNodeId, PlainNodeId};

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "generate_log_metadata")]
pub struct GenerateLogMetadataOpts {
    #[clap(long, default_value = "2")]
    version: NonZeroU32,
    /// Replication property
    #[clap(long, short)]
    replication: ReplicationProperty,
    /// A comma-separated list of the nodes in the nodeset. e.g. N1,N2,N4 or 1,2,3
    #[clap(long, required = true, value_delimiter=',', num_args = 1..)]
    nodeset: Vec<PlainNodeId>,
    /// The generational node id of the sequencer node, e.g. N1:1
    #[clap(long)]
    sequencer: GenerationalNodeId,
    /// The number of logs
    #[clap(long, short)]
    num_logs: u32,
    /// Pretty json?
    #[clap(long)]
    pretty: bool,
}

async fn generate_log_metadata(opts: &GenerateLogMetadataOpts) -> anyhow::Result<()> {
    let mut builder = LogsBuilder::default();
    for log_id in 0..opts.num_logs {
        let log_id = LogId::from(log_id);
        let segment_index = SegmentIndex::OLDEST;
        let loglet_params = ReplicatedLogletParams {
            loglet_id: LogletId::new(log_id, segment_index),
            sequencer: opts.sequencer,
            replication: opts.replication.clone(),
            nodeset: NodeSet::from_iter(opts.nodeset.clone()),
        };
        let params = LogletParams::from(loglet_params.serialize()?);

        builder
            .add_log(log_id, Chain::new(ProviderKind::Replicated, params))
            .unwrap();
    }

    builder.set_version(opts.version);

    let logs = builder.build();
    let output = if opts.pretty {
        serde_json::to_string_pretty(&logs)?
    } else {
        serde_json::to_string(&logs)?
    };
    println!("{output}");
    Ok(())
}
