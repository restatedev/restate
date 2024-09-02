// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;
use anyhow::{bail, Context};
use clap::Parser;
use restate_bifrost::loglet::{LogletOffset, LogletProvider};
use restate_bifrost::{Bifrost, BifrostService, Record};
use restate_core::{metadata, task_center, MetadataBuilder, TaskCenterBuilder, TaskKind};
use restate_rocksdb::RocksDbManager;
use restate_types::config::{set_base_temp_dir, Configuration};
use restate_types::logs::metadata::LogletParams;
use restate_types::logs::{LogId, SequenceNumber};
use restate_wal_protocol::Envelope;
use std::path::PathBuf;
use cling::{Collect, Run};
use crate::app::ConnectionInfo;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[clap()]
#[cling(run = "jsonify_log")]
pub struct JsonifyLog {
    /// Directory containing the Restate data. Note, it needs to be the node base directory which
    /// is by default restate-data/<hostname>.
    #[arg(short, long = "path", value_name = "FILE")]
    path: PathBuf,

    /// Specifies the number of logs to dump. The tool will iterate over the range [0, logs).
    #[arg(short, long = "logs", default_value_t = 64)]
    logs: u32,
}

async fn jsonify_log(_: &ConnectionInfo, opts: &JsonifyLog) -> anyhow::Result<()> {
    // update node base dir to point to correct directory
    set_base_temp_dir(opts.path.clone());

    let config = Configuration::default();
    restate_types::config::set_current_config(config);

    let config = Configuration::pinned();

    if !config.bifrost.local.data_dir().exists() {
        bail!("The specified path does not contain a local-loglet directory.")
    }

        let task_center = TaskCenterBuilder::default().build().unwrap();

        task_center.run_in_scope("main", None, async {
            let _rocksdb_manager =
                RocksDbManager::init(Configuration::mapped_updateable(|c| &c.common));

            let bifrost = Bifrost::init_local(MetadataBuilder::default().to_metadata()).await;

            for log_id in 0..opts.logs {
                let log_id = LogId::from(log_id);

                for record in bifrost.read_all(log_id).await.unwrap() {
                    if let Some(data) = record.try_decode::<Envelope>() {
                        println!("{}", serde_json::to_string(&data.unwrap()).unwrap());
                    }
                }
            }
        }).await;

    Ok(())
}
