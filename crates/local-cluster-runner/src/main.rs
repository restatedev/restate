// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{path::PathBuf, time::Duration};

use clap::Parser;
use clap_verbosity_flag::InfoLevel;
use tracing::{error, info};

use restate_local_cluster_runner::{cluster::Cluster, shutdown};

#[derive(Debug, Clone, clap::Parser)]
#[command(author, version, about)]
pub struct Arguments {
    #[arg(short, long = "cluster-file", value_name = "FILE", global = true)]
    cluster_file: Option<PathBuf>,
    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity<InfoLevel>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let arguments = Arguments::parse();
    let _upkeep = restate_clock::ClockUpkeep::start().expect("to start the clock upkeep");

    let cluster_file = match arguments.cluster_file {
        None => {
            eprintln!("--cluster-file must be provided",);
            std::process::exit(1);
        }
        Some(f) if !f.exists() => {
            eprintln!("Cluster file {} does not exist", f.display());
            std::process::exit(1);
        }
        Some(f) => f,
    };

    let cluster: Cluster = toml::from_str(
        std::fs::read_to_string(cluster_file)
            .expect("to be able to read the cluster file")
            .as_str(),
    )
    .expect("to read a valid toml cluster file");

    // start capturing signals
    let shutdown_fut = shutdown();

    let mut cluster = cluster.start().await.unwrap();

    shutdown_fut.await;

    match cluster.graceful_shutdown(Duration::from_secs(30)).await {
        Ok(_) => {
            info!("All nodes have exited")
        }
        Err(err) => {
            error!("Failed to observe cluster status: {err}");
            std::process::exit(1)
        }
    }
}
