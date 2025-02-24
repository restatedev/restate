// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use restate_core::config::Configuration;
use restate_rocksdb::RocksDbManager;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{info, warn};

pub(super) async fn shutdown() -> &'static str {
    let signal = tokio::select! {
        () = await_signal(SignalKind::interrupt()) => "SIGINT",
        () = await_signal(SignalKind::terminate()) => "SIGTERM"
    };

    info!(%signal, "Received signal, starting shutdown.");
    signal
}

/// Dump the configuration to stderr on SIGUSR1
pub(super) async fn sigusr1_dump_config() {
    let mut stream =
        signal(SignalKind::user_defined1()).expect("failed to register handler for SIGUSR1");

    loop {
        stream.recv().await;
        warn!("Received SIGUSR1, dumping configuration");
        let config = Configuration::with_current(|config| config.dump());
        match config {
            Err(e) => warn!("Failed to dump configuration: {}", e),
            Ok(config) => {
                let mut stderr = std::io::stderr().lock();
                let _ = writeln!(&mut stderr, "{config}");
            }
        }
    }
}

/// Trigger rocksdb flush+compaction on SIGUSR2
pub(super) async fn sighusr2_compact() {
    let mut stream =
        signal(SignalKind::user_defined2()).expect("failed to register handler for SIGUSR2");

    loop {
        stream.recv().await;
        warn!("Received SIGUSR2, flushing and compacting all databases");
        let manager = RocksDbManager::get();
        for db in manager.get_all_dbs() {
            let _ = match db.flush_all().await {
                Ok(_) => writeln!(std::io::stderr(), "Database '{}' flushed", db.name),
                Err(e) => writeln!(
                    std::io::stderr(),
                    "Database '{}' flush failed: {e}",
                    db.name
                ),
            };
            db.compact_all().await;
            let _ = writeln!(
                std::io::stderr(),
                "Database '{}' compaction requested",
                db.name
            );
        }
    }
}

async fn await_signal(kind: SignalKind) {
    signal(kind)
        .expect("failed to register signal handler")
        .recv()
        .await;
}
