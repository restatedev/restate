// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Unix-specific signal handling
#[cfg(unix)]
mod platform {
    use std::io::Write;

    use restate_rocksdb::RocksDbManager;
    use tokio::signal::unix::{SignalKind, signal};
    use tracing::{info, warn};

    use restate_types::config::Configuration;

    pub async fn shutdown() -> &'static str {
        let signal = tokio::select! {
            () = await_signal(SignalKind::interrupt()) => "SIGINT",
            () = await_signal(SignalKind::terminate()) => "SIGTERM"
        };

        info!(%signal, "Received signal, starting shutdown.");
        signal
    }

    /// Dump the configuration to stderr on SIGUSR1
    pub async fn sigusr1_dump_config() {
        let mut stream =
            signal(SignalKind::user_defined1()).expect("failed to register handler for SIGUSR1");

        loop {
            stream.recv().await;
            warn!("Received SIGUSR1, dumping configuration");
            let config = Configuration::pinned().dump();
            match config {
                Err(e) => warn!("Failed to dump configuration: {}", e),
                Ok(config) => {
                    let mut stderr = std::io::stderr().lock();
                    let _ = writeln!(&mut stderr, "{config}");
                }
            }
        }
    }

    /// Trigger rocksdb flush+compaction on SIGHUP
    pub async fn sighup_compact() {
        let mut stream =
            signal(SignalKind::hangup()).expect("failed to register handler for SIGHUP");

        loop {
            stream.recv().await;
            warn!("Received SIGHUP, flushing and compacting all databases");
            let manager = RocksDbManager::get();
            for db in manager.get_all_dbs() {
                let _ = match db.clone().flush_all().await {
                    Ok(_) => writeln!(std::io::stderr(), "Database '{}' flushed", db.name()),
                    Err(e) => writeln!(
                        std::io::stderr(),
                        "Database '{}' flush failed: {e}",
                        db.name()
                    ),
                };
                let db_name = db.name().to_owned();
                db.compact_all().await;
                let _ = writeln!(
                    std::io::stderr(),
                    "Database '{db_name}' compaction requested",
                );
            }
        }
    }

    pub async fn sigusr2_tokio_dump() {
        let mut stream =
            signal(SignalKind::user_defined2()).expect("failed to register handler for SIGHUP");

        let tc = restate_core::TaskCenter::current();

        loop {
            stream.recv().await;
            warn!("Received SIGUSR2, dumping tokio task backtraces");

            let _ = tc.spawn_unmanaged(restate_core::TaskKind::Disposable, "tokio-task-dump", {
                let tc = tc.clone();
                async move { tc.dump_tasks(std::io::stderr()).await }
            });
        }
    }

    async fn await_signal(kind: SignalKind) {
        signal(kind)
            .expect("failed to register signal handler")
            .recv()
            .await;
    }
}

// Windows-specific signal handling
#[cfg(windows)]
mod platform {
    use tracing::info;

    pub async fn shutdown() -> &'static str {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl-c");
        info!("Received CTRL_C, starting shutdown.");
        "CTRL_C"
    }

    /// No-op on Windows - SIGUSR1 doesn't exist
    pub async fn sigusr1_dump_config() {
        // Windows doesn't have SIGUSR1, so we just wait forever
        std::future::pending::<()>().await
    }

    /// No-op on Windows - SIGHUP doesn't exist
    pub async fn sighup_compact() {
        // Windows doesn't have SIGHUP, so we just wait forever
        std::future::pending::<()>().await
    }

    /// No-op on Windows - SIGUSR2 doesn't exist
    pub async fn sigusr2_tokio_dump() {
        // Windows doesn't have SIGUSR2, so we just wait forever
        std::future::pending::<()>().await
    }
}

pub(super) use platform::*;
