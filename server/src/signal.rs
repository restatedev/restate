// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_server::Configuration;
use tokio::signal::unix::{signal, SignalKind};
use tracing::info;

pub(super) async fn shutdown() -> &'static str {
    let signal = tokio::select! {
        () = await_signal(SignalKind::interrupt()) => "SIGINT",
        () = await_signal(SignalKind::terminate()) => "SIGTERM"
    };

    info!(%signal, "Received signal, starting shutdown.");
    signal
}

/// Dump the configuration to the log (level=info) on SIGUSR1
pub(super) async fn sigusr_dump_config() {
    let mut stream = signal(SignalKind::user_defined1()).expect(
        "failed to register handler for
SIGUSR",
    );

    loop {
        stream.recv().await;
        info!("Received SIGUSR1, dumping configuration");
        let config = Configuration::pinned();
        info!(
            "{}",
            toml::to_string(config.as_ref()).expect("valid config")
        );
    }
}

async fn await_signal(kind: SignalKind) {
    signal(kind)
        .expect("failed to register signal handler")
        .recv()
        .await;
}
