// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use once_cell::sync::Lazy;
use tokio::sync::watch;

static CONFIG_UPDATE: Lazy<watch::Sender<()>> = Lazy::new(|| watch::Sender::new(()));

pub fn config_watcher() -> ConfigWatch {
    ConfigWatch::new(CONFIG_UPDATE.subscribe())
}

#[derive(Clone)]
pub struct ConfigWatch {
    receiver: watch::Receiver<()>,
}

impl ConfigWatch {
    pub fn new(receiver: watch::Receiver<()>) -> Self {
        Self { receiver }
    }

    /// Blocks until a configuration update is signaled.
    pub async fn changed(&mut self) {
        // It's okay to ignore the result here since the sender is static, we don't
        // care much about changes that happen during shutdown.
        let _ = self.receiver.changed().await;
    }
}

/// Inform the watch that the offset has changed. This should be used from the configuration loader
/// thread, or it can be used in tests to simulate updates.
pub fn notify_config_update() {
    CONFIG_UPDATE.send_modify(|v| {
        *v = ();
    });
}
