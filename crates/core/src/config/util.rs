// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use tokio::sync::watch;

pub(crate) static CONFIG_UPDATE: LazyLock<watch::Sender<()>> =
    LazyLock::new(|| watch::Sender::new(()));

#[derive(Clone)]
pub struct ConfigWatch {
    receiver: Option<watch::Receiver<()>>,
}

impl ConfigWatch {
    pub fn new(receiver: watch::Receiver<()>) -> Self {
        Self {
            receiver: Some(receiver),
        }
    }

    /// Create a no-op [`ConfigWatch`]. This is useful if the underlying configuration cannot be
    /// updated.
    pub fn no_op() -> Self {
        Self { receiver: None }
    }

    /// Blocks until a configuration update is signaled.
    pub async fn changed(&mut self) {
        match &mut self.receiver {
            None => {
                // we are a no-op config watch
                futures::future::pending().await
            }
            Some(receiver) => {
                // It's okay to ignore the result here since the sender is static, we don't
                // care much about changes that happen during shutdown.
                let _ = receiver.changed().await;
            }
        }
    }
}

/// Inform the watch that the offset has changed. This should be used from the configuration loader
/// thread, or it can be used in tests to simulate updates.
pub(crate) fn notify_config_update() {
    CONFIG_UPDATE.send_modify(|v| {
        *v = ();
    });
}
