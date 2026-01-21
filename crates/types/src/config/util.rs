// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use figment::Figment;
use figment::providers::Serialized;
use serde::de::{Error, MapAccess, Visitor};
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::LazyLock;
use tokio::sync::watch;

pub(crate) static CONFIG_UPDATE: LazyLock<watch::Sender<()>> =
    LazyLock::new(|| watch::Sender::new(()));

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
pub(crate) fn notify_config_update() {
    CONFIG_UPDATE.send_modify(|v| {
        *v = ();
    });
}

/// Deserialize a struct from a map of key-value pairs and default values.
pub(crate) struct StructWithDefaults<T> {
    defaults: Figment,
    phantom_data: PhantomData<T>,
}

impl<T> StructWithDefaults<T> {
    pub fn new<I: serde::Serialize>(defaults: I) -> Self {
        Self {
            defaults: Figment::from(Serialized::defaults(defaults)),
            phantom_data: PhantomData,
        }
    }
}

impl<'de, T> Visitor<'de> for StructWithDefaults<T>
where
    T: serde::Deserialize<'de>,
{
    type Value = T;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "expecting a map of key-value pairs that make up the fields of the struct to deserialize"
        )
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some((key, value)) = map.next_entry::<String, figment::value::Value>()? {
            self.defaults = self.defaults.merge(Serialized::default(&key, value));
        }

        self.defaults.extract::<T>().map_err(Error::custom)
    }
}
