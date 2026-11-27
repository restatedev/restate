// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::{debug, info};

use restate_core::{Metadata, MetadataKind, TaskCenter, TaskKind, cancellation_watcher};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_metadata_store::MetadataStoreClient;
use restate_types::Version;
use restate_types::metadata_store::keys::SCHEMA_INFORMATION_KEY;
use restate_types::retries::RetryPolicy;
use restate_util_time::DurationExt;

/// Gated access to the node-local schema.
///
/// A node learns the schema from the metadata store, from peers, and from its own updates. A peer
/// can hand it an older schema, even the empty initial one, before its own fetch from the metadata
/// store lands, so a node's schema version alone does not say whether reads answer from a complete
/// catalog. The gate opens once the node's schema is at least as new as the version the metadata
/// store held when the gate started.
#[derive(Clone)]
pub struct SchemaGate {
    /// `Some(version)` while the node is still catching up to `version`, `None` once it has. The
    /// sender is dropped when the gate's task stops, which also releases the waiters.
    state: watch::Receiver<Option<Version>>,
    #[cfg(test)]
    waiters: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// Why a caller did not get past the gate. Readers must not answer schema-backed reads in either
/// case.
#[derive(Debug, thiserror::Error)]
pub enum NotCaughtUp {
    #[error("shutting down before the node's schema caught up")]
    Cancelled,
    #[error("the schema gate stopped before the node's schema caught up")]
    Closed,
}

impl SchemaGate {
    /// Starts the catch-up, reading the version the metadata store holds for the schema. It runs in
    /// its own task: only callers of [`Self::wait_until_caught_up`] wait for it.
    pub fn spawn(store: MetadataStoreClient) -> Result<Self, restate_core::ShutdownError> {
        let (sender, state) = watch::channel(Some(Version::INVALID));
        TaskCenter::spawn_child(TaskKind::Background, "schema-gate", async move {
            if !catch_up(&store, &sender).await {
                debug!("Stopped the schema gate before it opened");
            }
            Ok(())
        })?;

        Ok(Self {
            state,
            #[cfg(test)]
            waiters: Default::default(),
        })
    }

    /// Returns `Ok` once the node's schema is at least as new as the one the metadata store held when
    /// the gate started, and immediately once the gate has opened. Returns `Err` if the caller is
    /// cancelled or the gate stops first: the schema has not caught up in either case.
    pub async fn wait_until_caught_up(&self) -> Result<(), NotCaughtUp> {
        #[cfg(test)]
        let _waiter = self.waiter();
        let mut state = self.state.clone();
        loop {
            if state.borrow_and_update().is_none() {
                return Ok(());
            }
            let changed = tokio::select! {
                _ = cancellation_watcher() => return Err(NotCaughtUp::Cancelled),
                changed = state.changed() => changed,
            };
            if changed.is_err() {
                return Err(NotCaughtUp::Closed);
            }
        }
    }

    /// Observes the gate: `Some(version)` while catching up to `version` (`Version::INVALID` until
    /// the metadata store reported it), `None` once caught up.
    #[cfg(test)]
    pub(crate) fn watch(&self) -> watch::Receiver<Option<Version>> {
        self.state.clone()
    }

    #[cfg(test)]
    fn waiter(&self) -> Waiter {
        self.waiters
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Waiter(std::sync::Arc::clone(&self.waiters))
    }

    /// Number of callers currently blocked in [`Self::wait_until_caught_up`].
    #[cfg(test)]
    pub(crate) fn waiters(&self) -> usize {
        self.waiters.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Builds a gate over a state channel, for tests that drive the state themselves.
    #[cfg(test)]
    pub(crate) fn from_state(state: watch::Receiver<Option<Version>>) -> Self {
        Self {
            state,
            waiters: Default::default(),
        }
    }
}

#[cfg(test)]
struct Waiter(std::sync::Arc<std::sync::atomic::AtomicUsize>);

#[cfg(test)]
impl Drop for Waiter {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Reads the store's schema version and waits for the node's schema to reach it. Returns whether the
/// gate opened.
async fn catch_up(store: &MetadataStoreClient, state: &watch::Sender<Option<Version>>) -> bool {
    let started = Instant::now();

    let stored_version = tokio::select! {
        version = stored_schema_version(store)
            .log_slow_after(
                Duration::from_secs(1),
                tracing::Level::INFO,
                "Waiting for the metadata store to report the schema version",
            )
            .with_overdue(Duration::from_secs(5), tracing::Level::WARN) => version,
        _ = cancellation_watcher() => return false,
    };

    // A cluster whose schema was never written has nothing to catch up to.
    let Some(stored_version) = stored_version else {
        state.send_replace(None);
        return true;
    };
    state.send_replace(Some(stored_version));

    let metadata = Metadata::current();
    let reached = tokio::select! {
        reached = metadata
            .wait_for_version(MetadataKind::Schema, stored_version)
            .log_slow_after(
                Duration::from_secs(1),
                tracing::Level::INFO,
                "Waiting for the node's schema to catch up",
            )
            .with_overdue(Duration::from_secs(5), tracing::Level::WARN) => reached,
        _ = cancellation_watcher() => return false,
    };
    let Ok(reached) = reached else {
        return false;
    };

    state.send_replace(None);
    info!(
        %reached,
        caught_up_after = %started.elapsed().friendly().to_seconds_span(),
        "The node's schema caught up with the metadata store",
    );
    true
}

/// The schema version in the metadata store, retrying until the store answers.
///
/// The store can be unreachable when the node starts, and some of its errors are not retryable,
/// e.g. when no metadata server is known yet, so this retries every error and never gives up:
/// callers wait for the answer or for shutdown.
async fn stored_schema_version(store: &MetadataStoreClient) -> Option<Version> {
    let mut retry_iter = RetryPolicy::exponential(
        Duration::from_millis(100),
        1.5,
        None,
        Some(Duration::from_secs(1)),
    )
    .into_iter();

    loop {
        match store.get_version(SCHEMA_INFORMATION_KEY.clone()).await {
            Ok(version) => return version,
            Err(err) => {
                debug!("Failed to read the schema version from the metadata store: {err}");
                tokio::time::sleep(retry_iter.next().expect("the retry policy is infinite")).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;

    use super::*;

    #[restate_core::test]
    async fn a_waiter_on_an_open_gate_gets_ok() {
        let (sender, state) = watch::channel(Some(Version::INVALID));
        let gate = SchemaGate::from_state(state);
        sender.send_replace(None);

        assert_that!(gate.wait_until_caught_up().await, ok(eq(())));
    }

    #[restate_core::test]
    async fn a_waiter_gets_err_when_the_gate_stops_before_opening() {
        let (sender, state) = watch::channel(Some(Version::MIN.next()));
        let gate = SchemaGate::from_state(state);

        let waiter = tokio::spawn({
            let gate = gate.clone();
            async move { gate.wait_until_caught_up().await }
        });

        // The gate task holds the sender and drops it when it stops (shutdown, panic).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while gate.waiters() == 0 {
            assert!(tokio::time::Instant::now() < deadline, "never waited");
            tokio::task::yield_now().await;
        }
        drop(sender);

        assert!(matches!(
            waiter.await.expect("the waiter does not panic"),
            Err(NotCaughtUp::Closed)
        ));
    }
}
