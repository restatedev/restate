// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Node-level cache for the cluster-global limiter rule book.
//!
//! The cache polls the metadata store at a configurable interval and
//! surfaces the latest [`RuleBook`] via a [`tokio::sync::watch`] channel
//! that per-PP leaders subscribe to. The handle is bidirectional: state
//! machines that learn about a newer rule book through Bifrost replay
//! call [`RuleBookCacheHandle::notify_observed`] to feed it back, so
//! cross-node propagation does not have to wait for the next poll
//! tick.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::warn;

use restate_core::cancellation_watcher;
use restate_limiter::RuleBook;
use restate_metadata_store::MetadataStoreClient;
use restate_types::Versioned;
use restate_types::metadata_store::keys::RULE_BOOK_KEY;

/// Clonable handle into the [`RuleBookCache`].
///
/// Both the polling loop and any partition processor that decoded a
/// newer rule book from a `Command::UpsertRuleBook` envelope can push
/// updates through [`Self::notify_observed`]. Subscribers obtain a
/// `watch::Receiver` via [`Self::subscribe`].
#[derive(Clone)]
pub struct RuleBookCacheHandle {
    sender: watch::Sender<Arc<RuleBook>>,
}

impl RuleBookCacheHandle {
    /// Subscribe to rule-book updates. The receiver always holds at
    /// least the empty default book.
    pub fn subscribe(&self) -> watch::Receiver<Arc<RuleBook>> {
        self.sender.subscribe()
    }

    /// Push a rule book observed from any source. Updates the watch
    /// only if the observed version is strictly newer than the cached
    /// one.
    pub fn notify_observed(&self, book: &Arc<RuleBook>) {
        self.sender.send_if_modified(|current| {
            if book.version() > current.version() {
                *current = Arc::clone(book);
                true
            } else {
                false
            }
        });
    }

    /// Owned-input variant of [`Self::notify_observed`] for callers
    /// that produce a fresh [`RuleBook`] and don't need to retain it
    /// (e.g. the admin REST handlers after a successful write). The
    /// `Arc` allocation happens inside the cache only when the
    /// observed version is strictly newer; on the no-op branch the
    /// book is simply dropped.
    pub fn notify_observed_owned(&self, book: RuleBook) {
        self.sender.send_if_modified(move |current| {
            if book.version() > current.version() {
                *current = Arc::new(book);
                true
            } else {
                false
            }
        });
    }

    /// Detached handle used by tests and benchmarks. The associated
    /// watch has no polling task, so `notify_observed` and
    /// `subscribe` work but no metadata-store traffic occurs.
    pub fn detached() -> Self {
        let (sender, _) = watch::channel(Arc::new(RuleBook::default()));
        Self { sender }
    }
}

/// Node-level metadata-store poller for the rule book.
pub struct RuleBookCache {
    client: MetadataStoreClient,
    poll_interval: Duration,
    handle: RuleBookCacheHandle,
}

impl RuleBookCache {
    /// Build a new cache and its companion handle. The initial cached
    /// value is the empty default `RuleBook` (version `INVALID`); the
    /// first successful poll replaces it.
    pub fn create(
        client: MetadataStoreClient,
        poll_interval: Duration,
    ) -> (Self, RuleBookCacheHandle) {
        let (sender, _) = watch::channel(Arc::new(RuleBook::default()));
        let handle = RuleBookCacheHandle { sender };
        (
            Self {
                client,
                poll_interval,
                handle: handle.clone(),
            },
            handle,
        )
    }

    /// Long-running polling task. Exits cleanly when the task center
    /// requests cancellation. Errors from the metadata store produce
    /// rate-limited warnings; the cached book is left alone so
    /// subscribers continue to operate on the last-known-good value.
    pub async fn run(self) {
        let RuleBookCache {
            client,
            poll_interval,
            handle,
        } = self;

        let mut tick = tokio::time::interval(poll_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut cancellation = std::pin::pin!(cancellation_watcher());

        loop {
            tokio::select! {
                _ = &mut cancellation => break,
                _ = tick.tick() => {
                    poll_once(&client, &handle).await;
                }
            }
        }
    }
}

/// One poll iteration. Bandwidth optimization: ask the metadata store
/// for the current key version first and skip the body fetch when the
/// remote version isn't strictly greater than what we already cache.
///
/// `RuleBook::version()` and the metadata-store wrapper version stay
/// in lockstep — see the assertion in
/// `MetadataStoreClient::get` (`crates/metadata-store/src/metadata_store.rs:305-309`),
/// so comparing the metadata-store version to our cached
/// `RuleBook::version()` is sound.
async fn poll_once(client: &MetadataStoreClient, handle: &RuleBookCacheHandle) {
    let current_version = handle.sender.borrow().version();

    let remote_version = match client.get_version(RULE_BOOK_KEY.clone()).await {
        Ok(Some(v)) => v,
        Ok(None) => return, // empty default already in handle
        Err(err) => {
            warn!("rule-book version probe failed: {err:#}");
            return;
        }
    };

    if remote_version <= current_version {
        return;
    }

    match client.get::<RuleBook>(RULE_BOOK_KEY.clone()).await {
        Ok(Some(book)) => {
            handle.sender.send_if_modified(|current| {
                if book.version() > current.version() {
                    *current = Arc::new(book);
                    true
                } else {
                    false
                }
            });
        }
        // Race: the rule book was deleted between our version probe
        // and the body fetch. Leave the cache alone — subscribers
        // continue on the last-known book.
        Ok(None) => {}
        Err(err) => warn!("rule-book fetch failed: {err:#}"),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use restate_limiter::{NewRule, RuleChange, RuleId, RulePatch, UserLimits};
    use restate_types::Version;
    use restate_types::metadata::Precondition;

    use super::*;

    fn make_book_with_one_rule() -> RuleBook {
        let mut book = RuleBook::default();
        let pattern = "*".parse().unwrap();
        let id = RuleId::from(&pattern);
        book.apply_change(
            id,
            RuleChange::Create(NewRule {
                pattern,
                limits: UserLimits::new(NonZeroU32::new(100)),
                reason: None,
                disabled: false,
            }),
        )
        .unwrap();
        book
    }

    #[tokio::test]
    async fn observes_remote_book_writes() {
        let client = MetadataStoreClient::new_in_memory();
        let (_cache, handle) = RuleBookCache::create(client.clone(), Duration::from_secs(60));

        let mut book = make_book_with_one_rule();
        client
            .put(RULE_BOOK_KEY.clone(), &book, Precondition::None)
            .await
            .unwrap();

        // First poll fetches.
        poll_once(&client, &handle).await;
        let v_after_first = handle.sender.borrow().version();
        assert_eq!(v_after_first, book.version());

        let id = book.rules().map(|(id, _)| id).next().unwrap();

        let current_book_version = book.version();

        book.apply_change(
            *id,
            RuleChange::Patch(RulePatch {
                disabled: Some(true),
                ..RulePatch::default()
            }),
        )
        .unwrap();

        client
            .put(
                RULE_BOOK_KEY.clone(),
                &book,
                Precondition::MatchesVersion(current_book_version),
            )
            .await
            .unwrap();

        // Second poll sees the updated version.
        poll_once(&client, &handle).await;
        assert_eq!(
            handle.sender.borrow().version(),
            current_book_version.next()
        );
    }

    #[test]
    fn detached_handle_routes_through_notify() {
        let handle = RuleBookCacheHandle::detached();
        let mut rx = handle.subscribe();
        // Initial value is the empty default.
        assert_eq!(rx.borrow().version(), Version::INVALID);

        let new = Arc::new(make_book_with_one_rule());
        let new_version = new.version();
        handle.notify_observed(&new);

        // The watch advanced.
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();
        assert_eq!(rx.borrow().version(), new_version);

        // Re-pushing the same book is a no-op.
        handle.notify_observed(&new);
        assert!(!rx.has_changed().unwrap());
    }
}
