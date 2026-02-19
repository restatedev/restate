// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use dashmap::DashMap;
use tokio::sync::mpsc;

use restate_types::net::ServiceTag;

use super::RawSender;

static EPOCH: AtomicU64 = const { AtomicU64::new(1) };

type Epoch = u64;

type Map = DashMap<(ServiceTag, u64), Value<RawSender>, ahash::RandomState>;

#[derive(Clone, Default)]
pub struct Shards {
    items: Arc<Map>,
}

/// Held while registering a new network service shard.
///
/// While this token is held, other network messages requesting access to the same shard will wait
/// until this token is dropped or `done()` is called.
#[must_use]
pub struct RegistrationToken {
    key: (ServiceTag, u64),
    epoch: Epoch,
    map: Arc<Map>,
    receiver: Option<mpsc::UnboundedReceiver<()>>,
}

impl RegistrationToken {
    /// Marks the registration as done.
    ///
    /// This returns `true` if the registration was successful in the map. This
    /// will return `false` if the map was updated by force-removing the entry.
    pub fn done(mut self, sender: RawSender) -> bool {
        if let Some(_receiver) = self.receiver.take()
            && let dashmap::mapref::entry::Entry::Occupied(mut v) = self.map.entry(self.key)
        {
            match v.get() {
                Value::Waiting(epoch, _) if *epoch == self.epoch => {
                    v.insert(Value::Filled(sender));
                    return true;
                }
                Value::Filled(existing) if existing.same_channel(&sender) => {
                    // it's already registered
                    return true;
                }
                _ => {}
            }
        }
        false
    }
}

impl Drop for RegistrationToken {
    fn drop(&mut self) {
        if self.receiver.is_some() {
            match self.map.entry(self.key) {
                dashmap::mapref::entry::Entry::Occupied(v) => match v.get() {
                    Value::Waiting(epoch, _) if *epoch == self.epoch => {
                        let _ = v.remove();
                    }
                    _ => {}
                },
                dashmap::mapref::entry::Entry::Vacant(_) => {}
            }
        }
    }
}

/// A token to wait for an on-going shard registration.
///
/// This token needs to be awaited by calling `join()`. Once join() returns, the shard
/// state can be queried again.
#[must_use]
#[derive(Clone, Debug)]
pub struct WaitToken(mpsc::UnboundedSender<()>);

impl WaitToken {
    /// Waits for the registration token to be dropped
    pub async fn join(self) {
        self.0.closed().await
    }
}

pub enum MaybeValue {
    Filled(RawSender),
    Opening(WaitToken),
    Register(RegistrationToken),
}

impl Shards {
    pub fn maybe_register(&self, target: ServiceTag, sort_code: u64) -> MaybeValue {
        match self.items.entry((target, sort_code)) {
            dashmap::mapref::entry::Entry::Occupied(v) => match v.get() {
                Value::Filled(v) => MaybeValue::Filled(v.clone()),
                Value::Waiting(_, wait) => MaybeValue::Opening(wait.clone()),
            },
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let epoch = EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let (sender, receiver) = mpsc::unbounded_channel();
                entry.insert(Value::Waiting(epoch, WaitToken(sender)));
                MaybeValue::Register(RegistrationToken {
                    key: (target, sort_code),
                    map: Arc::clone(&self.items),
                    epoch,
                    receiver: Some(receiver),
                })
            }
        }
    }

    pub fn force_register(&self, target: ServiceTag, sort_code: u64, sender: RawSender) {
        self.items
            .insert((target, sort_code), Value::Filled(sender));
    }

    /// Remove an existing shard only if the sender matches the existing one
    pub fn remove_if_matches(&self, target: ServiceTag, sort_code: u64, sender: &RawSender) {
        if let dashmap::mapref::entry::Entry::Occupied(v) = self.items.entry((target, sort_code))
            && let Value::Filled(existing) = v.get()
            && existing.same_channel(sender)
        {
            v.remove();
        }
    }

    /// Remove an existing shard
    pub fn remove(&self, target: ServiceTag, sort_code: u64) -> Option<RawSender> {
        let entry = self.items.remove(&(target, sort_code))?;
        match entry {
            (_, Value::Filled(value)) => Some(value),
            (_, Value::Waiting(..)) => None,
        }
    }
}

#[derive(Debug)]
enum Value<V> {
    Waiting(Epoch, WaitToken),
    Filled(V),
}

#[cfg(test)]
mod tests {
    use restate_types::net::ServiceTag;

    use super::*;

    const SVC_A: ServiceTag = ServiceTag::LogServerDataService;
    const SVC_B: ServiceTag = ServiceTag::LogServerMetaService;

    /// Helper: creates a RawSender/receiver pair for testing.
    fn raw_sender() -> (
        super::super::RawSender,
        tokio::sync::mpsc::UnboundedReceiver<super::super::ServiceOp>,
    ) {
        super::super::RawSender::test_channel()
    }

    #[test]
    fn vacant_entry_returns_register_and_done_completes_it() {
        // First call on a vacant entry must yield Register, calling done() fills the slot,
        // and subsequent lookups must return Filled with the same channel.
        let shards = Shards::default();
        let (sender, _rx) = raw_sender();

        let token = match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register, got {}", variant_name(&other)),
        };

        assert!(
            token.done(sender.clone()),
            "done() should succeed on matching epoch"
        );

        // Now the entry is filled — a second lookup must return the same sender.
        match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Filled(s) => assert!(s.same_channel(&sender)),
            other => panic!("expected Filled, got {}", variant_name(&other)),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_lookup_returns_opening_and_join_resolves_on_done() {
        // While a registration is in-flight (token exists, done() not called), concurrent
        // callers must see Opening. WaitToken::join() must resolve once done() is called.
        let shards = Shards::default();
        let (sender, _rx) = raw_sender();

        let token = match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register, got {}", variant_name(&other)),
        };

        // Second caller while first registration is in progress.
        let wait = match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Opening(w) => w,
            other => panic!("expected Opening, got {}", variant_name(&other)),
        };

        // join() should not resolve yet.
        let join_handle = tokio::spawn(async move { wait.join().await });
        tokio::task::yield_now().await;
        assert!(!join_handle.is_finished(), "join should still be pending");

        // Complete registration.
        assert!(token.done(sender));

        // join() must now resolve.
        tokio::time::timeout(std::time::Duration::from_millis(100), join_handle)
            .await
            .expect("join should resolve after done()")
            .expect("task should not panic");
    }

    #[test]
    fn drop_token_without_done_cleans_up_entry() {
        // Dropping the RegistrationToken without calling done() must remove the
        // Waiting entry from the map — this is the cancellation-safety guarantee.
        let shards = Shards::default();

        let token = match shards.maybe_register(SVC_A, 42) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register, got {}", variant_name(&other)),
        };
        // Drop without done().
        drop(token);

        // The slot should be vacant again — next lookup must yield Register.
        match shards.maybe_register(SVC_A, 42) {
            MaybeValue::Register(t) => drop(t.done(raw_sender().0)), // clean up
            other => panic!(
                "expected Register after cancelled registration, got {}",
                variant_name(&other)
            ),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn drop_token_resolves_waiters() {
        // When a RegistrationToken is dropped (cancelled), WaitToken::join() must resolve
        // so that waiters can retry registration.
        let shards = Shards::default();

        let token = match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register, got {}", variant_name(&other)),
        };

        let wait = match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Opening(w) => w,
            other => panic!("expected Opening, got {}", variant_name(&other)),
        };

        let join_handle = tokio::spawn(async move { wait.join().await });

        // Cancel registration by dropping the token.
        drop(token);

        // Waiter must unblock.
        tokio::time::timeout(std::time::Duration::from_millis(100), join_handle)
            .await
            .expect("join should resolve after token drop")
            .expect("task should not panic");
    }

    #[test]
    fn force_register_overwrites_and_makes_token_done_fail() {
        // force_register unconditionally fills the slot. If a RegistrationToken from
        // a prior maybe_register exists, its done() must return false (epoch mismatch).
        let shards = Shards::default();
        let (force_sender, _rx1) = raw_sender();
        let (token_sender, _rx2) = raw_sender();

        let token = match shards.maybe_register(SVC_A, 7) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register, got {}", variant_name(&other)),
        };

        // Force-register overwrites the Waiting entry.
        shards.force_register(SVC_A, 7, force_sender.clone());

        // The old token's done() should fail because the entry is now Filled
        // with a different sender (epoch no longer matches).
        assert!(
            !token.done(token_sender),
            "done() should fail after force_register with different sender"
        );

        // The force-registered sender should be what we get back.
        match shards.maybe_register(SVC_A, 7) {
            MaybeValue::Filled(s) => assert!(s.same_channel(&force_sender)),
            other => panic!("expected Filled, got {}", variant_name(&other)),
        }
    }

    #[test]
    fn drop_token_after_force_register_does_not_remove_entry() {
        // If force_register overwrote the Waiting entry, dropping the stale
        // RegistrationToken must NOT remove the force-registered entry (epoch mismatch).
        let shards = Shards::default();
        let (force_sender, _rx) = raw_sender();

        let token = match shards.maybe_register(SVC_A, 3) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register, got {}", variant_name(&other)),
        };

        shards.force_register(SVC_A, 3, force_sender.clone());

        // Drop the stale token — must not clobber the force-registered entry.
        drop(token);

        match shards.maybe_register(SVC_A, 3) {
            MaybeValue::Filled(s) => assert!(s.same_channel(&force_sender)),
            other => panic!(
                "expected Filled to survive stale token drop, got {}",
                variant_name(&other)
            ),
        }
    }

    #[test]
    fn done_with_already_registered_same_sender_returns_true() {
        // If force_register already filled the slot with the *same* sender that
        // done() is called with, done() returns true (idempotent).
        let shards = Shards::default();
        let (sender, _rx) = raw_sender();

        let token = match shards.maybe_register(SVC_A, 5) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register, got {}", variant_name(&other)),
        };

        shards.force_register(SVC_A, 5, sender.clone());

        // done() with the same channel should succeed because same_channel matches.
        assert!(
            token.done(sender),
            "done() should return true for same channel"
        );
    }

    #[test]
    fn different_service_tags_are_independent() {
        // Entries are keyed by (ServiceTag, sort_code). Two different service tags
        // with the same sort_code must not interfere.
        let shards = Shards::default();
        let (sender_a, _rx_a) = raw_sender();
        let (sender_b, _rx_b) = raw_sender();

        // Register sort_code=1 under SVC_A
        let token_a = match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register for SVC_A, got {}", variant_name(&other)),
        };
        assert!(token_a.done(sender_a.clone()));

        // Register sort_code=1 under SVC_B — must NOT return Filled from SVC_A.
        let token_b = match shards.maybe_register(SVC_B, 1) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register for SVC_B, got {}", variant_name(&other)),
        };
        assert!(token_b.done(sender_b.clone()));

        // Each returns its own sender.
        match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Filled(s) => assert!(s.same_channel(&sender_a)),
            other => panic!("expected Filled(sender_a), got {}", variant_name(&other)),
        }
        match shards.maybe_register(SVC_B, 1) {
            MaybeValue::Filled(s) => assert!(s.same_channel(&sender_b)),
            other => panic!("expected Filled(sender_b), got {}", variant_name(&other)),
        }
    }

    #[test]
    fn remove_if_matches_only_removes_matching_sender() {
        let shards = Shards::default();
        let (sender_a, _rx_a) = raw_sender();
        let (sender_b, _rx_b) = raw_sender();

        shards.force_register(SVC_A, 1, sender_a.clone());

        // Attempting to remove with a different sender should be a no-op.
        shards.remove_if_matches(SVC_A, 1, &sender_b);
        match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Filled(s) => assert!(
                s.same_channel(&sender_a),
                "entry should survive non-matching remove"
            ),
            other => panic!("expected Filled, got {}", variant_name(&other)),
        }

        // Removing with the matching sender should succeed.
        shards.remove_if_matches(SVC_A, 1, &sender_a);
        match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Register(t) => drop(t), // clean up
            other => panic!(
                "expected Register after removal, got {}",
                variant_name(&other)
            ),
        }
    }

    #[test]
    fn remove_if_matches_ignores_waiting_entries() {
        // remove_if_matches should not remove an entry that is in Waiting state.
        let shards = Shards::default();
        let (sender, _rx) = raw_sender();

        let token = match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register, got {}", variant_name(&other)),
        };

        // Entry is Waiting — remove_if_matches must be a no-op.
        shards.remove_if_matches(SVC_A, 1, &sender);

        // Entry should still be Opening for a concurrent caller.
        match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Opening(_) => {} // expected
            other => panic!(
                "expected Opening (entry should survive), got {}",
                variant_name(&other)
            ),
        }

        drop(token); // clean up
    }

    #[test]
    fn remove_returns_sender_for_filled_none_for_waiting_or_absent() {
        let shards = Shards::default();
        let (sender, _rx) = raw_sender();

        // Absent entry.
        assert!(shards.remove(SVC_A, 99).is_none());

        // Waiting entry.
        let token = match shards.maybe_register(SVC_A, 10) {
            MaybeValue::Register(t) => t,
            other => panic!("expected Register, got {}", variant_name(&other)),
        };
        assert!(
            shards.remove(SVC_A, 10).is_none(),
            "remove on Waiting should return None"
        );
        // Token's drop should not panic even though the entry was force-removed.
        drop(token);

        // Filled entry.
        shards.force_register(SVC_A, 20, sender.clone());
        let removed = shards.remove(SVC_A, 20);
        assert!(removed.is_some());
        assert!(removed.unwrap().same_channel(&sender));

        // After removal, the slot is vacant.
        match shards.maybe_register(SVC_A, 20) {
            MaybeValue::Register(t) => drop(t),
            other => panic!(
                "expected Register after remove, got {}",
                variant_name(&other)
            ),
        }
    }

    #[test]
    fn force_register_then_remove_then_re_register() {
        // Full lifecycle: register → remove → re-register with a different sender.
        let shards = Shards::default();
        let (sender1, _rx1) = raw_sender();
        let (sender2, _rx2) = raw_sender();

        shards.force_register(SVC_A, 1, sender1.clone());
        shards.remove(SVC_A, 1);

        let token = match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Register(t) => t,
            other => panic!(
                "expected Register after remove, got {}",
                variant_name(&other)
            ),
        };
        assert!(token.done(sender2.clone()));

        match shards.maybe_register(SVC_A, 1) {
            MaybeValue::Filled(s) => {
                assert!(!s.same_channel(&sender1), "should not be the old sender");
                assert!(s.same_channel(&sender2), "should be the new sender");
            }
            other => panic!("expected Filled, got {}", variant_name(&other)),
        }
    }

    #[test]
    fn multiple_sort_codes_same_service_are_independent() {
        let shards = Shards::default();
        let (sender1, _rx1) = raw_sender();
        let (sender2, _rx2) = raw_sender();

        shards.force_register(SVC_A, 100, sender1.clone());
        shards.force_register(SVC_A, 200, sender2.clone());

        // Removing sort_code=100 should not affect sort_code=200.
        shards.remove(SVC_A, 100);

        assert!(shards.remove(SVC_A, 100).is_none(), "already removed");
        match shards.maybe_register(SVC_A, 200) {
            MaybeValue::Filled(s) => assert!(s.same_channel(&sender2)),
            other => panic!(
                "expected Filled for sort_code=200, got {}",
                variant_name(&other)
            ),
        }
    }

    /// Helper to name the MaybeValue variant for better assertion messages.
    fn variant_name(v: &MaybeValue) -> &'static str {
        match v {
            MaybeValue::Filled(_) => "Filled",
            MaybeValue::Opening(_) => "Opening",
            MaybeValue::Register(_) => "Register",
        }
    }
}
