// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`Quota`]: a single-owner budget for bytes and item slots, lent out as RAII leases.
//! Use it to bound memory and in-flight work independently, for example in a work queue or a
//! network receive window.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use futures::task::AtomicWaker;
use tokio::sync::futures::OwnedNotified;

use restate_util_bytecount::ByteCount;

use crate::{MemoryLease, MemoryPool};

/// Set in the leased byte count once the quota is gone: leases then return to the parent, still
/// counted until they do.
const DEAD: usize = 1 << (usize::BITS - 1);

/// An amount of quota: bytes, and slots counting the items they hold.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Amount {
    /// Bytes, drawn from the parent pool.
    pub bytes: usize,
    /// Items, counted locally. The owner decides how many may be in flight; slots do not reserve
    /// anything from the parent pool.
    pub slots: usize,
}

impl Amount {
    /// Nothing.
    pub const ZERO: Amount = Amount { bytes: 0, slots: 0 };

    /// Whether this covers `other` in both dimensions.
    pub const fn covers(self, other: Amount) -> bool {
        self.bytes >= other.bytes && self.slots >= other.slots
    }
}

/// How a [`Quota`] trades parent accesses for memory held. The default draws and gives back
/// exactly what is asked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuotaPolicy {
    /// Growth draws from the parent in multiples of this many bytes, so that a run of growth
    /// takes fewer parent accesses. Zero draws exactly the shortfall.
    pub refill: usize,
    /// Bytes kept above what is needed when shrinking, so that growth soon after needs no parent
    /// access. [`Quota::release`] gives them back too.
    pub spare: usize,
}

/// A single-owner budget for bytes and item slots, lent out as [`QuotaLease`]s.
///
/// For example, a work queue can bound both the bytes its jobs retain and the number of jobs in
/// flight. Bytes are reserved from a parent [`MemoryPool`]; slots are counted locally. A network
/// receive window can use the same accounting for bytes and messages.
///
/// The owner provisions capacity before lending it, so lending only ever draws on what the quota
/// already holds and never waits. Growing draws the shortfall from the parent pool, shrinking
/// gives back unused capacity, and the policy trades parent accesses for memory held:
///
/// ```text
///   parent MemoryPool ──grow, in refill batches──▶ Quota (held) ──try_lease──▶ QuotaLease
///         ▲                                          ▲    │                        │
///         └────── shrink, keeping spare ─────────────┘    └──── back on drop, ◀────┘
///                                                           waking the driver
/// ```
///
/// The owner's task—the **driver**—manages capacity and issues leases. Other tasks may hold and
/// drop those leases. A lease accounts for resources; it does not allocate storage or attach
/// itself to an item. Keep it alive for as long as the resources it accounts for remain in use:
///
/// ```text
///   driver                       worker
///   grow capacity
///   try_lease ── (job, lease) ──▶ process job
///      ▲                         drop job and lease
///      └──────── wake ─────────────────┘
/// ```
///
/// **Wakeups.** While the quota is alive, each lease drop wakes the driver through a shared
/// [`AtomicWaker`], and a growth the parent cannot fund now registers the same waker with the
/// parent, which wakes it once memory comes back. The driver registers its waker before it looks
/// at the quota, so neither wakeup can be lost. One waker can serve several quotas of one driver.
///
/// **Teardown.** Leases may outlive their quota. Dropping the quota gives back everything leases
/// do not hold; each lease still live gives its bytes straight back to the parent when it drops,
/// so the parent stays charged for exactly the memory still in use.
///
/// ```
/// # use std::num::NonZeroUsize;
/// # use std::sync::Arc;
/// use futures::task::AtomicWaker;
/// use restate_memory::{Amount, MemoryPool, NonZeroByteCount, Quota, QuotaPolicy};
///
/// let parent = MemoryPool::with_capacity(NonZeroByteCount::new(NonZeroUsize::new(1024).unwrap()));
/// let driver = Arc::new(AtomicWaker::new());
/// let mut quota = Quota::new(parent.clone(), 512, QuotaPolicy::default(), driver);
/// // Provision room for at most 256 bytes and four jobs.
/// assert!(quota.grow(Amount { bytes: 256, slots: 4 }));
///
/// // Account for one job retaining 100 bytes. Keep its lease until the job is done.
/// let lease = quota.try_lease(Amount { bytes: 100, slots: 1 }).unwrap();
/// assert!(quota.try_lease(Amount { bytes: 200, slots: 1 }).is_none(), "156 bytes left");
/// assert!(quota.try_lease(Amount { bytes: 0, slots: 4 }).is_none(), "3 slots left");
///
/// // The quota goes first: the parent stays charged for the lease alone, until it drops too.
/// drop(quota);
/// assert_eq!(parent.used().as_usize(), 100);
/// drop(lease);
/// assert_eq!(parent.used().as_usize(), 0);
/// ```
pub struct Quota {
    shared: Arc<Shared>,
    /// Bytes held from the parent.
    held: MemoryLease,
    /// Slots held.
    slots: usize,
    /// The most bytes the quota ever holds.
    limit: usize,
    policy: QuotaPolicy,
    /// Registered with the parent while a growth waits for it.
    waiting: Option<Pin<Box<OwnedNotified>>>,
}

/// What the quota shares with its leases and meters.
struct Shared {
    parent: MemoryPool,
    /// Bytes live leases hold, with [`DEAD`] set once the quota is gone.
    leased_bytes: AtomicUsize,
    /// Slots live leases hold.
    leased_slots: AtomicUsize,
    /// What the quota holds, for meters.
    held_bytes: AtomicUsize,
    held_slots: AtomicUsize,
    /// Calls that reached the parent pool.
    parent_accesses: AtomicU64,
    /// The owner's driver.
    driver: Arc<AtomicWaker>,
}

/// Given to the parent while growth waits: it wakes the driver.
impl Wake for Shared {
    fn wake(self: Arc<Self>) {
        self.driver.wake();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.driver.wake();
    }
}

impl Quota {
    /// The most bytes a quota may hold: the leased count's top bit marks a quota that is gone.
    pub const MAX_LIMIT: usize = DEAD - 1;

    /// An empty quota drawing from `parent`, never holding more than `limit` bytes, refill
    /// batches and spare included. Leases dropping, and parent memory coming back while a growth
    /// waits, wake `driver`.
    ///
    /// # Panics
    ///
    /// If `limit` is above [`Self::MAX_LIMIT`].
    pub fn new(
        parent: MemoryPool,
        limit: usize,
        policy: QuotaPolicy,
        driver: Arc<AtomicWaker>,
    ) -> Self {
        assert!(
            limit <= Self::MAX_LIMIT,
            "a quota's limit must be below {}, not {}",
            ByteCount::from(DEAD),
            ByteCount::from(limit)
        );
        Self {
            held: parent.empty_lease(),
            shared: Arc::new(Shared {
                parent,
                leased_bytes: AtomicUsize::new(0),
                leased_slots: AtomicUsize::new(0),
                held_bytes: AtomicUsize::new(0),
                held_slots: AtomicUsize::new(0),
                parent_accesses: AtomicU64::new(0),
                driver,
            }),
            slots: 0,
            limit,
            policy,
            waiting: None,
        }
    }

    /// What the quota holds.
    pub fn held(&self) -> Amount {
        Amount {
            bytes: self.held.size().as_usize(),
            slots: self.slots,
        }
    }

    /// What live leases hold. It falls whenever an owner drops one, on any task; bytes and slots
    /// are read one after the other.
    pub fn leased(&self) -> Amount {
        self.shared.leased()
    }

    /// A read-only view of the quota, for observers other than its owner.
    pub fn meter(&self) -> QuotaMeter {
        QuotaMeter {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Lends `amount` out of what the quota holds and no lease holds yet, or `None` if there is
    /// not that much. Never touches the parent.
    pub fn try_lease(&mut self, amount: Amount) -> Option<QuotaLease> {
        // Only the owner adds to the leased counts, and drops only lower them, so what is free
        // now stays free until this lease takes it.
        let leased = self.shared.leased();
        let free = Amount {
            bytes: self.held.size().as_usize() - leased.bytes,
            slots: self.slots - leased.slots,
        };
        if !free.covers(amount) {
            return None;
        }
        self.shared
            .leased_bytes
            .fetch_add(amount.bytes, Ordering::Relaxed);
        self.shared
            .leased_slots
            .fetch_add(amount.slots, Ordering::Relaxed);
        Some(QuotaLease {
            shared: Arc::clone(&self.shared),
            amount,
        })
    }

    /// Tries to hold at least `want`, drawing any shortfall of bytes from the parent: a whole
    /// number of refill batches, up to the limit, if the parent has them, else exactly the
    /// shortfall. Returns whether the quota holds `want` now. If not, the parent wakes the driver
    /// once memory comes back, and the owner asks again; one registration serves every ask until
    /// then. Past the limit, growth fails and waits for nothing.
    ///
    /// `want` is total capacity, not an additional amount: growing from 100 bytes to 150 asks the
    /// parent for a shortfall of 50. Unless `want.bytes` exceeds the limit, slot capacity grows
    /// even if the parent refuses the bytes and this returns `false`.
    pub fn grow(&mut self, want: Amount) -> bool {
        if want.bytes > self.limit {
            return false;
        }
        self.set_slots(self.slots.max(want.slots));
        let held = self.held.size().as_usize();
        if held >= want.bytes {
            self.waiting = None;
            return true;
        }
        let short = want.bytes - held;
        let batch = short
            .checked_next_multiple_of(self.policy.refill.max(1))
            .unwrap_or(usize::MAX)
            .min(self.limit - held);
        if self.draw(batch) || (batch > short && self.draw(short)) {
            return true;
        }
        // Register with the parent before looking again, so memory that comes back in between
        // still wakes the driver.
        let waker = Waker::from(Arc::clone(&self.shared));
        loop {
            if self.waiting.is_none() {
                let notified = self.shared.parent.availability_notified_owned();
                self.waiting = Some(Box::pin(notified.expect("only a bounded parent refuses")));
            }
            if self.draw(short) {
                return true;
            }
            let notified = self.waiting.as_mut().expect("registered above");
            match notified.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Pending => return false,
                Poll::Ready(()) => self.waiting = None,
            }
        }
    }

    /// Gives back what the quota holds above `keep`, in each dimension, but never what leases
    /// hold, and keeps up to the policy's spare bytes on top.
    pub fn shrink(&mut self, keep: Amount) {
        self.shrink_to(keep, self.policy.spare);
    }

    /// Gives back everything leases do not currently hold, spare included.
    ///
    /// Outstanding leases remain charged. This does not close the quota: the owner may grow and
    /// lend again. To give back capacity retained when those leases later drop, call this again
    /// or drop the quota.
    pub fn release(&mut self) {
        self.shrink_to(Amount::ZERO, 0);
    }

    fn shrink_to(&mut self, keep: Amount, spare: usize) {
        let leased = self.shared.leased();
        self.set_slots(self.slots.min(keep.slots.max(leased.slots)));
        let keep = keep.bytes.max(leased.bytes).saturating_add(spare);
        let held = self.held.size().as_usize();
        if held > keep {
            self.shared.parent_accesses.fetch_add(1, Ordering::Relaxed);
            self.held.shrink(held - keep);
            self.shared.held_bytes.store(keep, Ordering::Relaxed);
        }
    }

    /// Draws `bytes` more from the parent, if it has them.
    fn draw(&mut self, bytes: usize) -> bool {
        self.shared.parent_accesses.fetch_add(1, Ordering::Relaxed);
        if !self.held.try_grow(bytes) {
            return false;
        }
        self.waiting = None;
        self.shared
            .held_bytes
            .store(self.held.size().as_usize(), Ordering::Relaxed);
        true
    }

    fn set_slots(&mut self, slots: usize) {
        self.slots = slots;
        self.shared.held_slots.store(slots, Ordering::Relaxed);
    }
}

impl Drop for Quota {
    fn drop(&mut self) {
        // From here on, leases give their bytes straight back to the parent. Those leased at this
        // instant stay charged to it until they drop; everything else goes back now.
        let leased = self.shared.leased_bytes.fetch_or(DEAD, Ordering::Relaxed) & !DEAD;
        self.held.split(leased).forget();
        self.shared.held_bytes.store(0, Ordering::Relaxed);
        self.shared.held_slots.store(0, Ordering::Relaxed);
    }
}

impl Shared {
    fn leased(&self) -> Amount {
        Amount {
            bytes: self.leased_bytes.load(Ordering::Relaxed) & !DEAD,
            slots: self.leased_slots.load(Ordering::Relaxed),
        }
    }
}

/// A part of a [`Quota`], lent out. Dropping it, on any task, gives it back: to the quota, waking
/// its driver, or to the parent once the quota is gone.
#[must_use]
#[clippy::has_significant_drop]
pub struct QuotaLease {
    shared: Arc<Shared>,
    amount: Amount,
}

impl QuotaLease {
    /// What the lease holds.
    pub fn amount(&self) -> Amount {
        self.amount
    }
}

impl std::fmt::Debug for QuotaLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuotaLease")
            .field("amount", &self.amount)
            .finish()
    }
}

impl Drop for QuotaLease {
    fn drop(&mut self) {
        let Amount { bytes, slots } = self.amount;
        self.shared.leased_slots.fetch_sub(slots, Ordering::Relaxed);
        // One step both counts the return and orders it against the quota's drop: before it, the
        // quota gives these bytes back; after it, the lease does.
        let alive = self.shared.leased_bytes.fetch_sub(bytes, Ordering::Relaxed) & DEAD == 0;
        if alive {
            // After the counts, so the driver sees them lowered once woken.
            self.shared.driver.wake();
        } else {
            self.shared.parent.return_memory(bytes);
        }
    }
}

/// A read-only view of a [`Quota`]: what it holds and what its leases hold, as of the owner's
/// last change and the leases' last drop. Each count is a separate atomic reading, not a
/// snapshot of all of them at one instant.
#[derive(Clone)]
pub struct QuotaMeter {
    shared: Arc<Shared>,
}

impl QuotaMeter {
    /// What the quota holds; nothing once it is gone.
    pub fn held(&self) -> Amount {
        Amount {
            bytes: self.shared.held_bytes.load(Ordering::Relaxed),
            slots: self.shared.held_slots.load(Ordering::Relaxed),
        }
    }

    /// What live leases hold.
    pub fn leased(&self) -> Amount {
        self.shared.leased()
    }

    /// Calls the quota made to the parent pool so far, growing and shrinking: lending never
    /// makes one.
    pub fn parent_accesses(&self) -> u64 {
        self.shared.parent_accesses.load(Ordering::Relaxed)
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Quota>();
    assert_send_sync::<QuotaLease>();
    assert_send_sync::<QuotaMeter>();
};

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    use restate_util_bytecount::NonZeroByteCount;

    use super::*;

    fn pool(capacity: usize) -> MemoryPool {
        MemoryPool::with_capacity(NonZeroByteCount::new(NonZeroUsize::new(capacity).unwrap()))
    }

    const fn amount(bytes: usize, slots: usize) -> Amount {
        Amount { bytes, slots }
    }

    /// Counts the times it was woken.
    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Count {
        fn get(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn lends_grows_and_shrinks_by_policy() {
        let parent = pool(1000);
        let policy = QuotaPolicy {
            refill: 64,
            spare: 32,
        };
        let mut quota = Quota::new(parent.clone(), 1000, policy, Arc::default());

        // Growth draws whole refill batches; slots cost the parent nothing.
        assert!(quota.grow(amount(100, 4)));
        assert_eq!(
            (quota.held(), parent.used().as_usize()),
            (amount(128, 4), 128)
        );
        let first = quota.try_lease(amount(100, 1)).unwrap();
        assert!(quota.try_lease(amount(29, 1)).is_none(), "28 bytes free");
        let second = quota.try_lease(amount(28, 1)).unwrap();
        assert!(quota.try_lease(amount(0, 3)).is_none(), "2 slots free");
        assert!(quota.grow(amount(120, 4)), "already held");
        assert_eq!(
            quota.meter().parent_accesses(),
            1,
            "lending never reaches the parent"
        );

        // Shrinking never takes what leases hold, and keeps the spare above what is needed.
        quota.shrink(Amount::ZERO);
        assert_eq!(quota.held(), amount(128, 2));
        drop((first, second));
        assert_eq!(quota.leased(), Amount::ZERO);
        quota.shrink(amount(50, 2));
        assert_eq!(
            (quota.held(), parent.used().as_usize()),
            (amount(82, 2), 82)
        );
        assert_eq!(quota.meter().held(), quota.held());

        // A batch the parent cannot fund falls back to the exact shortfall.
        let other = parent.try_reserve(870).unwrap();
        assert!(
            quota.grow(amount(130, 2)),
            "48 more fits the 48 left, a batch of 64 does not"
        );
        assert_eq!(quota.held().bytes, 130);
        // A lower parent capacity takes nothing already held or lent; it only refuses growth.
        parent.set_capacity(NonZeroByteCount::new(NonZeroUsize::new(500).unwrap()));
        let lease = quota.try_lease(amount(130, 1)).unwrap();
        assert!(!quota.grow(amount(131, 2)));
        drop((lease, other));

        // Release gives back the spare too.
        quota.release();
        assert_eq!((quota.held().bytes, parent.used().as_usize()), (0, 0));

        // A batch stops at the limit, and growth past the limit fails.
        let mut small = Quota::new(parent, 100, policy, Arc::default());
        assert!(small.grow(amount(70, 1)));
        assert_eq!(small.held().bytes, 100, "a batch of 128, capped");
        assert!(!small.grow(amount(101, 1)));
    }

    #[test]
    fn bytes_stay_below_the_teardown_marker() {
        // Accounting only: nothing this large is allocated. The largest limit, with the most
        // extreme policy, keeps the parent charged exactly through both teardown orders.
        let max = Quota::MAX_LIMIT;
        let extreme = QuotaPolicy {
            refill: usize::MAX,
            spare: usize::MAX,
        };
        for quota_first in [true, false] {
            let parent = MemoryPool::with_capacity(NonZeroByteCount::new(NonZeroUsize::MAX));
            let mut quota = Quota::new(parent.clone(), max, extreme, Arc::default());
            assert!(!quota.grow(amount(max + 2, 1)), "past the limit");
            assert_eq!(quota.held().bytes, 0, "and holding nothing");
            assert!(quota.grow(amount(1 << 62, 1)));
            assert_eq!(quota.held().bytes, max, "one batch, stopped at the limit");
            let lease = quota.try_lease(amount(max, 1)).unwrap();
            quota.shrink(Amount::ZERO);
            assert_eq!((quota.leased().bytes, parent.used().as_usize()), (max, max));
            if quota_first {
                drop(quota);
                assert_eq!(parent.used().as_usize(), max, "the lease keeps it charged");
                drop(lease);
            } else {
                drop(lease);
                quota.release();
                drop(quota);
            }
            assert_eq!(parent.used().as_usize(), 0);
        }

        // Limits at and above the marker are refused.
        for limit in [max + 1, usize::MAX] {
            let quota = std::panic::catch_unwind(|| {
                Quota::new(MemoryPool::unlimited(), limit, extreme, Arc::default())
            });
            assert!(quota.is_err(), "limit {limit}");
        }
    }

    #[test]
    fn leases_outlive_their_quota_on_any_thread() {
        let parent = pool(1000);
        let mut quota = Quota::new(parent.clone(), 1000, QuotaPolicy::default(), Arc::default());
        assert!(quota.grow(amount(500, 2)));
        let meter = quota.meter();
        let lease = quota.try_lease(amount(200, 1)).unwrap();
        let kept = quota.try_lease(amount(100, 1)).unwrap();

        // The quota goes: the parent stays charged for what leases hold, and gets each back as
        // its lease drops, on whichever thread drops it.
        drop(quota);
        assert_eq!(
            (parent.used().as_usize(), meter.held()),
            (300, Amount::ZERO)
        );
        std::thread::spawn(move || drop(lease)).join().unwrap();
        assert_eq!(parent.used().as_usize(), 100);
        assert_eq!(meter.leased(), amount(100, 1));
        drop(kept);
        assert_eq!(
            (parent.used().as_usize(), meter.leased()),
            (0, Amount::ZERO)
        );
    }

    #[test]
    fn releases_and_parent_memory_wake_the_driver() {
        let parent = pool(100);
        let driver = Arc::new(AtomicWaker::new());
        let count = Arc::new(Count::default());
        let mut quota = Quota::new(
            parent.clone(),
            100,
            QuotaPolicy::default(),
            Arc::clone(&driver),
        );
        assert!(quota.grow(amount(40, 2)));
        let (first, second) = (
            quota.try_lease(amount(10, 1)).unwrap(),
            quota.try_lease(amount(10, 1)).unwrap(),
        );

        // A lease dropping wakes the registered driver once; drops before it registers again
        // coalesce into that wakeup.
        driver.register(&Waker::from(Arc::clone(&count)));
        drop(first);
        drop(second);
        assert_eq!(count.get(), 1);

        // A growth the parent cannot fund wakes the driver once memory comes back, and only then
        // succeeds; asking again meanwhile keeps the one registration.
        let other = parent.try_reserve(50).unwrap();
        driver.register(&Waker::from(Arc::clone(&count)));
        assert!(!quota.grow(amount(60, 2)));
        assert!(!quota.grow(amount(60, 2)));
        assert_eq!(count.get(), 1);
        drop(other);
        assert_eq!(count.get(), 2);
        assert!(quota.grow(amount(60, 2)));

        // Racing a release on another thread, a failed growth is always woken.
        for _ in 0..1000 {
            quota.release();
            let other = parent.try_reserve(100 - quota.held().bytes).unwrap();
            let count = Arc::new(Count::default());
            driver.register(&Waker::from(Arc::clone(&count)));
            let releaser = std::thread::spawn(move || drop(other));
            if !quota.grow(amount(60, 2)) {
                let deadline = Instant::now() + Duration::from_secs(10);
                while count.get() == 0 {
                    assert!(Instant::now() < deadline, "lost wakeup");
                    std::thread::yield_now();
                }
                releaser.join().unwrap();
                assert!(quota.grow(amount(60, 2)));
            } else {
                releaser.join().unwrap();
            }
        }
    }
}
