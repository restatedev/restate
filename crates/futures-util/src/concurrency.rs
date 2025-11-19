// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::poll_fn;
use std::marker::PhantomData;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use tokio::sync::Semaphore;
use tokio_util::sync::PollSemaphore;

#[derive(Clone)]
enum Inner {
    Unlimited,
    Limited { semaphore: PollSemaphore },
}

/// Shareable concurrency semaphore.
///
/// Allows permits to be acquired, merged, and split. It has no overhead when unlimited.
pub struct Concurrency<T>(Inner, PhantomData<T>);

impl<T> Clone for Concurrency<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}

impl<T> Concurrency<T> {
    pub const fn new_unlimited() -> Self {
        Self(Inner::Unlimited, PhantomData)
    }

    /// Creates a new concurrency semaphore with the given limit.
    ///
    /// If `limit` is `None`, the sempahore is unbounded and will return `unlimited` permits.
    /// Each returned permit can be split into unlimited number of permits again.
    pub fn new(limit: Option<NonZeroUsize>) -> Self {
        match limit {
            None => Self(Inner::Unlimited, PhantomData),
            Some(limit) => Self(
                Inner::Limited {
                    semaphore: PollSemaphore::new(Arc::new(Semaphore::new(limit.get()))),
                },
                PhantomData,
            ),
        }
    }

    /// Poll to acquire a permit from the concurrency semaphore.
    ///
    /// This can return the following values:
    ///
    /// - Poll::Pending if a permit is not currently available.
    /// - Poll::Ready(permit) if a permit was acquired.
    ///
    /// When this method returns Poll::Pending, the current task is scheduled to receive
    /// a wakeup when the permits become available, or when the semaphore is closed.
    ///
    /// Note that on multiple calls to poll_acquire, only the Waker from the Context passed
    /// to the most recent call is scheduled to receive a wakeup.
    pub fn poll_acquire(&mut self, cx: &mut Context<'_>) -> Poll<Permit<T>> {
        match &mut self.0 {
            Inner::Unlimited => Poll::Ready(Permit::new_unlimited()),
            Inner::Limited { semaphore, .. } => semaphore.poll_acquire(cx).map(|owned_permit| {
                // we never close the underlying semaphore.
                let owned_permit = owned_permit.unwrap();
                let permit = Permit {
                    // We don't _ever_ close() this semaphore
                    inner: Permits::Limited(NonZeroU32::new(1).unwrap()),
                    semaphore: Arc::downgrade(owned_permit.semaphore()),
                    phantom: PhantomData,
                };
                owned_permit.forget();
                permit
            }),
        }
    }

    /// Acquire a permit from the concurrency semaphore.
    pub async fn acquire(&mut self) -> Permit<T> {
        poll_fn(|cx| self.poll_acquire(cx)).await
    }

    /// Acquire a permit and merge it into the given `existing` permit.
    pub async fn acquire_and_merge(&mut self, existing: &mut Permit<T>) {
        poll_fn(|cx| self.poll_and_merge(cx, existing)).await
    }

    /// Returns `true` if a permit was acquired and merged into the given `existing` permit.
    pub fn poll_and_merge(&mut self, cx: &mut Context<'_>, existing: &mut Permit<T>) -> Poll<()> {
        match self.0 {
            Inner::Unlimited => {
                existing.merge(Permit::new_unlimited());
                Poll::Ready(())
            }
            Inner::Limited { ref mut semaphore } => semaphore.poll_acquire(cx).map(|permit| {
                // we never close the underlying semaphore.
                let permit = permit.unwrap();
                let semaphore = Arc::downgrade(permit.semaphore());
                // we take over the permit
                permit.forget();

                existing.merge(Permit {
                    // We don't _ever_ close() this semaphore
                    inner: Permits::Limited(NonZeroU32::new(1).unwrap()),
                    semaphore,
                    phantom: PhantomData,
                });
            }),
        }
    }

    /// Returns the number of permits available in the semaphore.
    pub fn available_permits(&self) -> usize {
        match self.0 {
            Inner::Unlimited => Semaphore::MAX_PERMITS,
            Inner::Limited { ref semaphore, .. } => semaphore.available_permits(),
        }
    }
}

#[derive(Default)]
enum Permits {
    Unlimited,
    #[default]
    Empty,
    Limited(NonZeroU32),
}

#[must_use]
#[clippy::has_significant_drop]
pub struct Permit<T> {
    inner: Permits,
    semaphore: Weak<Semaphore>,
    phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for Permit<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner {
            Permits::Unlimited => write!(f, "Permit::Unlimited"),
            Permits::Empty => write!(f, "Permit::Empty"),
            Permits::Limited(permits) => write!(f, "Permit::Limited({permits})"),
        }
    }
}

impl<T> Drop for Permit<T> {
    fn drop(&mut self) {
        if let Permits::Limited(permits) = self.inner
            && let Some(semaphore) = self.semaphore.upgrade()
        {
            semaphore.add_permits(permits.get() as usize);
        }
    }
}

impl<T> Permit<T> {
    /// An empty permit that can be used to merge other permits into.
    pub const fn new_empty() -> Self {
        Self {
            inner: Permits::Empty,
            semaphore: Weak::new(),
            phantom: PhantomData,
        }
    }

    /// Creates a new unlimited permit.
    ///
    /// Unlimited permits can be acquired and merged infinitely.
    const fn new_unlimited() -> Self {
        Self {
            inner: Permits::Unlimited,
            semaphore: Weak::new(),
            phantom: PhantomData,
        }
    }

    /// Returns `true` if the permit is empty (cannot be split).
    pub fn is_empty(&self) -> bool {
        match self.inner {
            Permits::Unlimited => false,
            Permits::Empty => true,
            Permits::Limited(_) => false,
        }
    }

    /// Merges the given `other` permit into `self`.
    pub fn merge(&mut self, mut other: Self) {
        // Makes sure other's drop doesn't change the underlying semaphore
        match (&mut self.inner, std::mem::take(&mut other.inner)) {
            (Permits::Unlimited, p) | (Permits::Empty, p) => {
                self.inner = p;
                std::mem::swap(&mut self.semaphore, &mut other.semaphore);
            }
            (Permits::Limited(l), Permits::Unlimited) => {
                // attempt to return our taken limit to the pool
                if let Some(semaphore) = std::mem::take(&mut self.semaphore).upgrade() {
                    semaphore.add_permits(l.get() as usize);
                }
                self.inner = Permits::Unlimited;
            }
            (Permits::Limited(_), Permits::Empty) => {
                // nothing to be done
            }
            (Permits::Limited(l1), Permits::Limited(l2)) => {
                self.inner = Permits::Limited(l1.saturating_add(l2.get()));
            }
        }
    }

    /// Split `n` permits from `self` and returns a new [`InvokerPermit`] instance that holds `n` permits.
    ///
    /// If there are insufficient permits and it's not possible to reduce by `n`, returns `None`.
    pub fn split(&mut self, n: usize) -> Option<Self> {
        match self.inner {
            Permits::Unlimited => Some(Self::new_unlimited()),
            Permits::Empty => None,
            Permits::Limited(limit) if n > limit.get() as usize => None,
            Permits::Limited(limit) if n == limit.get() as usize => {
                self.inner = Permits::Empty;
                Some(Self {
                    inner: Permits::Limited(limit),
                    semaphore: std::mem::take(&mut self.semaphore),
                    phantom: PhantomData,
                })
            }
            Permits::Limited(limit) if n < limit.get() as usize => {
                self.inner = Permits::Limited(NonZeroU32::new(limit.get() - n as u32).unwrap());
                Some(Self {
                    inner: Permits::Limited(NonZeroU32::new(n as u32).unwrap()),
                    semaphore: self.semaphore.clone(),
                    phantom: PhantomData,
                })
            }
            // compiler didn't figure out that we are already doing exhaustive matching
            Permits::Limited(_) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Sample;

    #[test]
    fn unlimited_concurrency() {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());

        let mut concurrency = Concurrency::<Sample>::new_unlimited();
        let Poll::Ready(mut permit) = concurrency.poll_acquire(&mut cx) else {
            panic!("should be able to acquire immediately");
        };
        // merging unlimited also gets us unlimited permits
        assert!(concurrency.poll_and_merge(&mut cx, &mut permit).is_ready());
        assert!(!permit.is_empty());
    }

    #[test]
    fn limited_concurrency() {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());

        let mut concurrency = Concurrency::<Sample>::new(Some(NonZeroUsize::new(2).unwrap()));
        assert_eq!(concurrency.available_permits(), 2);
        let Poll::Ready(permit1) = concurrency.poll_acquire(&mut cx) else {
            panic!("should be able to acquire immediately");
        };
        assert!(!permit1.is_empty());
        let Poll::Ready(permit2) = concurrency.poll_acquire(&mut cx) else {
            panic!("should be able to acquire immediately");
        };
        assert!(!permit2.is_empty());
        // out of permits.
        assert!(concurrency.poll_acquire(&mut cx).is_pending());

        // reclaim one
        drop(permit1);

        let Poll::Ready(permit1) = concurrency.poll_acquire(&mut cx) else {
            panic!("should be able to acquire immediately");
        };
        assert!(!permit1.is_empty());
        assert_eq!(concurrency.available_permits(), 0);

        // out of permits again.
        assert!(concurrency.poll_acquire(&mut cx).is_pending());
        assert_eq!(concurrency.available_permits(), 0);
        drop(permit1);
        drop(permit2);
        // needed to let PollSemahore drive its internal cached future, otherwise we would see 1
        // available permit since we have one that will be cached for that previously polled
        // future.
        assert!(concurrency.poll_acquire(&mut cx).is_ready());
        assert_eq!(concurrency.available_permits(), 2);
    }

    // test permit splits and merges
    #[tokio::test]
    async fn permit_splits_and_merges() {
        let mut concurrency = Concurrency::<Sample>::new(Some(NonZeroUsize::new(2).unwrap()));
        let mut permit = concurrency.acquire().await;
        assert!(!permit.is_empty());
        assert_eq!(concurrency.available_permits(), 1);

        let mut permit1 = permit.split(1).expect("should be able to split");
        // permit becomes empty
        assert!(permit.is_empty());
        // we cannot split an empty permit
        assert!(permit.split(1).is_none());
        assert_eq!(concurrency.available_permits(), 1);
        // dropping the empty permit doesn't change the semaphore
        drop(permit);
        assert_eq!(concurrency.available_permits(), 1);

        // acquire and merge with permit1
        concurrency.acquire_and_merge(&mut permit1).await;
        // no more permits left
        assert_eq!(concurrency.available_permits(), 0);

        let permit2 = permit1.split(1).expect("should be able to split");
        assert!(!permit1.is_empty());
        assert!(!permit2.is_empty());

        assert_eq!(concurrency.available_permits(), 0);

        // merge the back
        permit1.merge(permit2);
        // nothing was returned yet to the semaphore
        assert_eq!(concurrency.available_permits(), 0);
        drop(permit1);
        assert_eq!(concurrency.available_permits(), 2);
    }
}
