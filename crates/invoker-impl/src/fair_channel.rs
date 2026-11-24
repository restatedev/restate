// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A single-threaded channel whose senders each get their own soft-bounded queue.
//!
//! Sending never waits, so senders can send from sync code. Instead, a sender respects the bound
//! by waiting for [`Sender::capacity`] before doing the work that produces its next message. A
//! sender that produces at most one message per wait exceeds its capacity by at most one.
//!
//! The receiver drains the non-empty queues round-robin, one message at a time, so a busy sender
//! can't starve the others. Senders and the receiver share a thread, so `Rc` and `RefCell` suffice.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::task::{Context, Poll, Waker, ready};

use tokio::task::coop;

struct ReadyQueue<T> {
    /// The queues with messages, in the order the receiver drains them.
    queues: RefCell<VecDeque<Rc<SenderQueue<T>>>>,
    /// The receiver, while it waits for a message.
    waker: Cell<Option<Waker>>,
}

struct SenderQueue<T> {
    messages: RefCell<VecDeque<T>>,
    capacity: usize,
    /// Whether this queue is in the ready queue.
    ready: Cell<bool>,
    /// The sender, while it waits for capacity.
    waker: Cell<Option<Waker>>,
}

pub(crate) struct Receiver<T> {
    ready: Rc<ReadyQueue<T>>,
}

impl<T> Default for Receiver<T> {
    fn default() -> Self {
        Self {
            ready: Rc::new(ReadyQueue {
                queues: RefCell::default(),
                waker: Cell::new(None),
            }),
        }
    }
}

impl<T> Receiver<T> {
    /// Creates a sender with its own queue.
    pub(crate) fn sender(&self, capacity: NonZeroUsize) -> Sender<T> {
        Sender {
            queue: Rc::new(SenderQueue {
                messages: RefCell::default(),
                capacity: capacity.get(),
                ready: Cell::new(false),
                waker: Cell::new(None),
            }),
            ready: Rc::clone(&self.ready),
        }
    }

    /// Polls for the next message, round-robin across the senders.
    pub(crate) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<T> {
        // Like tokio's channels, consume task budget so that draining many queued messages
        // yields to the other tasks on this thread.
        let coop = ready!(coop::poll_proceed(cx));
        let Some(queue) = self.ready.queues.borrow_mut().pop_front() else {
            register(&self.ready.waker, cx.waker());
            return Poll::Pending;
        };
        coop.made_progress();
        let (message, remaining) = {
            let mut messages = queue.messages.borrow_mut();
            let message = messages.pop_front().expect("ready queues aren't empty");
            (message, messages.len())
        };
        let waker = if remaining < queue.capacity {
            queue.waker.take()
        } else {
            None
        };
        if remaining > 0 {
            self.ready.queues.borrow_mut().push_back(queue);
        } else {
            queue.ready.set(false);
        }
        if let Some(waker) = waker {
            waker.wake();
        }
        Poll::Ready(message)
    }

    /// Receives the next message, round-robin across the senders. Cancel safe.
    pub(crate) async fn recv(&self) -> T {
        poll_fn(|cx| self.poll_recv(cx)).await
    }
}

pub(crate) struct Sender<T> {
    queue: Rc<SenderQueue<T>>,
    ready: Rc<ReadyQueue<T>>,
}

impl<T> Sender<T> {
    /// Queues a message, even if the queue is full.
    pub(crate) fn send(&self, message: T) {
        self.queue.messages.borrow_mut().push_back(message);
        if !self.queue.ready.replace(true) {
            self.ready
                .queues
                .borrow_mut()
                .push_back(Rc::clone(&self.queue));
            if let Some(waker) = self.ready.waker.take() {
                waker.wake();
            }
        }
    }

    pub(crate) fn has_capacity(&self) -> bool {
        self.queue.messages.borrow().len() < self.queue.capacity
    }

    /// Waits until the queue has capacity. Cancel safe.
    pub(crate) async fn capacity(&self) {
        poll_fn(|cx| {
            if self.has_capacity() {
                Poll::Ready(())
            } else {
                register(&self.queue.waker, cx.waker());
                Poll::Pending
            }
        })
        .await
    }
}

fn register(slot: &Cell<Option<Waker>>, waker: &Waker) {
    let registered = match slot.take() {
        Some(mut registered) => {
            registered.clone_from(waker);
            registered
        }
        None => waker.clone(),
    };
    slot.set(Some(registered));
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake};

    use super::*;

    #[derive(Default)]
    struct CountingWaker(AtomicUsize);

    impl CountingWaker {
        fn wakes(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn capacity(capacity: usize) -> NonZeroUsize {
        NonZeroUsize::new(capacity).unwrap()
    }

    fn poll<F: Future>(future: F, waker: &Arc<CountingWaker>) -> Poll<F::Output> {
        let waker = Waker::from(Arc::clone(waker));
        pin!(future).poll(&mut Context::from_waker(&waker))
    }

    #[test]
    fn receives_round_robin() {
        let rx = Receiver::default();
        let a = rx.sender(capacity(8));
        let b = rx.sender(capacity(8));
        let rx_waker = Arc::new(CountingWaker::default());

        assert!(poll(rx.recv(), &rx_waker).is_pending());
        a.send("a1");
        assert_eq!(rx_waker.wakes(), 1);
        a.send("a2");
        a.send("a3");
        b.send("b1");
        // Only the first message after the receiver registers wakes it.
        assert_eq!(rx_waker.wakes(), 1);

        let received: Vec<_> = (0..4).map(|_| poll(rx.recv(), &rx_waker)).collect();
        assert_eq!(
            received,
            [
                Poll::Ready("a1"),
                Poll::Ready("b1"),
                Poll::Ready("a2"),
                Poll::Ready("a3")
            ]
        );

        // Drained queues leave the ready queue and rejoin on their next message.
        assert!(poll(rx.recv(), &rx_waker).is_pending());
        b.send("b2");
        assert_eq!(rx_waker.wakes(), 2);
        assert_eq!(poll(rx.recv(), &rx_waker), Poll::Ready("b2"));
    }

    #[test]
    fn waits_for_capacity() {
        let rx = Receiver::default();
        let tx = rx.sender(capacity(2));
        let tx_waker = Arc::new(CountingWaker::default());
        let rx_waker = Arc::new(CountingWaker::default());

        tx.send(1);
        assert!(tx.has_capacity());
        tx.send(2);
        assert!(!tx.has_capacity());
        assert!(poll(tx.capacity(), &tx_waker).is_pending());
        // Sending beyond capacity is allowed.
        tx.send(3);

        // Still full after one receive: no wake-up yet.
        assert_eq!(poll(rx.recv(), &rx_waker), Poll::Ready(1));
        assert!(!tx.has_capacity());
        assert_eq!(tx_waker.wakes(), 0);

        assert_eq!(poll(rx.recv(), &rx_waker), Poll::Ready(2));
        assert_eq!(tx_waker.wakes(), 1);
        assert!(poll(tx.capacity(), &tx_waker).is_ready());

        // Receives that leave the sender unregistered don't wake it again.
        assert_eq!(poll(rx.recv(), &rx_waker), Poll::Ready(3));
        assert_eq!(tx_waker.wakes(), 1);
    }
}
