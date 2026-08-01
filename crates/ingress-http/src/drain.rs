// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressDrainStatus {
    Active,
    Draining,
    Drained,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressDrainProgress {
    pub status: IngressDrainStatus,
    pub in_flight_requests: u64,
    pub in_flight_connections: u64,
}

#[derive(Debug, Clone)]
pub struct IngressDrainHandle {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    drain: CancellationToken,
    state: watch::Sender<State>,
    in_flight_requests: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct State {
    status: IngressDrainStatus,
    admission_closed: bool,
    in_flight_connections: u64,
    connections_awaiting_goaway: u64,
}

impl Default for IngressDrainHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl IngressDrainHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                drain: CancellationToken::new(),
                state: watch::Sender::new(State {
                    status: IngressDrainStatus::Active,
                    admission_closed: false,
                    in_flight_connections: 0,
                    connections_awaiting_goaway: 0,
                }),
                in_flight_requests: AtomicU64::new(0),
            }),
        }
    }

    /// Starts the irreversible drain and waits until the admission barrier has been crossed.
    ///
    /// Once this returns, listeners have been closed and every accepted connection has initiated
    /// graceful shutdown. Existing requests may still be running.
    pub async fn drain(&self) -> IngressDrainProgress {
        self.start_draining();

        let mut state = self.inner.state.subscribe();
        state
            .wait_for(|state| state.admission_closed && state.connections_awaiting_goaway == 0)
            .await
            .expect("the ingress drain state sender is retained by the handle");
        self.to_progress(*state.borrow())
    }

    pub fn progress(&self) -> IngressDrainProgress {
        self.to_progress(*self.inner.state.borrow())
    }

    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.inner.drain.clone()
    }

    pub(crate) fn start_draining(&self) {
        self.inner.state.send_if_modified(|state| {
            if state.status != IngressDrainStatus::Active {
                return false;
            }
            state.status = IngressDrainStatus::Draining;
            state.connections_awaiting_goaway = state.in_flight_connections;
            true
        });
        self.inner.drain.cancel();
    }

    pub(crate) fn admission_closed(&self) {
        self.inner.state.send_if_modified(|state| {
            if state.admission_closed {
                return false;
            }
            state.admission_closed = true;
            true
        });
    }

    pub(crate) fn drained(&self) {
        self.inner.state.send_if_modified(|state| {
            if state.status == IngressDrainStatus::Drained {
                return false;
            }
            debug_assert!(state.admission_closed);
            debug_assert_eq!(state.in_flight_connections, 0);
            state.status = IngressDrainStatus::Drained;
            true
        });
    }

    pub(crate) fn connection_started(&self) -> ConnectionGuard {
        self.inner.state.send_modify(|state| {
            state.in_flight_connections += 1;
            if state.status != IngressDrainStatus::Active {
                state.connections_awaiting_goaway += 1;
            }
        });
        ConnectionGuard {
            drain: self.clone(),
            goaway_initiated: false,
        }
    }

    pub(crate) fn request_started(&self) -> RequestGuard {
        self.inner
            .in_flight_requests
            .fetch_add(1, Ordering::Relaxed);
        RequestGuard(self.clone())
    }

    fn to_progress(&self, state: State) -> IngressDrainProgress {
        IngressDrainProgress {
            status: state.status,
            in_flight_requests: self.inner.in_flight_requests.load(Ordering::Relaxed),
            in_flight_connections: state.in_flight_connections,
        }
    }
}

pub(crate) struct ConnectionGuard {
    drain: IngressDrainHandle,
    goaway_initiated: bool,
}

impl ConnectionGuard {
    pub(crate) fn goaway_initiated(&mut self) {
        if self.goaway_initiated {
            return;
        }
        self.goaway_initiated = true;
        self.drain.inner.state.send_modify(|state| {
            debug_assert!(state.connections_awaiting_goaway > 0);
            state.connections_awaiting_goaway -= 1;
        });
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.drain.inner.state.send_modify(|state| {
            debug_assert!(state.in_flight_connections > 0);
            state.in_flight_connections -= 1;
            if state.status != IngressDrainStatus::Active && !self.goaway_initiated {
                debug_assert!(state.connections_awaiting_goaway > 0);
                state.connections_awaiting_goaway -= 1;
            }
        });
    }
}

pub(crate) struct RequestGuard(IngressDrainHandle);

impl Drop for RequestGuard {
    fn drop(&mut self) {
        let previous = self
            .0
            .inner
            .in_flight_requests
            .fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn drain_waits_for_admission_barrier_and_is_idempotent() {
        let drain = IngressDrainHandle::new();
        let mut connection = drain.connection_started();
        let request = drain.request_started();

        let pending_drain = tokio::spawn({
            let drain = drain.clone();
            async move { drain.drain().await }
        });
        tokio::task::yield_now().await;

        drain.admission_closed();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), pending_drain)
                .await
                .is_err(),
            "drain must wait until existing connections have initiated GOAWAY"
        );

        connection.goaway_initiated();
        let progress = tokio::time::timeout(Duration::from_secs(1), drain.drain())
            .await
            .unwrap();
        assert_eq!(progress.status, IngressDrainStatus::Draining);
        assert_eq!(progress.in_flight_connections, 1);
        assert_eq!(progress.in_flight_requests, 1);

        drop(request);
        drop(connection);
        drain.drained();
        let progress = drain.drain().await;
        assert_eq!(progress.status, IngressDrainStatus::Drained);
        assert_eq!(progress.in_flight_connections, 0);
        assert_eq!(progress.in_flight_requests, 0);
    }
}
