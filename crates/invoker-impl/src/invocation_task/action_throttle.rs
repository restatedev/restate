// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use gardal::futures::WeightedStream;
use gardal::{PaddedAtomicSharedStorage, TokioClock};
use metrics::{Counter, counter};
use tokio::time::Instant;

use crate::TokenBucket;
use crate::metric_definitions::{INVOKER_ACTION_THROTTLED, INVOKER_ACTION_THROTTLING_WAIT_MS};

type UnitWeightedStream<S> = WeightedStream<
    S,
    PaddedAtomicSharedStorage,
    TokioClock,
    fn(&<S as Stream>::Item) -> NonZeroU32,
>;

struct ThrottlingMetrics {
    throttled: Counter,
    wait_ms: Counter,
}

fn finish_wait(metrics: &Option<ThrottlingMetrics>, wait_started: &mut Option<Instant>) {
    if let (Some(metrics), Some(started)) = (metrics, wait_started.take()) {
        metrics
            .wait_ms
            .increment(started.elapsed().as_millis() as u64);
    }
}

pin_project_lite::pin_project! {
    /// Throttles every item of the inner stream by one action token, and records how often and
    /// how long items waited for a token.
    pub(super) struct ActionThrottledStream<S: Stream> {
        #[pin]
        inner: UnitWeightedStream<S>,
        metrics: Option<ThrottlingMetrics>,
        wait_started: Option<Instant>,
    }

    impl<S: Stream> PinnedDrop for ActionThrottledStream<S> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            finish_wait(this.metrics, this.wait_started);
        }
    }
}

impl<S: Stream> ActionThrottledStream<S> {
    pub(super) fn new(inner: S, bucket: Option<TokenBucket>) -> Self {
        let metrics = bucket.as_ref().map(|_| ThrottlingMetrics {
            throttled: counter!(INVOKER_ACTION_THROTTLED),
            wait_ms: counter!(INVOKER_ACTION_THROTTLING_WAIT_MS),
        });
        Self::with_metrics(inner, bucket, metrics)
    }

    fn with_metrics(
        inner: S,
        bucket: Option<TokenBucket>,
        metrics: Option<ThrottlingMetrics>,
    ) -> Self {
        let unit_weight: fn(&S::Item) -> NonZeroU32 = |_| NonZeroU32::MIN;
        Self {
            inner: WeightedStream::new(inner, bucket, unit_weight),
            metrics,
            wait_started: None,
        }
    }

    pub(super) fn inner(&self) -> &S {
        self.inner.inner()
    }

    /// Bypasses throttling, so it also ends any wait for a token in progress.
    pub(super) fn inner_pin_mut(self: Pin<&mut Self>) -> Pin<&mut S> {
        let this = self.project();
        finish_wait(this.metrics, this.wait_started);
        this.inner.inner_pin_mut()
    }
}

impl<S: Stream> Stream for ActionThrottledStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let poll = this.inner.as_mut().poll_next(cx);
        let Some(metrics) = this.metrics else {
            return poll;
        };

        match poll {
            Poll::Pending => {
                if this.wait_started.is_none() && this.inner.has_pending_item() {
                    *this.wait_started = Some(Instant::now());
                    metrics.throttled.increment(1);
                }
            }
            Poll::Ready(_) => finish_wait(this.metrics, this.wait_started),
        }
        poll
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use futures::{StreamExt, stream};
    use gardal::Limit;

    #[restate_core::test(start_paused = true)]
    async fn records_throttled_actions() {
        let throttled = Arc::new(AtomicU64::new(0));
        let wait_ms = Arc::new(AtomicU64::new(0));
        let metrics = ThrottlingMetrics {
            throttled: Counter::from_arc(Arc::clone(&throttled)),
            wait_ms: Counter::from_arc(Arc::clone(&wait_ms)),
        };
        let limit =
            Limit::per_second_and_burst(NonZeroU32::new(1).unwrap(), NonZeroU32::new(3).unwrap());
        let bucket = TokenBucket::new(limit, TokioClock);

        let start = Instant::now();
        let items = ActionThrottledStream::with_metrics(
            stream::iter(1..=5u32),
            Some(bucket),
            Some(metrics),
        )
        .collect::<Vec<_>>()
        .await;
        let elapsed = start.elapsed();

        // The burst of 3 passes immediately, the remaining 2 actions wait one second each.
        assert_eq!(items, vec![1, 2, 3, 4, 5]);
        assert!(elapsed >= Duration::from_secs(2));
        assert_eq!(throttled.load(Ordering::Relaxed), 2);
        let wait_ms = wait_ms.load(Ordering::Relaxed);
        assert!((2000..=elapsed.as_millis() as u64).contains(&wait_ms));

        let unthrottled = ActionThrottledStream::new(stream::iter(1..=5u32), None);
        assert!(unthrottled.metrics.is_none());
        assert_eq!(unthrottled.collect::<Vec<_>>().await, vec![1, 2, 3, 4, 5]);
    }
}
