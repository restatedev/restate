// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio_stream::adapters::Fuse;
use tokio_stream::{Stream, StreamExt};

use core::pin::Pin;
use core::task::{Context, Poll};

#[must_use = "streams do nothing unless polled"]
#[derive(Debug)]
#[pin_project::pin_project]
pub struct ChunksSize<F, S: Stream> {
    #[pin]
    stream: Fuse<S>,
    items: Vec<S::Item>,
    size: usize,
    cap: usize, // https://github.com/rust-lang/futures-rs/issues/1475
    size_fn: F,
}

impl<F, S: Stream> ChunksSize<F, S>
where
    F: Fn(&S::Item) -> usize,
{
    pub fn new(stream: S, max_size: usize, size_fn: F) -> Self {
        ChunksSize {
            stream: stream.fuse(),
            items: Vec::default(),
            size: 0,
            cap: max_size,
            size_fn,
        }
    }

    /// Drains the buffered items, returning them without waiting for the timeout or capacity limit.
    pub fn into_remainder(self) -> Vec<S::Item> {
        self.items
    }
}

impl<F, S: Stream> Stream for ChunksSize<F, S>
where
    F: Fn(&S::Item) -> usize,
{
    type Item = Vec<S::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut me = self.as_mut().project();
        loop {
            match me.stream.as_mut().poll_next(cx) {
                Poll::Pending if me.items.is_empty() => return Poll::Pending,
                Poll::Pending => {
                    *me.size = 0;
                    return Poll::Ready(Some(std::mem::take(me.items)));
                }
                Poll::Ready(Some(item)) => {
                    let item_size = (me.size_fn)(&item);

                    if *me.size + item_size <= *me.cap {
                        *me.size += item_size;
                        me.items.push(item);
                    } else if me.items.is_empty() {
                        *me.size = 0;
                        return Poll::Ready(Some(vec![item]));
                    } else {
                        let items = std::mem::replace(me.items, vec![item]);
                        *me.size = item_size;
                        return Poll::Ready(Some(items));
                    }
                }
                Poll::Ready(None) => {
                    // Returning Some here is only correct because we fuse the inner stream.
                    let last = if me.items.is_empty() {
                        None
                    } else {
                        Some(std::mem::take(me.items))
                    };

                    return Poll::Ready(last);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunk_len = if self.items.is_empty() { 0 } else { 1 };
        let (lower, upper) = self.stream.size_hint();
        let lower = (lower / self.cap).saturating_add(chunk_len);
        let upper = upper.and_then(|x| x.checked_add(chunk_len));
        (lower, upper)
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use futures::{StreamExt, stream::poll_fn};
    use tokio_stream::iter;

    use super::ChunksSize;

    #[tokio::test]
    async fn splits_into_size_bound_chunks() {
        let stream = iter([2usize, 2, 3, 1]);
        let chunks: Vec<Vec<usize>> = ChunksSize::new(stream, 5, |item| *item).collect().await;

        assert_eq!(chunks, vec![vec![2, 2], vec![3, 1]]);
    }

    #[tokio::test]
    async fn emits_item_larger_than_cap_as_its_own_chunk() {
        let stream = iter([10usize, 2]);
        let chunks: Vec<Vec<usize>> = ChunksSize::new(stream, 5, |item| *item).collect().await;

        assert_eq!(chunks, vec![vec![10], vec![2]]);
    }

    #[tokio::test]
    async fn flushes_buffer_when_inner_stream_is_pending() {
        let mut state = 0;
        let stream = poll_fn(move |_| {
            state += 1;
            match state {
                1 => Poll::Ready(Some(1usize)),
                2 => Poll::Pending,
                3 => Poll::Ready(Some(2usize)),
                _ => Poll::Ready(None),
            }
        });

        let chunks: Vec<Vec<usize>> = ChunksSize::new(stream, 10, |item| *item).collect().await;

        assert_eq!(chunks, vec![vec![1], vec![2]]);
    }
}
