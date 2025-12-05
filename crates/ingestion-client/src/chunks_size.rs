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
use pin_project_lite::pin_project;

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    #[derive(Debug)]
    pub struct ChunksSize<F, S: Stream> {
        #[pin]
        stream: Fuse<S>,
        items: Vec<S::Item>,
        size: usize,
        cap: usize, // https://github.com/rust-lang/futures-rs/issues/1475
        size_fn: F,
    }
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

                    if me.items.is_empty() || *me.size + item_size <= *me.cap {
                        *me.size += item_size;
                        me.items.push(item);
                    } else {
                        // not empty and adding the item will go over the cap
                        let items = std::mem::replace(me.items, vec![item]);
                        *me.size = item_size;
                        return Poll::Ready(Some(items));
                    }

                    if *me.size >= *me.cap {
                        *me.size = 0;
                        return Poll::Ready(Some(std::mem::take(me.items)));
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
