// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_channel::Receiver;
use futures::Stream;
use std::ops::Fn;
use std::pin::pin;

pub trait StreamExt
where
    Self: Stream,
{
    fn concurrent_map_unordered<T, Ctx, Fun, Fut>(
        self,
        concurrency_limit: usize,
        ctx: Ctx,
        fun: Fun,
    ) -> Receiver<T>
    where
        T: Send + 'static,
        Ctx: Send + Clone + 'static,
        Fun: Fn(Ctx, Self::Item) -> Fut + Copy + Send + 'static,
        Fut: Future<Output = T> + Send,
        Self: Sized + Send + 'static,
        <Self as Stream>::Item: Send + 'static,
    {
        let (in_tx, in_rx) = async_channel::bounded(concurrency_limit);
        let (out_tx, out_rx) = async_channel::unbounded();

        for _worker in 0..concurrency_limit {
            let rx = in_rx.clone();
            let tx = out_tx.clone();
            let ctx = ctx.clone();
            tokio::spawn(async move {
                while let Ok(item) = rx.recv().await {
                    let res = fun(ctx.clone(), item).await;
                    tx.send(res).await.unwrap();
                }
            });
        }

        tokio::spawn(async move {
            let mut this = pin!(self);
            while let Some(item) = futures::StreamExt::next(&mut this).await {
                in_tx.send(item).await.unwrap();
            }
            in_tx.close();
        });

        out_rx
    }
}

impl<S> StreamExt for S where S: Stream {}
