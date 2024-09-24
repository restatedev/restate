// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo(asoli): remove when fleshed out
#![allow(dead_code)]

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;

use restate_core::network::{Incoming, MessageRouterBuilder, TransportConnect};
use restate_core::{cancellation_watcher, Metadata};
use restate_types::config::ReplicatedLogletOptions;
use restate_types::net::replicated_loglet::Append;
use tracing::trace;

use super::provider::ReplicatedLogletProvider;

type MessageStream<T> = Pin<Box<dyn Stream<Item = Incoming<T>> + Send + Sync + 'static>>;

pub struct RequestPump {
    metadata: Metadata,
    append_stream: MessageStream<Append>,
}

impl RequestPump {
    pub fn new(
        _opts: &ReplicatedLogletOptions,
        metadata: Metadata,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        // todo(asoli) read from opts
        let queue_length = 10;
        let append_stream = router_builder.subscribe_to_stream(queue_length);
        Self {
            metadata,
            append_stream,
        }
    }

    /// Must run in task-center context
    pub async fn run<T: TransportConnect>(
        self,
        _provider: Arc<ReplicatedLogletProvider<T>>,
    ) -> anyhow::Result<()> {
        trace!("Starting replicated loglet request pump");
        let mut cancel = std::pin::pin!(cancellation_watcher());
        loop {
            tokio::select! {
                _ = &mut cancel => {
                    break;
                }
            }
        }
        Ok(())
    }
}
