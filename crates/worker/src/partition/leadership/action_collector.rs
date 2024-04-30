// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::shuffle;
use futures::{Stream, StreamExt};
use restate_types::identifiers::InvocationId;
use restate_wal_protocol::timer::TimerKeyValue;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub(crate) enum ActionEffectStream {
    Follower,
    Leader {
        invoker_stream: ReceiverStream<restate_invoker_api::Effect>,
        shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
        action_effects_stream: ReceiverStream<ActionEffect>,
    },
}

impl ActionEffectStream {
    pub(crate) fn leader(
        invoker_rx: mpsc::Receiver<restate_invoker_api::Effect>,
        shuffle_rx: mpsc::Receiver<shuffle::OutboxTruncation>,
        action_effects_rx: mpsc::Receiver<ActionEffect>,
    ) -> Self {
        ActionEffectStream::Leader {
            invoker_stream: ReceiverStream::new(invoker_rx),
            shuffle_stream: ReceiverStream::new(shuffle_rx),
            action_effects_stream: ReceiverStream::new(action_effects_rx),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ActionEffect {
    Invoker(restate_invoker_api::Effect),
    Shuffle(shuffle::OutboxTruncation),
    Timer(TimerKeyValue),
    ScheduleCleanupTimer(InvocationId, Duration),
}

impl Stream for ActionEffectStream {
    type Item = ActionEffect;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut() {
            ActionEffectStream::Follower => Poll::Pending,
            ActionEffectStream::Leader {
                invoker_stream,
                shuffle_stream,
                action_effects_stream,
            } => {
                let invoker_stream = invoker_stream.map(ActionEffect::Invoker);
                let shuffle_stream = shuffle_stream.map(ActionEffect::Shuffle);

                let mut all_streams =
                    futures::stream_select!(invoker_stream, shuffle_stream, action_effects_stream);
                Pin::new(&mut all_streams).poll_next(cx)
            }
        }
    }
}
