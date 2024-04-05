// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::services::non_deterministic;
use crate::partition::shuffle;
use futures::{Stream, StreamExt};
use restate_types::identifiers::InvocationId;
use restate_wal_protocol::effects::BuiltinServiceEffects;
use restate_wal_protocol::timer::TimerValue;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};

pub(crate) enum ActionEffectStream {
    Follower,
    Leader {
        invoker_stream: ReceiverStream<restate_invoker_api::Effect>,
        shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
        non_deterministic_service_invoker_stream: UnboundedReceiverStream<BuiltinServiceEffects>,
        action_effects_stream: ReceiverStream<ActionEffect>,
    },
}

impl ActionEffectStream {
    pub(crate) fn leader(
        invoker_rx: mpsc::Receiver<restate_invoker_api::Effect>,
        shuffle_rx: mpsc::Receiver<shuffle::OutboxTruncation>,
        non_deterministic_service_invoker_rx: non_deterministic::EffectsReceiver,
        action_effects_rx: mpsc::Receiver<ActionEffect>,
    ) -> Self {
        ActionEffectStream::Leader {
            invoker_stream: ReceiverStream::new(invoker_rx),
            shuffle_stream: ReceiverStream::new(shuffle_rx),
            non_deterministic_service_invoker_stream: UnboundedReceiverStream::new(
                non_deterministic_service_invoker_rx,
            ),
            action_effects_stream: ReceiverStream::new(action_effects_rx),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ActionEffect {
    Invoker(restate_invoker_api::Effect),
    Shuffle(shuffle::OutboxTruncation),
    Timer(TimerValue),
    BuiltInInvoker(BuiltinServiceEffects),
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
                non_deterministic_service_invoker_stream,
                action_effects_stream,
            } => {
                let invoker_stream = invoker_stream.map(ActionEffect::Invoker);
                let shuffle_stream = shuffle_stream.map(ActionEffect::Shuffle);
                let non_deterministic_service_invoker_stream =
                    non_deterministic_service_invoker_stream.map(ActionEffect::BuiltInInvoker);

                let mut all_streams = futures::stream_select!(
                    invoker_stream,
                    shuffle_stream,
                    non_deterministic_service_invoker_stream,
                    action_effects_stream
                );
                Pin::new(&mut all_streams).poll_next(cx)
            }
        }
    }
}
